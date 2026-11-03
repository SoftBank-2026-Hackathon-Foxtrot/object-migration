package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/engine"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/store"
)

var testCfg = Config{PollInterval: 5 * time.Millisecond, RetryBackoff: 5 * time.Millisecond}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// start runs a worker until the test ends and returns a function that stops
// it and waits for it to exit.
func start(t *testing.T, s store.Store, e engine.Engine) (*Worker, func()) {
	t.Helper()
	w := New(s, e, testCfg, quietLog())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()
	stop := func() {
		cancel()
		<-done
	}
	t.Cleanup(stop)
	return w, stop
}

func create(t *testing.T, s store.Store) migration.Migration {
	t.Helper()
	m, err := migration.New(
		migration.Endpoint{Kind: migration.KindS3, Bucket: "src", Region: "ap-northeast-1", Ref: "env:SRC"},
		migration.Endpoint{Kind: migration.KindGCS, Bucket: "dst", Ref: "env:DST"},
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	m, err = s.Create(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func waitFor(t *testing.T, s store.Store, id string, what string, ok func(migration.Migration) bool) migration.Migration {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var m migration.Migration
	for time.Now().Before(deadline) {
		var err error
		m, err = s.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if ok(m) {
			return m
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: phase=%s state=%s lastError=%+v", what, m.Phase, m.State, m.LastError)
	return m
}

func waitPhase(t *testing.T, s store.Store, id string, p migration.Phase) migration.Migration {
	t.Helper()
	return waitFor(t, s, id, string(p), func(m migration.Migration) bool { return m.Phase == p })
}

func command(t *testing.T, w *Worker, s store.Store, id string, c migration.Command) {
	t.Helper()
	req := migration.CommandRequest{Command: c, RequestedAt: time.Now()}
	if _, err := s.RequestCommand(context.Background(), id, req); err != nil {
		t.Fatalf("%s: %v", c, err)
	}
	w.Notify(id)
}

func TestFullFlow(t *testing.T) {
	s := store.NewMemory()
	w, _ := start(t, s, engine.NewFake(time.Millisecond))
	m := create(t, s)
	w.Notify(m.ID)

	ready := waitPhase(t, s, m.ID, migration.PhaseReady)
	if ready.Replication.InitialCopy.Completed != 10 || ready.Readiness.Cutover.Result != migration.ResultPass {
		t.Fatalf("at READY: initialCopy=%+v cutover=%+v", ready.Replication.InitialCopy, ready.Readiness.Cutover)
	}

	command(t, w, s, m.ID, migration.CommandFreeze)
	waitPhase(t, s, m.ID, migration.PhaseFrozen)
	command(t, w, s, m.ID, migration.CommandSwitch)
	waitPhase(t, s, m.ID, migration.PhaseSwitched)
	command(t, w, s, m.ID, migration.CommandResume)
	waitFor(t, s, m.ID, "verified", func(m migration.Migration) bool {
		return m.Phase == migration.PhaseVerifying && m.State == migration.StateSucceeded
	})
	command(t, w, s, m.ID, migration.CommandFinalize)
	done := waitPhase(t, s, m.ID, migration.PhaseCompleted)
	if done.Pending != nil || done.LastError != nil {
		t.Fatalf("completed with pending=%+v lastError=%+v", done.Pending, done.LastError)
	}
}

func TestRollbackAfterSwitch(t *testing.T) {
	s := store.NewMemory()
	w, _ := start(t, s, engine.NewFake(time.Millisecond))
	m := create(t, s)
	w.Notify(m.ID)

	waitPhase(t, s, m.ID, migration.PhaseReady)
	command(t, w, s, m.ID, migration.CommandFreeze)
	waitPhase(t, s, m.ID, migration.PhaseFrozen)
	command(t, w, s, m.ID, migration.CommandSwitch)
	waitPhase(t, s, m.ID, migration.PhaseSwitched)
	command(t, w, s, m.ID, migration.CommandRollback)
	waitPhase(t, s, m.ID, migration.PhaseRolledBack)
}

func TestAbortWhileSyncing(t *testing.T) {
	s := store.NewMemory()
	e := engine.NewFake(time.Millisecond)
	e.SyncRounds = 1 << 30 // never ready
	w, _ := start(t, s, e)
	m := create(t, s)
	w.Notify(m.ID)

	waitPhase(t, s, m.ID, migration.PhaseSyncing)
	command(t, w, s, m.ID, migration.CommandAbort)
	aborted := waitPhase(t, s, m.ID, migration.PhaseAborted)
	// Only the direction that ran is stopped; reverse never started.
	if aborted.Replication.Forward.State != migration.FlowStopped || aborted.Replication.Reverse.State != migration.FlowOff {
		t.Fatalf("replication = %+v", aborted.Replication)
	}
}

// flaky fails InitialSync a fixed number of times, then behaves like Fake.
type flaky struct {
	*engine.Fake
	failures atomic.Int32
	err      error
}

func (f *flaky) InitialSync(ctx context.Context, m migration.Migration, report func(migration.Progress)) error {
	if f.failures.Add(-1) >= 0 {
		return f.err
	}
	return f.Fake.InitialSync(ctx, m, report)
}

func TestRetriesEngineErrors(t *testing.T) {
	s := store.NewMemory()
	e := &flaky{Fake: engine.NewFake(time.Millisecond), err: errors.New("bucket unreachable")}
	e.failures.Store(2)
	w, _ := start(t, s, e)
	m := create(t, s)
	w.Notify(m.ID)

	failed := waitFor(t, s, m.ID, "error recorded", func(m migration.Migration) bool { return m.LastError != nil })
	if !failed.LastError.Retryable || failed.Phase != migration.PhaseInitialSync {
		t.Fatalf("lastError=%+v phase=%s", failed.LastError, failed.Phase)
	}
	ready := waitPhase(t, s, m.ID, migration.PhaseReady)
	if ready.LastError != nil {
		t.Fatalf("lastError not cleared after recovery: %+v", ready.LastError)
	}
}

func TestPermanentErrorFailsMigration(t *testing.T) {
	s := store.NewMemory()
	e := &flaky{Fake: engine.NewFake(time.Millisecond), err: errors.Join(engine.ErrPermanent, errors.New("access denied"))}
	e.failures.Store(1)
	w, _ := start(t, s, e)
	m := create(t, s)
	w.Notify(m.ID)

	failed := waitPhase(t, s, m.ID, migration.PhaseFailed)
	if failed.State != migration.StateFailed || failed.LastError == nil || failed.LastError.Retryable {
		t.Fatalf("state=%s lastError=%+v", failed.State, failed.LastError)
	}
}

func TestResumesAfterRestart(t *testing.T) {
	s := store.NewMemory()
	slow := engine.NewFake(200 * time.Millisecond)
	w1, stop1 := start(t, s, slow)
	m := create(t, s)
	w1.Notify(m.ID)

	waitPhase(t, s, m.ID, migration.PhaseInitialSync)
	stop1() // crash in the middle of the initial copy

	w2, _ := start(t, s, engine.NewFake(time.Millisecond))
	waitPhase(t, s, m.ID, migration.PhaseReady)
	command(t, w2, s, m.ID, migration.CommandFreeze)
	waitPhase(t, s, m.ID, migration.PhaseFrozen)
}

func TestPicksUpMigrationsWithoutNotify(t *testing.T) {
	s := store.NewMemory()
	m := create(t, s) // created before the worker starts, never notified
	start(t, s, engine.NewFake(time.Millisecond))
	waitPhase(t, s, m.ID, migration.PhaseReady)
}

// gated blocks Sync while gate is set, so a test can act during a sync round.
type gated struct {
	*engine.Fake
	gate    atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (g *gated) Sync(ctx context.Context, m migration.Migration) (engine.SyncReport, error) {
	if !g.gate.Load() {
		return g.Fake.Sync(ctx, m)
	}
	g.entered <- struct{}{}
	select {
	case <-g.release:
	case <-ctx.Done():
		return engine.SyncReport{}, ctx.Err()
	}
	return engine.SyncReport{Cutover: migration.Decide(migration.Check{Name: "lag_stable", OK: false}), LagSeconds: 5}, nil
}

func TestPendingCommandKeepsPhase(t *testing.T) {
	s := store.NewMemory()
	e := &gated{Fake: engine.NewFake(time.Millisecond), entered: make(chan struct{}), release: make(chan struct{})}
	w, _ := start(t, s, e)
	m := create(t, s)
	w.Notify(m.ID)
	waitPhase(t, s, m.ID, migration.PhaseReady)

	// Freeze arrives in the middle of a sync round that reports not ready.
	e.gate.Store(true)
	<-e.entered
	req := migration.CommandRequest{Command: migration.CommandFreeze, RequestedAt: time.Now()}
	if _, err := s.RequestCommand(context.Background(), m.ID, req); err != nil {
		t.Fatal(err)
	}
	e.gate.Store(false)
	close(e.release)
	w.Notify(m.ID)

	waitPhase(t, s, m.ID, migration.PhaseFrozen)
}

// scripted is a Fake whose sync, verification, and switch results a test can
// change while the worker runs.
type scripted struct {
	*engine.Fake
	notReady       atomic.Bool
	switchFailures atomic.Int32
}

func newScripted() *scripted { return &scripted{Fake: engine.NewFake(time.Millisecond)} }

func (s *scripted) Sync(ctx context.Context, m migration.Migration) (engine.SyncReport, error) {
	if s.notReady.Load() {
		return engine.SyncReport{Cutover: migration.Decide(migration.Check{Name: "lag_stable", OK: false}), Backlog: 40, LagSeconds: 9}, nil
	}
	return s.Fake.Sync(ctx, m)
}

func (s *scripted) Verify(ctx context.Context, m migration.Migration) (engine.VerifyReport, error) {
	if s.notReady.Load() {
		return engine.VerifyReport{
			Advance:  migration.Decide(migration.Check{Name: "diff", OK: false, Message: "3 objects differ"}),
			Rollback: migration.Decide(migration.Check{Name: "reverse_sync", OK: true}),
		}, nil
	}
	return s.Fake.Verify(ctx, m)
}

func (s *scripted) Switch(ctx context.Context, m migration.Migration) error {
	if s.switchFailures.Add(-1) >= 0 {
		return errors.New("routing update failed")
	}
	return s.Fake.Switch(ctx, m)
}

// toFrozen runs a fresh migration up to FROZEN.
func toFrozen(t *testing.T, w *Worker, s store.Store) migration.Migration {
	t.Helper()
	m := create(t, s)
	w.Notify(m.ID)
	waitPhase(t, s, m.ID, migration.PhaseReady)
	command(t, w, s, m.ID, migration.CommandFreeze)
	return waitPhase(t, s, m.ID, migration.PhaseFrozen)
}

func TestCommandRetriedAfterEngineError(t *testing.T) {
	s := store.NewMemory()
	e := newScripted()
	e.switchFailures.Store(2)
	w, _ := start(t, s, e)
	m := toFrozen(t, w, s)

	command(t, w, s, m.ID, migration.CommandSwitch)
	failed := waitFor(t, s, m.ID, "switch error recorded", func(m migration.Migration) bool { return m.LastError != nil })
	if failed.Phase != migration.PhaseFrozen || failed.Pending == nil || failed.Pending.Command != migration.CommandSwitch {
		t.Fatalf("after failed switch: phase=%s pending=%+v", failed.Phase, failed.Pending)
	}
	switched := waitPhase(t, s, m.ID, migration.PhaseSwitched)
	if switched.Pending != nil || switched.LastError != nil {
		t.Fatalf("after retry: pending=%+v lastError=%+v", switched.Pending, switched.LastError)
	}
}

func TestReadinessCanDropBackToSyncing(t *testing.T) {
	s := store.NewMemory()
	e := newScripted()
	w, _ := start(t, s, e)
	m := create(t, s)
	w.Notify(m.ID)
	waitPhase(t, s, m.ID, migration.PhaseReady)

	e.notReady.Store(true)
	syncing := waitPhase(t, s, m.ID, migration.PhaseSyncing)
	fwd := syncing.Replication.Forward
	if syncing.State != migration.StateRunning || syncing.Readiness.Cutover.Result != migration.ResultFail || fwd.LagSeconds != 9 || fwd.Backlog != 40 {
		t.Fatalf("state=%s cutover=%+v forward=%+v", syncing.State, syncing.Readiness.Cutover, fwd)
	}
	e.notReady.Store(false)
	waitPhase(t, s, m.ID, migration.PhaseReady)
}

func TestFinalizeWaitsForVerification(t *testing.T) {
	s := store.NewMemory()
	e := newScripted()
	w, _ := start(t, s, e)
	m := toFrozen(t, w, s)
	command(t, w, s, m.ID, migration.CommandSwitch)
	waitPhase(t, s, m.ID, migration.PhaseSwitched)

	e.notReady.Store(true)
	command(t, w, s, m.ID, migration.CommandResume)
	waitFor(t, s, m.ID, "failed verification", func(m migration.Migration) bool {
		return m.Phase == migration.PhaseVerifying && m.Readiness.Advance.Result == migration.ResultFail
	})
	req := migration.CommandRequest{Command: migration.CommandFinalize, RequestedAt: time.Now()}
	if _, err := s.RequestCommand(context.Background(), m.ID, req); !errors.Is(err, migration.ErrPrecondition) {
		t.Fatalf("finalize before verification: err = %v, want ErrPrecondition", err)
	}

	e.notReady.Store(false)
	waitFor(t, s, m.ID, "verified", func(m migration.Migration) bool { return m.State == migration.StateSucceeded })
	command(t, w, s, m.ID, migration.CommandFinalize)
	waitPhase(t, s, m.ID, migration.PhaseCompleted)
}

// blockingRollback never finishes a rollback until its context ends, like a
// worker that crashes in the middle of one.
type blockingRollback struct{ *engine.Fake }

func (blockingRollback) Rollback(ctx context.Context, _ migration.Migration) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestRollbackResumesAfterRestart(t *testing.T) {
	s := store.NewMemory()
	w1, stop1 := start(t, s, blockingRollback{engine.NewFake(time.Millisecond)})
	m := toFrozen(t, w1, s)
	command(t, w1, s, m.ID, migration.CommandRollback)
	waitPhase(t, s, m.ID, migration.PhaseRollingBack)
	stop1()

	start(t, s, engine.NewFake(time.Millisecond))
	waitPhase(t, s, m.ID, migration.PhaseRolledBack)
}

// conflicting makes the first n Update calls fail with ErrConflict, as if
// another writer got there first.
type conflicting struct {
	store.Store
	n atomic.Int32
}

func (c *conflicting) Update(ctx context.Context, m migration.Migration) (migration.Migration, error) {
	if c.n.Add(-1) >= 0 {
		return migration.Migration{}, store.ErrConflict
	}
	return c.Store.Update(ctx, m)
}

func TestVersionConflictsAreRetried(t *testing.T) {
	s := &conflicting{Store: store.NewMemory()}
	// 7 conflicts: the first save gives up after 5 in a row, its error is
	// recorded after 2 more, and the retried step then succeeds.
	s.n.Store(7)
	w, _ := start(t, s, engine.NewFake(time.Millisecond))
	m := create(t, s)
	w.Notify(m.ID)

	ready := waitPhase(t, s, m.ID, migration.PhaseReady)
	if ready.LastError != nil {
		t.Fatalf("lastError not cleared: %+v", ready.LastError)
	}
	if left := s.n.Load(); left > 0 {
		t.Fatalf("%d forced conflicts were never hit", left)
	}
}

// slowResume holds Resume until released, so a test can look at the status
// while the forward sync drains.
type slowResume struct {
	*engine.Fake
	release chan struct{}
}

func (e *slowResume) Resume(ctx context.Context, _ migration.Migration) error {
	select {
	case <-e.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestReplicationStatesFollowTheFlow(t *testing.T) {
	s := store.NewMemory()
	e := &slowResume{Fake: engine.NewFake(50 * time.Millisecond), release: make(chan struct{})}
	w, _ := start(t, s, e)
	m := create(t, s)
	w.Notify(m.ID)

	flows := func(m migration.Migration) (migration.FlowState, migration.FlowState) {
		return m.Replication.Forward.State, m.Replication.Reverse.State
	}
	want := func(m migration.Migration, fwd, rev migration.FlowState) {
		t.Helper()
		if f, r := flows(m); f != fwd || r != rev {
			t.Fatalf("phase %s: forward=%s reverse=%s, want %s/%s", m.Phase, f, r, fwd, rev)
		}
	}

	copying := waitPhase(t, s, m.ID, migration.PhaseInitialSync)
	want(copying, migration.FlowInitialCopy, migration.FlowOff)

	want(waitPhase(t, s, m.ID, migration.PhaseSyncing), migration.FlowStreaming, migration.FlowOff)
	want(toFrozenFrom(t, w, s, m.ID), migration.FlowStreaming, migration.FlowOff)
	command(t, w, s, m.ID, migration.CommandSwitch)
	want(waitPhase(t, s, m.ID, migration.PhaseSwitched), migration.FlowStreaming, migration.FlowOff)

	command(t, w, s, m.ID, migration.CommandResume)
	draining := waitFor(t, s, m.ID, "draining", func(m migration.Migration) bool {
		return m.Replication.Forward.State == migration.FlowDraining
	})
	want(draining, migration.FlowDraining, migration.FlowOff)
	close(e.release)

	verified := waitFor(t, s, m.ID, "verified", func(m migration.Migration) bool {
		return m.Phase == migration.PhaseVerifying && m.State == migration.StateSucceeded
	})
	want(verified, migration.FlowStopped, migration.FlowStreaming)

	command(t, w, s, m.ID, migration.CommandFinalize)
	want(waitPhase(t, s, m.ID, migration.PhaseCompleted), migration.FlowStopped, migration.FlowStopped)
}

// toFrozenFrom waits for an existing migration to be ready and freezes it.
func toFrozenFrom(t *testing.T, w *Worker, s store.Store, id string) migration.Migration {
	t.Helper()
	waitPhase(t, s, id, migration.PhaseReady)
	command(t, w, s, id, migration.CommandFreeze)
	return waitPhase(t, s, id, migration.PhaseFrozen)
}

// endless never finishes the initial copy unless its context is cancelled.
type endless struct {
	*engine.Fake
	started   chan struct{}
	cancelled atomic.Bool
}

func (e *endless) InitialSync(ctx context.Context, _ migration.Migration, report func(migration.Progress)) error {
	report(migration.Progress{Completed: 1, Total: 1000, Unit: "objects"})
	close(e.started)
	<-ctx.Done()
	e.cancelled.Store(true)
	return ctx.Err()
}

func TestAbortInterruptsInitialCopy(t *testing.T) {
	s := store.NewMemory()
	e := &endless{Fake: engine.NewFake(time.Millisecond), started: make(chan struct{})}
	w, _ := start(t, s, e)
	m := create(t, s)
	w.Notify(m.ID)
	<-e.started

	command(t, w, s, m.ID, migration.CommandAbort)
	got := waitPhase(t, s, m.ID, migration.PhaseAborted)
	if !e.cancelled.Load() {
		t.Fatal("the copy was not cancelled")
	}
	if got.LastError != nil {
		t.Fatalf("interruption recorded as an error: %+v", got.LastError)
	}
	if got.Replication.Forward.State != migration.FlowStopped {
		t.Fatalf("forward = %s, want STOPPED", got.Replication.Forward.State)
	}
}

// countingSyncs is a Fake that counts sync rounds.
type countingSyncs struct {
	*engine.Fake
	syncs atomic.Int32
}

func (c *countingSyncs) Sync(ctx context.Context, m migration.Migration) (engine.SyncReport, error) {
	c.syncs.Add(1)
	return c.Fake.Sync(ctx, m)
}

func TestSyncRoundsWaitSyncInterval(t *testing.T) {
	s := store.NewMemory()
	e := &countingSyncs{Fake: engine.NewFake(time.Millisecond)}
	cfg := testCfg
	cfg.SyncInterval = time.Hour
	w := New(s, e, cfg, quietLog())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	m := create(t, s)
	w.Notify(m.ID)
	waitFor(t, s, m.ID, "first sync round", func(m migration.Migration) bool {
		return m.Phase == migration.PhaseSyncing && m.Readiness.Cutover.Result != migration.ResultUnknown
	})
	time.Sleep(20 * testCfg.PollInterval)
	if n := e.syncs.Load(); n != 1 {
		t.Fatalf("%d sync rounds within the interval, want 1", n)
	}

	// A command does not wait for the next round.
	command(t, w, s, m.ID, migration.CommandAbort)
	waitPhase(t, s, m.ID, migration.PhaseAborted)
}

// oneWay is a Fake that does not copy anything back after resume.
type oneWay struct{ *engine.Fake }

func (oneWay) ReverseSync() bool { return false }

func TestReverseStaysOffWithoutReverseSync(t *testing.T) {
	s := store.NewMemory()
	w, _ := start(t, s, oneWay{engine.NewFake(time.Millisecond)})
	m := toFrozen(t, w, s)
	command(t, w, s, m.ID, migration.CommandSwitch)
	waitPhase(t, s, m.ID, migration.PhaseSwitched)
	command(t, w, s, m.ID, migration.CommandResume)
	got := waitPhase(t, s, m.ID, migration.PhaseVerifying)
	if got.Replication.Reverse.State != migration.FlowOff {
		t.Fatalf("reverse = %s, want OFF", got.Replication.Reverse.State)
	}
	waitFor(t, s, m.ID, "verified", func(m migration.Migration) bool { return m.State == migration.StateSucceeded })
	command(t, w, s, m.ID, migration.CommandFinalize)
	if done := waitPhase(t, s, m.ID, migration.PhaseCompleted); done.Replication.Reverse.State != migration.FlowOff {
		t.Fatalf("reverse after finalize = %s, want OFF", done.Replication.Reverse.State)
	}
}
