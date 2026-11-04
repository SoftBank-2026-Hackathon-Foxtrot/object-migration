// Package worker moves migrations through their phases. It reads state from
// the store, calls the engine, and writes the result back. Each migration
// runs in its own goroutine.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/engine"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/routing"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/store"
)

type Config struct {
	// PollInterval is how often a migration rechecks its state while it
	// waits, and how often the worker looks for migrations it is not running.
	PollInterval time.Duration
	// RetryBackoff is the wait after a failed engine call.
	RetryBackoff time.Duration
	// SyncInterval is the wait between incremental sync rounds and between
	// verification attempts. Zero means PollInterval.
	SyncInterval time.Duration
	// Apps tracks which route each app has applied. When set, a step that
	// changes the route waits until every live app applied it.
	Apps *routing.Apps
	// ConfirmTimeout is how long a step waits for apps when the command
	// gives no deadline. Zero means 30 seconds.
	ConfirmTimeout time.Duration
}

// ErrUnconfirmed means some apps did not apply a new route in time. The
// outcome is unknown until they do; the step keeps waiting for them.
var ErrUnconfirmed = errors.New("apps have not applied the new route")

// errSuperseded stops a command that another command replaced, before it
// moves apps any further.
var errSuperseded = errors.New("command replaced")

type Worker struct {
	store  store.Store
	engine engine.Engine
	cfg    Config
	log    *slog.Logger
	now    func() time.Time

	mu      sync.Mutex
	ctx     context.Context // set by Run; nil until then
	running map[string]chan struct{}
	// interrupt cancels the copy a migration is running, so a command
	// recorded meanwhile does not wait for it.
	interrupt map[string]context.CancelFunc
	wg        sync.WaitGroup
}

func New(s store.Store, e engine.Engine, cfg Config, log *slog.Logger) *Worker {
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = cfg.PollInterval
	}
	if cfg.ConfirmTimeout == 0 {
		cfg.ConfirmTimeout = 30 * time.Second
	}
	return &Worker{
		store:     s,
		engine:    e,
		cfg:       cfg,
		log:       log,
		now:       time.Now,
		running:   make(map[string]chan struct{}),
		interrupt: make(map[string]context.CancelFunc),
	}
}

// Run picks up every unfinished migration and blocks until ctx is done. On
// return no migration goroutine is left running.
func (w *Worker) Run(ctx context.Context) error {
	w.mu.Lock()
	w.ctx = ctx
	w.mu.Unlock()

	w.rescan(ctx)
	t := time.NewTicker(w.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			w.wg.Wait()
			return nil
		case <-t.C:
			w.rescan(ctx)
		}
	}
}

// Notify asks the worker to look at a migration now, for example after the
// API recorded a command. A copy in progress is interrupted so the command
// runs first; the copy picks up where it left off afterwards. Missing a
// notification only delays the work until the next poll.
func (w *Worker) Notify(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ctx == nil || w.ctx.Err() != nil {
		return
	}
	if wake, ok := w.running[id]; ok {
		if cancel, ok := w.interrupt[id]; ok {
			cancel()
		}
		// Wake it too: the copy may have just ended without seeing the cancel.
		select {
		case wake <- struct{}{}:
		default:
		}
		return
	}
	w.startLocked(id)
}

// interruptible returns a context for a copy that Notify may cancel, and a
// function to call when the copy is over. A notification that came in after
// the migration was loaded cancels the copy at once.
func (w *Worker) interruptible(ctx context.Context, id string) (context.Context, func()) {
	ictx, cancel := context.WithCancel(ctx)
	w.mu.Lock()
	defer w.mu.Unlock()
	select {
	case <-w.running[id]:
		cancel()
	default:
		w.interrupt[id] = cancel
	}
	return ictx, func() {
		w.mu.Lock()
		delete(w.interrupt, id)
		w.mu.Unlock()
		cancel()
	}
}

// interrupted reports whether a copy stopped because Notify cancelled it
// rather than because the worker is stopping.
func interrupted(ctx, ictx context.Context) bool {
	return ctx.Err() == nil && ictx.Err() != nil
}

func (w *Worker) rescan(ctx context.Context) {
	list, err := w.store.ListActive(ctx)
	if err != nil {
		if ctx.Err() == nil {
			w.log.Error("list active migrations", "err", err)
		}
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if ctx.Err() != nil {
		return
	}
	for _, m := range list {
		if _, ok := w.running[m.ID]; !ok {
			w.startLocked(m.ID)
		}
	}
}

func (w *Worker) startLocked(id string) {
	ctx := w.ctx // read under w.mu; the goroutine must not touch w.ctx
	wake := make(chan struct{}, 1)
	w.running[id] = wake
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer func() {
			w.mu.Lock()
			delete(w.running, id)
			w.mu.Unlock()
		}()
		w.loop(ctx, id, wake)
	}()
}

func (w *Worker) loop(ctx context.Context, id string, wake <-chan struct{}) {
	log := w.log.With("migration", id)
	for {
		m, err := w.store.Get(ctx, id)
		if err != nil {
			if ctx.Err() == nil {
				log.Error("load migration", "err", err)
			}
			return
		}
		if m.Phase.Terminal() {
			return
		}

		wait, err := w.step(ctx, m)
		if errors.Is(err, errSuperseded) {
			continue // run the command that replaced it
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			permanent := errors.Is(err, engine.ErrPermanent)
			log.Error("step failed", "phase", m.Phase, "err", err, "permanent", permanent)
			if recErr := w.recordError(ctx, id, err, permanent); recErr != nil {
				log.Error("record error", "err", recErr)
			}
			if permanent {
				return
			}
			wait = w.cfg.RetryBackoff
		}
		if wait == 0 {
			continue
		}

		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-wake:
		case <-t.C:
		}
		t.Stop()
	}
}

// step does one unit of work and returns how long to wait before the next.
func (w *Worker) step(ctx context.Context, m migration.Migration) (time.Duration, error) {
	if m.Pending != nil {
		return 0, w.runCommand(ctx, m)
	}

	switch m.Phase {
	case migration.PhaseCreated:
		if err := w.engine.Prepare(ctx, m); err != nil {
			return 0, err
		}
		return 0, w.mutate(ctx, m.ID, func(x *migration.Migration) error {
			x.Replication.Forward.State = migration.FlowInitialCopy
			return w.enter(x, migration.PhaseInitialSync, migration.StateRunning)
		})

	case migration.PhaseInitialSync:
		ictx, done := w.interruptible(ctx, m.ID)
		defer done()
		report := func(p migration.Progress) {
			err := w.mutate(ctx, m.ID, func(x *migration.Migration) error {
				x.Replication.InitialCopy = p
				return nil
			})
			if err != nil && ctx.Err() == nil {
				w.log.Warn("save progress", "migration", m.ID, "err", err)
			}
		}
		if err := w.engine.InitialSync(ictx, m, report); err != nil {
			if interrupted(ctx, ictx) {
				return 0, nil
			}
			return 0, err
		}
		return 0, w.mutate(ctx, m.ID, func(x *migration.Migration) error {
			x.LastError = nil
			x.Replication.Forward.State = migration.FlowStreaming
			return w.enter(x, migration.PhaseSyncing, migration.StateRunning)
		})

	case migration.PhaseSyncing, migration.PhaseReady:
		ictx, done := w.interruptible(ctx, m.ID)
		defer done()
		report, err := w.engine.Sync(ictx, m)
		if err != nil {
			if interrupted(ctx, ictx) {
				return 0, nil
			}
			return 0, err
		}
		wait := w.cfg.SyncInterval
		if report.Wait > 0 {
			wait = report.Wait
		}
		err = w.mutate(ctx, m.ID, func(x *migration.Migration) error {
			if x.Phase != migration.PhaseSyncing && x.Phase != migration.PhaseReady {
				return nil // a command moved it on while we synced
			}
			x.Readiness.Cutover, x.LastError = report.Cutover, nil
			x.Replication.Forward.Backlog = report.Backlog
			x.Replication.Forward.LagSeconds = report.LagSeconds
			if x.Pending != nil {
				return nil // keep the phase the pending command was accepted in
			}
			ready := report.Cutover.Result == migration.ResultPass
			switch {
			case ready && x.Phase == migration.PhaseSyncing:
				return w.enter(x, migration.PhaseReady, migration.StateWaiting)
			case !ready && x.Phase == migration.PhaseReady:
				return w.enter(x, migration.PhaseSyncing, migration.StateRunning)
			}
			return nil
		})
		return wait, err

	case migration.PhaseResumed:
		return 0, w.mutate(ctx, m.ID, func(x *migration.Migration) error {
			return w.enter(x, migration.PhaseVerifying, migration.StateRunning)
		})

	case migration.PhaseVerifying:
		// Verification goes on after it first passes: each round also copies
		// the target's new objects back to the source, so a rollback stays
		// possible until finalize.
		report, err := w.engine.Verify(ctx, m)
		if err != nil {
			return 0, err
		}
		err = w.mutate(ctx, m.ID, func(x *migration.Migration) error {
			if x.Phase != migration.PhaseVerifying {
				return nil
			}
			x.Readiness.Advance, x.Readiness.Rollback, x.LastError = report.Advance, report.Rollback, nil
			x.Replication.Reverse.Backlog = report.ReverseBacklog
			x.Replication.Reverse.LagSeconds = report.ReverseLagSeconds
			if report.Advance.Result == migration.ResultPass {
				x.State = migration.StateSucceeded
			} else {
				x.State = migration.StateRunning
			}
			return nil
		})
		// Verifying lists both buckets, like a sync round.
		return w.cfg.SyncInterval, err

	case migration.PhaseRollingBack:
		// Writes go back to the source while reads still find what was
		// written to the target; once the engine has copied that back, the
		// target is dropped.
		// A retry picks up after the last step that finished: going back to
		// reading the target once apps stopped deleting from it would find
		// objects deleted since.
		deadline := w.now().Add(w.cfg.ConfirmTimeout)
		if !routeIn(m.Routing, migration.RouteSource, migration.RouteSourceOnly) {
			if err := w.route(ctx, m.ID, deadline, migration.RouteBoth, nil); err != nil {
				return 0, err
			}
			if err := w.engine.Rollback(ctx, m); err != nil {
				return 0, err
			}
		}
		if err := w.routeSteps(ctx, m.ID, deadline, nil, migration.RouteSource, migration.RouteSourceOnly); err != nil {
			return 0, err
		}
		if err := w.engine.Release(ctx, m); err != nil {
			return 0, err
		}
		return 0, w.mutate(ctx, m.ID, func(x *migration.Migration) error {
			x.LastError = nil
			stopFlows(x)
			return w.enter(x, migration.PhaseRolledBack, migration.StateSucceeded)
		})
	}

	// FROZEN and SWITCHED wait for the next command.
	return w.cfg.PollInterval, nil
}

// runCommand carries out the pending command and clears it.
func (w *Worker) runCommand(ctx context.Context, m migration.Migration) error {
	cmd := *m.Pending
	var (
		act   func(context.Context, migration.Migration) error
		to    migration.Phase
		state migration.State
		// before runs ahead of the engine call so the status shows what is
		// happening while it runs; after records the result.
		before, after func(*migration.Migration)
	)
	switch cmd.Command {
	case migration.CommandFreeze:
		act, to, state = w.engine.Freeze, migration.PhaseFrozen, migration.StateWaiting
	case migration.CommandSwitch:
		// Reads cover both sides before writes move, so every app finds
		// objects written by apps on either route.
		act = func(ctx context.Context, m migration.Migration) error {
			if err := w.routeSteps(ctx, m.ID, w.deadline(cmd), &cmd, migration.RouteBoth, migration.RouteTarget); err != nil {
				return err
			}
			return w.engine.Switch(ctx, m)
		}
		to, state = migration.PhaseSwitched, migration.StateWaiting
	case migration.CommandResume:
		act, to, state = w.engine.Resume, migration.PhaseResumed, migration.StateRunning
		before = func(x *migration.Migration) { x.Replication.Forward.State = migration.FlowDraining }
		reverse := migration.FlowOff
		if w.engine.ReverseSync() {
			reverse = migration.FlowStreaming
		}
		after = func(x *migration.Migration) {
			x.Replication.Forward = migration.Flow{State: migration.FlowStopped}
			x.Replication.Reverse = migration.Flow{State: reverse}
		}
	case migration.CommandFinalize:
		act = func(ctx context.Context, m migration.Migration) error {
			if err := w.routeSteps(ctx, m.ID, w.deadline(cmd), &cmd, migration.RouteTargetAlone, migration.RouteTargetOnly); err != nil {
				return err
			}
			return w.engine.Finalize(ctx, m)
		}
		to, state = migration.PhaseCompleted, migration.StateSucceeded
		after = stopFlows
	case migration.CommandAbort:
		act = func(ctx context.Context, m migration.Migration) error {
			// Before the switch apps only write to the source; deletes stop
			// going to the target.
			if err := w.route(ctx, m.ID, w.deadline(cmd), migration.RouteSourceOnly, &cmd); err != nil {
				return err
			}
			return w.engine.Abort(ctx, m)
		}
		to, state = migration.PhaseAborted, migration.StateSucceeded
		after = stopFlows
	case migration.CommandRollback:
		// The engine work runs in ROLLING_BACK so a crash resumes it.
		to, state = migration.PhaseRollingBack, migration.StateRunning
	default:
		return fmt.Errorf("%w: unknown command %q", engine.ErrPermanent, cmd.Command)
	}

	if before != nil {
		err := w.mutate(ctx, m.ID, func(x *migration.Migration) error {
			if sameRequest(x.Pending, cmd) {
				before(x)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	if act != nil {
		if err := act(ctx, m); err != nil {
			return err
		}
	}
	if to.Terminal() {
		if err := w.stillPending(ctx, m.ID, cmd); err != nil {
			return err
		}
		if err := w.engine.Release(ctx, m); err != nil {
			return err
		}
	}
	return w.mutate(ctx, m.ID, func(x *migration.Migration) error {
		if !sameRequest(x.Pending, cmd) {
			return nil // already handled
		}
		x.Pending = nil
		x.LastError = nil
		if after != nil {
			after(x)
		}
		return w.enter(x, to, state)
	})
}

// stopFlows marks both directions as finished, keeping directions that
// never ran as they were.
func stopFlows(x *migration.Migration) {
	for _, f := range []*migration.Flow{&x.Replication.Forward, &x.Replication.Reverse} {
		if f.State != migration.FlowOff && f.State != migration.FlowNotStarted {
			*f = migration.Flow{State: migration.FlowStopped}
		}
	}
}

func sameRequest(p *migration.CommandRequest, c migration.CommandRequest) bool {
	return p != nil && p.Command == c.Command && p.IdempotencyKey == c.IdempotencyKey && p.RequestedAt.Equal(c.RequestedAt)
}

// deadline is when a command's wait for apps gives up.
func (w *Worker) deadline(cmd migration.CommandRequest) time.Time {
	if cmd.DeadlineMS > 0 {
		return w.now().Add(time.Duration(cmd.DeadlineMS) * time.Millisecond)
	}
	return w.now().Add(w.cfg.ConfirmTimeout)
}

// routeSteps moves the migration through steps in order, starting after
// the last step it is on already, so a retried command never moves apps
// back.
func (w *Worker) routeSteps(ctx context.Context, id string, deadline time.Time, cmd *migration.CommandRequest, steps ...migration.Route) error {
	m, err := w.store.Get(ctx, id)
	if err != nil {
		return err
	}
	from := 0
	for i, r := range steps {
		if routeIn(m.Routing, r) {
			from = i // re-confirm the step it is on, then go on
		}
	}
	for _, r := range steps[from:] {
		if err := w.route(ctx, id, deadline, r, cmd); err != nil {
			return err
		}
	}
	return nil
}

// routeIn reports whether cur is one of the routes, ignoring generations.
func routeIn(cur migration.Route, routes ...migration.Route) bool {
	for _, r := range routes {
		if cur.Write == r.Write && slices.Equal(cur.Read, r.Read) && slices.Equal(cur.Delete, r.Delete) {
			return true
		}
	}
	return false
}

// route moves the migration to r and waits until every live app applied
// it. With cmd set, it stops with errSuperseded as soon as cmd is no
// longer the pending command, before or while it waits.
func (w *Worker) route(ctx context.Context, id string, deadline time.Time, r migration.Route, cmd *migration.CommandRequest) error {
	var gen int64
	if err := w.mutate(ctx, id, func(x *migration.Migration) error {
		if cmd != nil && !sameRequest(x.Pending, *cmd) {
			return errSuperseded
		}
		gen = x.SetRoute(r)
		return nil
	}); err != nil {
		return err
	}
	if w.cfg.Apps == nil {
		return nil
	}
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for n := 0; len(w.cfg.Apps.Waiting(id, gen)) > 0; n++ {
		if !w.now().Before(deadline) {
			return fmt.Errorf("%w: generation %d, waiting for %s", ErrUnconfirmed, gen, strings.Join(w.cfg.Apps.Waiting(id, gen), ", "))
		}
		if cmd != nil && n%5 == 4 {
			if err := w.stillPending(ctx, id, *cmd); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
}

// stillPending returns errSuperseded once cmd is no longer pending.
func (w *Worker) stillPending(ctx context.Context, id string, cmd migration.CommandRequest) error {
	m, err := w.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if !sameRequest(m.Pending, cmd) {
		return errSuperseded
	}
	return nil
}

func (w *Worker) enter(x *migration.Migration, to migration.Phase, state migration.State) error {
	if err := x.TransitionTo(to, w.now()); err != nil {
		return err
	}
	x.State = state
	return nil
}

func (w *Worker) recordError(ctx context.Context, id string, cause error, permanent bool) error {
	if permanent {
		// A failed migration must not leave subscriptions behind, but
		// failing to remove them must not hide the cause.
		if m, err := w.store.Get(ctx, id); err == nil {
			for attempt := range 3 {
				err := w.engine.Release(ctx, m)
				if err == nil {
					break
				}
				w.log.Error("release after failure", "migration", id, "attempt", attempt+1, "err", err)
				select {
				case <-ctx.Done():
				case <-time.After(w.cfg.RetryBackoff):
				}
			}
		}
	}
	return w.mutate(ctx, id, func(x *migration.Migration) error {
		if errors.Is(cause, ErrUnconfirmed) {
			x.LastError = &migration.Error{Code: "APPS_NOT_CONFIRMED", Message: cause.Error(), Retryable: true}
			x.State = migration.StateUnknown
			return nil
		}
		x.LastError = &migration.Error{Code: "ENGINE_ERROR", Message: cause.Error(), Retryable: !permanent}
		if permanent {
			return w.enter(x, migration.PhaseFailed, migration.StateFailed)
		}
		return nil
	})
}

// mutate applies fn to a fresh copy and saves it, rereading on version
// conflicts. fn must be safe to run more than once.
func (w *Worker) mutate(ctx context.Context, id string, fn func(*migration.Migration) error) error {
	const attempts = 5
	for range attempts {
		m, err := w.store.Get(ctx, id)
		if err != nil {
			return err
		}
		if err := fn(&m); err != nil {
			return err
		}
		m.UpdatedAt = w.now()
		_, err = w.store.Update(ctx, m)
		if err == nil {
			return nil
		}
		if !errors.Is(err, store.ErrConflict) {
			return err
		}
	}
	return fmt.Errorf("save migration %s: %d version conflicts in a row", id, attempts)
}
