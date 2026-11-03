package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

// Fake pretends to move objects. Each step waits Delay and succeeds, so the
// whole phase flow can run without real storage.
type Fake struct {
	// Delay is how long each step takes.
	Delay time.Duration
	// SyncRounds is how many Sync calls a migration needs before it is ready.
	SyncRounds int

	mu     sync.Mutex
	rounds map[string]int
}

func NewFake(delay time.Duration) *Fake {
	return &Fake{Delay: delay, SyncRounds: 2, rounds: make(map[string]int)}
}

func (f *Fake) Name() string { return "fake" }

func (f *Fake) InitialSync(ctx context.Context, m migration.Migration, report func(migration.Progress)) error {
	const total = 10
	for done := int64(1); done <= total; done++ {
		if err := f.wait(ctx, f.Delay/total); err != nil {
			return err
		}
		report(migration.Progress{Completed: done, Total: total, Unit: "objects"})
	}
	return nil
}

func (f *Fake) Sync(ctx context.Context, m migration.Migration) (SyncReport, error) {
	if err := f.wait(ctx, f.Delay); err != nil {
		return SyncReport{}, err
	}
	f.mu.Lock()
	f.rounds[m.ID]++
	n := f.rounds[m.ID]
	f.mu.Unlock()

	ready := n >= f.SyncRounds
	r := SyncReport{Cutover: migration.Decide(migration.Check{
		Name:    "fake_sync_rounds",
		OK:      ready,
		Message: fmt.Sprintf("%d/%d rounds", min(n, f.SyncRounds), f.SyncRounds),
	})}
	if !ready {
		r.Backlog, r.LagSeconds = 1, 1
	}
	return r, nil
}

func (f *Fake) Verify(ctx context.Context, _ migration.Migration) (VerifyReport, error) {
	if err := f.wait(ctx, f.Delay); err != nil {
		return VerifyReport{}, err
	}
	return VerifyReport{
		Advance:  migration.Decide(migration.Check{Name: "fake_diff", OK: true}),
		Rollback: migration.Decide(migration.Check{Name: "fake_reverse_sync", OK: true}),
	}, nil
}

func (f *Fake) Freeze(ctx context.Context, _ migration.Migration) error { return f.wait(ctx, f.Delay) }
func (f *Fake) Switch(ctx context.Context, _ migration.Migration) error { return f.wait(ctx, f.Delay) }
func (f *Fake) Resume(ctx context.Context, _ migration.Migration) error { return f.wait(ctx, f.Delay) }
func (f *Fake) Finalize(ctx context.Context, _ migration.Migration) error {
	return f.wait(ctx, f.Delay)
}
func (f *Fake) Rollback(ctx context.Context, _ migration.Migration) error {
	return f.wait(ctx, f.Delay)
}
func (f *Fake) Abort(ctx context.Context, _ migration.Migration) error { return f.wait(ctx, f.Delay) }

func (f *Fake) wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
