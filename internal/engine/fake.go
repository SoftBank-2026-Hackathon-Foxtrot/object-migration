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
		report(migration.Progress{Done: done, Total: total, Unit: "objects"})
	}
	return nil
}

func (f *Fake) Sync(ctx context.Context, m migration.Migration) (migration.Readiness, migration.Lag, error) {
	if err := f.wait(ctx, f.Delay); err != nil {
		return migration.Readiness{}, migration.Lag{}, err
	}
	f.mu.Lock()
	f.rounds[m.ID]++
	n := f.rounds[m.ID]
	f.mu.Unlock()

	ready := n >= f.SyncRounds
	check := migration.Check{Name: "fake_sync_rounds", OK: ready, Message: fmt.Sprintf("%d/%d rounds", min(n, f.SyncRounds), f.SyncRounds)}
	lag := migration.Lag{}
	if !ready {
		lag = migration.Lag{Seconds: 1, Items: 1}
	}
	return migration.Readiness{Ready: ready, Checks: []migration.Check{check}}, lag, nil
}

func (f *Fake) Verify(ctx context.Context, _ migration.Migration) (migration.Readiness, error) {
	if err := f.wait(ctx, f.Delay); err != nil {
		return migration.Readiness{}, err
	}
	return migration.Readiness{Ready: true, Checks: []migration.Check{{Name: "fake_diff", OK: true}}}, nil
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
