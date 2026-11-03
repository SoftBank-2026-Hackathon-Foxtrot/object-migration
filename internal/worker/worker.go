// Package worker moves migrations through their phases. It reads state from
// the store, calls the engine, and writes the result back. Each migration
// runs in its own goroutine.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/engine"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/store"
)

type Config struct {
	// PollInterval is how often a migration rechecks its state while it
	// waits, and how often the worker looks for migrations it is not running.
	PollInterval time.Duration
	// RetryBackoff is the wait after a failed engine call.
	RetryBackoff time.Duration
}

type Worker struct {
	store  store.Store
	engine engine.Engine
	cfg    Config
	log    *slog.Logger
	now    func() time.Time

	mu      sync.Mutex
	ctx     context.Context // set by Run; nil until then
	running map[string]chan struct{}
	wg      sync.WaitGroup
}

func New(s store.Store, e engine.Engine, cfg Config, log *slog.Logger) *Worker {
	return &Worker{
		store:   s,
		engine:  e,
		cfg:     cfg,
		log:     log,
		now:     time.Now,
		running: make(map[string]chan struct{}),
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
// API recorded a command. Missing a notification only delays the work until
// the next poll.
func (w *Worker) Notify(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ctx == nil || w.ctx.Err() != nil {
		return
	}
	if wake, ok := w.running[id]; ok {
		select {
		case wake <- struct{}{}:
		default:
		}
		return
	}
	w.startLocked(id)
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
		return 0, w.mutate(ctx, m.ID, func(x *migration.Migration) error {
			return w.enter(x, migration.PhaseInitialSync, migration.StateRunning)
		})

	case migration.PhaseInitialSync:
		report := func(p migration.Progress) {
			err := w.mutate(ctx, m.ID, func(x *migration.Migration) error {
				x.Progress = p
				return nil
			})
			if err != nil && ctx.Err() == nil {
				w.log.Warn("save progress", "migration", m.ID, "err", err)
			}
		}
		if err := w.engine.InitialSync(ctx, m, report); err != nil {
			return 0, err
		}
		return 0, w.mutate(ctx, m.ID, func(x *migration.Migration) error {
			x.LastError = nil
			return w.enter(x, migration.PhaseSyncing, migration.StateRunning)
		})

	case migration.PhaseSyncing, migration.PhaseReady:
		readiness, lag, err := w.engine.Sync(ctx, m)
		if err != nil {
			return 0, err
		}
		err = w.mutate(ctx, m.ID, func(x *migration.Migration) error {
			if x.Phase != migration.PhaseSyncing && x.Phase != migration.PhaseReady {
				return nil // a command moved it on while we synced
			}
			x.Readiness, x.Lag, x.LastError = readiness, lag, nil
			if x.Pending != nil {
				return nil // keep the phase the pending command was accepted in
			}
			switch {
			case readiness.Ready && x.Phase == migration.PhaseSyncing:
				return w.enter(x, migration.PhaseReady, migration.StateWaiting)
			case !readiness.Ready && x.Phase == migration.PhaseReady:
				return w.enter(x, migration.PhaseSyncing, migration.StateRunning)
			}
			return nil
		})
		return w.cfg.PollInterval, err

	case migration.PhaseResumed:
		return 0, w.mutate(ctx, m.ID, func(x *migration.Migration) error {
			return w.enter(x, migration.PhaseVerifying, migration.StateRunning)
		})

	case migration.PhaseVerifying:
		if m.State == migration.StateSucceeded {
			return w.cfg.PollInterval, nil // verified; waiting for finalize or rollback
		}
		readiness, err := w.engine.Verify(ctx, m)
		if err != nil {
			return 0, err
		}
		err = w.mutate(ctx, m.ID, func(x *migration.Migration) error {
			if x.Phase != migration.PhaseVerifying {
				return nil
			}
			x.Readiness, x.LastError = readiness, nil
			if readiness.Ready {
				x.State = migration.StateSucceeded
			}
			return nil
		})
		return w.cfg.PollInterval, err

	case migration.PhaseRollingBack:
		if err := w.engine.Rollback(ctx, m); err != nil {
			return 0, err
		}
		return 0, w.mutate(ctx, m.ID, func(x *migration.Migration) error {
			x.LastError = nil
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
	)
	switch cmd.Command {
	case migration.CommandFreeze:
		act, to, state = w.engine.Freeze, migration.PhaseFrozen, migration.StateWaiting
	case migration.CommandSwitch:
		act, to, state = w.engine.Switch, migration.PhaseSwitched, migration.StateWaiting
	case migration.CommandResume:
		act, to, state = w.engine.Resume, migration.PhaseResumed, migration.StateRunning
	case migration.CommandFinalize:
		act, to, state = w.engine.Finalize, migration.PhaseCompleted, migration.StateSucceeded
	case migration.CommandAbort:
		act, to, state = w.engine.Abort, migration.PhaseAborted, migration.StateSucceeded
	case migration.CommandRollback:
		// The engine work runs in ROLLING_BACK so a crash resumes it.
		to, state = migration.PhaseRollingBack, migration.StateRunning
	default:
		return fmt.Errorf("%w: unknown command %q", engine.ErrPermanent, cmd.Command)
	}

	if act != nil {
		if err := act(ctx, m); err != nil {
			return err
		}
	}
	return w.mutate(ctx, m.ID, func(x *migration.Migration) error {
		if !sameRequest(x.Pending, cmd) {
			return nil // already handled
		}
		x.Pending = nil
		x.LastError = nil
		return w.enter(x, to, state)
	})
}

func sameRequest(p *migration.CommandRequest, c migration.CommandRequest) bool {
	return p != nil && p.Command == c.Command && p.IdempotencyKey == c.IdempotencyKey && p.RequestedAt.Equal(c.RequestedAt)
}

func (w *Worker) enter(x *migration.Migration, to migration.Phase, state migration.State) error {
	if err := x.TransitionTo(to, w.now()); err != nil {
		return err
	}
	x.State = state
	return nil
}

func (w *Worker) recordError(ctx context.Context, id string, cause error, permanent bool) error {
	return w.mutate(ctx, id, func(x *migration.Migration) error {
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
