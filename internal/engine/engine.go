// Package engine does the storage work behind each migration phase. The
// worker decides when to call it; the engine decides how.
package engine

import (
	"context"
	"errors"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

// ErrPermanent marks an error that retrying will not fix. The worker fails
// the migration instead of retrying.
var ErrPermanent = errors.New("permanent error")

// Engine runs the storage side of a migration. Every method must be safe to
// call again after a crash or a failed attempt.
type Engine interface {
	// Name identifies the engine in the capabilities response.
	Name() string
	// ReverseSync reports whether Resume starts copying the target's changes
	// back to the source, so the status can say whether that happens.
	ReverseSync() bool
	// Preflight checks that both buckets can be reached with the access the
	// migration needs, without changing anything the app can see.
	Preflight(ctx context.Context, source, target migration.Endpoint) []migration.Check
	// Prepare starts following changes in the source before anything is
	// copied, so no change made during the initial copy is missed.
	Prepare(ctx context.Context, m migration.Migration) error
	// InitialSync copies everything already in the source. report may be
	// called any number of times with the latest progress.
	InitialSync(ctx context.Context, m migration.Migration, report func(migration.Progress)) error
	// Sync runs one round of incremental sync and judges whether cutover can
	// start.
	Sync(ctx context.Context, m migration.Migration) (SyncReport, error)
	Freeze(ctx context.Context, m migration.Migration) error
	Switch(ctx context.Context, m migration.Migration) error
	Resume(ctx context.Context, m migration.Migration) error
	// Verify compares source and target after resume and checks that the
	// reverse sync can still carry a rollback.
	Verify(ctx context.Context, m migration.Migration) (VerifyReport, error)
	Finalize(ctx context.Context, m migration.Migration) error
	Rollback(ctx context.Context, m migration.Migration) error
	Abort(ctx context.Context, m migration.Migration) error
	// Release removes what Prepare set up. It runs once a migration is
	// over, however it ended, and also when Prepare never ran or ran only
	// in part.
	Release(ctx context.Context, m migration.Migration) error
}

// SyncReport is the result of one incremental sync round.
type SyncReport struct {
	Cutover    migration.Decision
	Backlog    int64
	LagSeconds float64
	// Wait, when set, is how long until the next round instead of the
	// worker's SyncInterval. An engine that follows changes as they happen
	// asks for the next round soon.
	Wait time.Duration
}

// VerifyReport is the result of comparing source and target after resume.
type VerifyReport struct {
	Advance           migration.Decision
	Rollback          migration.Decision
	ReverseBacklog    int64
	ReverseLagSeconds float64
}
