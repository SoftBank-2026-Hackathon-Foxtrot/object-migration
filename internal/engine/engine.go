// Package engine does the storage work behind each migration phase. The
// worker decides when to call it; the engine decides how.
package engine

import (
	"context"
	"errors"

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
	// InitialSync copies everything already in the source. report may be
	// called any number of times with the latest progress.
	InitialSync(ctx context.Context, m migration.Migration, report func(migration.Progress)) error
	// Sync runs one round of incremental sync and judges whether cutover can
	// start.
	Sync(ctx context.Context, m migration.Migration) (migration.Readiness, migration.Lag, error)
	Freeze(ctx context.Context, m migration.Migration) error
	Switch(ctx context.Context, m migration.Migration) error
	Resume(ctx context.Context, m migration.Migration) error
	// Verify compares source and target after resume. Ready means they match.
	Verify(ctx context.Context, m migration.Migration) (migration.Readiness, error)
	Finalize(ctx context.Context, m migration.Migration) error
	Rollback(ctx context.Context, m migration.Migration) error
	Abort(ctx context.Context, m migration.Migration) error
}
