// Package store keeps migrations. The API and the worker talk to each other
// only through a Store.
package store

import (
	"context"
	"errors"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

var (
	ErrNotFound = errors.New("migration not found")
	ErrExists   = errors.New("migration already exists")
	// ErrConflict means the migration changed since it was read.
	ErrConflict = errors.New("version conflict")
)

// Store persists migrations. Every write bumps Version; Update fails with
// ErrConflict when the caller's copy is stale.
type Store interface {
	Create(ctx context.Context, m migration.Migration) (migration.Migration, error)
	Get(ctx context.Context, id string) (migration.Migration, error)
	// RequestCommand checks the command against the current phase and records
	// it as pending in one step.
	RequestCommand(ctx context.Context, id string, req migration.CommandRequest) (migration.Migration, error)
	Update(ctx context.Context, m migration.Migration) (migration.Migration, error)
	// ListActive returns migrations that are not in a terminal phase.
	ListActive(ctx context.Context) ([]migration.Migration, error)
}
