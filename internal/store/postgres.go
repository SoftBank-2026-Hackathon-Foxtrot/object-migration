package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

// Postgres keeps migrations in PostgreSQL, so a restarted server picks up
// where it stopped. Each migration is one JSON document with its version
// beside it; Update writes only when the version still matches. It also
// keeps what the server must not forget about a running migration: the
// keys apps deleted, the change feed's resume point, and the responses
// replayed for Idempotency-Key.
type Postgres struct {
	pool *pgxpool.Pool
}

const schema = `
CREATE TABLE IF NOT EXISTS migrations (
	id         text PRIMARY KEY,
	version    bigint NOT NULL,
	terminal   boolean NOT NULL,
	created_at timestamptz NOT NULL,
	doc        jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS migrations_active ON migrations (created_at) WHERE NOT terminal;
CREATE TABLE IF NOT EXISTS deleted_keys (
	migration_id text NOT NULL,
	key          text NOT NULL,
	recorded_at  timestamptz NOT NULL,
	PRIMARY KEY (migration_id, key)
);
CREATE TABLE IF NOT EXISTS feed_positions (
	migration_id text PRIMARY KEY,
	position     text NOT NULL,
	updated_at   timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS idempotency_keys (
	key         text PRIMARY KEY,
	fingerprint bytea NOT NULL,
	status      integer NOT NULL,
	location    text NOT NULL,
	body        bytea NOT NULL,
	created_at  timestamptz NOT NULL DEFAULT now()
);`

// OpenPostgres connects to url and creates the tables if they are missing.
func OpenPostgres(ctx context.Context, url string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("create tables: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

func (s *Postgres) Close() { s.pool.Close() }

// querier is a pool or a transaction.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Postgres) Create(ctx context.Context, m migration.Migration) (migration.Migration, error) {
	m.Version = 1
	doc, err := json.Marshal(m)
	if err != nil {
		return migration.Migration{}, err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO migrations (id, version, terminal, created_at, doc) VALUES ($1, $2, $3, $4, $5)`,
		m.ID, m.Version, m.Phase.Terminal(), m.CreatedAt, doc)
	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return migration.Migration{}, ErrExists
	}
	if err != nil {
		return migration.Migration{}, err
	}
	return m, nil
}

func (s *Postgres) Get(ctx context.Context, id string) (migration.Migration, error) {
	return get(ctx, s.pool, id, "")
}

// get reads a migration; lock "FOR UPDATE" holds its row until the
// transaction ends.
func get(ctx context.Context, q querier, id, lock string) (migration.Migration, error) {
	var doc []byte
	err := q.QueryRow(ctx, `SELECT doc FROM migrations WHERE id = $1 `+lock, id).Scan(&doc)
	if errors.Is(err, pgx.ErrNoRows) {
		return migration.Migration{}, ErrNotFound
	}
	if err != nil {
		return migration.Migration{}, err
	}
	var m migration.Migration
	if err := json.Unmarshal(doc, &m); err != nil {
		return migration.Migration{}, fmt.Errorf("decode migration %s: %w", id, err)
	}
	return m, nil
}

func (s *Postgres) RequestCommand(ctx context.Context, id string, req migration.CommandRequest) (migration.Migration, error) {
	var out migration.Migration
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		m, err := get(ctx, tx, id, "FOR UPDATE")
		if err != nil {
			return err
		}
		before := m.Pending
		if err := m.RequestCommand(req); err != nil {
			return err
		}
		if before != nil && samePending(*before, *m.Pending) {
			out = m // a repeated request; nothing to write
			return nil
		}
		m.Version++
		out = m
		return write(ctx, tx, m, m.Version-1)
	})
	return out, err
}

func (s *Postgres) Update(ctx context.Context, m migration.Migration) (migration.Migration, error) {
	m.Version++
	if err := write(ctx, s.pool, m, m.Version-1); err != nil {
		return migration.Migration{}, err
	}
	return m, nil
}

// write saves m if the stored version is still was.
func write(ctx context.Context, q querier, m migration.Migration, was int64) error {
	doc, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tag, err := q.Exec(ctx,
		`UPDATE migrations SET version = $2, terminal = $3, doc = $4 WHERE id = $1 AND version = $5`,
		m.ID, m.Version, m.Phase.Terminal(), doc, was)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var exists bool
	if err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM migrations WHERE id = $1)`, m.ID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrNotFound
	}
	return ErrConflict
}

func (s *Postgres) ListActive(ctx context.Context) ([]migration.Migration, error) {
	rows, err := s.pool.Query(ctx, `SELECT doc FROM migrations WHERE NOT terminal ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	docs, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		return nil, err
	}
	out := make([]migration.Migration, len(docs))
	for i, doc := range docs {
		if err := json.Unmarshal(doc, &out[i]); err != nil {
			return nil, fmt.Errorf("decode migration: %w", err)
		}
	}
	return out, nil
}

// RecordDeletes saves keys apps deleted during a migration. A key keeps
// the time it was first recorded.
func (s *Postgres) RecordDeletes(ctx context.Context, id string, keys []string, at time.Time) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO deleted_keys (migration_id, key, recorded_at)
		 SELECT $1, k, $3 FROM unnest($2::text[]) AS k
		 ON CONFLICT DO NOTHING`, id, keys, at)
	return err
}

// LoadDeletes returns every recorded key, by migration.
func (s *Postgres) LoadDeletes(ctx context.Context) (map[string]map[string]time.Time, error) {
	rows, err := s.pool.Query(ctx, `SELECT migration_id, key, recorded_at FROM deleted_keys`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]map[string]time.Time)
	for rows.Next() {
		var id, key string
		var at time.Time
		if err := rows.Scan(&id, &key, &at); err != nil {
			return nil, err
		}
		if out[id] == nil {
			out[id] = make(map[string]time.Time)
		}
		out[id][key] = at
	}
	return out, rows.Err()
}

func (s *Postgres) ForgetDeletes(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM deleted_keys WHERE migration_id = $1`, id)
	return err
}

// FeedPosition returns where a migration's change feed was read up to, or
// "" if nothing was saved.
func (s *Postgres) FeedPosition(ctx context.Context, id string) (string, error) {
	var pos string
	err := s.pool.QueryRow(ctx, `SELECT position FROM feed_positions WHERE migration_id = $1`, id).Scan(&pos)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return pos, err
}

func (s *Postgres) SaveFeedPosition(ctx context.Context, id, pos string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO feed_positions (migration_id, position, updated_at) VALUES ($1, $2, now())
		 ON CONFLICT (migration_id) DO UPDATE SET position = $2, updated_at = now()`, id, pos)
	return err
}

func (s *Postgres) ForgetFeedPosition(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM feed_positions WHERE migration_id = $1`, id)
	return err
}

func (s *Postgres) SavedResponse(ctx context.Context, key string) (SavedResponse, bool, error) {
	var r SavedResponse
	var fp []byte
	err := s.pool.QueryRow(ctx,
		`SELECT fingerprint, status, location, body FROM idempotency_keys WHERE key = $1`, key,
	).Scan(&fp, &r.Status, &r.Location, &r.Body)
	if errors.Is(err, pgx.ErrNoRows) {
		return SavedResponse{}, false, nil
	}
	if err != nil {
		return SavedResponse{}, false, err
	}
	copy(r.Fingerprint[:], fp)
	return r, true, nil
}

func (s *Postgres) SaveResponse(ctx context.Context, key string, r SavedResponse) error {
	if r.Body == nil {
		r.Body = []byte{} // an empty response, which would otherwise be NULL
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO idempotency_keys (key, fingerprint, status, location, body) VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (key) DO NOTHING`, key, r.Fingerprint[:], r.Status, r.Location, r.Body)
	return err
}
