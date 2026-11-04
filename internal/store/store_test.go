package store

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

var ctx = context.Background()

// eachStore runs a test against the memory store and, when OM_POSTGRES_URL
// names a database, against an emptied Postgres store.
func eachStore(t *testing.T, test func(t *testing.T, s Store)) {
	t.Run("memory", func(t *testing.T) { test(t, NewMemory()) })
	t.Run("postgres", func(t *testing.T) { test(t, emptyPostgres(t)) })
}

func emptyPostgres(t *testing.T) *Postgres {
	t.Helper()
	url := os.Getenv("OM_POSTGRES_URL")
	if url == "" {
		t.Skip("OM_POSTGRES_URL not set")
	}
	s, err := OpenPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if _, err := s.pool.Exec(ctx, `TRUNCATE migrations, deleted_keys, feed_positions, idempotency_keys`); err != nil {
		t.Fatal(err)
	}
	return s
}

func create(t *testing.T, s Store) migration.Migration {
	t.Helper()
	m, err := migration.New(
		migration.Endpoint{Kind: migration.KindS3, Bucket: "src", Region: "ap-northeast-1", Ref: "secret://a"},
		migration.Endpoint{Kind: migration.KindGCS, Bucket: "dst", Ref: "secret://b"},
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	m, err = s.Create(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestCreateAndGet(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		m := create(t, s)
		if m.Version != 1 {
			t.Fatalf("version = %d, want 1", m.Version)
		}
		if _, err := s.Create(ctx, m); !errors.Is(err, ErrExists) {
			t.Fatalf("duplicate create: err = %v, want ErrExists", err)
		}
		got, err := s.Get(ctx, m.ID)
		if err != nil || got.ID != m.ID {
			t.Fatalf("get: %v %+v", err, got)
		}
		if _, err := s.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
}

func TestUpdateRejectsStaleCopy(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		m := create(t, s)
		stale := m

		m.Detail = "copying"
		if _, err := s.Update(ctx, m); err != nil {
			t.Fatal(err)
		}
		stale.Detail = "other"
		if _, err := s.Update(ctx, stale); !errors.Is(err, ErrConflict) {
			t.Fatalf("err = %v, want ErrConflict", err)
		}
	})
}

func TestConcurrentUpdatesOnlyOneWins(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		m := create(t, s)

		var wg sync.WaitGroup
		var mu sync.Mutex
		wins := 0
		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := s.Update(ctx, m); err == nil {
					mu.Lock()
					wins++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if wins != 1 {
			t.Fatalf("wins = %d, want 1", wins)
		}
	})
}

func TestRequestCommandBumpsVersionOnce(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		m := create(t, s)
		m.Phase = migration.PhaseReady
		m, err := s.Update(ctx, m)
		if err != nil {
			t.Fatal(err)
		}

		req := migration.CommandRequest{Command: migration.CommandFreeze, IdempotencyKey: "k"}
		first, err := s.RequestCommand(ctx, m.ID, req)
		if err != nil {
			t.Fatal(err)
		}
		second, err := s.RequestCommand(ctx, m.ID, req)
		if err != nil {
			t.Fatal(err)
		}
		if first.Version != m.Version+1 || second.Version != first.Version {
			t.Fatalf("versions = %d, %d (start %d)", first.Version, second.Version, m.Version)
		}
	})
}

func TestRequestCommandRejectsWrongPhase(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		m := create(t, s)
		_, err := s.RequestCommand(ctx, m.ID, migration.CommandRequest{Command: migration.CommandSwitch})
		if !errors.Is(err, migration.ErrInvalidPhase) {
			t.Fatalf("err = %v, want ErrInvalidPhase", err)
		}
		got, _ := s.Get(ctx, m.ID)
		if got.Pending != nil || got.Version != m.Version {
			t.Fatal("rejected command changed the stored migration")
		}
	})
}

func TestListActiveSkipsTerminal(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		active := create(t, s)
		done := create(t, s)
		done.Phase = migration.PhaseCompleted
		if _, err := s.Update(ctx, done); err != nil {
			t.Fatal(err)
		}
		list, err := s.ListActive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 1 || list[0].ID != active.ID {
			t.Fatalf("got %d migrations, want only %s", len(list), active.ID)
		}
	})
}

func TestReturnedCopiesAreIsolated(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		m := create(t, s)
		m.Readiness.Rollback.Checks[0].OK = false
		got, _ := s.Get(ctx, m.ID)
		if !got.Readiness.Rollback.Checks[0].OK {
			t.Fatal("caller's change leaked into the store")
		}
	})
}

func TestRequestCommandSavesAReplacedCommand(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		m := create(t, s)
		m.Phase, m.State = migration.PhaseFrozen, migration.StateWaiting
		m, err := s.Update(ctx, m)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.RequestCommand(ctx, m.ID, migration.CommandRequest{Command: migration.CommandSwitch, RequestedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		stuck, _ := s.Get(ctx, m.ID)
		stuck.State = migration.StateUnknown
		if _, err := s.Update(ctx, stuck); err != nil {
			t.Fatal(err)
		}
		before, _ := s.Get(ctx, m.ID)
		if _, err := s.RequestCommand(ctx, m.ID, migration.CommandRequest{Command: migration.CommandRollback, RequestedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		after, _ := s.Get(ctx, m.ID)
		if after.Pending == nil || after.Pending.Command != migration.CommandRollback || after.Version != before.Version+1 {
			t.Fatalf("pending %+v version %d -> %d; want the rollback saved with a new version", after.Pending, before.Version, after.Version)
		}
		// A stale writer holding the old version can no longer undo it.
		if _, err := s.Update(ctx, before); !errors.Is(err, ErrConflict) {
			t.Fatalf("stale update: %v, want conflict", err)
		}
	})
}

func TestSavedResponses(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		saved := s.(interface {
			SavedResponse(context.Context, string) (SavedResponse, bool, error)
			SaveResponse(context.Context, string, SavedResponse) error
		})
		if _, ok, err := saved.SavedResponse(ctx, "k"); ok || err != nil {
			t.Fatalf("before saving: %v %v", ok, err)
		}
		want := SavedResponse{Fingerprint: [32]byte{1}, Status: 201, Location: "/v1/migrations/x", Body: []byte(`{"id":"x"}`)}
		if err := saved.SaveResponse(ctx, "k", want); err != nil {
			t.Fatal(err)
		}
		// The first response stays.
		if err := saved.SaveResponse(ctx, "k", SavedResponse{Status: 500}); err != nil {
			t.Fatal(err)
		}
		got, ok, err := saved.SavedResponse(ctx, "k")
		if !ok || err != nil || got.Fingerprint != want.Fingerprint || got.Status != 201 || got.Location != want.Location || string(got.Body) != string(want.Body) {
			t.Fatalf("got %+v %v %v", got, ok, err)
		}
	})
}
