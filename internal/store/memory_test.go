package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

var ctx = context.Background()

func create(t *testing.T, s *Memory) migration.Migration {
	t.Helper()
	m, err := migration.New(
		migration.Endpoint{Kind: migration.KindS3, Bucket: "src", Ref: "secret://a"},
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
	s := NewMemory()
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
}

func TestUpdateRejectsStaleCopy(t *testing.T) {
	s := NewMemory()
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
}

func TestConcurrentUpdatesOnlyOneWins(t *testing.T) {
	s := NewMemory()
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
}

func TestRequestCommandBumpsVersionOnce(t *testing.T) {
	s := NewMemory()
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
}

func TestRequestCommandRejectsWrongPhase(t *testing.T) {
	s := NewMemory()
	m := create(t, s)
	_, err := s.RequestCommand(ctx, m.ID, migration.CommandRequest{Command: migration.CommandSwitch})
	if !errors.Is(err, migration.ErrInvalidPhase) {
		t.Fatalf("err = %v, want ErrInvalidPhase", err)
	}
	got, _ := s.Get(ctx, m.ID)
	if got.Pending != nil || got.Version != m.Version {
		t.Fatal("rejected command changed the stored migration")
	}
}

func TestListActiveSkipsTerminal(t *testing.T) {
	s := NewMemory()
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
}

func TestReturnedCopiesAreIsolated(t *testing.T) {
	s := NewMemory()
	m := create(t, s)
	m.Readiness.Rollback.Checks[0].OK = false
	got, _ := s.Get(ctx, m.ID)
	if !got.Readiness.Rollback.Checks[0].OK {
		t.Fatal("caller's change leaked into the store")
	}
}
