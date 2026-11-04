package store

import (
	"slices"
	"testing"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

func TestPostgresKeepsAWholeMigration(t *testing.T) {
	s := emptyPostgres(t)
	m := create(t, s)
	m.Phase, m.State = migration.PhaseSwitched, migration.StateWaiting
	m.SetRoute(migration.RouteTarget)
	m.Pending = &migration.CommandRequest{Command: migration.CommandResume, IdempotencyKey: "k", RequestedAt: time.Now()}
	m.Replication.InitialCopy = migration.Progress{Completed: 3, Total: 4, Unit: "objects", Bytes: 30, TotalBytes: 40}
	saved, err := s.Update(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != saved.Version || got.Routing.Generation != m.Routing.Generation || got.Routing.Write != migration.SideTarget ||
		!slices.Equal(got.Routing.Read, m.Routing.Read) || got.Pending == nil || !got.Pending.RequestedAt.Equal(m.Pending.RequestedAt) ||
		got.Replication.InitialCopy != m.Replication.InitialCopy || got.Source != m.Source {
		t.Fatalf("read back %+v\nwant %+v", got, saved)
	}
}

func TestPostgresKeepsDeletesAndFeedPositions(t *testing.T) {
	s := emptyPostgres(t)
	at := time.Now().Truncate(time.Microsecond)
	if err := s.RecordDeletes(ctx, "m1", []string{"a", "b"}, at); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordDeletes(ctx, "m1", []string{"a"}, at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordDeletes(ctx, "m2", []string{"c"}, at); err != nil {
		t.Fatal(err)
	}
	all, err := s.LoadDeletes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all["m1"]) != 2 || !all["m1"]["a"].Equal(at) || len(all["m2"]) != 1 {
		t.Fatalf("deletes = %v", all)
	}
	if err := s.ForgetDeletes(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	if all, _ = s.LoadDeletes(ctx); len(all["m1"]) != 0 || len(all["m2"]) != 1 {
		t.Fatalf("after forget: %v", all)
	}

	if pos, err := s.FeedPosition(ctx, "m1"); pos != "" || err != nil {
		t.Fatalf("no position yet: %q %v", pos, err)
	}
	for _, p := range []string{"100", "200"} {
		if err := s.SaveFeedPosition(ctx, "m1", p); err != nil {
			t.Fatal(err)
		}
	}
	if pos, _ := s.FeedPosition(ctx, "m1"); pos != "200" {
		t.Fatalf("position = %q", pos)
	}
	if err := s.ForgetFeedPosition(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	if pos, _ := s.FeedPosition(ctx, "m1"); pos != "" {
		t.Fatalf("position after forget = %q", pos)
	}
}
