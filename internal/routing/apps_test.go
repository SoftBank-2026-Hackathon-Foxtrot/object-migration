package routing

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestWaitingAndWait(t *testing.T) {
	a := NewApps(time.Second)
	now := time.Unix(1000, 0)
	a.now = func() time.Time { return now }
	a.started = now.Add(-2 * time.Second)

	a.Report("m1", "web-1", 1)
	a.Report("m1", "web-2", 2)
	a.Report("m2", "web-1", 1)
	if got := a.Waiting("m1", 2); !slices.Equal(got, []string{"web-1"}) {
		t.Fatalf("waiting = %v", got)
	}
	if a.Live("m1") != 2 {
		t.Fatalf("live = %d", a.Live("m1"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := a.Wait(ctx, "m1", 2); err == nil {
		t.Fatal("Wait returned while web-1 is behind")
	}
	a.Report("m1", "web-1", 2)
	if err := a.Wait(context.Background(), "m1", 2); err != nil {
		t.Fatal(err)
	}

	// An app that went quiet is not waited for.
	now = now.Add(2 * time.Second)
	a.Report("m1", "web-2", 2)
	if got := a.Waiting("m1", 3); !slices.Equal(got, []string{"web-2"}) {
		t.Fatalf("after web-1 went quiet: %v", got)
	}
	a.Forget("m1")
	if len(a.Waiting("m1", 3)) != 0 || a.Live("m2") != 0 {
		t.Fatal("forget or ttl")
	}
}

func TestDeletes(t *testing.T) {
	d := NewDeletes()
	now := time.Unix(1000, 0)
	d.now = func() time.Time { return now }
	ctx := context.Background()
	add := func(m string, keys ...string) {
		if err := d.Add(ctx, m, keys...); err != nil {
			t.Fatal(err)
		}
	}
	add("m1", "b", "a")
	add("m2", "c")
	now = now.Add(time.Minute)
	add("m1", "d", "a") // a keeps its first time
	if !d.Has("m1", "a") || d.Has("m1", "c") || !slices.Equal(d.Since("m1", time.Time{}), []string{"a", "b", "d"}) {
		t.Fatalf("keys = %v", d.Since("m1", time.Time{}))
	}
	if got := d.Since("m1", now); !slices.Equal(got, []string{"d"}) {
		t.Fatalf("since now = %v", got)
	}
	if err := d.Forget(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	if d.Has("m1", "a") || !d.Has("m2", "c") {
		t.Fatal("forget")
	}
	var none *Deletes
	if none.Has("m", "k") || none.Since("m", time.Time{}) != nil {
		t.Fatal("nil Deletes must know nothing")
	}
}

func TestWaitingAfterStart(t *testing.T) {
	a := NewApps(time.Second)
	now := time.Unix(1000, 0)
	a.now = func() time.Time { return now }
	a.started = now
	// Right after a start no app has reported yet, but one may still use
	// an older route.
	if got := a.Waiting("m1", 2); !slices.Equal(got, []string{Unheard}) {
		t.Fatalf("waiting at start = %v", got)
	}
	a.Report("m1", "web-1", 2)
	now = now.Add(1500 * time.Millisecond)
	if got := a.Waiting("m1", 2); len(got) != 0 {
		t.Fatalf("waiting after one TTL = %v", got)
	}
}
