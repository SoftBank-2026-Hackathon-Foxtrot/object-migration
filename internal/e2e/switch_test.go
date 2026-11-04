// Package e2e runs the API, the worker and app servers using the client
// module together, with buckets in memory, to check that apps see no
// failed request while a migration switches and rolls back.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/client"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/api"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/engine"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/routing"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/store"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/worker"
)

// bucket is a bucket in memory with a little latency, so requests overlap
// route changes.
type bucket struct {
	mu      sync.Mutex
	objs    map[string][]byte
	removed []string // deleted keys, like the deletion events of a feed
}

func newBucket() *bucket { return &bucket{objs: map[string][]byte{}} }

// pause makes a request take a little while, and now and then long enough
// to still be running after a route change.
func pause() {
	if rand.IntN(100) == 0 {
		time.Sleep(30 * time.Millisecond)
		return
	}
	time.Sleep(time.Duration(rand.IntN(400)) * time.Microsecond)
}

func (b *bucket) Put(_ context.Context, k string, v []byte) error {
	pause()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objs[k] = bytes.Clone(v)
	return nil
}

func (b *bucket) Get(_ context.Context, k string) ([]byte, error) {
	pause()
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.objs[k]
	if !ok {
		return nil, client.ErrNotFound
	}
	return v, nil
}

func (b *bucket) Delete(_ context.Context, k string) error {
	pause()
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objs, k)
	b.removed = append(b.removed, k)
	return nil
}

// applyDeletes removes from to the keys deleted from from, as the engine
// does with the source's deletion events when it drains the feed.
func applyDeletes(from, to *bucket) {
	from.mu.Lock()
	var gone []string
	for _, k := range from.removed {
		if _, ok := from.objs[k]; !ok {
			gone = append(gone, k)
		}
	}
	from.mu.Unlock()
	to.mu.Lock()
	defer to.mu.Unlock()
	for _, k := range gone {
		delete(to.objs, k)
	}
}

func (b *bucket) has(k string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.objs[k]
	return ok
}

// copyNew copies objects missing from to, like a copy that deletes
// nothing. It holds both buckets, standing in for the real engine's
// per-key ordering that keeps a copy from undoing a concurrent delete.
func copyNew(from, to *bucket) {
	from.mu.Lock()
	defer from.mu.Unlock()
	to.mu.Lock()
	defer to.mu.Unlock()
	for k, v := range from.objs {
		if _, ok := to.objs[k]; !ok {
			to.objs[k] = v
		}
	}
}

// mirror makes to hold exactly from's objects.
func mirror(from, to *bucket) {
	from.mu.Lock()
	defer from.mu.Unlock()
	to.mu.Lock()
	defer to.mu.Unlock()
	to.objs = make(map[string][]byte, len(from.objs))
	for k, v := range from.objs {
		to.objs[k] = v
	}
}

// mapEngine moves objects between two in-memory buckets the way the real
// engine does at each step.
type mapEngine struct {
	*engine.Fake
	src, dst *bucket
	deletes  *routing.Deletes
	id       string
}

// honor removes from b the keys apps deleted, as the real engine does after
// every copy.
func (e *mapEngine) honor(b *bucket) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, k := range e.deletes.Since(e.id, time.Time{}) {
		delete(b.objs, k)
	}
}

func (e *mapEngine) InitialSync(ctx context.Context, m migration.Migration, report func(migration.Progress)) error {
	e.id = m.ID
	mirror(e.src, e.dst)
	e.honor(e.dst)
	return nil
}

func (e *mapEngine) Sync(context.Context, migration.Migration) (engine.SyncReport, error) {
	mirror(e.src, e.dst) // before the switch only we write the target
	e.honor(e.dst)
	return engine.SyncReport{Cutover: migration.Decide(migration.Check{Name: "mirrored", OK: true})}, nil
}

func (e *mapEngine) Resume(context.Context, migration.Migration) error {
	applyDeletes(e.src, e.dst)
	copyNew(e.src, e.dst)
	e.honor(e.dst)
	return nil
}

func (e *mapEngine) Verify(context.Context, migration.Migration) (engine.VerifyReport, error) {
	copyNew(e.dst, e.src)
	e.honor(e.src)
	ok := migration.Check{Name: "ok", OK: true}
	return engine.VerifyReport{Advance: migration.Decide(ok), Rollback: migration.Decide(ok)}, nil
}

func (e *mapEngine) Rollback(context.Context, migration.Migration) error {
	copyNew(e.dst, e.src)
	e.honor(e.src)
	return nil
}

var (
	srcEP = migration.Endpoint{Kind: migration.KindS3, Bucket: "src", Region: "ap-northeast-1", Ref: "default"}
	dstEP = migration.Endpoint{Kind: migration.KindGCS, Bucket: "dst", Ref: "default"}
)

type harness struct {
	t        *testing.T
	srv      *httptest.Server
	src, dst *bucket
	id       string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, src: newBucket(), dst: newBucket()}
	st := store.NewMemory()
	apps := routing.NewApps(time.Second)
	deletes := routing.NewDeletes()
	eng := &mapEngine{Fake: engine.NewFake(time.Millisecond), src: h.src, dst: h.dst, deletes: deletes}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := worker.New(st, eng, worker.Config{
		PollInterval: 5 * time.Millisecond, RetryBackoff: 5 * time.Millisecond, SyncInterval: 5 * time.Millisecond,
		Apps: apps, ConfirmTimeout: 5 * time.Second,
	}, log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	h.srv = httptest.NewServer(api.NewHandler(api.Deps{Store: st, Notifier: w, Engine: "map", Apps: apps, Deletes: deletes, Logger: log}))
	t.Cleanup(func() { h.srv.Close(); cancel(); <-done })

	body, _ := json.Marshal(map[string]any{"source": srcEP, "target": dstEP})
	var m migration.Migration
	h.call(http.MethodPost, "/v1/migrations", string(body), &m)
	h.id = m.ID
	return h
}

func (h *harness) call(method, path, body string, out any) {
	h.t.Helper()
	req, _ := http.NewRequest(method, h.srv.URL+path, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		h.t.Fatalf("%s %s: %s %s", method, path, resp.Status, b)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			h.t.Fatal(err)
		}
	}
}

func (h *harness) command(c string) {
	h.t.Helper()
	if c == "abort" {
		h.call(http.MethodDelete, "/v1/migrations/"+h.id, "", nil)
		return
	}
	h.call(http.MethodPost, "/v1/migrations/"+h.id+"/"+c, "", nil)
}

func (h *harness) wait(what string, ok func(migration.Migration) bool) migration.Migration {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var m migration.Migration
	for time.Now().Before(deadline) {
		m = migration.Migration{} // fields absent from the response must not linger
		h.call(http.MethodGet, "/v1/migrations/"+h.id, "", &m)
		if ok(m) {
			return m
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s: phase=%s state=%s pending=%+v lastError=%+v route=%+v", what, m.Phase, m.State, m.Pending, m.LastError, m.Routing)
	return m
}

func (h *harness) waitPhase(p migration.Phase) migration.Migration {
	h.t.Helper()
	return h.wait(string(p), func(m migration.Migration) bool { return m.Phase == p && m.Pending == nil })
}

// load is app servers writing, reading each other's objects and deleting,
// all through the client module.
type load struct {
	failures atomic.Int64
	requests atomic.Int64
	errs     sync.Map // first few failures, for the report

	mu      sync.Mutex
	live    []string
	deleted []string
	locks   sync.Map // key -> *sync.RWMutex: a read never races its key's delete
}

func (l *load) fail(err error) {
	if n := l.failures.Add(1); n <= 5 {
		l.errs.Store(n, err.Error())
	}
}

func (l *load) lock(k string) *sync.RWMutex {
	v, _ := l.locks.LoadOrStore(k, &sync.RWMutex{})
	return v.(*sync.RWMutex)
}

func (h *harness) startApps(ctx context.Context, n int) (*load, *sync.WaitGroup) {
	h.t.Helper()
	l := &load{}
	var wg sync.WaitGroup
	open := func(e client.Endpoint) (client.Bucket, error) {
		switch e.Bucket {
		case "src":
			return h.src, nil
		case "dst":
			return h.dst, nil
		}
		return nil, errors.New("unknown bucket")
	}
	for i := range n {
		s, err := client.New(ctx, client.Options{
			Server: h.srv.URL, Migration: h.id, App: fmt.Sprintf("app-%d", i),
			Open: open, Interval: 10 * time.Millisecond,
		})
		if err != nil {
			h.t.Fatal(err)
		}
		wg.Go(func() { s.Run(ctx) })
		wg.Go(func() { // writes and some deletes
			for j := 0; ctx.Err() == nil; j++ {
				key := fmt.Sprintf("app-%d/%06d", i, j)
				l.requests.Add(1)
				if err := s.Put(ctx, key, []byte(key)); err != nil {
					l.fail(fmt.Errorf("put %s: %w", key, err))
					continue
				}
				l.mu.Lock()
				l.live = append(l.live, key)
				var victim string
				if j%4 == 3 && len(l.live) > 1 {
					k := rand.IntN(len(l.live) - 1)
					victim = l.live[k]
					l.live = append(l.live[:k], l.live[k+1:]...)
				}
				l.mu.Unlock()
				if victim != "" {
					mu := l.lock(victim)
					mu.Lock()
					l.requests.Add(1)
					if err := s.Delete(ctx, victim); err != nil {
						l.fail(fmt.Errorf("delete %s: %w", victim, err))
					}
					l.mu.Lock()
					l.deleted = append(l.deleted, victim)
					l.mu.Unlock()
					mu.Unlock()
				}
				time.Sleep(time.Millisecond)
			}
		})
		wg.Go(func() { // reads of deleted objects, which must stay gone
			for ctx.Err() == nil {
				l.mu.Lock()
				var key string
				if len(l.deleted) > 0 {
					key = l.deleted[rand.IntN(len(l.deleted))]
				}
				l.mu.Unlock()
				if key != "" {
					l.requests.Add(1)
					if _, err := s.Get(ctx, key); !errors.Is(err, client.ErrNotFound) {
						l.fail(fmt.Errorf("get deleted %s: %v, want not found", key, err))
					}
				}
				time.Sleep(time.Millisecond)
			}
		})
		wg.Go(func() { // reads of objects any app wrote
			for ctx.Err() == nil {
				l.mu.Lock()
				var key string
				if len(l.live) > 0 {
					key = l.live[rand.IntN(len(l.live))]
				}
				l.mu.Unlock()
				if key == "" {
					time.Sleep(time.Millisecond)
					continue
				}
				mu := l.lock(key)
				mu.RLock()
				l.mu.Lock()
				gone := false
				for _, d := range l.deleted {
					gone = gone || d == key
				}
				l.mu.Unlock()
				if !gone {
					l.requests.Add(1)
					if body, err := s.Get(ctx, key); err != nil || string(body) != key {
						l.fail(fmt.Errorf("get %s: %q, %v", key, body, err))
					}
				}
				mu.RUnlock()
			}
		})
	}
	return l, &wg
}

func (l *load) report(t *testing.T) {
	t.Helper()
	if n := l.failures.Load(); n > 0 {
		var msgs []string
		l.errs.Range(func(_, v any) bool { msgs = append(msgs, v.(string)); return true })
		t.Fatalf("%d of %d requests failed, e.g. %v", n, l.requests.Load(), msgs)
	}
	t.Logf("%d requests, none failed; %d objects live, %d deleted", l.requests.Load(), len(l.live), len(l.deleted))
}

func TestSwitchWithoutFailedRequests(t *testing.T) {
	h := newHarness(t)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	l, wg := h.startApps(ctx, 3)

	h.waitPhase(migration.PhaseReady)
	time.Sleep(50 * time.Millisecond)
	h.command("freeze")
	h.waitPhase(migration.PhaseFrozen)
	h.command("switch")
	sw := h.waitPhase(migration.PhaseSwitched)
	if sw.Routing.Write != migration.SideTarget || sw.Routing.Generation != 3 {
		t.Fatalf("route after switch: %+v", sw.Routing)
	}
	time.Sleep(50 * time.Millisecond)
	h.command("resume")
	h.wait("verified", func(m migration.Migration) bool {
		return m.Phase == migration.PhaseVerifying && m.State == migration.StateSucceeded
	})
	time.Sleep(50 * time.Millisecond)
	h.command("finalize")
	done := h.waitPhase(migration.PhaseCompleted)
	if done.Routing.Write != migration.SideTarget || len(done.Routing.Read) != 1 {
		t.Fatalf("route after finalize: %+v", done.Routing)
	}
	time.Sleep(50 * time.Millisecond)
	stop()
	wg.Wait()

	l.report(t)
	for _, k := range l.live {
		if !h.dst.has(k) {
			t.Fatalf("object %s written with success is not in the target", k)
		}
	}
	for _, k := range l.deleted {
		if h.dst.has(k) {
			t.Fatalf("deleted object %s is in the target", k)
		}
	}
}

func TestRollbackWithoutFailedRequests(t *testing.T) {
	h := newHarness(t)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	l, wg := h.startApps(ctx, 3)

	h.waitPhase(migration.PhaseReady)
	h.command("freeze")
	h.waitPhase(migration.PhaseFrozen)
	h.command("switch")
	h.waitPhase(migration.PhaseSwitched)
	time.Sleep(50 * time.Millisecond) // apps write to the target meanwhile
	h.command("rollback")
	back := h.waitPhase(migration.PhaseRolledBack)
	if back.Routing.Write != migration.SideSource || len(back.Routing.Read) != 1 {
		t.Fatalf("route after rollback: %+v", back.Routing)
	}
	time.Sleep(50 * time.Millisecond)
	stop()
	wg.Wait()

	l.report(t)
	for _, k := range l.live {
		if !h.src.has(k) {
			t.Fatalf("object %s written with success is not in the source after rollback", k)
		}
	}
}

func TestSwitchWaitsForEveryApp(t *testing.T) {
	h := newHarness(t)
	h.waitPhase(migration.PhaseReady)

	// An app that asked once and then stopped refreshing.
	h.call(http.MethodGet, "/v1/migrations/"+h.id+"/routing?app=stuck&applied=1", "", nil)
	h.command("freeze")
	h.waitPhase(migration.PhaseFrozen)
	h.call(http.MethodPost, "/v1/migrations/"+h.id+"/switch", `{"deadlineMs":50}`, nil)
	m := h.wait("unknown", func(m migration.Migration) bool { return m.State == migration.StateUnknown })
	if m.Phase != migration.PhaseFrozen || m.LastError == nil || m.LastError.Code != "APPS_NOT_CONFIRMED" ||
		!strings.Contains(m.LastError.Message, "stuck") {
		t.Fatalf("while waiting: phase=%s lastError=%+v", m.Phase, m.LastError)
	}

	// It catches up; the switch goes on.
	for _, gen := range []int{2, 3} {
		h.call(http.MethodGet, fmt.Sprintf("/v1/migrations/%s/routing?app=stuck&applied=%d", h.id, gen), "", nil)
		time.Sleep(20 * time.Millisecond)
	}
	done := h.waitPhase(migration.PhaseSwitched)
	if done.State != migration.StateWaiting || done.LastError != nil {
		t.Fatalf("after catching up: state=%s lastError=%+v", done.State, done.LastError)
	}
}

func TestRollbackReplacesASwitchStuckOnAnApp(t *testing.T) {
	h := newHarness(t)
	h.waitPhase(migration.PhaseReady)
	h.call(http.MethodGet, "/v1/migrations/"+h.id+"/routing?app=stuck&applied=1", "", nil)
	h.command("freeze")
	h.waitPhase(migration.PhaseFrozen)
	h.call(http.MethodPost, "/v1/migrations/"+h.id+"/switch", `{"deadlineMs":50}`, nil)
	h.wait("unknown", func(m migration.Migration) bool { return m.State == migration.StateUnknown })

	// The app never catches up; rollback takes over once it is gone.
	h.command("rollback")
	done := h.waitPhase(migration.PhaseRolledBack)
	// 2 was the switch's first step. The rollback goes on from there: 3
	// reads only the source, 4 deletes only from it. The replaced switch
	// never moved writes to the target.
	if done.Routing.Write != migration.SideSource || len(done.Routing.Delete) != 1 || done.Routing.Generation != 4 {
		t.Fatalf("route after rollback: %+v", done.Routing)
	}
}
