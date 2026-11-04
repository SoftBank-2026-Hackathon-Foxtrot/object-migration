package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// memBucket is a bucket in memory.
type memBucket struct {
	mu   sync.Mutex
	objs map[string][]byte
}

func (b *memBucket) Put(_ context.Context, k string, v []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objs[k] = v
	return nil
}

func (b *memBucket) Get(_ context.Context, k string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.objs[k]
	if !ok {
		return nil, ErrNotFound
	}
	return v, nil
}

func (b *memBucket) Delete(_ context.Context, k string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objs, k)
	return nil
}

var (
	src = Endpoint{Kind: "s3", Bucket: "src", Region: "ap-northeast-1"}
	dst = Endpoint{Kind: "gcs", Bucket: "dst"}
)

// routeServer serves a route the test can change, and records the
// generations apps report.
type routeServer struct {
	mu      sync.Mutex
	route   Route
	applied map[string]int64
	deleted []string
}

func (rs *routeServer) set(gen int64, write Endpoint, read ...Endpoint) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.route = Route{MigrationID: "om-1", Generation: gen, Write: write, Read: read, Delete: []Endpoint{dst, src}}
}

func (rs *routeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if r.Method == http.MethodPost && r.URL.Path == "/v1/migrations/om-1/deletes" {
		var body struct{ Keys []string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		rs.deleted = append(rs.deleted, body.Keys...)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.URL.Path != "/v1/migrations/om-1/routing" {
		http.NotFound(w, r)
		return
	}
	n, _ := strconv.ParseInt(r.URL.Query().Get("applied"), 10, 64)
	rs.applied[r.URL.Query().Get("app")] = n
	_ = json.NewEncoder(w).Encode(rs.route)
}

func setup(t *testing.T) (*Store, *routeServer, map[Endpoint]*memBucket) {
	t.Helper()
	rs := &routeServer{applied: map[string]int64{}}
	rs.set(1, src, src)
	srv := httptest.NewServer(rs)
	t.Cleanup(srv.Close)
	buckets := map[Endpoint]*memBucket{src: {objs: map[string][]byte{}}, dst: {objs: map[string][]byte{}}}
	s, err := New(context.Background(), Options{
		Server: srv.URL, Migration: "om-1", App: "web-1",
		Open: func(e Endpoint) (Bucket, error) {
			if b, ok := buckets[e]; ok {
				return b, nil
			}
			return nil, errors.New("unknown bucket")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return s, rs, buckets
}

func TestStoreFollowsTheRoute(t *testing.T) {
	ctx := context.Background()
	s, rs, b := setup(t)
	if err := s.Put(ctx, "a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if _, ok := b[src].objs["a"]; !ok {
		t.Fatal("write did not go to the source")
	}
	if rs.applied["web-1"] != 1 {
		t.Fatalf("server learned %d, want 1 right after the first route", rs.applied["web-1"])
	}

	// Switch: writes move to the target; reads still find "a" in the source.
	rs.set(3, dst, dst, src)
	if err := s.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if rs.applied["web-1"] != 3 {
		t.Fatalf("refresh reported %d, want the new generation 3 right away", rs.applied["web-1"])
	}
	if err := s.Put(ctx, "b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if _, ok := b[dst].objs["b"]; !ok {
		t.Fatal("write did not go to the target")
	}
	if got, err := s.Get(ctx, "a"); err != nil || string(got) != "1" {
		t.Fatalf("fallback read: %q, %v", got, err)
	}
	if _, err := s.Get(ctx, "none"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing object: %v", err)
	}

	// A delete removes the object from both, so it cannot come back.
	b[dst].objs["a"] = []byte("1") // copied meanwhile
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, ok := b[dst].objs["a"]; ok {
		t.Fatal("delete left the target's copy")
	}
	if len(rs.deleted) != 1 || rs.deleted[0] != "a" {
		t.Fatalf("recorded deletes = %v, want [a] before deleting", rs.deleted)
	}
	if _, err := s.Get(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted object still readable: %v", err)
	}
}

func TestStoreKeepsTheLastRouteWhenTheServerIsDown(t *testing.T) {
	s, rs, _ := setup(t)
	rs.mu.Lock()
	rs.route = Route{} // the server answers, but with nothing usable
	rs.mu.Unlock()
	if err := s.Refresh(context.Background()); err == nil {
		t.Fatal("refresh accepted an incomplete route")
	}
	if s.Route().Generation != 1 {
		t.Fatalf("route = %+v, want the last good one", s.Route())
	}
}

func TestStoreReportsTheOldestGenerationInUse(t *testing.T) {
	ctx := context.Background()
	s, rs, _ := setup(t)
	_, _, done, err := s.begin(ctx) // a request on generation 1 still running
	if err != nil {
		t.Fatal(err)
	}
	rs.set(2, src, src, dst)
	if err := s.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if rs.applied["web-1"] != 1 {
		t.Fatalf("reported %d while a generation 1 request runs, want 1", rs.applied["web-1"])
	}
	done()
	if err := s.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if rs.applied["web-1"] != 2 {
		t.Fatalf("reported %d after it ended, want 2", rs.applied["web-1"])
	}
}

func TestStoreFailsOnAStaleRoute(t *testing.T) {
	ctx := context.Background()
	s, rs, _ := setup(t)
	s.opt.Lease = 20 * time.Millisecond
	if err := s.Refresh(ctx); err != nil { // takes the shorter lease
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if err := s.Put(ctx, "a", []byte("1")); !errors.Is(err, ErrStaleRoute) {
		t.Fatalf("put on a stale route: %v", err)
	}
	if err := s.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "a", []byte("1")); err != nil {
		t.Fatalf("put after refresh: %v", err)
	}

	// Once the migration is over the route no longer changes.
	rs.mu.Lock()
	rs.route.Phase = "COMPLETED"
	rs.mu.Unlock()
	if err := s.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if err := s.Put(ctx, "b", []byte("2")); err != nil {
		t.Fatalf("put after the migration ended: %v", err)
	}
}

// slowBucket blocks a Put until its context ends.
type slowBucket struct{ *memBucket }

func (slowBucket) Put(ctx context.Context, _ string, _ []byte) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestLeaseCancelsRunningRequests(t *testing.T) {
	ctx := context.Background()
	s, _, b := setup(t)
	s.buckets[src] = slowBucket{b[src]}
	s.opt.Lease = 20 * time.Millisecond
	if err := s.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := s.Put(ctx, "a", []byte("1")); !errors.Is(err, ErrStaleRoute) {
		t.Fatalf("put: %v, want stale route", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("request outlived the lease")
	}
}

func TestLeaseIsHalfTheServerTTL(t *testing.T) {
	s, rs, _ := setup(t)
	rs.mu.Lock()
	rs.route.AppTTLMS = 2000
	rs.mu.Unlock()
	if err := s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.leaseLength != time.Second {
		t.Fatalf("lease = %v, want half the server's 2s TTL", s.leaseLength)
	}
}
