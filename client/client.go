// Package client is the storage module an app server uses while its
// objects are migrated. It asks the migration server where to write and in
// which order to read, follows changes within about a second, and reports
// which route it uses so the server knows when every app has moved.
//
// Reads fall back to the next bucket when an object is not found, which
// covers objects not yet copied. Deletes go to every bucket in the route,
// so a deleted object cannot reappear from the other one.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"
)

// ErrNotFound means no bucket in the route holds the object.
var ErrNotFound = errors.New("object not found")

// ErrStaleRoute means the route could not be refreshed for longer than
// Options.Lease while a migration runs. Other apps may have moved on, so
// using the old route could lose writes; requests fail until it refreshes.
var ErrStaleRoute = errors.New("route is stale: migration server not reached")

// Endpoint is a bucket as the migration server describes it.
type Endpoint struct {
	Kind   string `json:"kind"`
	Bucket string `json:"bucket"`
	Region string `json:"region,omitempty"`
	URL    string `json:"url,omitempty"`
}

// Route is where to write and in which order to read.
type Route struct {
	MigrationID string     `json:"migrationId"`
	Phase       string     `json:"phase"`
	Generation  int64      `json:"generation"`
	Write       Endpoint   `json:"write"`
	Read        []Endpoint `json:"read"`
	// Delete lists every bucket that may hold a copy of an object.
	Delete []Endpoint `json:"delete"`
	// AppTTLMS is how long the server waits for an app that stopped asking.
	AppTTLMS int64 `json:"appTtlMs,omitempty"`
}

// Bucket is one bucket reached with the app's own credentials.
type Bucket interface {
	Put(ctx context.Context, key string, body []byte) error
	// Get returns ErrNotFound when the bucket does not hold key.
	Get(ctx context.Context, key string) ([]byte, error)
	// Delete returns nil when the bucket does not hold key.
	Delete(ctx context.Context, key string) error
}

type Options struct {
	// Server is the migration server's base URL.
	Server string
	// Migration is the migration's ID.
	Migration string
	// App names this app server, unique among the app's servers.
	App string
	// Open connects to a bucket; it is called once per bucket.
	Open func(Endpoint) (Bucket, error)
	// Interval is how often the route is refreshed. Zero means 1 second.
	Interval time.Duration
	// Lease is how long the route stays usable without a refresh while a
	// migration runs, counted from when the last successful refresh was
	// sent. It is capped at half the server's APP_TTL, after which the
	// server stops waiting for this app. Zero means 5 seconds.
	Lease time.Duration
	// HTTP is the client for the migration server. Nil means a client with
	// a 5 second timeout.
	HTTP *http.Client
}

// Store reads and writes objects where the current route says.
type Store struct {
	opt Options

	mu       sync.Mutex
	route    Route
	buckets  map[Endpoint]Bucket
	inFlight map[int64]int // requests running, by the generation they use
	// lease is cancelled when the route goes stale; requests running on it
	// are cancelled with it. Each refresh pushes the expiry back.
	lease       context.Context
	expire      context.CancelFunc
	leaseTimer  *time.Timer
	leaseLength time.Duration
}

// New fetches the route and returns a Store using it. Call Run to keep the
// route current.
func New(ctx context.Context, opt Options) (*Store, error) {
	if opt.Interval == 0 {
		opt.Interval = time.Second
	}
	if opt.Lease == 0 {
		opt.Lease = 5 * time.Second
	}
	if opt.HTTP == nil {
		opt.HTTP = &http.Client{Timeout: 5 * time.Second}
	}
	s := &Store{opt: opt, buckets: make(map[Endpoint]Bucket), inFlight: make(map[int64]int)}
	s.lease, s.expire = context.WithCancel(context.Background())
	s.expire() // no route yet
	if err := s.Refresh(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// Run refreshes the route every Interval until ctx is done. While the
// migration server cannot be reached, the last route stays in use.
func (s *Store) Run(ctx context.Context) {
	t := time.NewTicker(s.opt.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = s.Refresh(ctx)
		}
	}
}

// Refresh asks for the route, reporting the generation in use. When the
// route changed, it asks once more so the server learns at once that the
// new one is in use.
func (s *Store) Refresh(ctx context.Context) error {
	for range 2 {
		sent := time.Now()
		r, err := s.fetch(ctx, s.applied())
		if err != nil {
			return err
		}
		// The server heard from us, so it waits for the generation we
		// reported; the route in use stays valid even if the new one cannot
		// be used yet.
		s.renew(sent, r.AppTTLMS)
		if err := s.open(r); err != nil {
			return err
		}
		s.mu.Lock()
		changed := r.Generation != s.route.Generation
		s.route = r
		s.mu.Unlock()
		if !changed {
			return nil
		}
	}
	return nil
}

// renew extends the lease to Lease after sent, or half the server's TTL if
// that is shorter.
func (s *Store) renew(sent time.Time, ttlMS int64) {
	length := s.opt.Lease
	if half := time.Duration(ttlMS) * time.Millisecond / 2; half > 0 && half < length {
		length = half
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leaseTimer != nil {
		s.leaseTimer.Stop()
	}
	if s.lease.Err() != nil {
		s.lease, s.expire = context.WithCancel(context.Background())
	}
	s.leaseLength = length
	s.leaseTimer = time.AfterFunc(time.Until(sent.Add(length)), s.expire)
}

// applied is the generation to report: the oldest one a request still
// uses. A request that started on an older route can still write where
// that route said, so the server must not take the next step before it
// ends.
func (s *Store) applied() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	gen := s.route.Generation
	for g, n := range s.inFlight {
		if n > 0 && g < gen {
			gen = g
		}
	}
	return gen
}

// begin returns the route for a request and a context for it, and holds
// the route's generation until the returned function is called. While a
// migration runs, the request is cancelled if the lease runs out.
func (s *Store) begin(ctx context.Context) (Route, context.Context, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.route
	cancel := func() {}
	if !terminal(r.Phase) {
		if s.lease.Err() != nil {
			return Route{}, nil, nil, ErrStaleRoute
		}
		var c context.CancelFunc
		ctx, c = context.WithCancel(ctx)
		stop := context.AfterFunc(s.lease, c)
		cancel = func() { stop(); c() }
	}
	s.inFlight[r.Generation]++
	return r, ctx, func() {
		cancel()
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.inFlight[r.Generation]--; s.inFlight[r.Generation] == 0 {
			delete(s.inFlight, r.Generation)
		}
	}, nil
}

// leaseErr turns the cancellation of a request whose lease ran out into
// ErrStaleRoute.
func (s *Store) leaseErr(ctx context.Context, err error) error {
	s.mu.Lock()
	expired := s.lease.Err() != nil
	s.mu.Unlock()
	if err != nil && expired && ctx.Err() != nil {
		return ErrStaleRoute
	}
	return err
}

// terminal reports whether the migration is over, so the route no longer
// changes and needs no lease.
func terminal(phase string) bool {
	switch phase {
	case "COMPLETED", "ROLLED_BACK", "ABORTED", "FAILED":
		return true
	}
	return false
}

func (s *Store) fetch(ctx context.Context, applied int64) (Route, error) {
	u := fmt.Sprintf("%s/v1/migrations/%s/routing?app=%s&applied=%d",
		s.opt.Server, url.PathEscape(s.opt.Migration), url.QueryEscape(s.opt.App), applied)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return Route{}, err
	}
	resp, err := s.opt.HTTP.Do(req)
	if err != nil {
		return Route{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Route{}, fmt.Errorf("routing: %s", resp.Status)
	}
	var r Route
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return Route{}, fmt.Errorf("routing: %w", err)
	}
	if len(r.Read) == 0 || r.Write.Bucket == "" {
		return Route{}, errors.New("routing: incomplete route")
	}
	return r, nil
}

// open connects to the route's buckets that are not connected yet, so a
// route is only used once all of its buckets can be.
func (s *Store) open(r Route) error {
	for _, e := range slices.Concat([]Endpoint{r.Write}, r.Read, r.Delete) {
		s.mu.Lock()
		_, ok := s.buckets[e]
		s.mu.Unlock()
		if ok {
			continue
		}
		b, err := s.opt.Open(e)
		if err != nil {
			return fmt.Errorf("open %s bucket %s: %w", e.Kind, e.Bucket, err)
		}
		s.mu.Lock()
		s.buckets[e] = b
		s.mu.Unlock()
	}
	return nil
}

func (s *Store) recordDelete(ctx context.Context, key string) error {
	body, err := json.Marshal(map[string][]string{"keys": {key}})
	if err != nil {
		return err
	}
	u := fmt.Sprintf("%s/v1/migrations/%s/deletes", s.opt.Server, url.PathEscape(s.opt.Migration))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.opt.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("record delete: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("record delete: %s", resp.Status)
	}
	return nil
}

// Route returns the route in use.
func (s *Store) Route() Route {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.route
}

func (s *Store) bucket(e Endpoint) Bucket {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buckets[e]
}

// Put writes an object to the write bucket.
func (s *Store) Put(ctx context.Context, key string, body []byte) error {
	r, rctx, done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	return s.leaseErr(rctx, s.bucket(r.Write).Put(rctx, key, body))
}

// Get reads an object from the first read bucket that holds it.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	r, rctx, done, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	for _, e := range r.Read {
		body, err := s.bucket(e).Get(rctx, key)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		return body, s.leaseErr(rctx, err)
	}
	return nil, ErrNotFound
}

// Delete removes an object from every bucket that may hold a copy of it,
// in the route's order. While a migration copies between buckets, the key
// is first recorded with the migration server, so a copy finishing after
// this delete is removed too; if that fails, nothing is deleted.
func (s *Store) Delete(ctx context.Context, key string) error {
	r, rctx, done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	if len(r.Delete) > 1 {
		if err := s.recordDelete(rctx, key); err != nil {
			return s.leaseErr(rctx, err)
		}
	}
	for _, e := range r.Delete {
		if err := s.bucket(e).Delete(rctx, key); err != nil {
			return s.leaseErr(rctx, err)
		}
	}
	return nil
}
