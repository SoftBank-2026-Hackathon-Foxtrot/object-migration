// Command appsim runs simulated app servers that write, read and delete
// objects through the client module while a migration runs, and reports
// every failed request and how long requests took in each step of the
// migration. At the end it writes the keys it left and the keys it deleted,
// to compare with the buckets.
//
// With -direct it skips the migration server and the client module and
// works on one bucket, to measure the same requests without a migration.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/client"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/appsim"
)

// app is what a simulated app server sends requests through: the client
// module, or one bucket with -direct.
type app interface {
	Put(ctx context.Context, key string, body []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	// step names the migration step the app's route is in, e.g.
	// "SWITCHED g3 gcs".
	step() string
}

type migrating struct{ *client.Store }

func (m migrating) step() string {
	r := m.Route()
	return fmt.Sprintf("%s g%d %s", r.Phase, r.Generation, r.Write.Kind)
}

type direct struct {
	*appsim.Bucket
	kind string
}

func (d direct) step() string { return "DIRECT " + d.kind }

type stats struct {
	mu       sync.Mutex
	live     []string
	deleted  []string
	inflight map[string]bool // keys being deleted; not read meanwhile
	lat      map[string][]time.Duration
	// byStep holds successful request times per step and op, and steps the
	// steps in the order they were first seen.
	byStep map[string]map[string][]time.Duration
	steps  []string
	// lastPut is when each app's last successful write ended; maxGap is the
	// longest time an app went without one.
	lastPut map[int]time.Time
	maxGap  time.Duration
	trace   *bufio.Writer

	requests, failures atomic.Int64
}

// done records a request that started at start, taking from the step the
// app was in when it was sent.
func (s *stats) done(id int, step, op string, start time.Time, err error) {
	d := time.Since(start)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.trace != nil {
		res := "ok"
		if err != nil {
			res = "fail"
		}
		fmt.Fprintf(s.trace, "%d,%d,%s,%.1f,%s,%s\n", start.UnixMilli(), id, op, float64(d.Microseconds())/1000, res, step)
	}
	if err != nil {
		return
	}
	s.lat[op] = append(s.lat[op], d)
	if s.byStep[step] == nil {
		s.byStep[step] = map[string][]time.Duration{}
		s.steps = append(s.steps, step)
	}
	s.byStep[step][op] = append(s.byStep[step][op], d)
	if op == "put" {
		now := time.Now()
		if last, ok := s.lastPut[id]; ok {
			s.maxGap = max(s.maxGap, now.Sub(last))
		}
		s.lastPut[id] = now
	}
}

func percentiles(d []time.Duration) string {
	slices.Sort(d)
	return fmt.Sprintf("n=%d p50=%s p95=%s p99=%s max=%s", len(d), d[len(d)/2].Round(time.Millisecond),
		d[len(d)*95/100].Round(time.Millisecond), d[len(d)*99/100].Round(time.Millisecond), d[len(d)-1].Round(time.Millisecond))
}

func main() {
	server := flag.String("server", "http://127.0.0.1:8080", "migration server")
	id := flag.String("migration", "", "migration ID")
	apps := flag.Int("apps", 3, "app servers")
	dur := flag.Duration("for", 3*time.Minute, "how long; Ctrl-C stops early")
	writeEvery := flag.Duration("write-every", 300*time.Millisecond, "time between writes, per app")
	readEvery := flag.Duration("read-every", 100*time.Millisecond, "time between reads, per app")
	deleteEvery := flag.Int("delete-every", 4, "delete an earlier object every N writes")
	ref := flag.String("ref", "default", "credentials ref the apps use for s3 and gcs buckets")
	seaweedRef := flag.String("seaweedfs-ref", "env:SEAWEEDAPP", "credentials ref the apps use for seaweedfs buckets")
	prefix := flag.String("prefix", "appsim/", "key prefix")
	out := flag.String("out", ".", "directory for live.txt, deleted.txt and requests.csv")
	directKind := flag.String("direct", "", "skip the migration: work on one bucket of this kind (s3, gcs, seaweedfs)")
	directBucket := flag.String("bucket", "", "bucket for -direct")
	directRegion := flag.String("region", "", "region for -direct")
	directURL := flag.String("url", "", "endpoint URL for -direct")
	flag.Parse()
	if *id == "" && *directKind == "" {
		log.Fatal("-migration or -direct is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *dur)
	defer cancel()

	traceFile, err := os.Create(filepath.Join(*out, "requests.csv"))
	if err != nil {
		log.Fatal(err)
	}
	defer traceFile.Close()
	st := &stats{
		inflight: map[string]bool{}, lat: map[string][]time.Duration{},
		byStep: map[string]map[string][]time.Duration{}, lastPut: map[int]time.Time{},
		trace: bufio.NewWriter(traceFile),
	}
	fmt.Fprintln(st.trace, "start_ms,app,op,ms,result,step")
	run := time.Now().Format("150405")
	var wg sync.WaitGroup
	sims := make([]app, *apps)
	var stores []*client.Store
	var target client.Endpoint // the bucket checked at the end with -direct
	if *directKind != "" {
		target = client.Endpoint{Kind: *directKind, Bucket: *directBucket, Region: *directRegion, URL: *directURL}
		r := *ref
		if *directKind == "seaweedfs" {
			r = *seaweedRef
		}
		b, err := appsim.Open(ctx, target, r)
		if err != nil {
			log.Fatal(err)
		}
		for i := range sims {
			sims[i] = direct{b, *directKind}
		}
	}
	for i := range *apps {
		if *directKind == "" {
			s, err := client.New(ctx, client.Options{
				Server: *server, Migration: *id, App: fmt.Sprintf("appsim-%s-%d", run, i),
				Open: func(e client.Endpoint) (client.Bucket, error) {
					if e.Kind == "seaweedfs" {
						return appsim.Open(ctx, e, *seaweedRef)
					}
					return appsim.Open(ctx, e, *ref)
				},
			})
			if err != nil {
				log.Fatal(err)
			}
			stores = append(stores, s)
			sims[i] = migrating{s}
			wg.Go(func() { s.Run(ctx) })
		}
		a := sims[i]
		wg.Go(func() { writer(ctx, i, a, st, fmt.Sprintf("%s%s/app-%d/", *prefix, run, i), *writeEvery, *deleteEvery) })
		wg.Go(func() { reader(ctx, i, a, st, *readEvery) })
	}
	wg.Go(func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				var gens []string
				for _, a := range sims {
					gens = append(gens, a.step())
				}
				st.mu.Lock()
				log.Printf("requests=%d failures=%d live=%d deleted=%d steps=%s",
					st.requests.Load(), st.failures.Load(), len(st.live), len(st.deleted), strings.Join(gens, ","))
				st.mu.Unlock()
			}
		}
	})
	wg.Wait()

	st.mu.Lock()
	defer st.mu.Unlock()
	write := func(name string, keys []string) {
		if err := os.WriteFile(filepath.Join(*out, name), []byte(strings.Join(keys, "\n")+"\n"), 0o644); err != nil {
			log.Fatal(err)
		}
	}
	write("live.txt", st.live)
	write("deleted.txt", st.deleted)
	fmt.Printf("run=%s requests=%d failures=%d live=%d deleted=%d\n", run, st.requests.Load(), st.failures.Load(), len(st.live), len(st.deleted))
	for _, op := range []string{"put", "get", "delete"} {
		if d := st.lat[op]; len(d) > 0 {
			fmt.Printf("  %-6s %s\n", op, percentiles(d))
		}
	}
	fmt.Printf("longest time an app went without a successful write: %s\n", st.maxGap.Round(time.Millisecond))
	fmt.Println("by step (the route the app held when it sent the request):")
	for _, step := range st.steps {
		fmt.Printf("  %s\n", step)
		for _, op := range []string{"put", "get", "delete"} {
			if d := st.byStep[step][op]; len(d) > 0 {
				fmt.Printf("    %-6s %s\n", op, percentiles(d))
			}
		}
	}
	if err := st.trace.Flush(); err != nil {
		log.Fatal(err)
	}
	ok := st.failures.Load() == 0
	r := client.Route{Phase: "COMPLETED", Write: target}
	if len(stores) > 0 {
		r = stores[0].Route()
	}
	if !verify(context.Background(), r, *prefix+run+"/", st, *ref, *seaweedRef) {
		ok = false
	}
	if !ok {
		os.Exit(1)
	}
}

// verify checks the bucket the apps write to at the end: every object
// written with success is there, and no deleted one. It runs only once the
// migration is over, when that bucket is the only one.
func verify(ctx context.Context, r client.Route, prefix string, st *stats, ref, seaweedRef string) bool {
	switch r.Phase {
	case "COMPLETED", "ROLLED_BACK", "ABORTED":
	default:
		fmt.Printf("verify: skipped, migration is %s\n", r.Phase)
		return true
	}
	if r.Write.Kind == "seaweedfs" {
		ref = seaweedRef
	}
	b, err := appsim.Open(ctx, r.Write, ref)
	if err != nil {
		fmt.Printf("verify: %v\n", err)
		return false
	}
	keys, err := b.Keys(ctx, prefix)
	if err != nil {
		fmt.Printf("verify: list %s: %v\n", r.Write.Bucket, err)
		return false
	}
	var missing, left int
	for _, k := range st.live {
		if !keys[k] {
			missing++
		}
	}
	for _, k := range st.deleted {
		if keys[k] {
			left++
		}
	}
	fmt.Printf("verify: %s %s: %d written objects missing, %d deleted objects left\n", r.Write.Kind, r.Write.Bucket, missing, left)
	return missing == 0 && left == 0
}

func writer(ctx context.Context, id int, s app, st *stats, prefix string, every time.Duration, deleteEvery int) {
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		key := fmt.Sprintf("%s%06d.txt", prefix, n)
		start, step := time.Now(), s.step()
		st.requests.Add(1)
		if err := s.Put(ctx, key, []byte(key+"\n")); err != nil {
			if ctx.Err() == nil {
				st.failures.Add(1)
				st.done(id, step, "put", start, err)
				log.Printf("FAIL put %s: %v", key, err)
			}
			continue
		}
		st.done(id, step, "put", start, nil)
		st.mu.Lock()
		st.live = append(st.live, key)
		var victim string
		if deleteEvery > 0 && n%deleteEvery == deleteEvery-1 && len(st.live) > 2 {
			i := rand.IntN(len(st.live) - 1)
			victim = st.live[i]
			st.live = slices.Delete(st.live, i, i+1)
			st.inflight[victim] = true
		}
		st.mu.Unlock()
		if victim == "" {
			continue
		}
		start, step = time.Now(), s.step()
		st.requests.Add(1)
		err := s.Delete(ctx, victim)
		st.mu.Lock()
		delete(st.inflight, victim)
		if err == nil {
			st.deleted = append(st.deleted, victim)
		}
		st.mu.Unlock()
		if err != nil && ctx.Err() == nil {
			st.failures.Add(1)
			st.done(id, step, "delete", start, err)
			log.Printf("FAIL delete %s: %v", victim, err)
		} else if err == nil {
			st.done(id, step, "delete", start, nil)
		}
	}
}

// reader reads objects any app wrote, and now and then one that was
// deleted, which must not be found.
func reader(ctx context.Context, id int, s app, st *stats, every time.Duration) {
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		st.mu.Lock()
		var key string
		wantGone := n%5 == 4 && len(st.deleted) > 0
		switch {
		case wantGone:
			key = st.deleted[rand.IntN(len(st.deleted))]
		case len(st.live) > 0:
			key = st.live[rand.IntN(len(st.live))]
		}
		if st.inflight[key] {
			key = ""
		}
		st.mu.Unlock()
		if key == "" {
			continue
		}
		start, step := time.Now(), s.step()
		st.requests.Add(1)
		body, err := s.Get(ctx, key)
		if ctx.Err() != nil {
			return
		}
		switch {
		case wantGone && err == client.ErrNotFound:
			st.done(id, step, "get", start, nil)
		case wantGone:
			st.failures.Add(1)
			st.done(id, step, "get", start, fmt.Errorf("found a deleted key"))
			log.Printf("FAIL get deleted %s: %v, want not found", key, err)
		case err != nil || string(body) != key+"\n":
			// A key deleted after we picked it is not a failure.
			st.mu.Lock()
			gone := slices.Contains(st.deleted, key) || st.inflight[key]
			st.mu.Unlock()
			if !gone {
				st.failures.Add(1)
				st.done(id, step, "get", start, fmt.Errorf("wrong body"))
				log.Printf("FAIL get %s: %q, %v", key, body, err)
			}
		default:
			st.done(id, step, "get", start, nil)
		}
	}
}
