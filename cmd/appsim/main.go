// Command appsim runs simulated app servers that write, read and delete
// objects through the client module while a migration runs, and reports
// every failed request. At the end it writes the keys it left and the keys
// it deleted, to compare with the buckets.
package main

import (
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

type stats struct {
	mu       sync.Mutex
	live     []string
	deleted  []string
	inflight map[string]bool // keys being deleted; not read meanwhile
	lat      map[string][]time.Duration

	requests, failures atomic.Int64
}

func (s *stats) took(op string, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lat[op] = append(s.lat[op], d)
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
	out := flag.String("out", ".", "directory for live.txt and deleted.txt")
	flag.Parse()
	if *id == "" {
		log.Fatal("-migration is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *dur)
	defer cancel()

	st := &stats{inflight: map[string]bool{}, lat: map[string][]time.Duration{}}
	run := time.Now().Format("150405")
	var wg sync.WaitGroup
	stores := make([]*client.Store, *apps)
	for i := range *apps {
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
		stores[i] = s
		wg.Go(func() { s.Run(ctx) })
		wg.Go(func() { writer(ctx, s, st, fmt.Sprintf("%s%s/app-%d/", *prefix, run, i), *writeEvery, *deleteEvery) })
		wg.Go(func() { reader(ctx, s, st, *readEvery) })
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
				for _, s := range stores {
					r := s.Route()
					gens = append(gens, fmt.Sprintf("%d:%s", r.Generation, r.Write.Bucket))
				}
				st.mu.Lock()
				log.Printf("requests=%d failures=%d live=%d deleted=%d routes=%s",
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
		d := st.lat[op]
		slices.Sort(d)
		if len(d) > 0 {
			fmt.Printf("  %-6s n=%d p50=%s p99=%s max=%s\n", op, len(d), d[len(d)/2].Round(time.Millisecond),
				d[len(d)*99/100].Round(time.Millisecond), d[len(d)-1].Round(time.Millisecond))
		}
	}
	ok := st.failures.Load() == 0
	if !verify(context.Background(), stores[0].Route(), *prefix+run+"/", st, *ref, *seaweedRef) {
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

func writer(ctx context.Context, s *client.Store, st *stats, prefix string, every time.Duration, deleteEvery int) {
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		key := fmt.Sprintf("%s%06d.txt", prefix, n)
		start := time.Now()
		st.requests.Add(1)
		if err := s.Put(ctx, key, []byte(key+"\n")); err != nil {
			if ctx.Err() == nil {
				st.failures.Add(1)
				log.Printf("FAIL put %s: %v", key, err)
			}
			continue
		}
		st.took("put", time.Since(start))
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
		start = time.Now()
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
			log.Printf("FAIL delete %s: %v", victim, err)
		} else if err == nil {
			st.took("delete", time.Since(start))
		}
	}
}

// reader reads objects any app wrote, and now and then one that was
// deleted, which must not be found.
func reader(ctx context.Context, s *client.Store, st *stats, every time.Duration) {
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
		start := time.Now()
		st.requests.Add(1)
		body, err := s.Get(ctx, key)
		if ctx.Err() != nil {
			return
		}
		switch {
		case wantGone && err == client.ErrNotFound:
		case wantGone:
			st.failures.Add(1)
			log.Printf("FAIL get deleted %s: %v, want not found", key, err)
		case err != nil || string(body) != key+"\n":
			// A key deleted after we picked it is not a failure.
			st.mu.Lock()
			gone := slices.Contains(st.deleted, key) || st.inflight[key]
			st.mu.Unlock()
			if !gone {
				st.failures.Add(1)
				log.Printf("FAIL get %s: %q, %v", key, body, err)
			}
		default:
			st.took("get", time.Since(start))
		}
	}
}
