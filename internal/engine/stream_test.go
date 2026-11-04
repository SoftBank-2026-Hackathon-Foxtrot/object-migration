package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/copier"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/eventsource"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/routing"
)

// memFeed is a change feed in memory. Tests push changes with emit; canary
// objects written to the source produce events by themselves unless
// dropCanary is set, like S3 notifications would.
type memFeed struct {
	src fs.Fs

	mu           sync.Mutex
	queue        []eventsource.Message
	emitted      map[string]bool
	acked        int
	nacked       int
	subscribes   int
	unsubscribes int
	dropCanary   bool
	backlog      int64
}

func (f *memFeed) emit(key string, removed bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queue = append(f.queue, eventsource.Message{Changes: []eventsource.Change{{Key: key, Removed: removed, At: time.Now()}}})
}

func (f *memFeed) Subscribe(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribes++
	return nil
}

func (f *memFeed) Unsubscribe(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unsubscribes++
	return nil
}

func (f *memFeed) Receive(ctx context.Context, wait time.Duration) ([]eventsource.Message, error) {
	if !f.dropCanary {
		entries, _ := f.src.List(ctx, strings.TrimSuffix(copier.ProbePrefix, "/"))
		for _, e := range entries {
			f.mu.Lock()
			seen := f.emitted[e.Remote()]
			if f.emitted == nil {
				f.emitted = map[string]bool{}
			}
			f.emitted[e.Remote()] = true
			f.mu.Unlock()
			if !seen {
				f.emit(e.Remote(), false)
			}
		}
	}
	f.mu.Lock()
	msgs := f.queue
	f.queue = nil
	f.mu.Unlock()
	if len(msgs) == 0 {
		time.Sleep(min(wait, 2*time.Millisecond))
	}
	return msgs, nil
}

func (f *memFeed) Ack(_ context.Context, msgs []eventsource.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked += len(msgs)
	return nil
}

func (f *memFeed) Nack(_ context.Context, msgs []eventsource.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nacked += len(msgs)
	f.queue = append(f.queue, msgs...)
	return nil
}

func (f *memFeed) Backlog(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.backlog + int64(len(f.queue)), nil
}

// streamEngine returns an engine whose source has a memFeed, with short
// timings for tests.
func streamEngine(t *testing.T) (*Rclone, *memFeed, migration.Migration, fs.Fs, fs.Fs) {
	t.Helper()
	r, m, src, dst := memEngine(t)
	putObject(t, src, "seed") // the in-memory buckets exist once they hold something
	putObject(t, dst, "seed")
	feed := &memFeed{src: src}
	r.feed = func(context.Context, migration.Migration) (Feed, error) { return feed, nil }
	r.Stream = StreamOptions{
		Window:           20 * time.Millisecond,
		FullCopyInterval: time.Hour,
		DiffInterval:     0,
		InFlightGrace:    0,
		CanaryInterval:   0,
		CanaryTimeout:    time.Second,
		MaxCanaryDelay:   time.Second,
		MaxLag:           time.Second,
		MaxBacklog:       10,
		DrainTimeout:     time.Second,
	}
	return r, feed, m, src, dst
}

func checkByName(d migration.Decision, name string) migration.Check {
	for _, c := range d.Checks {
		if c.Name == name {
			return c
		}
	}
	return migration.Check{Name: name, Message: "missing"}
}

func exists(t *testing.T, f fs.Fs, key string) bool {
	t.Helper()
	_, err := f.NewObject(context.Background(), key)
	if err != nil && !errors.Is(err, fs.ErrorObjectNotFound) {
		t.Fatal(err)
	}
	return err == nil
}

func TestStreamFollowsChanges(t *testing.T) {
	ctx := context.Background()
	r, feed, m, src, dst := streamEngine(t)
	if err := r.Prepare(ctx, m); err != nil {
		t.Fatal(err)
	}
	putObject(t, src, "old")
	if err := r.InitialSync(ctx, m, func(migration.Progress) {}); err != nil {
		t.Fatal(err)
	}

	putObject(t, src, "new")
	feed.emit("new", false)
	if err := copier.Delete(ctx, src, "old"); err != nil {
		t.Fatal(err)
	}
	feed.emit("old", true)

	// Round one writes a canary; its event comes in the same round or the
	// next.
	var rep SyncReport
	for range 3 {
		var err error
		if rep, err = r.Sync(ctx, m); err != nil {
			t.Fatal(err)
		}
		if rep.Cutover.Result == migration.ResultPass {
			break
		}
	}
	if !exists(t, dst, "new") || exists(t, dst, "old") {
		t.Fatalf("target: new=%v old=%v, want the new object copied and the old one deleted", exists(t, dst, "new"), exists(t, dst, "old"))
	}
	if rep.Cutover.Result != migration.ResultPass {
		t.Fatalf("not ready: %+v", rep.Cutover)
	}
	if rep.Wait == 0 || feed.acked < 2 {
		t.Fatalf("wait=%v acked=%d", rep.Wait, feed.acked)
	}
	if exists(t, src, canaryPrefix(m.ID)) {
		t.Fatal("canary left in the source")
	}
	if feed.subscribes != 1 {
		t.Fatalf("subscribed %d times, want 1", feed.subscribes)
	}
}

func TestStreamNotReadyWithoutCanaryEvents(t *testing.T) {
	ctx := context.Background()
	r, feed, m, _, _ := streamEngine(t)
	feed.dropCanary = true
	r.Stream.CanaryTimeout = 30 * time.Millisecond
	if err := r.InitialSync(ctx, m, func(migration.Progress) {}); err != nil {
		t.Fatal(err)
	}
	rep, err := r.Sync(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if c := checkByName(rep.Cutover, "changes_arrive"); c.OK || rep.Cutover.Result != migration.ResultFail {
		t.Fatalf("first round: %+v", rep.Cutover)
	}
	time.Sleep(40 * time.Millisecond)
	rep, err = r.Sync(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if c := checkByName(rep.Cutover, "changes_arrive"); c.OK || !strings.Contains(c.Message, "no event for the canary") {
		t.Fatalf("after timeout: %+v", c)
	}
	if feed.subscribes != 2 {
		t.Fatalf("subscribes = %d, want a second one after the lost canary", feed.subscribes)
	}
}

func TestStreamFullCopyCatchesLostEvents(t *testing.T) {
	ctx := context.Background()
	r, _, m, src, dst := streamEngine(t)
	if err := r.InitialSync(ctx, m, func(migration.Progress) {}); err != nil {
		t.Fatal(err)
	}
	putObject(t, src, "no-event") // its event is lost

	rep, err := r.Sync(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if exists(t, dst, "no-event") {
		t.Fatal("copied before the full copy was due")
	}
	if c := checkByName(rep.Cutover, "target_matches_source"); c.OK {
		t.Fatalf("diff hid a missing object: %+v", c)
	}

	r.Stream.FullCopyInterval = 0
	if _, err := r.Sync(ctx, m); err != nil {
		t.Fatal(err)
	}
	if !exists(t, dst, "no-event") {
		t.Fatal("full copy did not catch the lost event")
	}
}

func TestStreamToleratesObjectsInFlight(t *testing.T) {
	ctx := context.Background()
	r, _, m, src, _ := streamEngine(t)
	r.Stream.InFlightGrace = time.Hour
	if err := r.InitialSync(ctx, m, func(migration.Progress) {}); err != nil {
		t.Fatal(err)
	}
	putObject(t, src, "just-written") // no event yet
	rep, err := r.Sync(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	c := checkByName(rep.Cutover, "target_matches_source")
	if !c.OK || !strings.Contains(c.Message, "1 missing (1 written") {
		t.Fatalf("in-flight object counted as a difference: %+v", c)
	}
}

func TestStreamBacklogLimit(t *testing.T) {
	ctx := context.Background()
	r, feed, m, _, _ := streamEngine(t)
	feed.backlog = 11
	rep, err := r.Sync(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if c := checkByName(rep.Cutover, "backlog"); c.OK || rep.Backlog != 11 {
		t.Fatalf("backlog check %+v, backlog %d", c, rep.Backlog)
	}
}

func TestReleaseUnsubscribesAndRemovesCanaries(t *testing.T) {
	ctx := context.Background()
	r, feed, m, src, _ := streamEngine(t)
	feed.dropCanary = true
	if _, err := r.Sync(ctx, m); err != nil { // leaves a canary waiting for its event
		t.Fatal(err)
	}
	entries, _ := src.List(ctx, strings.TrimSuffix(copier.ProbePrefix, "/"))
	if len(entries) != 1 {
		t.Fatalf("canaries before release: %d", len(entries))
	}
	if err := r.Release(ctx, m); err != nil {
		t.Fatal(err)
	}
	entries, _ = src.List(ctx, strings.TrimSuffix(copier.ProbePrefix, "/"))
	if feed.unsubscribes != 1 || len(entries) != 0 {
		t.Fatalf("unsubscribes=%d canaries=%d", feed.unsubscribes, len(entries))
	}

	// Release works after a restart too, when the engine has no stream.
	r.streams = map[string]*stream{}
	if err := r.Release(ctx, m); err != nil || feed.unsubscribes != 2 {
		t.Fatalf("second release: %v, unsubscribes=%d", err, feed.unsubscribes)
	}
}

func TestPrepareConflictIsPermanent(t *testing.T) {
	r, m, src, dst := memEngine(t)
	putObject(t, src, "seed")
	putObject(t, dst, "seed")
	r.feed = func(context.Context, migration.Migration) (Feed, error) {
		return conflictFeed{}, nil
	}
	if err := r.Prepare(context.Background(), m); !errors.Is(err, ErrPermanent) {
		t.Fatalf("err = %v, want permanent", err)
	}
}

type conflictFeed struct{ *memFeed }

func (conflictFeed) Subscribe(context.Context) error {
	return eventsource.ErrConflict
}

func TestPercentile(t *testing.T) {
	ds := []time.Duration{5, 1, 4, 2, 3, 10, 9, 8, 7, 6}
	if got := percentile(ds, 0.95); got != 10 {
		t.Fatalf("p95 = %v", got)
	}
	if percentile(nil, 0.95) != 0 {
		t.Fatal("empty p95")
	}
}

func TestKeySyncerSyncsAgainAfterALateChange(t *testing.T) {
	ks := newKeySyncer(nil, 4, time.Now, nil)
	entered, release := make(chan struct{}, 4), make(chan struct{})
	var mu sync.Mutex
	calls := map[string]int{}
	ks.sync = func(_ context.Context, key string) error {
		mu.Lock()
		calls[key]++
		first := calls[key] == 1
		mu.Unlock()
		if first && key == "a" {
			entered <- struct{}{}
			<-release
		}
		return nil
	}
	ctx := context.Background()
	var done []string
	var dmu sync.Mutex
	record := func(s string) func(error) {
		return func(err error) {
			if err != nil {
				t.Error(err)
			}
			dmu.Lock()
			done = append(done, s)
			dmu.Unlock()
		}
	}

	ks.submit(ctx, "a", time.Now(), record("a1"))
	<-entered
	ks.submit(ctx, "a", time.Now(), record("a2")) // arrives while a syncs
	ks.submit(ctx, "a", time.Now(), record("a3"))
	ks.submit(ctx, "b", time.Now(), record("b1"))
	close(release)
	ks.wait()

	if calls["a"] != 2 || calls["b"] != 1 {
		t.Fatalf("calls = %v, want a twice (once more after the late changes) and b once", calls)
	}
	if len(done) != 4 || len(ks.lags()) != 2 {
		t.Fatalf("done = %v, lags = %v", done, ks.lags())
	}
}

func TestKeySyncerTracksKeysDuringFullCopy(t *testing.T) {
	ks := newKeySyncer(nil, 1, time.Now, nil)
	ks.sync = func(context.Context, string) error { return nil }
	ks.submit(context.Background(), "before", time.Time{}, func(error) {})
	ks.track()
	ks.submit(context.Background(), "during", time.Time{}, func(error) {})
	ks.wait()
	if got := ks.untrack(); !slices.Equal(got, []string{"during"}) {
		t.Fatalf("tracked %v", got)
	}
}

func TestKeySyncerKeepsFailedMessagesForRetry(t *testing.T) {
	ks := newKeySyncer(nil, 1, time.Now, nil)
	ks.sync = func(_ context.Context, key string) error {
		if key == "bad" {
			return errors.New("access denied")
		}
		return nil
	}
	r := &Rclone{now: time.Now, Log: slog.New(slog.DiscardHandler)}
	st := &stream{}
	var acks ackQueue
	r.submit(context.Background(), st, nil, ks, eventsource.Message{Changes: []eventsource.Change{{Key: "ok"}}}, &acks)
	r.submit(context.Background(), st, nil, ks, eventsource.Message{Changes: []eventsource.Change{{Key: "ok"}, {Key: "bad"}}}, &acks)
	ks.wait()
	if len(acks.done) != 1 || len(acks.retry) != 1 || ks.firstError() == nil {
		t.Fatalf("acked %d, handed back %d, error %v; want the message with a failure handed back", len(acks.done), len(acks.retry), ks.firstError())
	}
}

func TestStreamPrunesObjectsDeletedWhileEventsWereLost(t *testing.T) {
	ctx := context.Background()
	r, _, m, src, dst := streamEngine(t)
	if err := r.InitialSync(ctx, m, func(migration.Progress) {}); err != nil {
		t.Fatal(err)
	}
	putObject(t, src, "gone")
	if _, err := copier.SyncKey(ctx, src, dst, "gone"); err != nil {
		t.Fatal(err)
	}
	if err := copier.Delete(ctx, src, "gone"); err != nil { // its event is lost
		t.Fatal(err)
	}
	rep, err := r.Sync(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if exists(t, dst, "gone") {
		t.Fatal("object deleted from the source while its event was lost is still in the target")
	}
	if c := checkByName(rep.Cutover, "target_matches_source"); !c.OK {
		t.Fatalf("after pruning: %+v", c)
	}
}

func TestPrepareRefusesATargetThatIsNotClean(t *testing.T) {
	ctx := context.Background()
	r, feed, m, src, dst := streamEngine(t)
	putObject(t, dst, "owners-own-data")
	err := r.Prepare(ctx, m)
	if !errors.Is(err, ErrPermanent) || !strings.Contains(err.Error(), "owners-own-data") {
		t.Fatalf("err = %v, want a permanent error naming the object", err)
	}
	if feed.subscribes != 0 {
		t.Fatal("subscribed although the target is not clean")
	}

	// A target that holds only copies of source objects is fine.
	if err := copier.Delete(ctx, dst, "owners-own-data"); err != nil {
		t.Fatal(err)
	}
	putObject(t, src, "a")
	if _, err := copier.SyncKey(ctx, src, dst, "a"); err != nil {
		t.Fatal(err)
	}
	if err := r.Prepare(ctx, m); err != nil {
		t.Fatal(err)
	}
	checks := r.Preflight(ctx, m.Source, m.Target)
	if c := checkByName(migration.Decision{Checks: checks}, "target_clean"); !c.OK {
		t.Fatalf("preflight: %+v", c)
	}
}

func TestKeySyncerReportsAChangeLeftUnhandledByCancel(t *testing.T) {
	ks := newKeySyncer(nil, 1, time.Now, nil)
	ctx, cancel := context.WithCancel(context.Background())
	entered, release := make(chan struct{}), make(chan struct{})
	ks.sync = func(context.Context, string) error {
		close(entered)
		<-release
		return nil // the first sync itself succeeds
	}
	errs := make(chan error, 2)
	ks.submit(ctx, "k", time.Now(), func(err error) { errs <- err })
	<-entered
	ks.submit(ctx, "k", time.Now(), func(err error) { errs <- err }) // needs a second sync
	cancel()
	close(release)
	ks.wait()
	for range 2 {
		if err := <-errs; err == nil {
			t.Fatal("a change whose sync was skipped was reported as handled")
		}
	}
}

// slowFeed delivers one change, then blocks the next receive until the
// round is interrupted.
type slowFeed struct {
	*memFeed
	delivered bool
}

func (f *slowFeed) Receive(ctx context.Context, wait time.Duration) ([]eventsource.Message, error) {
	if !f.delivered {
		f.delivered = true
		return []eventsource.Message{{Changes: []eventsource.Change{{Key: "late", At: time.Now()}}}}, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestInterruptedRoundFinishesTheChangesItTook(t *testing.T) {
	ctx := context.Background()
	r, feed, m, src, dst := streamEngine(t)
	r.Stream.Window = time.Hour
	sf := &slowFeed{memFeed: feed}
	r.feed = func(context.Context, migration.Migration) (Feed, error) { return sf, nil }
	if err := r.InitialSync(ctx, m, func(migration.Progress) {}); err != nil {
		t.Fatal(err)
	}
	putObject(t, src, "late")

	rctx, cancel := context.WithCancel(ctx)
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	if _, err := r.Sync(rctx, m); err == nil {
		t.Fatal("interrupted round reported success")
	}
	if !exists(t, dst, "late") || feed.acked != 1 {
		t.Fatalf("after interruption: copied=%v acked=%d; want the taken change finished and acknowledged", exists(t, dst, "late"), feed.acked)
	}
}

func TestCopiesHonorKeysAppsDeleted(t *testing.T) {
	ctx := context.Background()
	r, _, m, src, dst := streamEngine(t)
	r.Deletes = routing.NewDeletes()

	// A copy that lands after an app's delete is removed again.
	putObject(t, src, "racing")
	r.Deletes.Add(m.ID, "racing") // the app recorded it, and is about to delete it
	if act, err := r.syncKey(ctx, m.ID, src, dst, "racing"); err != nil || exists(t, dst, "racing") {
		t.Fatalf("syncKey: %v, %v; the deleted key must not stay in the target", act, err)
	}

	// So is one a bulk copy brought back.
	putObject(t, src, "bulk")
	if err := r.InitialSync(ctx, m, func(migration.Progress) {}); err != nil {
		t.Fatal(err)
	}
	if !exists(t, dst, "bulk") {
		t.Fatal("bulk copy")
	}
	r.Deletes.Add(m.ID, "bulk")
	if err := r.Resume(ctx, m); err != nil {
		t.Fatal(err)
	}
	if exists(t, dst, "bulk") {
		t.Fatal("final copy left a key an app deleted")
	}

	// The reverse copy honors them too.
	putObject(t, dst, "new-in-target")
	r.Deletes.Add(m.ID, "new-in-target")
	if _, err := r.copyBack(ctx, m.ID, src, dst); err != nil {
		t.Fatal(err)
	}
	if exists(t, src, "new-in-target") {
		t.Fatal("reverse copy brought back a key an app deleted")
	}
	if err := r.Release(ctx, m); err != nil || r.Deletes.Has(m.ID, "bulk") {
		t.Fatalf("release: %v, deletes kept: %v", err, r.Deletes.Has(m.ID, "bulk"))
	}
}

func TestDrainDoesNotWaitForChangesMadeAfterItStarted(t *testing.T) {
	ctx := context.Background()
	r, feed, m, src, dst := streamEngine(t)
	r.Stream.DrainTimeout = 5 * time.Second
	if err := r.InitialSync(ctx, m, func(migration.Progress) {}); err != nil {
		t.Fatal(err)
	}
	putObject(t, src, "before-resume")
	feed.mu.Lock()
	feed.queue = append(feed.queue, eventsource.Message{Changes: []eventsource.Change{{Key: "before-resume", At: time.Now().Add(-time.Minute)}}})
	feed.mu.Unlock()

	// Apps keep deleting from the source the whole time.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
				feed.emit(fmt.Sprintf("deleted-%d", i), true)
			}
		}
	}()
	start := time.Now()
	if err := r.Resume(ctx, m); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 3*time.Second || !exists(t, dst, "before-resume") {
		t.Fatalf("resume took %s, before-resume copied: %v", time.Since(start), exists(t, dst, "before-resume"))
	}
}
