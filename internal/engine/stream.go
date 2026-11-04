package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rclone/rclone/fs"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/copier"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/eventsource"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

// canaryPrefix holds a migration's canary objects in the source.
func canaryPrefix(id string) string { return copier.ProbePrefix + "canary-" + id + "-" }

// follow runs one sync round for a source with a change feed: Window of
// making changed keys match the source as their events arrive, with a full
// copy and a full comparison running alongside when they are due. It
// reports whether cutover may start.
func (r *Rclone) follow(ctx context.Context, m migration.Migration, st *stream, src, dst fs.Fs) (SyncReport, error) {
	opt := r.Stream
	ks := r.keySyncer(m.ID, src, dst)

	// The full copy and the comparison list whole buckets, which takes a
	// while; changes keep flowing meanwhile. They stop if the round fails.
	bgCtx, stopBg := context.WithCancel(ctx)
	defer stopBg()
	var (
		bg               sync.WaitGroup
		fullErr, diffErr error
		fullStart        time.Time
	)
	if r.now().Sub(st.lastFull) >= opt.FullCopyInterval {
		fullStart = r.now()
		ks.track()
		bg.Go(func() {
			if _, err := copier.Copy(bgCtx, src, dst, r.Options, nil); err != nil {
				fullErr = fmt.Errorf("full copy: %w", err)
			}
		})
	}
	if r.now().Sub(st.diffAt) >= opt.DiffInterval {
		bg.Go(func() { diffErr = r.refreshDiff(bgCtx, st, src, dst) })
	}
	bgDone := make(chan struct{})
	go func() { bg.Wait(); close(bgDone) }()
	background := func() bool {
		select {
		case <-bgDone:
			return false
		default:
			return true
		}
	}

	if err := r.writeCanary(ctx, m, st, src); err != nil {
		stopBg()
		<-bgDone
		return SyncReport{}, err
	}
	// When the round is interrupted it stops taking changes, but finishes
	// and acknowledges the ones it took: a change dropped half way would
	// stay hidden in the feed long after the step that interrupted us.
	work := context.WithoutCancel(ctx)
	var acks ackQueue
	var err error
	for deadline := r.now().Add(opt.Window); err == nil && (r.now().Before(deadline) || background()); {
		var msgs []eventsource.Message
		// A round may run up to a second past its window: SQS waits in
		// whole seconds, and a shorter wait would poll in a busy loop.
		if msgs, err = st.feed.Receive(ctx, time.Second); err != nil {
			err = fmt.Errorf("receive changes: %w", err)
			break
		}
		for _, msg := range msgs {
			r.submit(work, st, src, ks, msg, &acks)
		}
		err = acks.flush(work, st.feed)
	}
	if err != nil {
		stopBg()
	}
	<-bgDone
	if err == nil {
		err = errors.Join(fullErr, diffErr)
	}
	if err == nil && len(st.staleKeys) > 0 {
		// Deleted from the source while their events were lost. Prepare
		// made sure the target held nothing else, so they are ours.
		var pruned atomic.Int64
		for _, key := range st.staleKeys {
			ks.submit(work, key, time.Time{}, func(err error) {
				if err == nil {
					pruned.Add(1)
				}
			})
		}
		ks.wait()
		st.staleExtra -= pruned.Load()
		st.staleKeys = nil
	}
	if !fullStart.IsZero() && (st.unapplied.IsZero() || fullStart.Before(st.unapplied)) {
		st.unapplied = fullStart
	}
	if err == nil && !st.unapplied.IsZero() {
		// A full copy may have brought back keys apps deleted while it ran,
		// even one cut short in an earlier round; remove them. A key that
		// changed while it ran may have been copied from what the source
		// held before the change; sync it again.
		if err = r.applyDeletes(work, m.ID, dst, st.unapplied); err == nil {
			st.unapplied = time.Time{}
		}
		for _, key := range ks.untrack() {
			ks.submit(work, key, time.Time{}, func(error) {})
		}
		if !fullStart.IsZero() {
			st.lastFull = fullStart
		}
	}
	ks.wait()
	if ferr := acks.flush(work, st.feed); err == nil {
		err = ferr
	}
	if err == nil {
		err = ks.firstError()
	}
	if err != nil {
		return SyncReport{}, err
	}

	backlog, err := st.feed.Backlog(ctx)
	if err != nil {
		return SyncReport{}, fmt.Errorf("backlog: %w", err)
	}
	lags := ks.lags()
	lag := percentile(lags, 0.95)
	return SyncReport{
		Cutover: migration.Decide(
			r.canaryCheck(st),
			migration.Check{
				Name:    "backlog",
				OK:      backlog <= opt.MaxBacklog,
				Message: fmt.Sprintf("%d changes waiting, limit %d", backlog, opt.MaxBacklog),
			},
			lagCheck(lags, lag, opt.MaxLag),
			migration.Check{
				Name: "target_matches_source",
				OK:   st.diff.Missing-st.inFlight == 0 && st.diff.Differ == 0 && st.staleExtra == 0,
				Message: fmt.Sprintf("%d missing (%d written in the last %s), %d different, %d only in target, %d identical, checked %s ago",
					st.diff.Missing, st.inFlight, opt.InFlightGrace, st.diff.Differ, st.staleExtra, st.diff.Matched,
					r.now().Sub(st.diffAt).Round(time.Second)),
			},
		),
		Backlog:    backlog,
		LagSeconds: lag.Seconds(),
		// Changes keep coming; start the next round right away.
		Wait: 10 * time.Millisecond,
	}, nil
}

// submit hands a message's changes to the key syncer and queues the
// message for acknowledgement once all of them are in the target.
func (r *Rclone) submit(ctx context.Context, st *stream, src fs.Fs, ks *keySyncer, msg eventsource.Message, acks *ackQueue) {
	var keys []eventsource.Change
	for _, c := range msg.Changes {
		if strings.HasPrefix(c.Key, copier.ProbePrefix) {
			if c.Key == st.canaryKey && !c.Removed {
				r.canaryArrived(ctx, st, src)
			}
			continue
		}
		r.Log.Debug("change", "key", c.Key, "removed", c.Removed, "at", c.At)
		keys = append(keys, c)
	}
	if len(keys) == 0 {
		acks.add(msg)
		return
	}
	var (
		mu     sync.Mutex
		left   = len(keys)
		failed bool
	)
	for _, c := range keys {
		ks.submit(ctx, c.Key, c.At, func(err error) {
			mu.Lock()
			defer mu.Unlock()
			failed = failed || err != nil
			if left--; left == 0 {
				if failed {
					acks.nack(msg) // back to the feed, to be tried again
				} else {
					acks.add(msg)
				}
			}
		})
	}
}

// ackQueue collects messages that are handled, to acknowledge them, and
// messages that failed, to hand them back.
type ackQueue struct {
	mu          sync.Mutex
	done, retry []eventsource.Message
}

func (q *ackQueue) add(m eventsource.Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.done = append(q.done, m)
}

func (q *ackQueue) nack(m eventsource.Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.retry = append(q.retry, m)
}

func (q *ackQueue) flush(ctx context.Context, feed Feed) error {
	q.mu.Lock()
	done, retry := q.done, q.retry
	q.done, q.retry = nil, nil
	q.mu.Unlock()
	if len(done) > 0 {
		if err := feed.Ack(ctx, done); err != nil {
			return fmt.Errorf("ack changes: %w", err)
		}
	}
	if len(retry) > 0 {
		if err := feed.Nack(ctx, retry); err != nil {
			return fmt.Errorf("hand back changes: %w", err)
		}
	}
	return nil
}

// keySyncer makes keys match the source, many at once but each key one
// sync at a time: a change that arrives while its key is being synced
// makes the key sync again afterwards, so the last sync of a key always
// starts after its last change.
type keySyncer struct {
	sync  func(ctx context.Context, key string) error
	log   *slog.Logger
	slots chan struct{}
	now   func() time.Time

	mu       sync.Mutex
	running  map[string]*keyRun
	tracking map[string]bool // keys changed while a full copy runs
	durs     []time.Duration
	errs     map[string]error
	wg       sync.WaitGroup
}

type keyRun struct {
	again bool
	at    time.Time // oldest change this run answers for
	done  []func(error)
}

// keySyncer returns a key syncer from src to dst for a migration.
func (r *Rclone) keySyncer(id string, src, dst fs.Fs) *keySyncer {
	log := r.Log.With("migration", id)
	return newKeySyncer(func(ctx context.Context, key string) error {
		act, err := r.syncKey(ctx, id, src, dst, key)
		log.Debug("sync key", "key", key, "action", act, "err", err)
		return err
	}, r.Options.Transfers, r.now, log)
}

func newKeySyncer(sync func(ctx context.Context, key string) error, parallel int, now func() time.Time, log *slog.Logger) *keySyncer {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &keySyncer{
		log:     log,
		sync:    sync,
		now:     now,
		slots:   make(chan struct{}, max(parallel, 1)),
		running: make(map[string]*keyRun),
		errs:    make(map[string]error),
	}
}

// submit syncs key, or syncs it once more if it is running now. done is
// called when a sync that started after this call has finished.
func (k *keySyncer) submit(ctx context.Context, key string, at time.Time, done func(error)) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.tracking != nil {
		k.tracking[key] = true
	}
	if run, ok := k.running[key]; ok {
		run.again = true
		run.done = append(run.done, done)
		if run.at.IsZero() || (!at.IsZero() && at.Before(run.at)) {
			run.at = at
		}
		return
	}
	run := &keyRun{at: at, done: []func(error){done}}
	k.running[key] = run
	k.wg.Go(func() { k.run(ctx, key, run) })
}

func (k *keySyncer) run(ctx context.Context, key string, run *keyRun) {
	for {
		k.slots <- struct{}{}
		err := k.sync(ctx, key)
		<-k.slots

		k.mu.Lock()
		if run.again {
			if ctx.Err() == nil {
				run.again = false
				k.mu.Unlock()
				continue
			}
			// A change came in after this sync started and is not handled.
			err = ctx.Err()
		}
		delete(k.running, key)
		if err != nil {
			k.errs[key] = err
		} else {
			delete(k.errs, key) // a later sync of the key succeeded
			if !run.at.IsZero() {
				k.durs = append(k.durs, k.now().Sub(run.at))
			}
		}
		done := run.done
		k.mu.Unlock()
		for _, d := range done {
			d(err)
		}
		return
	}
}

func (k *keySyncer) track() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.tracking = make(map[string]bool)
}

func (k *keySyncer) untrack() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	keys := slices.Collect(maps.Keys(k.tracking))
	k.tracking = nil
	return keys
}

func (k *keySyncer) wait() { k.wg.Wait() }

// lags is how long each synced change took from the source to the target.
func (k *keySyncer) lags() []time.Duration {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Clone(k.durs)
}

func (k *keySyncer) firstError() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	for key, err := range k.errs {
		// Its message comes back later; report one cause.
		return fmt.Errorf("sync %q: %w", key, err)
	}
	return nil
}

// writeCanary puts a small object in the source now and then. Its event
// shows that changes still reach us, and how fast; a quiet bucket with a
// broken feed would otherwise look caught up.
func (r *Rclone) writeCanary(ctx context.Context, m migration.Migration, st *stream, src fs.Fs) error {
	now := r.now()
	switch {
	case st.canaryKey == "" && now.Sub(st.canarySeen) >= r.Stream.CanaryInterval:
	case st.canaryKey != "" && now.Sub(st.canaryPut) > r.Stream.CanaryTimeout:
		// Lost. Someone may have removed our notification: subscribe again,
		// which is harmless if it is still there, and try a new canary.
		_ = copier.Delete(ctx, src, st.canaryKey)
		if st.canaryLost.IsZero() {
			st.canaryLost = st.canaryPut
		}
		if err := st.feed.Subscribe(ctx); err != nil {
			return feedError(fmt.Errorf("subscribe to source changes: %w", err))
		}
	default:
		return nil
	}
	key := fmt.Sprintf("%s%d", canaryPrefix(m.ID), now.UnixNano())
	if err := copier.Put(ctx, src, key, "object-migration canary\n"); err != nil {
		return fmt.Errorf("write canary: %w", err)
	}
	st.canaryKey, st.canaryPut = key, now
	return nil
}

func (r *Rclone) canaryArrived(ctx context.Context, st *stream, src fs.Fs) {
	now := r.now()
	st.canarySeen, st.canaryDelay = now, now.Sub(st.canaryPut)
	_ = copier.Delete(ctx, src, st.canaryKey) // its removal event is ignored
	st.canaryKey, st.canaryLost = "", time.Time{}
}

func (r *Rclone) canaryCheck(st *stream) migration.Check {
	c := migration.Check{Name: "changes_arrive"}
	lostSince := st.canaryLost
	if lostSince.IsZero() && st.canaryKey != "" && r.now().Sub(st.canaryPut) > r.Stream.CanaryTimeout {
		lostSince = st.canaryPut
	}
	switch {
	case !lostSince.IsZero():
		c.Message = fmt.Sprintf("no event for the canary written %s ago", r.now().Sub(lostSince).Round(time.Second))
	case st.canarySeen.IsZero():
		c.Message = "waiting for the first canary event"
	case st.canaryDelay > r.Stream.MaxCanaryDelay:
		c.Message = fmt.Sprintf("canary event took %s, limit %s", st.canaryDelay.Round(time.Millisecond), r.Stream.MaxCanaryDelay)
	default:
		c.OK = true
		c.Message = fmt.Sprintf("canary event arrived in %s", st.canaryDelay.Round(time.Millisecond))
	}
	return c
}

// refreshDiff compares source and target in full. Objects written in the
// last InFlightGrace may still be on their way and are counted apart.
func (r *Rclone) refreshDiff(ctx context.Context, st *stream, src, dst fs.Fs) error {
	d, err := copier.Check(ctx, src, dst, r.Options)
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}
	var inFlight int64
	if int64(len(d.MissingKeys)) == d.Missing {
		for _, key := range d.MissingKeys {
			t, err := copier.ModTime(ctx, src, key)
			switch {
			case errors.Is(err, fs.ErrorObjectNotFound):
				inFlight++ // deleted again since the listing
			case err != nil:
				return fmt.Errorf("check %q: %w", key, err)
			case r.now().Sub(t) < r.Stream.InFlightGrace:
				inFlight++ // just written; its event may still be on the way
			}
		}
	}
	// An object only in the target is usually a deletion on its way; look
	// again now that the listing is over. When the listing named every such
	// object, only the ones still there count.
	var staleKeys []string
	for _, key := range d.ExtraKeys {
		inDst, err := present(ctx, dst, key)
		if err != nil {
			return fmt.Errorf("check %q: %w", key, err)
		}
		inSrc, err := present(ctx, src, key)
		if err != nil {
			return fmt.Errorf("check %q: %w", key, err)
		}
		if inDst && !inSrc {
			staleKeys = append(staleKeys, key)
		}
	}
	staleExtra := d.Extra
	if int64(len(d.ExtraKeys)) == d.Extra {
		staleExtra = int64(len(staleKeys))
	}
	st.diff, st.inFlight, st.staleExtra, st.staleKeys, st.diffAt = d, inFlight, staleExtra, staleKeys, r.now()
	return nil
}

func present(ctx context.Context, f fs.Fs, key string) (bool, error) {
	_, err := f.NewObject(ctx, key)
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return false, nil
	}
	return err == nil, err
}

func lagCheck(lags []time.Duration, p95, limit time.Duration) migration.Check {
	if len(lags) == 0 {
		return migration.Check{Name: "lag", OK: true, Message: "no changes in the last round"}
	}
	return migration.Check{
		Name:    "lag",
		OK:      p95 <= limit,
		Message: fmt.Sprintf("p95 %s over %d changes, limit %s", p95.Round(time.Millisecond), len(lags), limit),
	}
}

func percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	return s[min(int(float64(len(s))*p), len(s)-1)]
}

// drain handles every change waiting in the feed. It ends when the feed
// is empty, or fails after Stream.DrainTimeout so the step is retried.
func (r *Rclone) drain(ctx context.Context, m migration.Migration, st *stream, src, dst fs.Fs) error {
	log := r.Log.With("migration", m.ID)
	ks := r.keySyncer(m.ID, src, dst)
	defer ks.wait() // never leave syncs running into a retry
	var acks ackQueue
	start := r.now()
	deadline := start.Add(r.Stream.DrainTimeout)
	log.Debug("drain start")
	// The changes to wait for are the ones made before now. Apps keep
	// deleting from the source after it, but each such delete went to the
	// target too, so those are handled on the way and not waited for. A
	// round counts as idle when it brought no older change; the feed's
	// counts are approximate, so it takes two idle rounds in a row.
	for idle := 0; idle < 2; {
		msgs, err := st.feed.Receive(ctx, time.Second)
		if err != nil {
			return fmt.Errorf("receive changes: %w", err)
		}
		older := false
		for _, msg := range msgs {
			older = older || slices.ContainsFunc(msg.Changes, func(c eventsource.Change) bool { return c.At.Before(start) })
			r.submit(ctx, st, src, ks, msg, &acks)
		}
		if len(msgs) > 0 && !older {
			idle++
			if err := acks.flush(ctx, st.feed); err != nil {
				return err
			}
		} else if len(msgs) == 0 {
			ks.wait()
			if err := acks.flush(ctx, st.feed); err != nil {
				return err
			}
			if err := ks.firstError(); err != nil {
				return err
			}
			backlog, err := st.feed.Backlog(ctx)
			if err != nil {
				return fmt.Errorf("backlog: %w", err)
			}
			log.Debug("drain idle", "backlog", backlog)
			if backlog == 0 {
				idle++
			} else {
				idle = 0
			}
		} else {
			idle = 0
			if err := acks.flush(ctx, st.feed); err != nil {
				return err
			}
		}
		if r.now().After(deadline) {
			ks.wait()
			return fmt.Errorf("source changes still waiting after %s", r.Stream.DrainTimeout)
		}
	}
	// No more canaries; remove the one in flight, if any.
	if err := copier.DeleteAll(ctx, src, canaryPrefix(m.ID)); err != nil {
		return fmt.Errorf("remove canaries: %w", err)
	}
	st.canaryKey = ""
	return nil
}
