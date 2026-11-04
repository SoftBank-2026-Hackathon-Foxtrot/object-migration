package engine

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rclone/rclone/fs"
	pubsub "google.golang.org/api/pubsub/v1"
	storage "google.golang.org/api/storage/v1"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/copier"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/creds"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/eventsource"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/routing"
)

// Rclone copies objects with rclone. Before anything is copied it starts
// following changes in the source, where the source kind has a change feed.
// The initial sync copies the whole source; each sync round then makes every
// changed key match the source as its event comes in, and now and then
// copies the whole source again in case an event was lost. Sources without
// a feed are caught up by copying the whole source every round.
//
// Freeze, switch and resume do nothing yet: the app keeps using the source
// until switching lands.
type Rclone struct {
	Options copier.Options
	Stream  StreamOptions
	// Log gets one debug line per change handled. Nil discards them.
	Log *slog.Logger
	// GCPProject is the platform's Google Cloud project, which holds the
	// Pub/Sub topics that follow GCS sources. Empty means GCS sources have
	// no change feed and are mirrored every round.
	GCPProject string
	// Deletes holds the keys apps deleted during each migration. Every copy
	// honors them: a key an app deleted is removed from where it was copied,
	// however the copy and the delete interleaved. Nil means apps record
	// none.
	Deletes *routing.Deletes
	// Positions keeps where each feed that resumes from a position was read
	// up to, so a restarted server goes on from there. Nil starts such
	// feeds a little before now.
	Positions Positions
	// PreflightTimeout bounds each preflight check.
	PreflightTimeout time.Duration

	// open connects to an endpoint and feed returns the source's change
	// feed, or nil if its kind has none; tests replace them.
	open func(ctx context.Context, name string, e migration.Endpoint) (fs.Fs, error)
	feed func(ctx context.Context, m migration.Migration) (Feed, error)
	// checkFeed tells preflight whether a change feed can be added to the
	// source; nil skips the check.
	checkFeed func(ctx context.Context, source migration.Endpoint) error
	now       func() time.Time

	mu      sync.Mutex
	streams map[string]*stream
}

// Feed is a change feed the engine sets up for a migration and removes when
// it is over.
type Feed interface {
	eventsource.Source
	Subscribe(ctx context.Context) error
	Unsubscribe(ctx context.Context) error
}

// Positions keeps change feed positions by migration.
type Positions interface {
	FeedPosition(ctx context.Context, id string) (string, error)
	SaveFeedPosition(ctx context.Context, id, pos string) error
	ForgetFeedPosition(ctx context.Context, id string) error
}

// Resumable is a feed that reads from a position the server keeps, rather
// than one that holds unhandled changes itself like a queue.
type Resumable interface {
	// Position is where reading can start again without losing an
	// unacknowledged change.
	Position() string
	// ResumeFrom sets where Subscribe starts reading.
	ResumeFrom(pos string) error
}

// StreamOptions tune how a migration with a change feed follows the source
// and when it is ready for cutover.
type StreamOptions struct {
	// Window is how long one sync round follows changes before it reports.
	Window time.Duration
	// FullCopyInterval is how often the whole source is copied again, to
	// catch changes whose events were lost.
	FullCopyInterval time.Duration
	// DiffInterval is how often source and target are compared in full.
	DiffInterval time.Duration
	// InFlightGrace is how recent a source object may be and still be
	// missing from the target without counting as a difference.
	InFlightGrace time.Duration
	// CanaryInterval is how often a canary object is written to the source
	// to time how fast its event arrives.
	CanaryInterval time.Duration
	// CanaryTimeout is how long a canary may go without its event.
	CanaryTimeout time.Duration
	// MaxCanaryDelay, MaxLag and MaxBacklog are the limits for cutover.
	MaxCanaryDelay time.Duration
	MaxLag         time.Duration
	MaxBacklog     int64
	// DrainTimeout bounds how long resume waits for the feed to empty.
	DrainTimeout time.Duration
}

func DefaultStreamOptions() StreamOptions {
	return StreamOptions{
		Window:           5 * time.Second,
		FullCopyInterval: 5 * time.Minute,
		DiffInterval:     time.Minute,
		InFlightGrace:    30 * time.Second,
		CanaryInterval:   15 * time.Second,
		CanaryTimeout:    30 * time.Second,
		MaxCanaryDelay:   10 * time.Second,
		MaxLag:           5 * time.Second,
		MaxBacklog:       100,
		DrainTimeout:     5 * time.Minute,
	}
}

// stream is what the engine keeps in memory about a migration with a feed.
// It is rebuilt after a restart; the feed itself keeps the changes, or
// Positions keeps where to read them from again.
type stream struct {
	feed       Feed
	subscribed bool
	savedPos   string // the feed position last saved

	lastFull time.Time

	canaryKey   string // the canary in flight, if any
	canaryPut   time.Time
	canaryLost  time.Time // when the oldest canary still without an event was written
	canarySeen  time.Time // when the last canary's event arrived
	canaryDelay time.Duration

	diffAt     time.Time
	diff       copier.Diff
	inFlight   int64    // missing objects younger than InFlightGrace
	staleExtra int64    // objects only in the target, still so on a second look
	staleKeys  []string // their keys, to delete from the target
	// unapplied is when the oldest full copy started whose round ended
	// before the keys apps deleted were removed after it; zero if none.
	unapplied time.Time
}

func NewRclone(opt copier.Options) *Rclone {
	r := &Rclone{
		Options:          opt,
		Stream:           DefaultStreamOptions(),
		PreflightTimeout: 15 * time.Second,
		open:             copier.Open,
		now:              time.Now,
		Log:              slog.New(slog.DiscardHandler),
		streams:          make(map[string]*stream),
	}
	r.feed = r.defaultFeed
	r.checkFeed = r.checkSourceFeed
	return r
}

func (r *Rclone) checkSourceFeed(ctx context.Context, source migration.Endpoint) error {
	if source.Kind == migration.KindSeaweedFS && source.Filer != "" {
		return eventsource.CheckSeaweedFS(ctx, source.Filer, source.Bucket)
	}
	if source.Kind == migration.KindGCS {
		if r.GCPProject == "" {
			return nil
		}
		opts, err := creds.GCSOptions(source.Ref)
		if err != nil {
			return err
		}
		st, err := storage.NewService(ctx, opts...)
		if err != nil {
			return err
		}
		return eventsource.CheckGCS(ctx, st, source.Bucket)
	}
	if source.Kind != migration.KindS3 {
		return nil
	}
	bucketAWS, err := creds.AWSConfig(ctx, source.Ref, source.Region)
	if err != nil {
		return err
	}
	return eventsource.CheckS3(ctx, bucketAWS, source.Bucket)
}

// defaultFeed follows S3 sources through SQS, and SeaweedFS sources with a
// filer address through the filer's metadata subscription. For S3, the
// queue is created with the server's own AWS credentials, the bucket's
// notification with the bucket owner's.
func (r *Rclone) defaultFeed(ctx context.Context, m migration.Migration) (Feed, error) {
	if m.Source.Kind == migration.KindSeaweedFS && m.Source.Filer != "" {
		return eventsource.NewSeaweedFS(eventsource.SeaweedFSConfig{
			Filer: m.Source.Filer, Bucket: m.Source.Bucket, Name: "object-migration-" + m.ID,
		}), nil
	}
	if m.Source.Kind == migration.KindGCS {
		if r.GCPProject == "" {
			return nil, nil
		}
		opts, err := creds.GCSOptions(m.Source.Ref)
		if err != nil {
			return nil, err
		}
		st, err := storage.NewService(ctx, opts...)
		if err != nil {
			return nil, err
		}
		ps, err := pubsub.NewService(ctx) // the platform's own credentials
		if err != nil {
			return nil, err
		}
		return eventsource.NewGCS(eventsource.GCSConfig{
			Bucket: m.Source.Bucket, Storage: st, PubSub: ps, Project: r.GCPProject, Name: m.ID,
		}), nil
	}
	if m.Source.Kind != migration.KindS3 {
		return nil, nil
	}
	bucketAWS, err := creds.AWSConfig(ctx, m.Source.Ref, m.Source.Region)
	if err != nil {
		return nil, err
	}
	platformAWS, err := creds.AWSConfig(ctx, creds.Default, m.Source.Region)
	if err != nil {
		return nil, err
	}
	return eventsource.NewS3(eventsource.S3Config{
		Bucket: m.Source.Bucket, BucketAWS: bucketAWS, QueueAWS: platformAWS, Name: m.ID,
	}), nil
}

// stream returns the migration's stream, subscribing its feed if that has
// not been done since the server started. Subscribing again is harmless.
func (r *Rclone) stream(ctx context.Context, m migration.Migration) (*stream, error) {
	r.mu.Lock()
	st, ok := r.streams[m.ID]
	r.mu.Unlock()
	if !ok {
		feed, err := r.feed(ctx, m)
		if err != nil {
			return nil, feedError(err)
		}
		// A copy cut short by a restart may have brought back keys apps
		// deleted; the next round removes every key deleted so far.
		st = &stream{feed: feed, unapplied: m.CreatedAt}
		if err := r.resumeFeed(ctx, m.ID, st); err != nil {
			return nil, err
		}
		r.mu.Lock()
		r.streams[m.ID] = st
		r.mu.Unlock()
	}
	if st.feed != nil && !st.subscribed {
		if err := st.feed.Subscribe(ctx); err != nil {
			return nil, feedError(fmt.Errorf("subscribe to source changes: %w", err))
		}
		st.subscribed = true
	}
	return st, nil
}

// resumeFeed points a resumable feed at its saved position.
func (r *Rclone) resumeFeed(ctx context.Context, id string, st *stream) error {
	feed, ok := st.feed.(Resumable)
	if !ok || r.Positions == nil {
		return nil
	}
	pos, err := r.Positions.FeedPosition(ctx, id)
	if err != nil {
		return fmt.Errorf("load feed position: %w", err)
	}
	if pos == "" {
		return nil
	}
	if err := feed.ResumeFrom(pos); err != nil {
		return fmt.Errorf("resume feed from %q: %w", pos, err)
	}
	st.savedPos = pos
	r.Log.Info("change feed resumes from its saved position", "migration", id, "position", pos)
	return nil
}

// savePosition saves a resumable feed's position if it moved. A position
// that could not be saved only makes a restart read some changes again.
func (r *Rclone) savePosition(ctx context.Context, id string, st *stream) {
	feed, ok := st.feed.(Resumable)
	if !ok || r.Positions == nil {
		return
	}
	pos := feed.Position()
	if pos == st.savedPos {
		return
	}
	if err := r.Positions.SaveFeedPosition(ctx, id, pos); err != nil {
		r.Log.Warn("save feed position", "migration", id, "err", err)
		return
	}
	st.savedPos = pos
}

func feedError(err error) error {
	if errors.Is(err, creds.ErrBadRef) || errors.Is(err, eventsource.ErrConflict) {
		return fmt.Errorf("%w: %w", ErrPermanent, err)
	}
	return err
}

func (r *Rclone) Name() string { return "rclone" }

// ReverseSync is true: after resume, Verify copies the target's new objects
// back to the source.
func (r *Rclone) ReverseSync() bool { return true }

func (r *Rclone) Preflight(ctx context.Context, source, target migration.Endpoint) []migration.Check {
	var checks []migration.Check
	check := func(name string, fn func(context.Context) error) bool {
		ctx, cancel := context.WithTimeout(ctx, r.PreflightTimeout)
		defer cancel()
		c := migration.Check{Name: name, OK: true}
		if err := fn(ctx); err != nil {
			c.OK, c.Message = false, err.Error()
		}
		checks = append(checks, c)
		return c.OK
	}

	var src, dst fs.Fs
	srcOK := check("source_read", func(ctx context.Context) (err error) {
		if src, err = r.open(ctx, "source", source); err != nil {
			return err
		}
		return copier.ProbeList(ctx, src)
	})
	if check("target_read", func(ctx context.Context) (err error) {
		if dst, err = r.open(ctx, "target", target); err != nil {
			return err
		}
		return copier.ProbeList(ctx, dst)
	}) {
		check("target_write", func(ctx context.Context) error { return copier.ProbeWrite(ctx, dst) })
		if srcOK {
			// Lists both buckets in full, so it gets the whole request's
			// time rather than one check's.
			c, err := targetClean(ctx, src, dst)
			if err != nil {
				c = migration.Check{Name: "target_clean", Message: err.Error()}
			}
			checks = append(checks, c)
		}
	}
	if r.checkFeed != nil && (source.Kind == migration.KindS3 || source.Filer != "" || (source.Kind == migration.KindGCS && r.GCPProject != "")) {
		check("source_events", func(ctx context.Context) error { return r.checkFeed(ctx, source) })
	}
	return checks
}

// Prepare checks that the target is clean and subscribes to the source's
// changes, so changes made during the initial copy wait in the feed.
func (r *Rclone) Prepare(ctx context.Context, m migration.Migration) error {
	src, dst, err := r.pair(ctx, m)
	if err != nil {
		return err
	}
	c, err := targetClean(ctx, src, dst)
	if err != nil {
		return err
	}
	if !c.OK {
		return fmt.Errorf("%w: target is not clean: %s", ErrPermanent, c.Message)
	}
	_, err = r.stream(ctx, m)
	return err
}

// targetClean passes when the target holds nothing but copies of source
// objects. The migration then owns everything in the target, so an object
// that later turns up only in the target was deleted from the source and
// may be deleted from the target too. A target holding anything else could
// be the bucket owner's data, which must never be deleted or overwritten.
func targetClean(ctx context.Context, src, dst fs.Fs) (migration.Check, error) {
	d, err := copier.Check(ctx, src, dst, copier.Options{})
	if err != nil {
		return migration.Check{}, fmt.Errorf("compare target with source: %w", err)
	}
	c := migration.Check{Name: "target_clean", OK: d.Extra == 0 && d.Differ == 0}
	if c.OK {
		c.Message = fmt.Sprintf("target holds %d copies of source objects and nothing else", d.Matched)
	} else {
		c.Message = fmt.Sprintf("target holds %d objects the source does not have and %d that differ from it, for example %q; use an empty target",
			d.Extra, d.Differ, append(d.ExtraKeys, "")[0])
	}
	return c, nil
}

// Release removes the feed and any canary left in the source. It works
// from the migration alone, so it also cleans up after a restart.
func (r *Rclone) Release(ctx context.Context, m migration.Migration) error {
	r.mu.Lock()
	st, ok := r.streams[m.ID]
	r.mu.Unlock()
	feed := Feed(nil)
	if ok {
		feed = st.feed
	} else {
		var err error
		if feed, err = r.feed(ctx, m); errors.Is(err, creds.ErrBadRef) {
			return nil // nothing could have been set up with these credentials
		} else if err != nil {
			return err
		}
	}
	if feed != nil {
		if err := feed.Unsubscribe(ctx); err != nil {
			return fmt.Errorf("unsubscribe from source changes: %w", err)
		}
		src, err := r.open(ctx, "source", m.Source)
		if err != nil {
			return fmt.Errorf("remove canaries: %w", err)
		}
		if err := copier.DeleteAll(ctx, src, canaryPrefix(m.ID)); err != nil {
			return fmt.Errorf("remove canaries: %w", err)
		}
	}
	if r.Positions != nil {
		if err := r.Positions.ForgetFeedPosition(ctx, m.ID); err != nil {
			return fmt.Errorf("forget feed position: %w", err)
		}
	}
	if err := r.Deletes.Forget(ctx, m.ID); err != nil {
		return fmt.Errorf("forget deleted keys: %w", err)
	}
	r.mu.Lock()
	delete(r.streams, m.ID)
	r.mu.Unlock()
	return nil
}

func (r *Rclone) InitialSync(ctx context.Context, m migration.Migration, report func(migration.Progress)) error {
	src, dst, err := r.pair(ctx, m)
	if err != nil {
		return err
	}
	total, totalBytes, err := copier.Count(ctx, src)
	if err != nil {
		return fmt.Errorf("count source objects: %w", err)
	}
	progress := func(res copier.Result) migration.Progress {
		done, bytes := res.Copied+res.Matched, res.Bytes+res.MatchedBytes
		// Objects written to the source during the copy, and retried
		// transfers, can push past the count taken before it.
		return migration.Progress{
			Completed: done, Total: max(total, done), Unit: "objects",
			Bytes: bytes, TotalBytes: max(totalBytes, bytes),
		}
	}
	report(progress(copier.Result{}))
	start := r.now()
	res, err := copier.Copy(ctx, src, dst, r.Options, func(res copier.Result) { report(progress(res)) })
	if err != nil {
		// What this copy wrote may include keys apps deleted meanwhile; the
		// next round removes them.
		if st, serr := r.stream(ctx, m); serr == nil && (st.unapplied.IsZero() || start.Before(st.unapplied)) {
			st.unapplied = start
		}
		return fmt.Errorf("copy: %w", err)
	}
	if err := r.applyDeletes(ctx, m.ID, dst, start); err != nil {
		return err
	}
	report(progress(res))
	if st, err := r.stream(ctx, m); err == nil {
		st.lastFull = start
	}
	return nil
}

func (r *Rclone) Sync(ctx context.Context, m migration.Migration) (SyncReport, error) {
	src, dst, err := r.pair(ctx, m)
	if err != nil {
		return SyncReport{}, err
	}
	st, err := r.stream(ctx, m)
	if err != nil {
		return SyncReport{}, err
	}
	if st.feed != nil {
		return r.follow(ctx, m, st, src, dst)
	}
	return r.copyRound(ctx, m.ID, src, dst)
}

// copyRound catches up a source without a change feed by mirroring it:
// new and changed objects are copied, deleted ones removed from the target,
// which Prepare made sure holds nothing else. Cutover may start once a
// round changes nothing and the target holds exactly the source's objects.
func (r *Rclone) copyRound(ctx context.Context, id string, src, dst fs.Fs) (SyncReport, error) {
	start := r.now()
	res, err := copier.Mirror(ctx, src, dst, r.Options, nil)
	if err != nil {
		return SyncReport{}, fmt.Errorf("mirror: %w", err)
	}
	if err := r.applyDeletes(ctx, id, dst, start); err != nil {
		return SyncReport{}, err
	}
	idle := migration.Check{
		Name:    "no_new_changes",
		OK:      res.Copied == 0 && res.Deleted == 0,
		Message: fmt.Sprintf("last round copied %d objects and deleted %d", res.Copied, res.Deleted),
	}
	if !idle.OK {
		return SyncReport{Cutover: migration.Decide(idle), Backlog: res.Copied + res.Deleted}, nil
	}
	same, d, err := r.compare(ctx, src, dst)
	if err != nil {
		return SyncReport{}, err
	}
	return SyncReport{Cutover: migration.Decide(idle, same), Backlog: d.Missing + d.Differ}, nil
}

// Verify runs after resume, when apps write to the target. Each call is
// also a round of the reverse copy: what apps wrote to the target is
// copied back to the source, so a rollback loses nothing. It does not
// delete from the source.
func (r *Rclone) Verify(ctx context.Context, m migration.Migration) (VerifyReport, error) {
	src, dst, err := r.pair(ctx, m)
	if err != nil {
		return VerifyReport{}, err
	}
	back, err := r.copyBack(ctx, m.ID, src, dst)
	if err != nil {
		return VerifyReport{}, err
	}
	d, err := copier.Check(ctx, src, dst, r.Options)
	if err != nil {
		return VerifyReport{}, fmt.Errorf("check: %w", err)
	}
	// A source object missing from the target was either deleted by an
	// app whose delete of the source copy has not happened yet, or written
	// by a request that started before the switch and ended after resume.
	// Settle each: syncKey removes the first and copies the second.
	if int64(len(d.MissingKeys)) < d.Missing {
		// Too many to name: copy in bulk, honor deletes, and look again.
		start := r.now()
		if _, err := copier.Copy(ctx, src, dst, r.Options, nil); err != nil {
			return VerifyReport{}, fmt.Errorf("copy late objects: %w", err)
		}
		if err := r.applyDeletes(ctx, m.ID, dst, start); err != nil {
			return VerifyReport{}, err
		}
		if d, err = copier.Check(ctx, src, dst, r.Options); err != nil {
			return VerifyReport{}, fmt.Errorf("check: %w", err)
		}
	}
	if int64(len(d.MissingKeys)) == d.Missing {
		for _, key := range d.MissingKeys {
			if r.Deletes.Has(m.ID, key) {
				if err := copier.Delete(ctx, src, key); err != nil {
					return VerifyReport{}, fmt.Errorf("remove deleted %q: %w", key, err)
				}
				d.Missing--
				continue
			}
			if act, err := r.syncKey(ctx, m.ID, src, dst, key); err != nil {
				return VerifyReport{}, fmt.Errorf("copy late %q: %w", key, err)
			} else if act == copier.Copied || act == copier.Deleted {
				d.Missing--
			}
		}
	}
	return VerifyReport{
		Advance: migration.Decide(migration.Check{
			Name:    "target_has_source",
			OK:      d.Missing == 0 && d.Differ == 0,
			Message: fmt.Sprintf("%d source objects missing from the target, %d different", d.Missing, d.Differ),
		}),
		// Rolling back copies the rest; what matters is that the reverse
		// copy works.
		Rollback: migration.Decide(migration.Check{
			Name:    "reverse_copy",
			OK:      true,
			Message: fmt.Sprintf("copied %d objects back to the source; %d new in the target since", back, d.Extra),
		}),
		ReverseBacklog: d.Extra,
	}, nil
}

func (r *Rclone) compare(ctx context.Context, src, dst fs.Fs) (migration.Check, copier.Diff, error) {
	d, err := copier.Check(ctx, src, dst, r.Options)
	if err != nil {
		return migration.Check{}, d, fmt.Errorf("check: %w", err)
	}
	return migration.Check{
		Name:    "target_matches_source",
		OK:      d.Same(),
		Message: fmt.Sprintf("%d missing, %d different, %d only in target, %d identical", d.Missing, d.Differ, d.Extra, d.Matched),
	}, d, nil
}

// pair connects to both sides of a migration. A credentials reference that
// cannot be resolved is permanent: retrying cannot fix it.
func (r *Rclone) pair(ctx context.Context, m migration.Migration) (src, dst fs.Fs, err error) {
	if src, err = r.open(ctx, "source", m.Source); err != nil {
		return nil, nil, openError("source", err)
	}
	if dst, err = r.open(ctx, "target", m.Target); err != nil {
		return nil, nil, openError("target", err)
	}
	return src, dst, nil
}

func openError(side string, err error) error {
	if errors.Is(err, creds.ErrBadRef) {
		return fmt.Errorf("%w: open %s: %w", ErrPermanent, side, err)
	}
	return fmt.Errorf("open %s: %w", side, err)
}

// Switching is not implemented yet, so these steps have nothing to do.

// Freeze has nothing to do: keys are never rewritten, so writes need not
// stop. Switch has nothing to do either: the worker moves the apps, and
// changes they make to the source meanwhile wait in the feed.
func (r *Rclone) Freeze(context.Context, migration.Migration) error { return nil }
func (r *Rclone) Switch(context.Context, migration.Migration) error { return nil }

// Resume runs once every app writes to the target. It handles the source
// changes still waiting in the feed, then copies the source once more in
// case an event was lost. It copies rather than mirrors: the target now
// holds the apps' new objects.
func (r *Rclone) Resume(ctx context.Context, m migration.Migration) error {
	src, dst, err := r.pair(ctx, m)
	if err != nil {
		return err
	}
	st, err := r.stream(ctx, m)
	if err != nil {
		return err
	}
	if st.feed != nil {
		if err := r.drain(ctx, m, st, src, dst); err != nil {
			return err
		}
	}
	start := r.now()
	if _, err := copier.Copy(ctx, src, dst, r.Options, nil); err != nil {
		return fmt.Errorf("final copy: %w", err)
	}
	// After a restart, also remove what a copy cut short by it brought back.
	since := start
	if !st.unapplied.IsZero() && st.unapplied.Before(since) {
		since = st.unapplied
	}
	if err := r.applyDeletes(ctx, m.ID, dst, since); err != nil {
		return err
	}
	st.unapplied = time.Time{}
	return nil
}

// Rollback runs once every app writes to the source again and still reads
// the target. It copies the target's new objects back, so the target can
// be dropped from reads.
func (r *Rclone) Rollback(ctx context.Context, m migration.Migration) error {
	src, dst, err := r.pair(ctx, m)
	if err != nil {
		return err
	}
	if _, err := r.copyBack(ctx, m.ID, src, dst); err != nil {
		return err
	}
	d, err := copier.Check(ctx, dst, src, r.Options)
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}
	if d.Missing > 0 || d.Differ > 0 {
		return fmt.Errorf("source still lacks %d target objects and %d differ", d.Missing, d.Differ)
	}
	return nil
}

// copyBack copies the objects only in the target to the source and returns
// how many. It goes key by key and looks at each key again afterwards: an
// app deletes from the target first, so a key gone from the target by then
// was deleted while it was copied, and is deleted from the source too.
// Only target-only keys are touched; the source's other objects are not.
func (r *Rclone) copyBack(ctx context.Context, id string, src, dst fs.Fs) (int64, error) {
	d, err := copier.Check(ctx, dst, src, r.Options)
	if err != nil {
		return 0, fmt.Errorf("reverse copy: %w", err)
	}
	keys := d.MissingKeys
	if int64(len(keys)) < d.Missing {
		// Too many to name: copy in bulk, then settle the listed ones.
		start := r.now()
		if _, err := copier.Copy(ctx, dst, src, r.Options, nil); err != nil {
			return 0, fmt.Errorf("reverse copy: %w", err)
		}
		if err := r.applyDeletes(ctx, id, src, start); err != nil {
			return 0, err
		}
	}
	var (
		copied atomic.Int64
		mu     sync.Mutex
		first  error
		wg     sync.WaitGroup
		slots  = make(chan struct{}, max(r.Options.Transfers, 1))
	)
	for _, key := range keys {
		wg.Go(func() {
			slots <- struct{}{}
			defer func() { <-slots }()
			if r.Deletes.Has(id, key) {
				// An app deleted it: from the target first, so either that
				// delete is under way or a copy cut short by a restart
				// brought the key back. It goes from both.
				if err := errors.Join(copier.Delete(ctx, dst, key), copier.Delete(ctx, src, key)); err != nil {
					mu.Lock()
					first = cmp.Or(first, fmt.Errorf("remove deleted %q: %w", key, err))
					mu.Unlock()
				}
				return
			}
			for pass := range 2 {
				act, err := r.syncKey(ctx, id, dst, src, key)
				if err != nil {
					mu.Lock()
					first = cmp.Or(first, fmt.Errorf("reverse copy %q: %w", key, err))
					mu.Unlock()
					return
				}
				if pass == 0 && act == copier.Copied {
					copied.Add(1)
				}
			}
		})
	}
	wg.Wait()
	return copied.Load(), first
}

// syncKey is copier.SyncKey that honors the keys apps deleted. An app
// records a key before deleting it, so a key copied and then found recorded
// was deleted while it was copied, and the copy is removed.
func (r *Rclone) syncKey(ctx context.Context, id string, from, to fs.Fs, key string) (copier.Action, error) {
	if r.Deletes.Has(id, key) {
		return copier.Deleted, copier.Delete(ctx, to, key)
	}
	act, err := copier.SyncKey(ctx, from, to, key)
	if err == nil && act == copier.Copied && r.Deletes.Has(id, key) {
		return copier.Deleted, copier.Delete(ctx, to, key)
	}
	return act, err
}

// deleteMargin is how long before a bulk copy started its deletes are
// checked: an app records a key a moment before deleting it.
const deleteMargin = time.Minute

// applyDeletes removes from f the keys apps deleted since a little before
// a bulk copy into f started, which the copy may have brought back.
func (r *Rclone) applyDeletes(ctx context.Context, id string, f fs.Fs, copyStart time.Time) error {
	for _, key := range r.Deletes.Since(id, copyStart.Add(-deleteMargin)) {
		if err := copier.Delete(ctx, f, key); err != nil {
			return fmt.Errorf("remove deleted %q: %w", key, err)
		}
	}
	return nil
}

func (r *Rclone) Finalize(context.Context, migration.Migration) error { return nil }
func (r *Rclone) Abort(context.Context, migration.Migration) error    { return nil }

// Direction is one pair of storage kinds the engine migrates between, with
// how it follows the source's changes and how it compares objects.
type Direction struct {
	Source     migration.Kind `json:"source"`
	Target     migration.Kind `json:"target"`
	ChangeFeed string         `json:"changeFeed"`
	Compare    string         `json:"compare"`
}

// Directions lists every source and target kind the engine supports.
func (r *Rclone) Directions() []Direction {
	kinds := []migration.Kind{migration.KindS3, migration.KindGCS, migration.KindSeaweedFS}
	feed := map[migration.Kind]string{
		migration.KindS3:        "sqs",
		migration.KindGCS:       "mirror (set GCP_PROJECT for pubsub)",
		migration.KindSeaweedFS: "filer metadata subscription when the endpoint has filer, else mirror",
	}
	if r.GCPProject != "" {
		feed[migration.KindGCS] = "pubsub"
	}
	var out []Direction
	for _, src := range kinds {
		for _, dst := range kinds {
			// S3 and SeaweedFS keep no MD5 of multipart uploads, nor GCS of
			// composite objects; those compare by size.
			compare := "size and md5; size only for objects without an md5"
			out = append(out, Direction{Source: src, Target: dst, ChangeFeed: feed[src], Compare: compare})
		}
	}
	return out
}
