package copier

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/rc"
	rsync "github.com/rclone/rclone/fs/sync"
)

// Options tunes a copy.
type Options struct {
	// Transfers is how many objects are copied at once.
	Transfers int
	// Checkers is how many objects are compared at once.
	Checkers int
	// ProgressInterval is how often Copy reports progress.
	ProgressInterval time.Duration
}

func (o Options) withDefaults() Options {
	if o.Transfers <= 0 {
		o.Transfers = 16
	}
	if o.Checkers <= 0 {
		o.Checkers = 16
	}
	if o.ProgressInterval <= 0 {
		o.ProgressInterval = time.Second
	}
	return o
}

// Result counts what a copy did with the source's objects.
type Result struct {
	// Copied objects were missing or different in the target and are now
	// written.
	Copied int64
	// Matched objects were already identical in the target.
	Matched int64
	// Failed objects could not be copied this time.
	Failed int64
	// Vanished objects were listed but deleted from the source before they
	// could be copied. They are not failures: the deletion is the newer
	// state.
	Vanished int64
	// Bytes is how much was written to the target, counting objects still
	// being written.
	Bytes int64
	// MatchedBytes is the size of the matched objects.
	MatchedBytes int64
	// Deleted objects were only in the target and are now removed. Only
	// Mirror deletes.
	Deleted int64
}

// Copy copies every source object that is missing or different in the
// target. It deletes nothing, so it is safe to run again at any time, and a
// copy that was cancelled continues where it stopped. progress is called
// every Options.ProgressInterval while the copy runs.
func Copy(ctx context.Context, src, dst fs.Fs, opt Options, progress func(Result)) (Result, error) {
	return transfer(ctx, src, dst, opt, progress, false)
}

// Mirror is Copy that also deletes target objects the source does not
// have, so the target ends up holding exactly the source's objects. Use it
// only on a target that holds nothing but copies from this source.
func Mirror(ctx context.Context, src, dst fs.Fs, opt Options, progress func(Result)) (Result, error) {
	return transfer(ctx, src, dst, opt, progress, true)
}

func transfer(ctx context.Context, src, dst fs.Fs, opt Options, progress func(Result), mirror bool) (Result, error) {
	opt = opt.withDefaults()
	ctx = compareConfig(ctx, opt)
	ctx, stats, release := statsGroup(ctx)
	defer release()
	var matched, matchedBytes, failed, vanished atomic.Int64
	ctx = operations.WithLogger(ctx, func(ctx context.Context, sigil operations.Sigil, obj, _ fs.DirEntry, _ error) {
		if _, dir := obj.(fs.Directory); dir {
			return
		}
		switch sigil {
		case operations.Match:
			matched.Add(1)
			if obj != nil {
				matchedBytes.Add(max(obj.Size(), 0))
			}
		case operations.TransferError:
			if obj != nil && gone(ctx, src, obj.Remote()) {
				vanished.Add(1)
			} else {
				failed.Add(1)
			}
		}
	})
	snapshot := func() Result {
		return Result{
			Copied: stats.GetTransfers(), Matched: matched.Load(), Failed: failed.Load(), Vanished: vanished.Load(),
			Bytes: stats.GetBytes(), MatchedBytes: matchedBytes.Load(), Deleted: stats.GetDeletes(),
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	if progress != nil {
		wg.Go(func() {
			t := time.NewTicker(opt.ProgressInterval)
			defer t.Stop()
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					progress(snapshot())
				}
			}
		})
	}
	var err error
	if mirror {
		err = rsync.Sync(ctx, dst, src, false)
	} else {
		err = rsync.CopyDir(ctx, dst, src, false)
	}
	close(stop)
	wg.Wait()
	res := snapshot()
	if err != nil && ctx.Err() == nil && res.Failed == 0 && res.Vanished > 0 && stats.GetErrors() <= res.Vanished {
		// Every error was an object deleted while we copied.
		err = nil
	}
	if err == nil {
		// A cancelled copy can end without an error; it is still not done.
		err = ctx.Err()
	}
	return res, err
}

// compareConfig sets how objects are compared and which are left out.
func compareConfig(ctx context.Context, opt Options) context.Context {
	ctx, ci := fs.AddConfig(ctx)
	// Compare by size and hash: modification times do not survive every
	// pair of storage systems, and the copy must not rewrite objects to fix
	// them.
	ci.CheckSum = true
	ci.NoUpdateModTime = true
	ci.Transfers, ci.Checkers = opt.Transfers, opt.Checkers
	return filter.ReplaceConfig(ctx, internalFilter)
}

// internalFilter leaves out the objects this server writes for itself.
var internalFilter = func() *filter.Filter {
	opt := filter.Opt // the defaults, which turn off age and size limits
	opt.ExcludeRule = []string{"/" + ProbePrefix + "**"}
	f, err := filter.NewFilter(&opt)
	if err != nil {
		panic(err)
	}
	return f
}()

// gone reports whether an object is no longer in a bucket.
func gone(ctx context.Context, f fs.Fs, remote string) bool {
	_, err := f.NewObject(ctx, remote)
	return errors.Is(err, fs.ErrorObjectNotFound)
}

// Action is what SyncKey did.
type Action string

const (
	Copied    Action = "copied"
	Deleted   Action = "deleted"
	Unchanged Action = "unchanged"
)

// SyncKey makes one key in the target match the source: it copies the
// object if the target lacks it or holds a different one, and deletes it
// from the target if the source no longer has it. It looks at the source
// rather than at what an event said, so running it late, twice or out of
// order still ends in the right state.
func SyncKey(ctx context.Context, src, dst fs.Fs, key string) (Action, error) {
	ctx = compareConfig(ctx, Options{}.withDefaults())
	ctx, _, release := statsGroup(ctx)
	defer release()

	so, err := src.NewObject(ctx, key)
	if errors.Is(err, fs.ErrorObjectNotFound) {
		do, err := dst.NewObject(ctx, key)
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return Unchanged, nil
		}
		if err != nil {
			return "", err
		}
		if err := do.Remove(ctx); err != nil && !gone(ctx, dst, key) {
			return "", err
		}
		return Deleted, nil
	}
	if err != nil {
		return "", err
	}
	do, err := dst.NewObject(ctx, key)
	if errors.Is(err, fs.ErrorObjectNotFound) {
		do = nil
	} else if err != nil {
		return "", err
	}
	if do != nil && !operations.NeedTransfer(ctx, do, so) {
		return Unchanged, nil
	}
	if _, err := operations.Copy(ctx, dst, do, key, so); err != nil {
		if gone(ctx, src, key) {
			// Deleted meanwhile; the next event for it removes the copy.
			return Unchanged, nil
		}
		return "", err
	}
	return Copied, nil
}

// Diff is how the target differs from the source.
type Diff struct {
	// Missing objects are in the source but not in the target.
	Missing int64
	// Differ objects are in both but with a different size or hash.
	Differ int64
	// Matched objects are identical in both.
	Matched int64
	// Extra objects are in the target but not in the source: deleted from
	// the source without the deletion reaching the target, or there before
	// the migration started.
	Extra int64
	// MissingKeys and ExtraKeys name up to MaxListedKeys of each.
	MissingKeys, ExtraKeys []string
}

// MaxListedKeys bounds Diff.MissingKeys.
const MaxListedKeys = 1000

// Same reports whether the target holds exactly the source's objects.
func (d Diff) Same() bool { return d.Missing == 0 && d.Differ == 0 && d.Extra == 0 }

// Check compares source and target by size and hash, both ways.
func Check(ctx context.Context, src, dst fs.Fs, opt Options) (Diff, error) {
	ctx = compareConfig(ctx, opt.withDefaults())
	ctx, _, release := statsGroup(ctx)
	defer release()

	var missing, extra, differ, match lineCounter
	missing.keep, extra.keep = MaxListedKeys, MaxListedKeys
	err := operations.Check(ctx, &operations.CheckOpt{
		Fdst: dst, Fsrc: src,
		MissingOnDst: &missing, MissingOnSrc: &extra, Differ: &differ, Match: &match,
	})
	d := Diff{
		Missing: missing.n.Load(), Extra: extra.n.Load(), Differ: differ.n.Load(), Matched: match.n.Load(),
		MissingKeys: missing.lines, ExtraKeys: extra.lines,
	}
	// Check also fails when it finds differences; that is the answer, not
	// an error.
	if err != nil && d.Same() {
		return d, err
	}
	return d, nil
}

// Count lists a bucket and returns how many objects it holds and their
// total size.
func Count(ctx context.Context, f fs.Fs) (objects, bytes int64, err error) {
	ctx = filter.ReplaceConfig(ctx, internalFilter)
	ctx, _, release := statsGroup(ctx)
	defer release()
	objects, bytes, _, err = operations.Count(ctx, f)
	return objects, bytes, err
}

// lineCounter counts the lines rclone writes, one per object, and keeps
// the first keep of them.
type lineCounter struct {
	n     atomic.Int64
	keep  int
	mu    sync.Mutex
	lines []string
}

func (c *lineCounter) Write(p []byte) (int, error) {
	c.n.Add(int64(strings.Count(string(p), "\n")))
	if c.keep > 0 {
		c.mu.Lock()
		for line := range strings.SplitSeq(strings.TrimSuffix(string(p), "\n"), "\n") {
			if len(c.lines) < c.keep && line != "" {
				c.lines = append(c.lines, line)
			}
		}
		c.mu.Unlock()
	}
	return len(p), nil
}

// statsGroup gives one call its own rclone counters, so concurrent
// migrations do not add up each other's numbers.
func statsGroup(ctx context.Context) (context.Context, *accounting.StatsInfo, func()) {
	group := "om-" + randomHex(8)
	ctx = accounting.WithStatsGroup(ctx, group)
	stats := accounting.StatsGroup(ctx, group)
	return ctx, stats, func() {
		if call := rc.Calls.Get("core/stats-delete"); call != nil {
			_, _ = call.Fn(context.Background(), rc.Params{"group": group})
		}
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// errFound stops a listing once it has seen an entry.
var errFound = errors.New("found")

// ProbeList lists the start of a bucket, which proves the credentials can
// read it, without walking the whole bucket.
func ProbeList(ctx context.Context, f fs.Fs) error {
	var err error
	if lp, ok := f.(fs.ListPer); ok {
		err = lp.ListP(ctx, "", func(fs.DirEntries) error { return errFound })
		if errors.Is(err, errFound) {
			err = nil
		}
	} else {
		_, err = f.List(ctx, "")
	}
	if errors.Is(err, fs.ErrorDirNotFound) {
		return fmt.Errorf("bucket %q not found", f.Root())
	}
	return err
}

// ProbePrefix holds the objects this server writes for itself, such as
// preflight probes and canaries. Copies and checks leave it out, and the
// app must ignore it.
const ProbePrefix = ".object-migration/"

// Put writes a small object.
func Put(ctx context.Context, f fs.Fs, key, body string) error {
	_, err := operations.Rcat(ctx, f, key, io.NopCloser(strings.NewReader(body)), time.Now(), nil)
	return err
}

// Delete removes an object if it is there. Someone else deleting it at the
// same time is fine.
func Delete(ctx context.Context, f fs.Fs, key string) error {
	o, err := f.NewObject(ctx, key)
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := o.Remove(ctx); err != nil && !gone(ctx, f, key) {
		return err
	}
	return nil
}

// DeleteAll removes every object whose key starts with prefix. Only the
// prefix's directory is listed, not the whole bucket.
func DeleteAll(ctx context.Context, f fs.Fs, prefix string) error {
	dir := path.Dir(prefix)
	if dir == "." {
		dir = ""
	}
	entries, err := f.List(ctx, dir)
	if errors.Is(err, fs.ErrorDirNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if o, ok := e.(fs.Object); ok && strings.HasPrefix(o.Remote(), prefix) {
			if err := o.Remove(ctx); err != nil && !errors.Is(err, fs.ErrorObjectNotFound) {
				return err
			}
		}
	}
	return nil
}

// ModTime is when an object was last written.
func ModTime(ctx context.Context, f fs.Fs, key string) (time.Time, error) {
	o, err := f.NewObject(ctx, key)
	if err != nil {
		return time.Time{}, err
	}
	return o.ModTime(ctx), nil
}

// ProbeWrite writes a small object under ProbePrefix and deletes it again,
// which proves the credentials can write the bucket.
func ProbeWrite(ctx context.Context, f fs.Fs) error {
	name := ProbePrefix + "preflight-" + randomHex(8)
	obj, err := operations.Rcat(ctx, f, name, io.NopCloser(strings.NewReader("object-migration preflight\n")), time.Now(), nil)
	if err != nil {
		return err
	}
	return obj.Remove(ctx)
}

func init() {
	// Keep rclone to errors so the server's log stays readable.
	fs.GetConfig(context.Background()).LogLevel = fs.LogLevelError
}
