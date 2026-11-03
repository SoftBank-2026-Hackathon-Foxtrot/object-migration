package copier

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
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
	// Bytes is how much was written to the target, counting objects still
	// being written.
	Bytes int64
	// MatchedBytes is the size of the matched objects.
	MatchedBytes int64
}

// Copy copies every source object that is missing or different in the
// target. It deletes nothing, so it is safe to run again at any time, and a
// copy that was cancelled continues where it stopped. progress is called
// every Options.ProgressInterval while the copy runs.
func Copy(ctx context.Context, src, dst fs.Fs, opt Options, progress func(Result)) (Result, error) {
	opt = opt.withDefaults()
	ctx, ci := fs.AddConfig(ctx)
	// Compare by size and hash: modification times do not survive every
	// pair of storage systems, and the copy must not rewrite objects to fix
	// them.
	ci.CheckSum = true
	ci.NoUpdateModTime = true
	ci.Transfers, ci.Checkers = opt.Transfers, opt.Checkers

	ctx, stats, release := statsGroup(ctx)
	defer release()
	var matched, matchedBytes, failed atomic.Int64
	ctx = operations.WithLogger(ctx, func(_ context.Context, sigil operations.Sigil, src, _ fs.DirEntry, _ error) {
		if _, dir := src.(fs.Directory); dir {
			return
		}
		switch sigil {
		case operations.Match:
			matched.Add(1)
			if src != nil {
				matchedBytes.Add(max(src.Size(), 0))
			}
		case operations.TransferError:
			failed.Add(1)
		}
	})
	snapshot := func() Result {
		return Result{
			Copied: stats.GetTransfers(), Matched: matched.Load(), Failed: failed.Load(),
			Bytes: stats.GetBytes(), MatchedBytes: matchedBytes.Load(),
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
	err := rsync.CopyDir(ctx, dst, src, false)
	if err == nil {
		// A cancelled copy can end without an error; it is still not done.
		err = ctx.Err()
	}
	close(stop)
	wg.Wait()
	return snapshot(), err
}

// Diff is how the target differs from the source. Objects only in the
// target are not counted.
type Diff struct {
	// Missing objects are in the source but not in the target.
	Missing int64
	// Differ objects are in both but with a different size or hash.
	Differ int64
	// Matched objects are identical in both.
	Matched int64
}

// Same reports whether every source object is identical in the target.
func (d Diff) Same() bool { return d.Missing == 0 && d.Differ == 0 }

// Check compares every source object with the target by size and hash.
func Check(ctx context.Context, src, dst fs.Fs, opt Options) (Diff, error) {
	opt = opt.withDefaults()
	ctx, ci := fs.AddConfig(ctx)
	ci.Checkers = opt.Checkers
	ctx, _, release := statsGroup(ctx)
	defer release()

	var missing, differ, match lineCounter
	err := operations.Check(ctx, &operations.CheckOpt{
		Fdst: dst, Fsrc: src, OneWay: true,
		MissingOnDst: &missing, Differ: &differ, Match: &match,
	})
	d := Diff{Missing: missing.n.Load(), Differ: differ.n.Load(), Matched: match.n.Load()}
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
	ctx, _, release := statsGroup(ctx)
	defer release()
	objects, bytes, _, err = operations.Count(ctx, f)
	return objects, bytes, err
}

// lineCounter counts the lines rclone writes, one per object.
type lineCounter struct{ n atomic.Int64 }

func (c *lineCounter) Write(p []byte) (int, error) {
	c.n.Add(int64(strings.Count(string(p), "\n")))
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

// ProbePrefix holds the objects preflight writes. The app must ignore it.
const ProbePrefix = ".object-migration/"

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
	// rclone logs to stderr on its own; keep it to errors so the server's
	// JSON log stays readable.
	fs.GetConfig(context.Background()).LogLevel = fs.LogLevelError
}
