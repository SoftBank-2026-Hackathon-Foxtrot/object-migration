package copier

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/memory"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
)

// memFs returns a fresh in-memory bucket. Buckets live as long as the
// process, so each call gets its own name.
func memFs(t *testing.T, name string) fs.Fs {
	t.Helper()
	f, err := fs.NewFs(context.Background(), ":memory:"+strings.ToLower(t.Name())+"-"+name+"-"+randomHex(4))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func put(t *testing.T, f fs.Fs, name, body string) {
	t.Helper()
	if _, err := operations.Rcat(context.Background(), f, name, io.NopCloser(strings.NewReader(body)), time.Now(), nil); err != nil {
		t.Fatal(err)
	}
}

func fill(t *testing.T, f fs.Fs, n int) {
	t.Helper()
	for i := range n {
		put(t, f, fmt.Sprintf("dir%d/obj-%03d", i%3, i), fmt.Sprintf("body %d", i))
	}
}

func TestCopyThenCheck(t *testing.T) {
	ctx := context.Background()
	src, dst := memFs(t, "src"), memFs(t, "dst")
	fill(t, src, 30)

	before, err := Check(ctx, src, dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if before.Missing != 30 || before.Same() {
		t.Fatalf("before copy: %+v, want 30 missing", before)
	}

	res, err := Copy(ctx, src, dst, Options{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Copied != 30 || res.Matched != 0 || res.Failed != 0 || res.Bytes == 0 {
		t.Fatalf("first copy: %+v", res)
	}

	after, err := Check(ctx, src, dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !after.Same() || after.Matched != 30 {
		t.Fatalf("after copy: %+v", after)
	}
}

func TestCopyAgainCopiesOnlyWhatChanged(t *testing.T) {
	ctx := context.Background()
	src, dst := memFs(t, "src"), memFs(t, "dst")
	fill(t, src, 20)
	if _, err := Copy(ctx, src, dst, Options{}, nil); err != nil {
		t.Fatal(err)
	}

	put(t, src, "new-object", "new")
	put(t, src, "dir0/obj-000", "changed body") // same key, new content
	res, err := Copy(ctx, src, dst, Options{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Copied != 2 || res.Matched != 19 || res.MatchedBytes == 0 {
		t.Fatalf("second copy: %+v, want 2 copied and 19 matched", res)
	}
}

func TestCopyKeepsTargetOnlyObjects(t *testing.T) {
	ctx := context.Background()
	src, dst := memFs(t, "src"), memFs(t, "dst")
	fill(t, src, 5)
	put(t, dst, "only-in-target", "x")

	if _, err := Copy(ctx, src, dst, Options{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.NewObject(ctx, "only-in-target"); err != nil {
		t.Fatalf("copy deleted a target-only object: %v", err)
	}
	d, err := Check(ctx, src, dst, Options{})
	if err != nil || d.Same() || d.Extra != 1 || d.ExtraKeys[0] != "only-in-target" || d.Missing != 0 {
		t.Fatalf("check: %+v, %v; want the target-only object reported", d, err)
	}
}

func TestCheckFindsDifferences(t *testing.T) {
	ctx := context.Background()
	src, dst := memFs(t, "src"), memFs(t, "dst")
	fill(t, src, 4)
	if _, err := Copy(ctx, src, dst, Options{}, nil); err != nil {
		t.Fatal(err)
	}
	put(t, dst, "dir0/obj-000", "tampered!")
	put(t, src, "late", "x")

	d, err := Check(ctx, src, dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if d.Missing != 1 || d.Differ != 1 || d.Matched != 3 || d.Same() {
		t.Fatalf("got %+v, want 1 missing, 1 differ, 3 matched", d)
	}
}

func TestCopyReportsProgress(t *testing.T) {
	ctx := context.Background()
	src, dst := memFs(t, "src"), memFs(t, "dst")
	fill(t, src, 50)

	var calls int
	_, err := Copy(ctx, src, dst, Options{ProgressInterval: time.Microsecond, Transfers: 1}, func(r Result) {
		calls++
		if r.Copied < 0 || r.Copied > 50 {
			t.Errorf("progress out of range: %+v", r)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls == 0 {
		t.Fatal("progress never reported")
	}
}

func TestCopyStopsWhenCancelled(t *testing.T) {
	src, dst := memFs(t, "src"), memFs(t, "dst")
	fill(t, src, 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Copy(ctx, src, dst, Options{}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	// The next copy finishes the job.
	res, err := Copy(context.Background(), src, dst, Options{}, nil)
	if err != nil || res.Copied+res.Matched != 10 {
		t.Fatalf("resumed copy: %+v, %v", res, err)
	}
}

func TestCount(t *testing.T) {
	f := memFs(t, "b")
	fill(t, f, 7)
	n, size, err := Count(context.Background(), f)
	if err != nil || n != 7 || size == 0 {
		t.Fatalf("Count = %d, %d, %v", n, size, err)
	}
}

func TestProbes(t *testing.T) {
	ctx := context.Background()
	if err := ProbeList(ctx, memFs(t, "missing")); err == nil {
		t.Fatal("listing a bucket that does not exist succeeded")
	}
	f := memFs(t, "b")
	fill(t, f, 3)
	if err := ProbeList(ctx, f); err != nil {
		t.Fatalf("list: %v", err)
	}
	if err := ProbeWrite(ctx, f); err != nil {
		t.Fatalf("write: %v", err)
	}
	n, _, err := Count(ctx, f)
	if err != nil || n != 3 {
		t.Fatalf("probe object left behind: %d objects, %v", n, err)
	}
}

func TestSyncKeyFollowsTheSource(t *testing.T) {
	ctx := context.Background()
	src, dst := memFs(t, "src"), memFs(t, "dst")
	put(t, src, "a", "one")
	put(t, dst, "keep", "x") // the bucket must exist for the memory backend

	steps := []struct {
		name string
		do   func()
		want Action
	}{
		{"new object", func() {}, Copied},
		{"again", func() {}, Unchanged},
		{"changed", func() { put(t, src, "a", "two!") }, Copied},
		{"deleted", func() {
			if err := Delete(ctx, src, "a"); err != nil {
				t.Fatal(err)
			}
		}, Deleted},
		{"deleted again", func() {}, Unchanged},
	}
	for _, s := range steps {
		s.do()
		got, err := SyncKey(ctx, src, dst, "a")
		if err != nil || got != s.want {
			t.Fatalf("%s: %q, %v; want %q", s.name, got, err, s.want)
		}
	}
	if _, err := dst.NewObject(ctx, "keep"); err != nil {
		t.Fatalf("other objects were touched: %v", err)
	}
}

func TestInternalObjectsAreLeftOut(t *testing.T) {
	ctx := context.Background()
	src, dst := memFs(t, "src"), memFs(t, "dst")
	fill(t, src, 3)
	put(t, src, ProbePrefix+"canary-1", "x")

	res, err := Copy(ctx, src, dst, Options{}, nil)
	if err != nil || res.Copied != 3 {
		t.Fatalf("copy: %+v, %v", res, err)
	}
	if _, err := dst.NewObject(ctx, ProbePrefix+"canary-1"); err == nil {
		t.Fatal("internal object was copied")
	}
	n, _, err := Count(ctx, src)
	if err != nil || n != 3 {
		t.Fatalf("count = %d, %v", n, err)
	}
	d, err := Check(ctx, src, dst, Options{})
	if err != nil || !d.Same() {
		t.Fatalf("check: %+v, %v", d, err)
	}
}

func TestCheckNamesMissingKeys(t *testing.T) {
	ctx := context.Background()
	src, dst := memFs(t, "src"), memFs(t, "dst")
	put(t, src, "x/one", "1")
	put(t, src, "x/two", "2")
	put(t, dst, "x/one", "1")
	d, err := Check(ctx, src, dst, Options{})
	if err != nil || d.Missing != 1 || len(d.MissingKeys) != 1 || d.MissingKeys[0] != "x/two" {
		t.Fatalf("diff = %+v, %v", d, err)
	}
}

func TestMirrorDeletesTargetOnlyObjects(t *testing.T) {
	ctx := context.Background()
	src, dst := memFs(t, "src"), memFs(t, "dst")
	fill(t, src, 4)
	put(t, dst, "stale", "deleted from the source long ago")
	put(t, dst, ProbePrefix+"preflight-x", "internal")

	res, err := Mirror(ctx, src, dst, Options{}, nil)
	if err != nil || res.Copied != 4 || res.Deleted != 1 {
		t.Fatalf("mirror: %+v, %v", res, err)
	}
	if _, err := dst.NewObject(ctx, "stale"); err == nil {
		t.Fatal("stale object kept")
	}
	if _, err := dst.NewObject(ctx, ProbePrefix+"preflight-x"); err != nil {
		t.Fatalf("internal object deleted: %v", err)
	}
	d, err := Check(ctx, src, dst, Options{})
	if err != nil || !d.Same() {
		t.Fatalf("check: %+v, %v", d, err)
	}
}

func TestSetLoggerDropsComparisonLines(t *testing.T) {
	var buf bytes.Buffer
	h := rcloneHandler{slog.NewTextHandler(&buf, nil)}
	log := slog.New(h).With("object", "a.txt")
	log.Error("file not in S3 bucket dst")
	log.Error("failed to copy: access denied")
	if got := buf.String(); strings.Contains(got, "file not in") || !strings.Contains(got, "access denied") {
		t.Fatalf("log = %q", got)
	}
}
