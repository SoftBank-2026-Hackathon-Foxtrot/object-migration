package copier

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	if err != nil || !d.Same() {
		t.Fatalf("check: %+v, %v", d, err)
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
