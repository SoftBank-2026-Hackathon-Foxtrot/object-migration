package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/memory"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/copier"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

// memEngine returns an Rclone engine whose buckets live in memory, and a
// migration between two of them.
func memEngine(t *testing.T) (*Rclone, migration.Migration, fs.Fs, fs.Fs) {
	t.Helper()
	// In-memory buckets live as long as the process, so each run gets its own.
	prefix := strings.ToLower(t.Name()) + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	r := NewRclone(copier.Options{ProgressInterval: time.Millisecond})
	r.open = func(ctx context.Context, _ string, e migration.Endpoint) (fs.Fs, error) {
		return fs.NewFs(ctx, ":memory:"+prefix+"-"+e.Bucket)
	}
	m, err := migration.New(
		migration.Endpoint{Kind: migration.KindS3, Bucket: "src", Region: "ap-northeast-1", Ref: "default"},
		migration.Endpoint{Kind: migration.KindGCS, Bucket: "dst", Ref: "default"},
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	src, _ := r.open(context.Background(), "", m.Source)
	dst, _ := r.open(context.Background(), "", m.Target)
	return r, m, src, dst
}

func putObject(t *testing.T, f fs.Fs, name string) {
	t.Helper()
	_, err := operations.Rcat(context.Background(), f, name, io.NopCloser(strings.NewReader("body of "+name)), time.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRcloneInitialSyncCopiesAndReportsProgress(t *testing.T) {
	r, m, src, dst := memEngine(t)
	for i := range 25 {
		putObject(t, src, fmt.Sprintf("p/%02d", i))
	}
	var last migration.Progress
	var reports int
	if err := r.InitialSync(context.Background(), m, func(p migration.Progress) { last = p; reports++ }); err != nil {
		t.Fatal(err)
	}
	if last.Completed != 25 || last.Total != 25 || last.Unit != "objects" || reports < 2 {
		t.Fatalf("last progress %+v after %d reports", last, reports)
	}
	if last.Bytes == 0 || last.Bytes != last.TotalBytes {
		t.Fatalf("bytes %d of %d", last.Bytes, last.TotalBytes)
	}

	// Run again: everything is already there and counts as done.
	if err := r.InitialSync(context.Background(), m, func(p migration.Progress) { last = p }); err != nil {
		t.Fatal(err)
	}
	if last.Completed != 25 || last.Bytes != last.TotalBytes {
		t.Fatalf("second run: %+v", last)
	}
	n, _, err := copier.Count(context.Background(), dst)
	if err != nil || n != 25 {
		t.Fatalf("target has %d objects, %v", n, err)
	}
}

func TestRcloneSyncIsReadyOnlyWhenCaughtUp(t *testing.T) {
	ctx := context.Background()
	r, m, src, _ := memEngine(t)
	putObject(t, src, "a")
	if err := r.InitialSync(ctx, m, func(migration.Progress) {}); err != nil {
		t.Fatal(err)
	}

	putObject(t, src, "b")
	rep, err := r.Sync(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Cutover.Result != migration.ResultFail || rep.Backlog != 1 {
		t.Fatalf("round with a new object: %+v", rep)
	}

	rep, err = r.Sync(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Cutover.Result != migration.ResultPass || rep.Backlog != 0 || len(rep.Cutover.Checks) != 2 {
		t.Fatalf("idle round: %+v", rep)
	}

	v, err := r.Verify(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if v.Advance.Result != migration.ResultPass || v.Rollback.Result != migration.ResultPass {
		t.Fatalf("verify: %+v", v)
	}
}

func TestRcloneVerifyFailsWhenTargetDiffers(t *testing.T) {
	ctx := context.Background()
	r, m, src, dst := memEngine(t)
	putObject(t, src, "a")
	if err := r.InitialSync(ctx, m, func(migration.Progress) {}); err != nil {
		t.Fatal(err)
	}
	if err := operations.Purge(ctx, dst, ""); err != nil {
		t.Fatal(err)
	}
	v, err := r.Verify(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if v.Advance.Result != migration.ResultFail {
		t.Fatalf("verify passed with an empty target: %+v", v.Advance)
	}
}

func TestRclonePreflight(t *testing.T) {
	r, m, src, _ := memEngine(t)
	putObject(t, src, "a")
	// The in-memory target bucket only exists once something is in it.
	dst, _ := r.open(context.Background(), "", m.Target)
	putObject(t, dst, "existing")

	checks := r.Preflight(context.Background(), m.Source, m.Target)
	want := []string{"source_read", "target_read", "target_write"}
	if len(checks) != len(want) {
		t.Fatalf("checks: %+v", checks)
	}
	for i, c := range checks {
		if c.Name != want[i] || !c.OK {
			t.Fatalf("check %d: %+v", i, c)
		}
	}
}

func TestRclonePreflightReportsMissingBucket(t *testing.T) {
	r, m, src, _ := memEngine(t)
	putObject(t, src, "a")
	checks := r.Preflight(context.Background(), m.Source, m.Target)
	if len(checks) != 2 || !checks[0].OK || checks[1].OK || checks[1].Message == "" {
		t.Fatalf("want source ok, target_read failed and no write check: %+v", checks)
	}
}

func TestRcloneBadRefIsPermanent(t *testing.T) {
	r := NewRclone(copier.Options{})
	m, err := migration.New(
		migration.Endpoint{Kind: migration.KindS3, Bucket: "src", Region: "ap-northeast-1", Ref: "env:OM_TEST_UNSET"},
		migration.Endpoint{Kind: migration.KindGCS, Bucket: "dst", Ref: "default"},
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Sync(context.Background(), m)
	if !errors.Is(err, ErrPermanent) || !errors.Is(err, copier.ErrBadRef) {
		t.Fatalf("err = %v, want a permanent bad ref error", err)
	}
}
