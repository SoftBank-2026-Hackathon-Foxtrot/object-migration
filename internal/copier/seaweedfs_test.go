package copier

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/operations"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

// seaweedBuckets connects to two buckets of a running SeaweedFS, named by
// OM_SEAWEEDFS_URL (its S3 endpoint), OM_SEAWEEDFS_SRC and OM_SEAWEEDFS_DST,
// with credentials in SEAWEED_ACCESS_KEY_ID and SEAWEED_SECRET_ACCESS_KEY.
// The test is skipped without them.
func seaweedBuckets(t *testing.T) (fs.Fs, fs.Fs) {
	t.Helper()
	url := os.Getenv("OM_SEAWEEDFS_URL")
	if url == "" {
		t.Skip("OM_SEAWEEDFS_URL not set")
	}
	open := func(bucket string) fs.Fs {
		f, err := Open(context.Background(), "seaweedfs", migration.Endpoint{
			Kind: migration.KindSeaweedFS, Bucket: bucket, URL: url, Ref: "env:SEAWEED",
		})
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	return open(os.Getenv("OM_SEAWEEDFS_SRC")), open(os.Getenv("OM_SEAWEEDFS_DST"))
}

func randomBody(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// TestSeaweedFSCompatibility checks the S3 behaviors the migration relies
// on against a real SeaweedFS.
func TestSeaweedFSCompatibility(t *testing.T) {
	ctx := context.Background()
	src, dst := seaweedBuckets(t)
	prefix := "compat-" + time.Now().Format("150405.000") + "/"
	t.Cleanup(func() {
		for _, f := range []fs.Fs{src, dst} {
			_ = operations.Purge(ctx, f, prefix[:len(prefix)-1])
		}
	})

	// Upload and download, with the modification time kept as metadata.
	body := randomBody(t, 64<<10)
	mtime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if _, err := operations.Rcat(ctx, src, prefix+"small.bin", io.NopCloser(bytes.NewReader(body)), mtime, nil); err != nil {
		t.Fatalf("upload: %v", err)
	}
	o, err := src.NewObject(ctx, prefix+"small.bin") // HEAD
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if o.Size() != int64(len(body)) || !o.ModTime(ctx).Equal(mtime) {
		t.Fatalf("head: size %d, mtime %v", o.Size(), o.ModTime(ctx))
	}
	rc, err := o.Open(ctx)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("download: %d bytes, %v", len(got), err)
	}

	// MD5 is what copies are compared by.
	if md5, err := o.Hash(ctx, hash.MD5); err != nil || len(md5) != 32 {
		t.Fatalf("md5 = %q, %v", md5, err)
	}

	// A missing object is "not found", not some other error.
	if _, err := src.NewObject(ctx, prefix+"missing"); !errors.Is(err, fs.ErrorObjectNotFound) {
		t.Fatalf("missing object: %v", err)
	}

	// Multipart upload: above the upload cutoff set for this test.
	big := randomBody(t, 12<<20)
	if _, err := operations.RcatSize(ctx, src, prefix+"big.bin", io.NopCloser(bytes.NewReader(big)), int64(len(big)), time.Now(), nil); err != nil {
		t.Fatalf("multipart upload: %v", err)
	}

	// Listing finds both; copy, compare and key sync work across buckets.
	n, _, err := Count(ctx, src)
	if err != nil || n < 2 {
		t.Fatalf("list: %d, %v", n, err)
	}
	res, err := Copy(ctx, src, dst, Options{}, nil)
	if err != nil || res.Copied < 2 {
		t.Fatalf("copy: %+v, %v", res, err)
	}
	d, err := Check(ctx, src, dst, Options{})
	if err != nil || d.Missing != 0 || d.Differ != 0 {
		t.Fatalf("check: %+v, %v", d, err)
	}
	if err := Delete(ctx, src, prefix+"small.bin"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if act, err := SyncKey(ctx, src, dst, prefix+"small.bin"); err != nil || act != Deleted {
		t.Fatalf("sync deleted key: %v, %v", act, err)
	}
	if err := ProbeWrite(ctx, dst); err != nil {
		t.Fatalf("probe write: %v", err)
	}
}
