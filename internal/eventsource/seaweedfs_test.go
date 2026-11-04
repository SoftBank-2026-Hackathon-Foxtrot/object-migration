package eventsource

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/encoding/protowire"
)

func entry(name string, dir bool) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendString(b, name)
	if dir {
		b = protowire.AppendTag(b, 2, protowire.VarintType)
		b = protowire.AppendVarint(b, 1)
	}
	// A field this client does not read.
	b = protowire.AppendTag(b, 9, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte("content"))
	return b
}

func response(dir string, ts int64, old, new []byte, newParent string) []byte {
	var ev []byte
	if old != nil {
		ev = protowire.AppendTag(ev, 1, protowire.BytesType)
		ev = protowire.AppendBytes(ev, old)
	}
	if new != nil {
		ev = protowire.AppendTag(ev, 2, protowire.BytesType)
		ev = protowire.AppendBytes(ev, new)
	}
	if newParent != "" {
		ev = protowire.AppendTag(ev, 4, protowire.BytesType)
		ev = protowire.AppendString(ev, newParent)
	}
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendString(b, dir)
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendBytes(b, ev)
	b = protowire.AppendTag(b, 3, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(ts))
	return b
}

func TestSeaweedFSEvents(t *testing.T) {
	s := NewSeaweedFS(SeaweedFSConfig{Bucket: "photos"})
	tests := []struct {
		name string
		raw  []byte
		want []Change
	}{
		{"created", response("/buckets/photos/2026", 5, nil, entry("a.jpg", false), ""),
			[]Change{{Key: "2026/a.jpg"}}},
		{"updated", response("/buckets/photos", 5, entry("a.jpg", false), entry("a.jpg", false), ""),
			[]Change{{Key: "a.jpg"}}},
		{"deleted", response("/buckets/photos", 5, entry("a.jpg", false), nil, ""),
			[]Change{{Key: "a.jpg", Removed: true}}},
		{"renamed", response("/buckets/photos/in", 5, entry("a.jpg", false), entry("b.jpg", false), "/buckets/photos/out"),
			[]Change{{Key: "in/a.jpg", Removed: true}, {Key: "out/b.jpg"}}},
		{"folder", response("/buckets/photos", 5, nil, entry("2026", true), ""), nil},
		{"multipart staging", response("/buckets/photos/.uploads/xyz", 5, nil, entry("0001.part", false), ""), nil},
		{"other bucket", response("/buckets/photosarchive", 5, nil, entry("a.jpg", false), ""), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev, err := parseSubscribeResponse(tt.raw)
			if err != nil {
				t.Fatal(err)
			}
			got := s.changes(ev)
			for i := range got {
				if !got[i].At.Equal(time.Unix(0, 5)) {
					t.Errorf("at = %v", got[i].At)
				}
				got[i].At = time.Time{}
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("changes = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestSeaweedFSAckMovesTheResumePoint(t *testing.T) {
	s := NewSeaweedFS(SeaweedFSConfig{Bucket: "b"})
	s.stop = func() {} // pretend subscribed
	s.queue = []Message{
		{Changes: []Change{{Key: "a"}}, handle: "1", at: 100},
		{Changes: []Change{{Key: "b"}}, handle: "2", at: 200},
	}
	msgs, err := s.Receive(context.Background(), time.Millisecond)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("receive: %v, %v", msgs, err)
	}
	if err := s.Ack(context.Background(), msgs[1:]); err != nil {
		t.Fatal(err)
	}
	if s.since != 99 {
		t.Fatalf("since = %d, want just before the unacknowledged change", s.since)
	}
	if err := s.Nack(context.Background(), msgs[:1]); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Backlog(context.Background()); n != 1 {
		t.Fatalf("backlog = %d", n)
	}
	again, _ := s.Receive(context.Background(), time.Millisecond)
	if len(again) != 1 || again[0].Changes[0].Key != "a" {
		t.Fatalf("handed back message not received again: %+v", again)
	}
}

// TestSeaweedFSFeed follows a real SeaweedFS, named by OM_SEAWEEDFS_URL
// (S3), OM_SEAWEEDFS_FILER (gRPC) and OM_SEAWEEDFS_SRC, with keys in
// SEAWEED_ACCESS_KEY_ID and SEAWEED_SECRET_ACCESS_KEY.
func TestSeaweedFSFeed(t *testing.T) {
	url, filer, bucket := os.Getenv("OM_SEAWEEDFS_URL"), os.Getenv("OM_SEAWEEDFS_FILER"), os.Getenv("OM_SEAWEEDFS_SRC")
	if url == "" || filer == "" {
		t.Skip("OM_SEAWEEDFS_URL and OM_SEAWEEDFS_FILER not set")
	}
	ctx := context.Background()
	if err := CheckSeaweedFS(ctx, filer, bucket); err != nil {
		t.Fatalf("check: %v", err)
	}
	feed := NewSeaweedFS(SeaweedFSConfig{Filer: filer, Bucket: bucket, Name: "object-migration-test"})
	if err := feed.Subscribe(ctx); err != nil {
		t.Fatal(err)
	}
	defer feed.Unsubscribe(ctx)

	c := s3.New(s3.Options{
		BaseEndpoint: aws.String(url), Region: "us-east-1", UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(os.Getenv("SEAWEED_ACCESS_KEY_ID"), os.Getenv("SEAWEED_SECRET_ACCESS_KEY"), ""),
	})
	key := "feed-test/" + time.Now().Format("150405.000") + ".txt"
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: strings.NewReader("hello")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key}); err != nil {
		t.Fatal(err)
	}

	var got []Change
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !slices.ContainsFunc(got, func(c Change) bool { return c.Key == key && c.Removed }) {
		msgs, err := feed.Receive(ctx, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			got = append(got, m.Changes...)
		}
		if err := feed.Ack(ctx, msgs); err != nil {
			t.Fatal(err)
		}
	}
	created := slices.ContainsFunc(got, func(c Change) bool { return c.Key == key && !c.Removed })
	removed := slices.ContainsFunc(got, func(c Change) bool { return c.Key == key && c.Removed })
	if !created || !removed {
		t.Fatalf("changes = %+v; want %s created then removed", got, key)
	}
}

// TestSeaweedFSStreamStaysOpen checks that the filer keeps serving the
// subscription: it ends streams of subscribers without a client ID.
func TestSeaweedFSStreamStaysOpen(t *testing.T) {
	filer, bucket := os.Getenv("OM_SEAWEEDFS_FILER"), os.Getenv("OM_SEAWEEDFS_SRC")
	if filer == "" {
		t.Skip("OM_SEAWEEDFS_FILER not set")
	}
	f := NewSeaweedFS(SeaweedFSConfig{Filer: filer, Bucket: bucket, Name: "debug"})
	f.since = time.Now().Add(-30 * time.Minute).UnixNano()
	if err := f.Subscribe(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(8 * time.Second)
	f.mu.Lock()
	t.Logf("reconnects=%d lastErr=%v queued=%d", f.reconnects, f.lastErr, len(f.queue))
	if f.reconnects > 0 {
		t.Errorf("the subscription was reopened %d times in 8s", f.reconnects)
	}
	f.mu.Unlock()
	f.Unsubscribe(context.Background())
}

func TestSeaweedFSResumePointFollowsHandledChanges(t *testing.T) {
	s := NewSeaweedFS(SeaweedFSConfig{Bucket: "b"})
	s.stop = func() {}
	s.since, s.read = 10, 300
	s.queue = []Message{{Changes: []Change{{Key: "a"}}, handle: "1", at: 300}}
	msgs, _ := s.Receive(context.Background(), time.Millisecond)
	if err := s.Ack(context.Background(), msgs); err != nil {
		t.Fatal(err)
	}
	if s.since != 300 {
		t.Fatalf("since = %d after everything was handled, want the last event read", s.since)
	}
}

func TestSeaweedFSReportsAFailingSubscription(t *testing.T) {
	s := NewSeaweedFS(SeaweedFSConfig{Bucket: "b", Filer: "f:1"})
	s.stop = func() {}
	s.failing, s.lastErr = maxFailing, errors.New("connection refused")
	if _, err := s.Receive(context.Background(), time.Millisecond); err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("receive: %v, want the subscription's error", err)
	}
}
