// Package appsim simulates app servers that use the client module against
// real buckets, to test that a migration switches without failed requests.
package appsim

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/client"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/copier"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

// Bucket reaches a real bucket through rclone.
type Bucket struct{ f fs.Fs }

// Open connects to a bucket of a route with the given credentials ref.
func Open(ctx context.Context, e client.Endpoint, ref string) (*Bucket, error) {
	f, err := copier.Open(ctx, "app", migration.Endpoint{
		Kind: migration.Kind(e.Kind), Bucket: e.Bucket, Region: e.Region, URL: e.URL, Ref: ref,
	})
	if err != nil {
		return nil, err
	}
	return &Bucket{f: f}, nil
}

func (b *Bucket) Put(ctx context.Context, key string, body []byte) error {
	_, err := operations.Rcat(ctx, b.f, key, io.NopCloser(bytes.NewReader(body)), time.Now(), nil)
	return err
}

func (b *Bucket) Get(ctx context.Context, key string) ([]byte, error) {
	o, err := b.f.NewObject(ctx, key)
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return nil, client.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rc, err := o.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (b *Bucket) Delete(ctx context.Context, key string) error {
	return copier.Delete(ctx, b.f, key)
}

// Keys lists the keys under prefix.
func (b *Bucket) Keys(ctx context.Context, prefix string) (map[string]bool, error) {
	keys := make(map[string]bool)
	err := operations.ListFn(ctx, b.f, func(o fs.Object) {
		if strings.HasPrefix(o.Remote(), prefix) {
			keys[o.Remote()] = true
		}
	})
	return keys, err
}
