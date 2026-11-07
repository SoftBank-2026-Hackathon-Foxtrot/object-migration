package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/creds"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

func TestPubSubCredentialsDefaultToADC(t *testing.T) {
	if got := (&Rclone{}).pubsubRef(); got != creds.Default {
		t.Fatalf("pubsubRef() = %q, want %q", got, creds.Default)
	}
	if got := (&Rclone{GCPCredentials: "env:GCS"}).pubsubRef(); got != "env:GCS" {
		t.Fatalf("pubsubRef() = %q, want env:GCS", got)
	}
}

func TestGCSFeedCheckRefusesUnresolvablePubSubCredentials(t *testing.T) {
	r := &Rclone{GCPProject: "p", GCPCredentials: "env:OM_TEST_MISSING_PUBSUB"}
	err := r.checkSourceFeed(context.Background(), migration.Endpoint{Kind: migration.KindGCS, Bucket: "b", Ref: creds.Default})
	if !errors.Is(err, creds.ErrBadRef) {
		t.Fatalf("checkSourceFeed() = %v, want ErrBadRef", err)
	}
	r.GCPProject = "" // no feed: nothing to check
	if err := r.checkSourceFeed(context.Background(), migration.Endpoint{Kind: migration.KindGCS, Bucket: "b", Ref: creds.Default}); err != nil {
		t.Fatalf("checkSourceFeed() without GCP_PROJECT = %v, want nil", err)
	}
}
