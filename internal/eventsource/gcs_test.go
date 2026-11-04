package eventsource

import (
	"testing"
	"time"

	pubsub "google.golang.org/api/pubsub/v1"
)

func TestGCSChanges(t *testing.T) {
	msg := func(event, key, at string) *pubsub.PubsubMessage {
		return &pubsub.PubsubMessage{Attributes: map[string]string{"eventType": event, "objectId": key, "eventTime": at}}
	}
	at := "2026-11-04T01:02:03.456789Z"
	when, _ := time.Parse(time.RFC3339Nano, at)
	tests := []struct {
		name string
		m    *pubsub.PubsubMessage
		want []Change
	}{
		{"written", msg("OBJECT_FINALIZE", "photos/a b.jpg", at), []Change{{Key: "photos/a b.jpg", At: when}}},
		{"deleted", msg("OBJECT_DELETE", "a.jpg", at), []Change{{Key: "a.jpg", Removed: true, At: when}}},
		{"archived in a versioned bucket", msg("OBJECT_ARCHIVE", "a.jpg", at), []Change{{Key: "a.jpg", Removed: true, At: when}}},
		{"metadata only", msg("OBJECT_METADATA_UPDATE", "a.jpg", at), nil},
		{"no key", msg("OBJECT_FINALIZE", "", at), nil},
		{"nil", nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := gcsChanges(tt.m)
			if len(got) != len(tt.want) || (len(got) == 1 && got[0] != tt.want[0]) {
				t.Fatalf("changes = %+v, want %+v", got, tt.want)
			}
		})
	}
}
