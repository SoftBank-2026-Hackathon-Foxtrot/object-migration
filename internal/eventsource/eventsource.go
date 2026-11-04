// Package eventsource follows changes in a bucket. Each kind of storage
// reports changes its own way; a Source hands them over in one form.
package eventsource

import (
	"context"
	"errors"
	"time"
)

// ErrConflict means the bucket cannot report changes to us without
// breaking something its owner already set up. Retrying does not help.
var ErrConflict = errors.New("change feed conflicts with the bucket's setup")

// Change says that an object changed in the source.
type Change struct {
	Key string
	// Removed is what the event said. Events can arrive late, twice or out
	// of order, so a handler should look at the source rather than trust it.
	Removed bool
	// At is when the change happened in the source.
	At time.Time
}

// Message is one delivery from a change feed. It is acknowledged as a whole
// once all of its changes are handled; an unacknowledged message comes back.
type Message struct {
	Changes []Change
	handle  string
	at      int64 // when the feed produced it, for feeds that resume by time
}

// Source delivers the changes of one bucket.
type Source interface {
	// Receive returns the next messages, waiting up to wait for the first.
	// It returns none when nothing came in time.
	Receive(ctx context.Context, wait time.Duration) ([]Message, error)
	// Ack removes handled messages from the feed.
	Ack(ctx context.Context, msgs []Message) error
	// Nack hands messages back to the feed at once, to be received again.
	Nack(ctx context.Context, msgs []Message) error
	// Backlog is about how many messages are waiting or being handled.
	Backlog(ctx context.Context) (int64, error)
}
