package eventsource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protowire"
)

// SeaweedFSConfig describes a SeaweedFS bucket to follow through its filer.
type SeaweedFSConfig struct {
	// Filer is the filer's gRPC address, host:port (port 18888 by default).
	Filer  string
	Bucket string
	// Name identifies this subscriber to the filer.
	Name string
}

// SeaweedFS follows a bucket through the filer's metadata subscription,
// which streams every change under a path from a given time. Nothing is set
// up on the SeaweedFS side; the feed lives in this process and, after a
// lost connection, resumes from the oldest change not yet acknowledged.
// Position tells where that is, so a restarted process can resume there.
type SeaweedFS struct {
	cfg    SeaweedFSConfig
	prefix string // the bucket's directory in the filer, with a trailing slash

	mu      sync.Mutex
	cond    chan struct{} // closed and replaced when messages arrive
	queue   []Message
	pending map[string]int64 // handed out, not yet acknowledged: handle -> event time
	next    int64            // handle counter
	since   int64            // resume point: events after this were not all acknowledged
	read    int64            // time of the last event read from the stream
	stop    context.CancelFunc
	done    chan struct{}
	lastErr error
	// reconnects counts how often the subscription had to be opened again,
	// failing how often it ended with an error in a row.
	reconnects, failing int
	// clientID names this subscriber to the filer, which serves only
	// subscribers with a non-zero ID beyond the first batch; each
	// reconnect raises the epoch so the filer replaces the old stream.
	clientID int32
	epoch    int32
}

// NewSeaweedFS returns the change feed of a bucket. Subscribe starts it.
func NewSeaweedFS(cfg SeaweedFSConfig) *SeaweedFS {
	return &SeaweedFS{
		cfg:      cfg,
		prefix:   "/buckets/" + cfg.Bucket + "/",
		cond:     make(chan struct{}),
		pending:  make(map[string]int64),
		clientID: rand.Int32N(math.MaxInt32) + 1,
	}
}

// Subscribe starts following changes from the position given to
// ResumeFrom, or from now, unless it already runs.
func (s *SeaweedFS) Subscribe(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stop != nil {
		return nil
	}
	conn, err := grpc.NewClient(s.cfg.Filer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("filer %s: %w", s.cfg.Filer, err)
	}
	if s.since == 0 {
		// A minute early, in case this clock is ahead of the filer's; the
		// changes seen twice are harmless.
		s.since = time.Now().Add(-time.Minute).UnixNano()
	}
	run, stop := context.WithCancel(context.Background())
	s.stop, s.done = stop, make(chan struct{})
	go func() {
		defer close(s.done)
		defer conn.Close()
		s.follow(run, conn)
	}()
	return nil
}

// Position is the resume point: every change after it that was read is
// either acknowledged or still waiting. It is nanoseconds since 1970.
func (s *SeaweedFS) Position() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strconv.FormatInt(s.since, 10)
}

// ResumeFrom makes Subscribe start after pos, a Position saved earlier.
func (s *SeaweedFS) ResumeFrom(pos string) error {
	since, err := strconv.ParseInt(pos, 10, 64)
	if err != nil || since <= 0 {
		return fmt.Errorf("bad position %q", pos)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.since = since
	return nil
}

// Unsubscribe stops following.
func (s *SeaweedFS) Unsubscribe(ctx context.Context) error {
	s.mu.Lock()
	stop, done := s.stop, s.done
	s.stop = nil
	s.mu.Unlock()
	if stop != nil {
		stop()
		<-done
	}
	return nil
}

// follow keeps a subscription open, reconnecting after errors from the
// resume point.
func (s *SeaweedFS) follow(ctx context.Context, conn *grpc.ClientConn) {
	for ctx.Err() == nil {
		s.mu.Lock()
		// The stream starts again from since, which is before every queued
		// change; drop them rather than queue them twice.
		s.queue = nil
		since := s.since
		s.epoch++
		epoch := s.epoch
		s.mu.Unlock()
		err := s.stream(ctx, conn, since, epoch)
		s.mu.Lock()
		s.lastErr = err
		s.reconnects++
		s.failing++
		s.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

func (s *SeaweedFS) stream(ctx context.Context, conn *grpc.ClientConn, since int64, epoch int32) error {
	st, err := subscribe(ctx, conn, s.cfg.Name, s.prefix, since, 0, s.clientID, epoch)
	if err != nil {
		return err
	}
	for {
		// Changes wait in memory here, not in the filer; stop reading while
		// many wait, and the stream holds the rest back.
		for s.queued() >= maxQueued {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		var raw []byte
		if err := st.RecvMsg(&raw); err != nil {
			return err
		}
		ev, err := parseSubscribeResponse(raw)
		s.mu.Lock()
		s.failing = 0
		if err != nil {
			// Skip it rather than fail on it forever; the full copy and the
			// comparison catch what it described.
			s.lastErr = fmt.Errorf("unreadable filer event: %w", err)
			s.mu.Unlock()
			continue
		}
		if ev.tsNs > s.read {
			s.read = ev.tsNs
		}
		if changes := s.changes(ev); len(changes) > 0 {
			s.next++
			s.queue = append(s.queue, Message{Changes: changes, handle: strconv.FormatInt(s.next, 10), at: ev.tsNs})
			close(s.cond)
			s.cond = make(chan struct{})
		} else if len(s.queue) == 0 && len(s.pending) == 0 {
			s.since = s.read // nothing waits; later reconnects need not go back further
		}
		s.mu.Unlock()
	}
}

// maxQueued bounds the changes kept in memory before they are received.
const maxQueued = 10000

// maxFailing is how many failed subscriptions in a row make Receive fail.
const maxFailing = 5

func (s *SeaweedFS) queued() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

// changes turns a filer event into changes of keys in the bucket. Folders,
// and the staging area of multipart uploads, are not objects.
func (s *SeaweedFS) changes(ev filerEvent) []Change {
	at := time.Unix(0, ev.tsNs)
	key := func(dir, name string) (string, bool) {
		k, ok := strings.CutPrefix(strings.TrimSuffix(dir, "/")+"/"+name, s.prefix)
		if !ok || k == "" || strings.HasPrefix(k, ".uploads/") {
			return "", false
		}
		return k, true
	}
	var out []Change
	if ev.old != nil && !ev.old.isDir {
		if k, ok := key(ev.dir, ev.old.name); ok {
			moved := ev.new == nil || (ev.newParent != "" && ev.newParent != ev.dir) || ev.new.name != ev.old.name
			if moved {
				out = append(out, Change{Key: k, Removed: true, At: at})
			}
		}
	}
	if ev.new != nil && !ev.new.isDir {
		dir := ev.dir
		if ev.newParent != "" {
			dir = ev.newParent
		}
		if k, ok := key(dir, ev.new.name); ok {
			out = append(out, Change{Key: k, At: at})
		}
	}
	return out
}

func (s *SeaweedFS) Receive(ctx context.Context, wait time.Duration) ([]Message, error) {
	t := time.NewTimer(wait)
	defer t.Stop()
	for {
		s.mu.Lock()
		if len(s.queue) > 0 {
			n := min(len(s.queue), 10)
			msgs := slices.Clone(s.queue[:n])
			s.queue = s.queue[n:]
			for _, m := range msgs {
				s.pending[m.handle] = m.at
			}
			s.mu.Unlock()
			return msgs, nil
		}
		if s.stop == nil {
			s.mu.Unlock()
			return nil, errors.New("not subscribed")
		}
		if s.failing >= maxFailing {
			err := s.lastErr
			s.mu.Unlock()
			return nil, fmt.Errorf("filer %s: subscription keeps failing: %w", s.cfg.Filer, err)
		}
		cond := s.cond
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
			return nil, nil
		case <-cond:
		}
	}
}

func (s *SeaweedFS) Ack(_ context.Context, msgs []Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range msgs {
		delete(s.pending, m.handle)
	}
	s.advance()
	return nil
}

func (s *SeaweedFS) Nack(_ context.Context, msgs []Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var back []Message
	for _, m := range msgs {
		if _, ok := s.pending[m.handle]; ok {
			delete(s.pending, m.handle)
			back = append(back, m)
		}
	}
	s.queue = append(back, s.queue...)
	close(s.cond)
	s.cond = make(chan struct{})
	return nil
}

// advance moves the resume point to just before the oldest change not yet
// acknowledged. Called with s.mu held.
func (s *SeaweedFS) advance() {
	oldest := int64(0)
	for _, at := range s.pending {
		if oldest == 0 || at < oldest {
			oldest = at
		}
	}
	for _, m := range s.queue {
		if oldest == 0 || m.at < oldest {
			oldest = m.at
		}
	}
	switch {
	case oldest != 0:
		s.since = oldest - 1
	case s.read > s.since:
		s.since = s.read // everything read is handled
	}
}

func (s *SeaweedFS) Backlog(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.queue) + len(s.pending)), nil
}

// CheckSeaweedFS reports whether a filer accepts a metadata subscription.
func CheckSeaweedFS(ctx context.Context, filer, bucket string) error {
	conn, err := grpc.NewClient(filer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("filer %s: %w", filer, err)
	}
	defer conn.Close()
	now := time.Now().UnixNano()
	st, err := subscribe(ctx, conn, "object-migration-preflight", "/buckets/"+bucket+"/", now, now+1, rand.Int32N(math.MaxInt32)+1, 1)
	if err != nil {
		return fmt.Errorf("filer %s: %w", filer, err)
	}
	var raw []byte
	if err := st.RecvMsg(&raw); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("filer %s: %w", filer, err)
	}
	return nil
}

// The filer's gRPC service is called with hand-encoded protobuf: the few
// fields needed here are cheaper to read than SeaweedFS's Go packages,
// which bring most of SeaweedFS with them. Field numbers are from
// weed/pb/filer.proto.

const subscribeMethod = "/filer_pb.SeaweedFiler/SubscribeMetadata"

func subscribe(ctx context.Context, conn *grpc.ClientConn, name, prefix string, sinceNs, untilNs int64, clientID, epoch int32) (grpc.ClientStream, error) {
	var req []byte
	req = protowire.AppendTag(req, 1, protowire.BytesType) // client_name
	req = protowire.AppendString(req, name)
	req = protowire.AppendTag(req, 2, protowire.BytesType) // path_prefix
	req = protowire.AppendString(req, prefix)
	req = protowire.AppendTag(req, 3, protowire.VarintType) // since_ns
	req = protowire.AppendVarint(req, uint64(sinceNs))
	req = protowire.AppendTag(req, 7, protowire.VarintType) // client_id
	req = protowire.AppendVarint(req, uint64(clientID))
	if untilNs > 0 {
		req = protowire.AppendTag(req, 8, protowire.VarintType) // until_ns
		req = protowire.AppendVarint(req, uint64(untilNs))
	}
	req = protowire.AppendTag(req, 9, protowire.VarintType) // client_epoch
	req = protowire.AppendVarint(req, uint64(epoch))
	st, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, subscribeMethod, grpc.ForceCodec(rawCodec{}))
	if err != nil {
		return nil, err
	}
	if err := st.SendMsg(&req); err != nil {
		return nil, err
	}
	if err := st.CloseSend(); err != nil {
		return nil, err
	}
	return st, nil
}

// rawCodec passes protobuf bytes through untouched.
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) { return *v.(*[]byte), nil }
func (rawCodec) Unmarshal(data []byte, v any) error {
	*v.(*[]byte) = slices.Clone(data)
	return nil
}
func (rawCodec) Name() string { return "proto" }

type filerEntry struct {
	name  string
	isDir bool
}

type filerEvent struct {
	dir       string
	tsNs      int64
	old, new  *filerEntry
	newParent string
}

func parseSubscribeResponse(b []byte) (filerEvent, error) {
	var ev filerEvent
	err := fields(b, func(num protowire.Number, typ protowire.Type, v []byte, n uint64) error {
		switch {
		case num == 1 && typ == protowire.BytesType: // directory
			ev.dir = string(v)
		case num == 2 && typ == protowire.BytesType: // event_notification
			return fields(v, func(num protowire.Number, typ protowire.Type, v []byte, _ uint64) error {
				switch {
				case num == 1 && typ == protowire.BytesType: // old_entry
					e, err := parseEntry(v)
					ev.old = &e
					return err
				case num == 2 && typ == protowire.BytesType: // new_entry
					e, err := parseEntry(v)
					ev.new = &e
					return err
				case num == 4 && typ == protowire.BytesType: // new_parent_path
					ev.newParent = string(v)
				}
				return nil
			})
		case num == 3 && typ == protowire.VarintType: // ts_ns
			ev.tsNs = int64(n)
		}
		return nil
	})
	return ev, err
}

func parseEntry(b []byte) (filerEntry, error) {
	var e filerEntry
	err := fields(b, func(num protowire.Number, typ protowire.Type, v []byte, n uint64) error {
		switch {
		case num == 1 && typ == protowire.BytesType: // name
			e.name = string(v)
		case num == 2 && typ == protowire.VarintType: // is_directory
			e.isDir = n != 0
		}
		return nil
	})
	return e, err
}

// fields calls fn for each field of a protobuf message, with the bytes of
// length-delimited fields and the value of varints.
func fields(b []byte, fn func(num protowire.Number, typ protowire.Type, v []byte, n uint64) error) error {
	for len(b) > 0 {
		num, typ, l := protowire.ConsumeTag(b)
		if l < 0 {
			return protowire.ParseError(l)
		}
		b = b[l:]
		var v []byte
		var n uint64
		switch typ {
		case protowire.VarintType:
			n, l = protowire.ConsumeVarint(b)
		case protowire.BytesType:
			v, l = protowire.ConsumeBytes(b)
		default:
			l = protowire.ConsumeFieldValue(num, typ, b)
		}
		if l < 0 {
			return protowire.ParseError(l)
		}
		b = b[l:]
		if err := fn(num, typ, v, n); err != nil {
			return err
		}
	}
	return nil
}
