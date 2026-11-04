package eventsource

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"google.golang.org/api/googleapi"
	pubsub "google.golang.org/api/pubsub/v1"
	storage "google.golang.org/api/storage/v1"
)

// GCSConfig describes a GCS bucket to follow through Pub/Sub.
type GCSConfig struct {
	Bucket string
	// Storage acts as the bucket owner, to add the bucket's notification.
	Storage *storage.Service
	// PubSub acts as the platform, which owns the topic and subscription.
	PubSub *pubsub.Service
	// Project is the platform's project for the topic and subscription.
	Project string
	// Name identifies the topic, subscription and notification. It must
	// be unique per migration and valid as a Pub/Sub ID.
	Name string
}

// GCS follows a GCS bucket through its Pub/Sub notifications, sent to a
// topic in the platform's project, which may differ from the bucket's.
// GCS allows several notifications per bucket, so the bucket's own are
// never in the way.
type GCS struct {
	cfg GCSConfig

	mu      sync.Mutex
	pending map[string]bool // ack IDs handed out, not yet acknowledged
}

func NewGCS(cfg GCSConfig) *GCS { return &GCS{cfg: cfg, pending: make(map[string]bool)} }

func (g *GCS) topic() string { return "projects/" + g.cfg.Project + "/topics/" + g.cfg.Name }
func (g *GCS) subscription() string {
	return "projects/" + g.cfg.Project + "/subscriptions/" + g.cfg.Name
}

// Subscribe creates the topic and subscription, lets the bucket's project
// publish to the topic, and adds a notification for every object written
// or deleted. Calling it again is harmless.
func (g *GCS) Subscribe(ctx context.Context) error {
	ps := g.cfg.PubSub.Projects
	if _, err := ps.Topics.Create(g.topic(), &pubsub.Topic{Labels: map[string]string{"object-migration": g.cfg.Name}}).Context(ctx).Do(); err != nil && !isStatus(err, http.StatusConflict) {
		return fmt.Errorf("create topic: %w", err)
	}

	// GCS publishes as the bucket project's service agent.
	b, err := g.cfg.Storage.Buckets.Get(g.cfg.Bucket).Fields("projectNumber").Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("bucket project: %w", err)
	}
	agent, err := g.cfg.Storage.Projects.ServiceAccount.Get(fmt.Sprint(b.ProjectNumber)).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("storage service agent: %w", err)
	}
	member := "serviceAccount:" + agent.EmailAddress
	// Granting publish on the topic needs IAM rights in the platform's
	// project. Without them the agent may publish through a project-wide
	// grant instead; adding the notification below checks that it can.
	policy, err := ps.Topics.GetIamPolicy(g.topic()).Context(ctx).Do()
	switch {
	case isStatus(err, http.StatusForbidden):
	case err != nil:
		return fmt.Errorf("topic policy: %w", err)
	case !hasMember(policy, "roles/pubsub.publisher", member):
		policy.Bindings = append(policy.Bindings, &pubsub.Binding{Role: "roles/pubsub.publisher", Members: []string{member}})
		if _, err := ps.Topics.SetIamPolicy(g.topic(), &pubsub.SetIamPolicyRequest{Policy: policy}).Context(ctx).Do(); err != nil && !isStatus(err, http.StatusForbidden) {
			return fmt.Errorf("topic policy: %w", err)
		}
	}

	_, err = ps.Subscriptions.Create(g.subscription(), &pubsub.Subscription{
		Topic: g.topic(),
		// Changes wait here during the initial copy, which can be long.
		MessageRetentionDuration: "604800s",
		AckDeadlineSeconds:       120,
		Labels:                   map[string]string{"object-migration": g.cfg.Name},
	}).Context(ctx).Do()
	if err != nil && !isStatus(err, http.StatusConflict) {
		return fmt.Errorf("create subscription: %w", err)
	}

	if n, err := g.notification(ctx); err != nil || n != nil {
		return err
	}
	_, err = g.cfg.Storage.Notifications.Insert(g.cfg.Bucket, &storage.Notification{
		Topic:         "//pubsub.googleapis.com/" + g.topic(),
		PayloadFormat: "JSON_API_V1",
		// In a versioned bucket, deleting the live object archives it.
		EventTypes:       []string{"OBJECT_FINALIZE", "OBJECT_DELETE", "OBJECT_ARCHIVE"},
		CustomAttributes: map[string]string{"object-migration": g.cfg.Name},
	}).Context(ctx).Do()
	if isStatus(err, http.StatusForbidden) || isStatus(err, http.StatusBadRequest) {
		return fmt.Errorf("add bucket notification: %w (the bucket's storage service agent %s needs roles/pubsub.publisher on %s or its project)", err, agent.EmailAddress, g.topic())
	}
	if err != nil {
		return fmt.Errorf("add bucket notification: %w", err)
	}
	return nil
}

// notification finds our notification on the bucket.
func (g *GCS) notification(ctx context.Context) (*storage.Notification, error) {
	ours, err := g.notifications(ctx)
	if err != nil || len(ours) == 0 {
		return nil, err
	}
	return ours[0], nil
}

func (g *GCS) notifications(ctx context.Context) ([]*storage.Notification, error) {
	list, err := g.cfg.Storage.Notifications.List(g.cfg.Bucket).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("read bucket notifications: %w", err)
	}
	var ours []*storage.Notification
	for _, n := range list.Items {
		if n.CustomAttributes["object-migration"] == g.cfg.Name {
			ours = append(ours, n)
		}
	}
	return ours, nil
}

// Unsubscribe removes our notifications from the bucket and deletes the
// subscription and topic. Parts that are already gone are skipped; a part
// that fails does not stop the others.
func (g *GCS) Unsubscribe(ctx context.Context) error {
	var errs []error
	ours, err := g.notifications(ctx)
	if err != nil && !isStatus(errors.Unwrap(err), http.StatusNotFound) {
		errs = append(errs, err)
	}
	for _, n := range ours {
		if err := g.cfg.Storage.Notifications.Delete(g.cfg.Bucket, n.Id).Context(ctx).Do(); err != nil && !isStatus(err, http.StatusNotFound) {
			errs = append(errs, fmt.Errorf("remove bucket notification: %w", err))
		}
	}
	ps := g.cfg.PubSub.Projects
	if _, err := ps.Subscriptions.Delete(g.subscription()).Context(ctx).Do(); err != nil && !isStatus(err, http.StatusNotFound) {
		errs = append(errs, fmt.Errorf("delete subscription: %w", err))
	}
	if _, err := ps.Topics.Delete(g.topic()).Context(ctx).Do(); err != nil && !isStatus(err, http.StatusNotFound) {
		errs = append(errs, fmt.Errorf("delete topic: %w", err))
	}
	return errors.Join(errs...)
}

// Receive pulls without waiting on the server and polls until wait is up.
// Cancelling a pull that waits on the server could lose the messages it
// was given: they stay hidden until their ack deadline.
func (g *GCS) Receive(ctx context.Context, wait time.Duration) ([]Message, error) {
	deadline := time.Now().Add(wait)
	var out *pubsub.PullResponse
	for {
		var err error
		//lint:ignore SA1019 returnImmediately is the only way to pull without a server-side wait over REST.
		out, err = g.cfg.PubSub.Projects.Subscriptions.Pull(g.subscription(), &pubsub.PullRequest{MaxMessages: 10, ReturnImmediately: true}).Context(ctx).Do()
		if err != nil {
			return nil, err
		}
		if len(out.ReceivedMessages) > 0 || !time.Now().Before(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(min(200*time.Millisecond, time.Until(deadline))):
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	msgs := make([]Message, 0, len(out.ReceivedMessages))
	for _, rm := range out.ReceivedMessages {
		g.pending[rm.AckId] = true
		msgs = append(msgs, Message{Changes: gcsChanges(rm.Message), handle: rm.AckId})
	}
	return msgs, nil
}

// gcsChanges reads a notification. GCS also reports deleting an object's
// old generation when it is overwritten; the handler looks at the bucket,
// so that is harmless.
func gcsChanges(m *pubsub.PubsubMessage) []Change {
	if m == nil {
		return nil
	}
	a := m.Attributes
	key := a["objectId"]
	if key == "" {
		return nil
	}
	var removed bool
	switch a["eventType"] {
	case "OBJECT_FINALIZE":
	case "OBJECT_DELETE", "OBJECT_ARCHIVE":
		removed = true
	default:
		return nil
	}
	at, err := time.Parse(time.RFC3339Nano, a["eventTime"])
	if err != nil {
		at = time.Time{}
	}
	return []Change{{Key: key, Removed: removed, At: at}}
}

func (g *GCS) Ack(ctx context.Context, msgs []Message) error {
	ids := g.take(msgs)
	if len(ids) == 0 {
		return nil
	}
	_, err := g.cfg.PubSub.Projects.Subscriptions.Acknowledge(g.subscription(), &pubsub.AcknowledgeRequest{AckIds: ids}).Context(ctx).Do()
	return err
}

func (g *GCS) Nack(ctx context.Context, msgs []Message) error {
	ids := g.take(msgs)
	if len(ids) == 0 {
		return nil
	}
	_, err := g.cfg.PubSub.Projects.Subscriptions.ModifyAckDeadline(g.subscription(), &pubsub.ModifyAckDeadlineRequest{AckIds: ids, AckDeadlineSeconds: 0}).Context(ctx).Do()
	return err
}

func (g *GCS) take(msgs []Message) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	var ids []string
	for _, m := range msgs {
		if g.pending[m.handle] {
			delete(g.pending, m.handle)
			ids = append(ids, m.handle)
		}
	}
	return ids
}

// Backlog counts the messages handed out and not acknowledged. Pub/Sub
// does not tell how many wait undelivered without the Monitoring API, so
// waiting ones show up only as they are received: for a GCS source the
// cutover backlog check and the end of the drain at resume rest on empty
// pulls. Deletes made by apps do not depend on it (they are recorded), and
// the final copy at resume catches new objects.
func (g *GCS) Backlog(context.Context) (int64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return int64(len(g.pending)), nil
}

// CheckGCS reports whether a feed could be added to a bucket: its
// notifications can be read and changed.
func CheckGCS(ctx context.Context, s *storage.Service, bucket string) error {
	if _, err := s.Notifications.List(bucket).Context(ctx).Do(); err != nil {
		return fmt.Errorf("read bucket notifications: %w", err)
	}
	perm, err := s.Buckets.TestIamPermissions(bucket, []string{"storage.buckets.update"}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("bucket permissions: %w", err)
	}
	if !slices.Contains(perm.Permissions, "storage.buckets.update") {
		return errors.New("cannot add bucket notifications: storage.buckets.update not granted")
	}
	return nil
}

func hasMember(p *pubsub.Policy, role, member string) bool {
	for _, b := range p.Bindings {
		if b.Role == role && slices.Contains(b.Members, member) {
			return true
		}
	}
	return false
}

func isStatus(err error, code int) bool {
	var ge *googleapi.Error
	return errors.As(err, &ge) && ge.Code == code
}
