package eventsource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
)

// S3Config describes an S3 bucket to follow through an SQS queue.
type S3Config struct {
	Bucket string
	// BucketAWS has the bucket owner's credentials, used to change the
	// bucket's event notifications. Its region is the bucket's.
	BucketAWS aws.Config
	// QueueAWS has the platform's credentials, which own the queue. S3 only
	// notifies queues in the bucket's region, so the queue is created there.
	QueueAWS aws.Config
	// Name identifies the queue and the notification. It must be unique per
	// migration and valid as an SQS queue name.
	Name string
}

// SQS follows an S3 bucket through its event notifications sent to an SQS
// queue owned by the platform. The queue may be in another account.
type SQS struct {
	cfg    S3Config
	s3     *s3.Client
	sqs    *sqs.Client
	region string
	url    string
}

// NewS3 returns the change feed of a bucket. It does not touch AWS;
// Subscribe sets the feed up.
func NewS3(cfg S3Config) *SQS {
	qcfg := cfg.QueueAWS.Copy()
	qcfg.Region = cfg.BucketAWS.Region
	return &SQS{
		cfg:    cfg,
		s3:     s3.NewFromConfig(cfg.BucketAWS),
		sqs:    sqs.NewFromConfig(qcfg),
		region: cfg.BucketAWS.Region,
	}
}

// notificationID marks our entry in the bucket's notification configuration.
func (q *SQS) notificationID() string { return "object-migration-" + q.cfg.Name }

// Subscribe creates the queue and adds a notification for every object
// created or removed in the bucket. The bucket's other notifications are
// kept. Calling it again is harmless.
func (q *SQS) Subscribe(ctx context.Context) error {
	out, err := q.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(q.cfg.Name),
		Attributes: map[string]string{
			// Changes wait here during the initial copy, which can be long.
			string(sqstypes.QueueAttributeNameMessageRetentionPeriod): "1209600",
			string(sqstypes.QueueAttributeNameVisibilityTimeout):      "120",
		},
		Tags: map[string]string{"object-migration": q.cfg.Name},
	})
	if err != nil {
		return fmt.Errorf("create queue: %w", err)
	}
	q.url = aws.ToString(out.QueueUrl)

	attrs, err := q.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return fmt.Errorf("queue arn: %w", err)
	}
	queueARN := attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]

	owner, err := sts.NewFromConfig(q.cfg.BucketAWS).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("bucket account: %w", err)
	}
	policy, err := queuePolicy(queueARN, q.cfg.Bucket, aws.ToString(owner.Account))
	if err != nil {
		return err
	}
	_, err = q.sqs.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl:   out.QueueUrl,
		Attributes: map[string]string{string(sqstypes.QueueAttributeNamePolicy): policy},
	})
	if err != nil {
		return fmt.Errorf("queue policy: %w", err)
	}

	// S3 checks that it may send to the queue, and a fresh queue policy can
	// take a few seconds to apply.
	var lastErr error
	for attempt := range 6 {
		if attempt > 0 {
			if err := sleep(ctx, time.Duration(attempt)*2*time.Second); err != nil {
				return err
			}
		}
		lastErr = q.updateNotifications(ctx, func(c *s3types.NotificationConfiguration) (bool, error) {
			return addQueue(c, q.notificationID(), queueARN)
		})
		if lastErr == nil || errors.Is(lastErr, ErrConflict) || !isValidationError(lastErr) {
			return lastErr
		}
	}
	return lastErr
}

// Unsubscribe removes our notification from the bucket and deletes the
// queue. Parts that are already gone are skipped.
func (q *SQS) Unsubscribe(ctx context.Context) error {
	err := q.updateNotifications(ctx, func(c *s3types.NotificationConfiguration) (bool, error) {
		return removeQueue(c, q.notificationID()), nil
	})
	if err != nil && !isCode(err, "NoSuchBucket") {
		return err
	}
	url, err := q.queueURL(ctx)
	if isCode(err, "AWS.SimpleQueueService.NonExistentQueue") || isCode(err, "QueueDoesNotExist") {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err := q.sqs.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(url)}); err != nil {
		return fmt.Errorf("delete queue: %w", err)
	}
	return nil
}

func (q *SQS) updateNotifications(ctx context.Context, change func(*s3types.NotificationConfiguration) (bool, error)) error {
	cur, err := q.s3.GetBucketNotificationConfiguration(ctx, &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String(q.cfg.Bucket)})
	if err != nil {
		return fmt.Errorf("read bucket notifications: %w", err)
	}
	c := &s3types.NotificationConfiguration{
		EventBridgeConfiguration:     cur.EventBridgeConfiguration,
		LambdaFunctionConfigurations: cur.LambdaFunctionConfigurations,
		QueueConfigurations:          cur.QueueConfigurations,
		TopicConfigurations:          cur.TopicConfigurations,
	}
	changed, err := change(c)
	if err != nil || !changed {
		return err
	}
	_, err = q.s3.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
		Bucket:                    aws.String(q.cfg.Bucket),
		NotificationConfiguration: c,
	})
	if err != nil {
		return fmt.Errorf("write bucket notifications: %w", err)
	}
	return nil
}

func (q *SQS) queueURL(ctx context.Context) (string, error) {
	if q.url != "" {
		return q.url, nil
	}
	out, err := q.sqs.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(q.cfg.Name)})
	if err != nil {
		return "", err
	}
	q.url = aws.ToString(out.QueueUrl)
	return q.url, nil
}

func (q *SQS) Receive(ctx context.Context, wait time.Duration) ([]Message, error) {
	url, err := q.queueURL(ctx)
	if err != nil {
		return nil, err
	}
	out, err := q.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(url),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     int32(min(max(wait, 0), 20*time.Second) / time.Second),
	})
	if err != nil {
		return nil, err
	}
	msgs := make([]Message, 0, len(out.Messages))
	for _, m := range out.Messages {
		msgs = append(msgs, Message{Changes: parseS3Event(aws.ToString(m.Body)), handle: aws.ToString(m.ReceiptHandle)})
	}
	return msgs, nil
}

func (q *SQS) Ack(ctx context.Context, msgs []Message) error {
	url, err := q.queueURL(ctx)
	if err != nil {
		return err
	}
	for start := 0; start < len(msgs); start += 10 {
		var entries []sqstypes.DeleteMessageBatchRequestEntry
		for i, m := range msgs[start:min(start+10, len(msgs))] {
			entries = append(entries, sqstypes.DeleteMessageBatchRequestEntry{
				Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(m.handle),
			})
		}
		out, err := q.sqs.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: aws.String(url), Entries: entries})
		if err != nil {
			return err
		}
		if len(out.Failed) > 0 {
			return fmt.Errorf("ack: %d messages failed: %s", len(out.Failed), aws.ToString(out.Failed[0].Message))
		}
	}
	return nil
}

func (q *SQS) Nack(ctx context.Context, msgs []Message) error {
	url, err := q.queueURL(ctx)
	if err != nil {
		return err
	}
	for start := 0; start < len(msgs); start += 10 {
		var entries []sqstypes.ChangeMessageVisibilityBatchRequestEntry
		for i, m := range msgs[start:min(start+10, len(msgs))] {
			entries = append(entries, sqstypes.ChangeMessageVisibilityBatchRequestEntry{
				Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(m.handle), VisibilityTimeout: 0,
			})
		}
		out, err := q.sqs.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: aws.String(url), Entries: entries})
		if err != nil {
			return err
		}
		if len(out.Failed) > 0 {
			return fmt.Errorf("nack: %d messages failed: %s", len(out.Failed), aws.ToString(out.Failed[0].Message))
		}
	}
	return nil
}

func (q *SQS) Backlog(ctx context.Context) (int64, error) {
	url, err := q.queueURL(ctx)
	if err != nil {
		return 0, err
	}
	names := []sqstypes.QueueAttributeName{
		sqstypes.QueueAttributeNameApproximateNumberOfMessages,
		sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		sqstypes.QueueAttributeNameApproximateNumberOfMessagesDelayed,
	}
	out, err := q.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(url), AttributeNames: names})
	if err != nil {
		return 0, err
	}
	var n int64
	for _, name := range names {
		v, _ := strconv.ParseInt(out.Attributes[string(name)], 10, 64)
		n += v
	}
	return n, nil
}

// CheckS3 reports whether a feed could be added to a bucket: its
// notifications can be read and none of them already takes object events.
func CheckS3(ctx context.Context, bucketAWS aws.Config, bucket string) error {
	cur, err := s3.NewFromConfig(bucketAWS).GetBucketNotificationConfiguration(ctx, &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil {
		return fmt.Errorf("read bucket notifications: %w", err)
	}
	_, err = addQueue(&s3types.NotificationConfiguration{
		LambdaFunctionConfigurations: cur.LambdaFunctionConfigurations,
		QueueConfigurations:          cur.QueueConfigurations,
		TopicConfigurations:          cur.TopicConfigurations,
	}, "object-migration-preflight", "arn:aws:sqs:::preflight")
	return err
}

// objectEvents are the events we subscribe to.
var objectEvents = []s3types.Event{"s3:ObjectCreated:*", "s3:ObjectRemoved:*"}

// addQueue adds our queue to the configuration. S3 sends each event type
// of a key to one destination only, so any other entry that takes object
// created or removed events is a conflict.
func addQueue(c *s3types.NotificationConfiguration, id, queueARN string) (bool, error) {
	takes := func(owner string, events []s3types.Event) error {
		for _, e := range events {
			if strings.HasPrefix(string(e), "s3:ObjectCreated") || strings.HasPrefix(string(e), "s3:ObjectRemoved") {
				return fmt.Errorf("%w: notification %q already receives %s; S3 sends an event to one destination only", ErrConflict, owner, e)
			}
		}
		return nil
	}
	for _, x := range c.QueueConfigurations {
		if aws.ToString(x.Id) == id {
			return false, nil
		}
		if err := takes(aws.ToString(x.Id), x.Events); err != nil {
			return false, err
		}
	}
	for _, x := range c.TopicConfigurations {
		if err := takes(aws.ToString(x.Id), x.Events); err != nil {
			return false, err
		}
	}
	for _, x := range c.LambdaFunctionConfigurations {
		if err := takes(aws.ToString(x.Id), x.Events); err != nil {
			return false, err
		}
	}
	c.QueueConfigurations = append(c.QueueConfigurations, s3types.QueueConfiguration{
		Id: aws.String(id), QueueArn: aws.String(queueARN), Events: objectEvents,
	})
	return true, nil
}

func removeQueue(c *s3types.NotificationConfiguration, id string) bool {
	kept := c.QueueConfigurations[:0:0]
	for _, x := range c.QueueConfigurations {
		if aws.ToString(x.Id) != id {
			kept = append(kept, x)
		}
	}
	changed := len(kept) != len(c.QueueConfigurations)
	c.QueueConfigurations = kept
	return changed
}

// queuePolicy lets S3 send events from one bucket of one account.
func queuePolicy(queueARN, bucket, account string) (string, error) {
	b, err := json.Marshal(map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{{
			"Sid":       "S3BucketEvents",
			"Effect":    "Allow",
			"Principal": map[string]string{"Service": "s3.amazonaws.com"},
			"Action":    "sqs:SendMessage",
			"Resource":  queueARN,
			"Condition": map[string]any{
				"ArnEquals":    map[string]string{"aws:SourceArn": "arn:aws:s3:::" + bucket},
				"StringEquals": map[string]string{"aws:SourceAccount": account},
			},
		}},
	})
	return string(b), err
}

// s3Event is the body S3 sends to the queue.
type s3Event struct {
	Event   string `json:"Event"`
	Records []struct {
		EventName string    `json:"eventName"`
		EventTime time.Time `json:"eventTime"`
		S3        struct {
			Object struct {
				Key string `json:"key"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

// parseS3Event reads the changes in a message body. The test event S3
// sends when a notification is added, and bodies it cannot read, carry no
// changes.
func parseS3Event(body string) []Change {
	var e s3Event
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		return nil
	}
	var changes []Change
	for _, r := range e.Records {
		// Keys come URL-encoded, with spaces as "+".
		key, err := url.QueryUnescape(r.S3.Object.Key)
		if err != nil || key == "" {
			continue
		}
		changes = append(changes, Change{
			Key:     key,
			Removed: strings.HasPrefix(r.EventName, "ObjectRemoved"),
			At:      r.EventTime,
		})
	}
	return changes
}

func isCode(err error, code string) bool {
	var ae smithy.APIError
	return errors.As(err, &ae) && ae.ErrorCode() == code
}

// isValidationError is S3 refusing a destination it could not send a test
// event to, which a fresh queue policy causes until it applies.
func isValidationError(err error) bool {
	var ae smithy.APIError
	return errors.As(err, &ae) && ae.ErrorCode() == "InvalidArgument" &&
		strings.Contains(ae.ErrorMessage(), "validate the following destination")
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
