package eventsource

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestParseS3Event(t *testing.T) {
	body := `{"Records":[
		{"eventName":"ObjectCreated:Put","eventTime":"2026-11-04T01:02:03.456Z","s3":{"object":{"key":"photos/my+cat%281%29.jpg","size":10}}},
		{"eventName":"ObjectRemoved:Delete","eventTime":"2026-11-04T01:02:04Z","s3":{"object":{"key":"old.txt"}}}
	]}`
	got := parseS3Event(body)
	if len(got) != 2 {
		t.Fatalf("changes = %+v", got)
	}
	if got[0].Key != "photos/my cat(1).jpg" || got[0].Removed || !got[0].At.Equal(time.Date(2026, 11, 4, 1, 2, 3, 456e6, time.UTC)) {
		t.Errorf("created: %+v", got[0])
	}
	if got[1].Key != "old.txt" || !got[1].Removed {
		t.Errorf("removed: %+v", got[1])
	}
}

func TestParseS3EventWithoutChanges(t *testing.T) {
	for _, body := range []string{
		`{"Service":"Amazon S3","Event":"s3:TestEvent","Time":"2026-11-04T01:00:00Z","Bucket":"b"}`,
		`not json`,
		`{"Records":[{"eventName":"ObjectCreated:Put","s3":{"object":{"key":"%zz"}}}]}`,
	} {
		if got := parseS3Event(body); len(got) != 0 {
			t.Errorf("%s: changes = %+v", body, got)
		}
	}
}

func TestAddQueueKeepsOtherNotifications(t *testing.T) {
	c := &s3types.NotificationConfiguration{
		EventBridgeConfiguration: &s3types.EventBridgeConfiguration{},
		TopicConfigurations: []s3types.TopicConfiguration{
			{Id: aws.String("restores"), TopicArn: aws.String("arn:aws:sns:x"), Events: []s3types.Event{"s3:ObjectRestore:Completed"}},
		},
	}
	changed, err := addQueue(c, "object-migration-om-1", "arn:aws:sqs:q")
	if err != nil || !changed {
		t.Fatalf("add: changed=%v err=%v", changed, err)
	}
	if len(c.TopicConfigurations) != 1 || c.EventBridgeConfiguration == nil || len(c.QueueConfigurations) != 1 {
		t.Fatalf("configuration lost entries: %+v", c)
	}

	changed, err = addQueue(c, "object-migration-om-1", "arn:aws:sqs:q")
	if err != nil || changed || len(c.QueueConfigurations) != 1 {
		t.Fatalf("second add: changed=%v err=%v queues=%d", changed, err, len(c.QueueConfigurations))
	}

	if !removeQueue(c, "object-migration-om-1") || len(c.QueueConfigurations) != 0 || len(c.TopicConfigurations) != 1 {
		t.Fatalf("remove: %+v", c)
	}
	if removeQueue(c, "object-migration-om-1") {
		t.Fatal("second remove reported a change")
	}
}

func TestAddQueueRefusesOverlap(t *testing.T) {
	for _, c := range []*s3types.NotificationConfiguration{
		{LambdaFunctionConfigurations: []s3types.LambdaFunctionConfiguration{{Id: aws.String("thumbs"), Events: []s3types.Event{"s3:ObjectCreated:Put"}}}},
		{QueueConfigurations: []s3types.QueueConfiguration{{Id: aws.String("audit"), Events: []s3types.Event{"s3:ObjectRemoved:*"}}}},
		{TopicConfigurations: []s3types.TopicConfiguration{{Id: aws.String("all"), Events: []s3types.Event{"s3:ObjectCreated:*"}}}},
	} {
		if _, err := addQueue(c, "object-migration-om-1", "arn"); !errors.Is(err, ErrConflict) {
			t.Errorf("err = %v, want ErrConflict", err)
		}
	}
}

func TestQueuePolicyLimitsSender(t *testing.T) {
	p, err := queuePolicy("arn:aws:sqs:ap-northeast-1:111:om-1", "src-bucket", "222")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Statement []struct {
			Resource  string
			Condition map[string]map[string]string
		}
	}
	if err := json.Unmarshal([]byte(p), &doc); err != nil {
		t.Fatal(err)
	}
	s := doc.Statement[0]
	if s.Resource != "arn:aws:sqs:ap-northeast-1:111:om-1" ||
		s.Condition["ArnEquals"]["aws:SourceArn"] != "arn:aws:s3:::src-bucket" ||
		s.Condition["StringEquals"]["aws:SourceAccount"] != "222" {
		t.Fatalf("policy = %s", p)
	}
}
