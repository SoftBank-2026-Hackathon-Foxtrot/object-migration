package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

// Dynamo keeps migrations in a DynamoDB table of this service's own, so a
// restarted server picks up where it stopped. Each migration is one JSON
// document with its version beside it; a write goes through only when the
// version still matches. The table also keeps what the server must not
// forget about a running migration: the keys apps deleted, the change
// feed's resume point, and the responses replayed for Idempotency-Key.
//
// Items, by partition key (pk) and sort key (sk):
//
//	migrations          <id>        version, terminal, doc
//	deleted#<id>        <key>       at (unix ns, first recorded)
//	feed#<id>           position    position
//	response#<key>      response    fingerprint, status, location, body
//
// All migrations share one partition, so ListActive is a query rather than
// a scan; a platform runs a handful of migrations at a time.
type Dynamo struct {
	db    *dynamodb.Client
	table string
}

const (
	migrationsPK   = "migrations"
	deletedPrefix  = "deleted#"
	feedPrefix     = "feed#"
	responsePrefix = "response#"
)

// OpenDynamo uses table, creating it (on-demand capacity) if it is missing.
func OpenDynamo(ctx context.Context, db *dynamodb.Client, table string) (*Dynamo, error) {
	s := &Dynamo{db: db, table: table}
	_, err := db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &table})
	if missing := (*types.ResourceNotFoundException)(nil); errors.As(err, &missing) {
		_, err = db.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: &table,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
			},
			BillingMode: types.BillingModePayPerRequest,
		})
		if inUse := (*types.ResourceInUseException)(nil); errors.As(err, &inUse) {
			err = nil // another server is creating it
		}
	}
	if err != nil {
		return nil, fmt.Errorf("table %s: %w", table, err)
	}
	waiter := dynamodb.NewTableExistsWaiter(db, func(o *dynamodb.TableExistsWaiterOptions) { o.MinDelay = time.Second })
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: &table}, 2*time.Minute); err != nil {
		return nil, fmt.Errorf("table %s: %w", table, err)
	}
	return s, nil
}

func str(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func num(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

func itemKey(pk, sk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"pk": str(pk), "sk": str(sk)}
}

func getS(item map[string]types.AttributeValue, name string) string {
	v, _ := item[name].(*types.AttributeValueMemberS)
	if v == nil {
		return ""
	}
	return v.Value
}

func getN(item map[string]types.AttributeValue, name string) int64 {
	v, _ := item[name].(*types.AttributeValueMemberN)
	if v == nil {
		return 0
	}
	n, _ := strconv.ParseInt(v.Value, 10, 64)
	return n
}

func getB(item map[string]types.AttributeValue, name string) []byte {
	v, _ := item[name].(*types.AttributeValueMemberB)
	if v == nil {
		return nil
	}
	return v.Value
}

func conditionFailed(err error) (*types.ConditionalCheckFailedException, bool) {
	var cf *types.ConditionalCheckFailedException
	return cf, errors.As(err, &cf)
}

func migrationItem(m migration.Migration) (map[string]types.AttributeValue, error) {
	doc, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	item := itemKey(migrationsPK, m.ID)
	item["version"] = num(m.Version)
	item["terminal"] = &types.AttributeValueMemberBOOL{Value: m.Phase.Terminal()}
	item["doc"] = str(string(doc))
	return item, nil
}

func decodeMigration(item map[string]types.AttributeValue) (migration.Migration, error) {
	var m migration.Migration
	if err := json.Unmarshal([]byte(getS(item, "doc")), &m); err != nil {
		return migration.Migration{}, fmt.Errorf("decode migration %s: %w", getS(item, "sk"), err)
	}
	return m, nil
}

func (s *Dynamo) Create(ctx context.Context, m migration.Migration) (migration.Migration, error) {
	m.Version = 1
	item, err := migrationItem(m)
	if err != nil {
		return migration.Migration{}, err
	}
	_, err = s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           &s.table,
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	})
	if _, ok := conditionFailed(err); ok {
		return migration.Migration{}, ErrExists
	}
	if err != nil {
		return migration.Migration{}, err
	}
	return m, nil
}

func (s *Dynamo) Get(ctx context.Context, id string) (migration.Migration, error) {
	out, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.table, Key: itemKey(migrationsPK, id), ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return migration.Migration{}, err
	}
	if out.Item == nil {
		return migration.Migration{}, ErrNotFound
	}
	return decodeMigration(out.Item)
}

// RequestCommand reads, checks and writes back, starting over when a
// worker wrote in between.
func (s *Dynamo) RequestCommand(ctx context.Context, id string, req migration.CommandRequest) (migration.Migration, error) {
	for {
		m, err := s.Get(ctx, id)
		if err != nil {
			return migration.Migration{}, err
		}
		before := m.Pending
		if err := m.RequestCommand(req); err != nil {
			return migration.Migration{}, err
		}
		if before != nil && samePending(*before, *m.Pending) {
			return m, nil // a repeated request; nothing to write
		}
		m.Version++
		err = s.write(ctx, m, m.Version-1)
		if errors.Is(err, ErrConflict) && ctx.Err() == nil {
			continue
		}
		if err != nil {
			return migration.Migration{}, err
		}
		return m, nil
	}
}

func (s *Dynamo) Update(ctx context.Context, m migration.Migration) (migration.Migration, error) {
	m.Version++
	if err := s.write(ctx, m, m.Version-1); err != nil {
		return migration.Migration{}, err
	}
	return m, nil
}

// write saves m if the stored version is still was.
func (s *Dynamo) write(ctx context.Context, m migration.Migration, was int64) error {
	item, err := migrationItem(m)
	if err != nil {
		return err
	}
	_, err = s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                           &s.table,
		Item:                                item,
		ConditionExpression:                 aws.String("#v = :was"),
		ExpressionAttributeNames:            map[string]string{"#v": "version"},
		ExpressionAttributeValues:           map[string]types.AttributeValue{":was": num(was)},
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if cf, ok := conditionFailed(err); ok {
		if len(cf.Item) == 0 {
			return ErrNotFound
		}
		return ErrConflict
	}
	return err
}

func (s *Dynamo) ListActive(ctx context.Context) ([]migration.Migration, error) {
	var out []migration.Migration
	pages := dynamodb.NewQueryPaginator(s.db, &dynamodb.QueryInput{
		TableName:                 &s.table,
		KeyConditionExpression:    aws.String("pk = :pk"),
		FilterExpression:          aws.String("#t = :no"),
		ExpressionAttributeNames:  map[string]string{"#t": "terminal"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": str(migrationsPK), ":no": &types.AttributeValueMemberBOOL{Value: false}},
		ConsistentRead:            aws.Bool(true),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			m, err := decodeMigration(item)
			if err != nil {
				return nil, err
			}
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// sortKeys returns the sort keys in partition pk.
func (s *Dynamo) sortKeys(ctx context.Context, pk string) ([]string, error) {
	var out []string
	pages := dynamodb.NewQueryPaginator(s.db, &dynamodb.QueryInput{
		TableName:                 &s.table,
		KeyConditionExpression:    aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": str(pk)},
		ProjectionExpression:      aws.String("sk"),
		ConsistentRead:            aws.Bool(true),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			out = append(out, getS(item, "sk"))
		}
	}
	return out, nil
}

// RecordDeletes saves keys apps deleted during a migration. A key keeps
// the time it was first recorded.
func (s *Dynamo) RecordDeletes(ctx context.Context, id string, keys []string, at time.Time) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(16)
	for _, key := range keys {
		g.Go(func() error {
			_, err := s.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName:                 &s.table,
				Key:                       itemKey(deletedPrefix+id, key),
				UpdateExpression:          aws.String("SET #at = if_not_exists(#at, :at)"),
				ExpressionAttributeNames:  map[string]string{"#at": "at"},
				ExpressionAttributeValues: map[string]types.AttributeValue{":at": num(at.UnixNano())},
			})
			return err
		})
	}
	return g.Wait()
}

// LoadDeletes returns every recorded key, by migration. It scans the
// table, which only happens when the server starts.
func (s *Dynamo) LoadDeletes(ctx context.Context) (map[string]map[string]time.Time, error) {
	out := make(map[string]map[string]time.Time)
	pages := dynamodb.NewScanPaginator(s.db, &dynamodb.ScanInput{
		TableName:                 &s.table,
		FilterExpression:          aws.String("begins_with(pk, :p)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":p": str(deletedPrefix)},
		ConsistentRead:            aws.Bool(true),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			id := strings.TrimPrefix(getS(item, "pk"), deletedPrefix)
			if out[id] == nil {
				out[id] = make(map[string]time.Time)
			}
			out[id][getS(item, "sk")] = time.Unix(0, getN(item, "at"))
		}
	}
	return out, nil
}

func (s *Dynamo) ForgetDeletes(ctx context.Context, id string) error {
	keys, err := s.sortKeys(ctx, deletedPrefix+id)
	if err != nil {
		return err
	}
	for len(keys) > 0 {
		n := min(len(keys), 25) // the most one BatchWriteItem takes
		reqs := make([]types.WriteRequest, n)
		for i, key := range keys[:n] {
			reqs[i] = types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: itemKey(deletedPrefix+id, key)}}
		}
		if err := s.batchWrite(ctx, reqs); err != nil {
			return err
		}
		keys = keys[n:]
	}
	return nil
}

// batchWrite sends reqs, resending what DynamoDB left unprocessed.
func (s *Dynamo) batchWrite(ctx context.Context, reqs []types.WriteRequest) error {
	wait := 50 * time.Millisecond
	for len(reqs) > 0 {
		out, err := s.db.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{s.table: reqs},
		})
		if err != nil {
			return err
		}
		reqs = out.UnprocessedItems[s.table]
		if len(reqs) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		wait = min(2*wait, 2*time.Second)
	}
	return nil
}

// FeedPosition returns where a migration's change feed was read up to, or
// "" if nothing was saved.
func (s *Dynamo) FeedPosition(ctx context.Context, id string) (string, error) {
	out, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.table, Key: itemKey(feedPrefix+id, "position"), ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return "", err
	}
	return getS(out.Item, "position"), nil
}

func (s *Dynamo) SaveFeedPosition(ctx context.Context, id, pos string) error {
	item := itemKey(feedPrefix+id, "position")
	item["position"] = str(pos)
	_, err := s.db.PutItem(ctx, &dynamodb.PutItemInput{TableName: &s.table, Item: item})
	return err
}

func (s *Dynamo) ForgetFeedPosition(ctx context.Context, id string) error {
	_, err := s.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: &s.table, Key: itemKey(feedPrefix+id, "position")})
	return err
}

func (s *Dynamo) SavedResponse(ctx context.Context, key string) (SavedResponse, bool, error) {
	out, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.table, Key: itemKey(responsePrefix+key, "response"), ConsistentRead: aws.Bool(true),
	})
	if err != nil || out.Item == nil {
		return SavedResponse{}, false, err
	}
	r := SavedResponse{
		Status:   int(getN(out.Item, "status")),
		Location: getS(out.Item, "location"),
		Body:     getB(out.Item, "body"),
	}
	copy(r.Fingerprint[:], getB(out.Item, "fingerprint"))
	return r, true, nil
}

func (s *Dynamo) SaveResponse(ctx context.Context, key string, r SavedResponse) error {
	item := itemKey(responsePrefix+key, "response")
	item["fingerprint"] = &types.AttributeValueMemberB{Value: r.Fingerprint[:]}
	item["status"] = num(int64(r.Status))
	item["location"] = str(r.Location)
	item["body"] = &types.AttributeValueMemberB{Value: append([]byte{}, r.Body...)} // never nil, even when empty
	_, err := s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           &s.table,
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	})
	if _, ok := conditionFailed(err); ok {
		return nil // the first response stays
	}
	return err
}
