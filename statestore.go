package kinesumer

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/guregu/dynamo"
	"github.com/pkg/errors"
)

// Error codes.
var (
	ErrNoShardCache  = errors.New("kinesumer: shard cache not found")
	ErrEmptyShardIDs = errors.New("kinesumer: empty shard ids given")
)

type (
	// StateStore is a distributed key-value store for managing states.
	StateStore interface {
		GetShards(ctx context.Context, stream string) (Shards, error)
		UpdateShards(ctx context.Context, stream string, shards Shards) error
		ListAllAliveClientIDs(ctx context.Context) ([]string, error)
		RegisterClient(ctx context.Context, clientID string) error
		DeregisterClient(ctx context.Context, clientID string) error
		PingClientAliveness(ctx context.Context, clientID string) error
		PruneClients(ctx context.Context) error
		ListCheckPoints(ctx context.Context, stream string, shardIDs []string) (map[string]string, error)
		UpdateCheckPoints(ctx context.Context, checkpoints []*shardCheckPoint) error
	}

	db struct {
		client *dynamo.DB
		table  *dynamo.Table
	}

	// stateStore implements the StateStore with AWS DynamoDB. (default)
	stateStore struct {
		app string
		db  *db
	}
)

// newStateStore initializes the state store.
func newStateStore(cfg *Config) (StateStore, error) {
	awsCfg := aws.NewConfig()
	awsCfg.WithRegion(cfg.DynamoDBRegion)
	if cfg.DynamoDBEndpoint != "" {
		awsCfg.WithEndpoint(cfg.DynamoDBEndpoint)
	}
	sess, err := session.NewSession(awsCfg)
	if err != nil {
		return nil, errors.Wrap(err, "kinesumer: failed to create an aws session")
	}
	// Ping-like request to check if client can reach to DynamoDB.
	client := dynamo.New(sess)
	table := client.Table(cfg.DynamoDBTable)
	if _, err := table.Describe().Run(); err != nil {
		return nil, errors.Wrap(err, "kinesumer: client can't access to dynamodb")
	}
	return &stateStore{
		app: cfg.App,
		db: &db{
			client: client,
			table:  &table,
		},
	}, nil
}

// GetShards fetches a cached shard list.
func (s *stateStore) GetShards(
	ctx context.Context, stream string,
) (Shards, error) {
	var (
		key   = buildShardCacheKey(s.app)
		cache *stateShardCache
	)
	err := s.db.table.
		Get("pk", key).
		Range("sk", dynamo.Equal, stream).
		Consistent(true).
		OneWithContext(ctx, &cache)
	if errors.Is(err, dynamo.ErrNotFound) {
		return nil, errors.WithStack(ErrNoShardCache)
	} else if err != nil {
		return nil, errors.WithStack(err)
	}
	return cache.Shards, nil
}

// UpdateShards updates a shard list cache.
func (s *stateStore) UpdateShards(
	ctx context.Context, stream string, shards Shards,
) error {
	key := buildShardCacheKey(s.app)
	err := s.db.table.
		Update("pk", key).
		Range("sk", stream).
		Set("shards", shards).
		RunWithContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// ListAllAliveClientIDs fetches an id list of all alive clients.
func (s *stateStore) ListAllAliveClientIDs(ctx context.Context) ([]string, error) {
	var (
		key     = buildClientKey(s.app)
		now     = time.Now()
		clients []*stateClient
	)
	err := s.db.table.
		Get("pk", key).
		Range("sk", dynamo.Greater, " ").
		Filter("last_update > ?", now.Add(-outdatedGap)).
		Order(dynamo.Ascending).
		AllWithContext(ctx, &clients)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var ids []string
	for _, client := range clients {
		ids = append(ids, client.ClientID)
	}
	return ids, nil
}

// RegisterClient registers a client to state store.
func (s *stateStore) RegisterClient(
	ctx context.Context, clientID string,
) error {
	var (
		key = buildClientKey(s.app)
		now = time.Now()
	)
	client := stateClient{
		ClientKey:  key,
		ClientID:   clientID,
		LastUpdate: now,
	}
	if err := s.db.table.Put(client).RunWithContext(ctx); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// DeregisterClient de-registers a client from the state store.
func (s *stateStore) DeregisterClient(
	ctx context.Context, clientID string,
) error {
	key := buildClientKey(s.app)
	err := s.db.table.
		Delete("pk", key).
		Range("sk", clientID).
		RunWithContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *stateStore) PingClientAliveness(
	ctx context.Context, clientID string,
) error {
	var (
		key = buildClientKey(s.app)
		now = time.Now()
	)
	err := s.db.table.
		Update("pk", key).
		Range("sk", clientID).
		Set("last_update", now).
		RunWithContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// PruneClients prune clients that have been inactive for a certain amount of time.
func (s *stateStore) PruneClients(ctx context.Context) error {
	var (
		key = buildClientKey(s.app)
		now = time.Now()
	)
	var outdated []*stateClient
	err := s.db.table.
		Get("pk", key).
		Range("last_update", dynamo.Less, now.Add(-outdatedGap)).
		Index("index-client-key-last-update").
		AllWithContext(ctx, &outdated)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(outdated) == 0 {
		return nil
	}

	var keys []dynamo.Keyed
	for _, client := range outdated {
		keys = append(
			keys, dynamo.Keys{client.ClientKey, client.ClientID},
		)
	}

	_, err = s.db.table.
		Batch("pk", "sk").
		Write().
		Delete(keys...).
		RunWithContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// ListCheckPoints fetches check point sequence numbers for multiple shards.
func (s *stateStore) ListCheckPoints(
	ctx context.Context, stream string, shardIDs []string,
) (map[string]string, error) {
	if len(shardIDs) == 0 {
		return nil, ErrEmptyShardIDs
	}

	var (
		keys   []dynamo.Keyed
		seqMap = make(map[string]string)
	)
	for _, id := range shardIDs {
		keys = append(
			keys,
			dynamo.Keys{buildCheckPointKey(s.app, stream), id},
		)
	}

	var checkPoints []*stateCheckPoint
	err := s.db.table.
		Batch("pk", "sk").
		Get(keys...).
		AllWithContext(ctx, &checkPoints)
	if errors.Is(err, dynamo.ErrNotFound) {
		return seqMap, nil
	} else if err != nil {
		return nil, errors.WithStack(err)
	}

	for _, checkPoint := range checkPoints {
		seqMap[checkPoint.ShardID] = checkPoint.SequenceNumber
	}
	return seqMap, nil
}

// UpdateCheckPoints updates the check point sequence numbers for a shards.
// length of checkpoints parameter can't be no more than 25.
// Reference: https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/#DynamoDB.BatchWriteItem
func (s *stateStore) UpdateCheckPoints(ctx context.Context, checkpoints []*shardCheckPoint) error {
	var (
		stateCheckPoints []interface{}
		now              = time.Now()
	)
	for _, checkpoint := range checkpoints {
		stateCheckPoints = append(stateCheckPoints, stateCheckPoint{
			StreamKey:      buildCheckPointKey(s.app, checkpoint.Stream),
			ShardID:        checkpoint.ShardID,
			SequenceNumber: checkpoint.SequenceNumber,
			LastUpdate:     now,
		})
	}

	// TODO(proost): check written bytes
	_, err := s.db.table.
		Batch("pk", "sk").
		Write().
		Put(stateCheckPoints...).
		RunWithContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
