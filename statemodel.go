package kinesumer

import (
	"fmt"
	"time"
)

var buildKeyFn = fmt.Sprintf

const (
	shardCacheKeyFmt = "shard_cache#%s"    // shard_cache#<app>.
	clientKeyFmt     = "client#%s"         // client#<app>.
	checkPointKeyFmt = "check_point#%s#%s" // check_point#<app>#<stream>.
)

// stateShardCache manages shard id list cache.
type stateShardCache struct {
	ShardCacheKey string   `dynamo:"pk,pk"`
	Stream        string   `dynamo:"sk,sk"`
	Shards        Shards   `dynamo:"shards"`
	ShardIDs      []string `dynamo:"shard_ids"` // Deprecated.
}

func buildShardCacheKey(app string) string {
	return buildKeyFn(shardCacheKeyFmt, app)
}

// stateClient manages consumer client.
type stateClient struct {
	ClientKey  string    `dynamo:"pk,pk"`
	ClientID   string    `dynamo:"sk,sk"`
	LastUpdate time.Time `dynamo:"last_update,lsi1"`
}

func buildClientKey(app string) string {
	return buildKeyFn(clientKeyFmt, app)
}

// ShardCheckPoint manages a shard check point.
type ShardCheckPoint struct {
	Stream         string
	ShardID        string
	SequenceNumber string
	UpdatedAt      time.Time
}

// stateCheckPoint manages record check points.
type stateCheckPoint struct {
	StreamKey      string    `dynamo:"pk,pk"`
	ShardID        string    `dynamo:"sk,sk"`
	SequenceNumber string    `dynamo:"sequence_number"`
	LastUpdate     time.Time `dynamo:"last_update"`
}

func buildCheckPointKey(app, stream string) string {
	return buildKeyFn(checkPointKeyFmt, app, stream)
}
