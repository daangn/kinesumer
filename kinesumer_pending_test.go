package kinesumer

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

func TestPendingRecordsDiscardedWhenShardAssignmentChanges(t *testing.T) {
	store := newBackpressureStateStore("")
	store.shardsByStream = map[string]Shards{"first": {}}
	client := &Kinesumer{
		stateStore: store,
		shards: map[string]Shards{
			"first": {{ID: "shard-first"}},
		},
		nextIters: map[string]*sync.Map{
			"first": {},
		},
		checkPoints: map[string]*sync.Map{
			"first": {},
		},
		offsets: map[string]*sync.Map{
			"first": {},
		},
		stop:  make(chan struct{}),
		close: make(chan struct{}),
		mu:    &sync.Mutex{},
	}
	key := pendingRecordsKey{stream: "first", shardID: "shard-first"}
	client.pendingRecords.Store(key, []*kinesis.Record{{}})

	if err := client.syncShardInfoForStream(context.Background(), "first", 0, 1); err != nil {
		t.Fatalf("sync shard assignment: %v", err)
	}
	if _, ok := client.pendingRecords.Load(key); ok {
		t.Fatal("pending records for a shard no longer owned by this client were retained")
	}
}
