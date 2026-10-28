package kinesumer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
)

type kinesisRequestLog struct {
	mu        sync.Mutex
	iterators map[string]int
	records   map[string]int
}

func newKinesisTestServer(t *testing.T) (*httptest.Server, *kinesisRequestLog) {
	t.Helper()
	requests := &kinesisRequestLog{
		iterators: make(map[string]int),
		records:   make(map[string]int),
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		switch {
		case strings.HasSuffix(r.Header.Get("X-Amz-Target"), ".GetShardIterator"):
			var input struct {
				ShardID string `json:"ShardId"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			requests.mu.Lock()
			requests.iterators[input.ShardID]++
			requests.mu.Unlock()
			_ = json.NewEncoder(w).Encode(struct {
				ShardIterator string `json:"ShardIterator"`
			}{ShardIterator: "iterator-" + input.ShardID})
		case strings.HasSuffix(r.Header.Get("X-Amz-Target"), ".GetRecords"):
			var input struct {
				ShardIterator string `json:"ShardIterator"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			requests.mu.Lock()
			requests.records[input.ShardIterator]++
			requests.mu.Unlock()
			_ = json.NewEncoder(w).Encode(struct {
				Records            []interface{} `json:"Records"`
				NextShardIterator  string        `json:"NextShardIterator"`
				MillisBehindLatest int64         `json:"MillisBehindLatest"`
			}{NextShardIterator: input.ShardIterator})
		default:
			http.Error(w, "unexpected Kinesis operation", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	return server, requests
}

func (r *kinesisRequestLog) iteratorCount(shardID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.iterators[shardID]
}

func (r *kinesisRequestLog) recordCount(iterator string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.records[iterator]
}

func restoreTestEnv(key, old string, existed bool) func() {
	return func() {
		if existed {
			_ = os.Setenv(key, old)
		} else {
			_ = os.Unsetenv(key)
		}
	}
}

func setTestAWSCredentials(t *testing.T) {
	t.Helper()
	for key, value := range map[string]string{
		"AWS_ACCESS_KEY_ID":     "test-access-key",
		"AWS_SECRET_ACCESS_KEY": "test-secret-key",
		"AWS_DEFAULT_REGION":    "us-east-1",
	} {
		old, existed := os.LookupEnv(key)
		if err := os.Setenv(key, value); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
		t.Cleanup(restoreTestEnv(key, old, existed))
	}
}

func newCheckpointTestKinesumer(
	t *testing.T, store *MockStateStore, endpoint string, scanInterval time.Duration,
) *Kinesumer {
	t.Helper()
	setTestAWSCredentials(t)
	var stateStore StateStore = store
	k, err := NewKinesumer(&Config{
		App:             "test",
		ClientID:        "client",
		KinesisRegion:   "us-east-1",
		KinesisEndpoint: endpoint,
		StateStore:      &stateStore,
		ScanInterval:    scanInterval,
		Commit:          &CommitConfig{Auto: false},
	})
	if err != nil {
		t.Fatalf("NewKinesumer failed: %v", err)
	}
	return k
}

func waitForKinesisRequest(t *testing.T, count func() int) {
	waitForKinesisRequestCount(t, count, 0)
}

func waitForKinesisRequestCount(t *testing.T, count func() int, previous int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if count() > previous {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for Kinesis request")
}

func isChannelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestKinesumer_ConsumeCheckpointLookupFailureDoesNotStartPolling(t *testing.T) {
	server, requests := newKinesisTestServer(t)
	ctrl := gomock.NewController(t)
	store := NewMockStateStore(ctrl)
	lookupErr := errors.New("checkpoint lookup failed")
	store.EXPECT().RegisterClient(gomock.Any(), "client").Return(nil)
	store.EXPECT().ListAllAliveClientIDs(gomock.Any()).Return([]string{"client"}, nil)
	store.EXPECT().GetShards(gomock.Any(), "stream").Return(Shards{&Shard{ID: "shard-1"}}, nil)
	store.EXPECT().ListCheckPoints(gomock.Any(), "stream", []string{"shard-1"}).Return(nil, lookupErr)

	k := newCheckpointTestKinesumer(t, store, server.URL, time.Hour)
	_, err := k.Consume([]string{"stream"})
	if !errors.Is(err, lookupErr) {
		t.Fatalf("Consume error = %v, want checkpoint lookup error", err)
	}
	if len(k.shards) != 0 || k.nextIters["stream"] != nil || k.checkPoints["stream"] != nil || k.offsets["stream"] != nil {
		t.Fatalf("checkpoint lookup failure published partial assignment state: shards=%v", k.shards)
	}
	if requests.iteratorCount("shard-1") != 0 {
		t.Fatalf("checkpoint lookup failure requested an iterator for shard-1")
	}

	// Close must remain safe when no worker was started for the failed assignment.
	k.Close()
}

func TestKinesumer_ConsumeCanRetryAfterCheckpointLookupFailure(t *testing.T) {
	server, requests := newKinesisTestServer(t)
	ctrl := gomock.NewController(t)
	store := NewMockStateStore(ctrl)
	lookupErr := errors.New("checkpoint lookup failed")
	store.EXPECT().RegisterClient(gomock.Any(), "client").Return(nil)
	store.EXPECT().ListAllAliveClientIDs(gomock.Any()).Return([]string{"client"}, nil).AnyTimes()
	store.EXPECT().GetShards(gomock.Any(), "stream").Return(Shards{&Shard{ID: "shard-1"}}, nil).AnyTimes()
	store.EXPECT().ListCheckPoints(gomock.Any(), "stream", []string{"shard-1"}).Return(nil, lookupErr)
	store.EXPECT().ListCheckPoints(gomock.Any(), "stream", []string{"shard-1"}).Return(map[string]string{"shard-1": "sequence-1"}, nil).AnyTimes()
	store.EXPECT().DeregisterClient(gomock.Any(), "client").Return(nil).AnyTimes()

	k := newCheckpointTestKinesumer(t, store, server.URL, time.Millisecond)
	defer k.Close()
	// Keep the worker's state readable so an invalid startup can be observed without a panic.
	k.checkPoints["stream"] = &sync.Map{}
	k.offsets["stream"] = &sync.Map{}
	if _, err := k.Consume([]string{"stream"}); !errors.Is(err, lookupErr) {
		t.Fatalf("first Consume error = %v, want checkpoint lookup error", err)
	}
	if len(k.shards) != 0 || k.nextIters["stream"] != nil {
		t.Fatalf("failed first Consume changed state needed for retry: shards=%v", k.shards)
	}
	time.Sleep(20 * time.Millisecond)
	if requests.iteratorCount("shard-1") != 0 {
		t.Fatal("failed first Consume requested an iterator for shard-1")
	}

	if _, err := k.Consume([]string{"stream"}); err != nil {
		t.Fatalf("retry after checkpoint lookup failure failed: %v", err)
	}
	waitForKinesisRequest(t, func() int { return requests.iteratorCount("shard-1") })
	if got := k.shards["stream"].ids(); len(got) != 1 || got[0] != "shard-1" {
		t.Fatalf("retry produced shards %v", got)
	}
	if got, ok := k.checkPoints["stream"].Load("shard-1"); !ok || got != "sequence-1" {
		t.Fatalf("retry checkpoint = %v, present=%t", got, ok)
	}
}

func TestKinesumer_CheckpointLookupFailurePreservesPreviousAssignment(t *testing.T) {
	server, requests := newKinesisTestServer(t)
	ctrl := gomock.NewController(t)
	store := NewMockStateStore(ctrl)
	lookupErr := errors.New("checkpoint lookup failed")
	store.EXPECT().RegisterClient(gomock.Any(), "client").Return(nil)
	store.EXPECT().GetShards(gomock.Any(), "stream").Return(Shards{&Shard{ID: "shard-new"}}, nil)
	store.EXPECT().ListCheckPoints(gomock.Any(), "stream", []string{"shard-new"}).Return(nil, lookupErr)
	store.EXPECT().UpdateCheckPoints(gomock.Any(), gomock.Any()).Return(nil).Times(2)

	k := newCheckpointTestKinesumer(t, store, server.URL, time.Millisecond)
	oldIterator := "iterator-shard-old"
	k.shards["stream"] = Shards{&Shard{ID: "shard-old"}}
	k.checkPoints["stream"] = &sync.Map{}
	k.checkPoints["stream"].Store("shard-old", "sequence-old")
	k.offsets["stream"] = &sync.Map{}
	k.offsets["stream"].Store("shard-old", "offset-old")
	k.nextIters["stream"] = &sync.Map{}
	k.nextIters["stream"].Store("shard-old", &oldIterator)
	t.Cleanup(func() {
		if !isChannelClosed(k.stop) {
			k.pause()
		}
	})
	k.start()
	waitForKinesisRequest(t, func() int { return requests.recordCount(oldIterator) })

	err := k.syncShardInfoForStream(context.Background(), "stream", 0, 1)
	if !errors.Is(err, lookupErr) {
		t.Fatalf("syncShardInfoForStream error = %v, want checkpoint lookup error", err)
	}
	if got := k.shards["stream"].ids(); len(got) != 1 || got[0] != "shard-old" {
		t.Fatalf("failed replacement changed assignment to %v", got)
	}
	if got, ok := k.checkPoints["stream"].Load("shard-old"); !ok || got != "sequence-old" {
		t.Fatalf("failed replacement changed checkpoint state: value=%v present=%t", got, ok)
	}
	if got, ok := k.offsets["stream"].Load("shard-old"); !ok || got != "offset-old" {
		t.Fatalf("failed replacement changed uncommitted offset state: value=%v present=%t", got, ok)
	}
	if _, ok := k.nextIters["stream"].Load("shard-old"); !ok {
		t.Fatal("failed replacement removed the previous shard iterator")
	}
	requestsBeforeResume := requests.recordCount(oldIterator)
	waitForKinesisRequestCount(t, func() int { return requests.recordCount(oldIterator) }, requestsBeforeResume)
	if requests.iteratorCount("shard-new") != 0 {
		t.Fatal("failed replacement requested an iterator for shard-new")
	}
}

func TestKinesumer_SuccessfulStartupAndReassignmentStartCompleteAssignments(t *testing.T) {
	server, requests := newKinesisTestServer(t)
	ctrl := gomock.NewController(t)
	store := NewMockStateStore(ctrl)
	store.EXPECT().RegisterClient(gomock.Any(), "client").Return(nil)
	store.EXPECT().ListAllAliveClientIDs(gomock.Any()).Return([]string{"client"}, nil)
	store.EXPECT().GetShards(gomock.Any(), "stream").Return(Shards{&Shard{ID: "shard-old"}}, nil)
	store.EXPECT().ListCheckPoints(gomock.Any(), "stream", []string{"shard-old"}).Return(map[string]string{"shard-old": "sequence-old"}, nil)
	store.EXPECT().GetShards(gomock.Any(), "stream").Return(Shards{&Shard{ID: "shard-new"}}, nil)
	store.EXPECT().ListCheckPoints(gomock.Any(), "stream", []string{"shard-new"}).Return(map[string]string{"shard-new": "sequence-new"}, nil)
	store.EXPECT().DeregisterClient(gomock.Any(), "client").Return(nil)

	k := newCheckpointTestKinesumer(t, store, server.URL, time.Millisecond)
	defer k.Close()

	if _, err := k.Consume([]string{"stream"}); err != nil {
		t.Fatalf("initial shard assignment failed: %v", err)
	}
	waitForKinesisRequest(t, func() int { return requests.iteratorCount("shard-old") })
	if got, ok := k.checkPoints["stream"].Load("shard-old"); !ok || got != "sequence-old" {
		t.Fatalf("initial assignment checkpoint = %v, present=%t", got, ok)
	}

	if err := k.syncShardInfoForStream(context.Background(), "stream", 0, 1); err != nil {
		t.Fatalf("shard reassignment failed: %v", err)
	}
	waitForKinesisRequest(t, func() int { return requests.iteratorCount("shard-new") })
	if got := k.shards["stream"].ids(); len(got) != 1 || got[0] != "shard-new" {
		t.Fatalf("successful reassignment produced shards %v", got)
	}
	if got, ok := k.checkPoints["stream"].Load("shard-new"); !ok || got != "sequence-new" {
		t.Fatalf("reassigned checkpoint = %v, present=%t", got, ok)
	}
	if _, ok := k.checkPoints["stream"].Load("shard-old"); ok {
		t.Fatal("successful reassignment retained the old checkpoint")
	}
}
