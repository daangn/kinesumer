package kinesumer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type backpressureStateStore struct {
	gateStream      string
	shardsByStream  map[string]Shards
	lookupStarted   chan struct{}
	lookupReleased  chan struct{}
	lookupReturned  chan struct{}
	releaseOnce     sync.Once
	returnedOnce    sync.Once
	checkpointMu    sync.Mutex
	checkpointCalls [][]*ShardCheckPoint
}

func newBackpressureStateStore(gateStream string) *backpressureStateStore {
	return &backpressureStateStore{
		gateStream:     gateStream,
		lookupStarted:  make(chan struct{}, 1),
		lookupReleased: make(chan struct{}),
		lookupReturned: make(chan struct{}),
	}
}

func (s *backpressureStateStore) GetShards(_ context.Context, stream string) (Shards, error) {
	if shards, ok := s.shardsByStream[stream]; ok {
		return shards, nil
	}
	if stream == s.gateStream {
		select {
		case s.lookupStarted <- struct{}{}:
		default:
		}
		<-s.lookupReleased
		s.returnedOnce.Do(func() { close(s.lookupReturned) })
	}
	return Shards{{ID: "shard-" + stream}}, nil
}

func (s *backpressureStateStore) UpdateShards(context.Context, string, Shards) error { return nil }

func (s *backpressureStateStore) ListAllAliveClientIDs(context.Context) ([]string, error) {
	return []string{"test-client"}, nil
}

func (s *backpressureStateStore) RegisterClient(context.Context, string) error   { return nil }
func (s *backpressureStateStore) DeregisterClient(context.Context, string) error { return nil }
func (s *backpressureStateStore) PingClientAliveness(context.Context, string) error {
	return nil
}
func (s *backpressureStateStore) PruneClients(context.Context) error { return nil }

func (s *backpressureStateStore) ListCheckPoints(
	context.Context, string, []string,
) (map[string]string, error) {
	return map[string]string{}, nil
}

func (s *backpressureStateStore) UpdateCheckPoints(
	_ context.Context, checkpoints []*ShardCheckPoint,
) error {
	copy := append([]*ShardCheckPoint(nil), checkpoints...)
	s.checkpointMu.Lock()
	s.checkpointCalls = append(s.checkpointCalls, copy)
	s.checkpointMu.Unlock()
	return nil
}

func (s *backpressureStateStore) releaseLookup() {
	s.releaseOnce.Do(func() { close(s.lookupReleased) })
}

func (s *backpressureStateStore) savedCheckpoints() []*ShardCheckPoint {
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()

	var checkpoints []*ShardCheckPoint
	for _, call := range s.checkpointCalls {
		checkpoints = append(checkpoints, call...)
	}
	return checkpoints
}

type backpressureKinesis struct {
	recordsStream string
	batchSize     int
	batchServed   chan struct{}
	batchOnce     sync.Once
	mu            sync.Mutex
	served        bool
}

func (f *backpressureKinesis) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var input struct {
		StreamName    string
		ShardIterator string
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	switch strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "Kinesis_20131202.") {
	case "GetShardIterator":
		_ = json.NewEncoder(w).Encode(struct {
			ShardIterator string
		}{ShardIterator: "iterator:" + input.StreamName})
	case "GetRecords":
		stream := strings.TrimPrefix(input.ShardIterator, "iterator:")
		stream = strings.TrimPrefix(stream, "next:")
		var records []struct {
			Data           string
			PartitionKey   string
			SequenceNumber string
		}

		f.mu.Lock()
		if stream == f.recordsStream && strings.HasPrefix(input.ShardIterator, "iterator:") && !f.served {
			f.served = true
			for i := 1; i <= f.batchSize; i++ {
				records = append(records, struct {
					Data           string
					PartitionKey   string
					SequenceNumber string
				}{Data: "eA==", PartitionKey: "key", SequenceNumber: strconv.Itoa(i)})
			}
		}
		f.mu.Unlock()
		if len(records) > 0 {
			f.batchOnce.Do(func() { close(f.batchServed) })
		}

		_ = json.NewEncoder(w).Encode(struct {
			Records            interface{}
			NextShardIterator  string
			MillisBehindLatest int64
		}{Records: records, NextShardIterator: "next:" + stream, MillisBehindLatest: 0})
	default:
		http.Error(w, "unsupported Kinesis operation", http.StatusBadRequest)
	}
}

func newBackpressureClient(
	t *testing.T, store *backpressureStateStore, recordsStream string, batchSize int, autoCommit bool,
) (*Kinesumer, *backpressureKinesis, func()) {
	t.Helper()
	setBackpressureTestEnv(t, "AWS_ACCESS_KEY_ID", "test-access-key")
	setBackpressureTestEnv(t, "AWS_SECRET_ACCESS_KEY", "test-secret-key")
	setBackpressureTestEnv(t, "AWS_EC2_METADATA_DISABLED", "true")

	fixture := &backpressureKinesis{
		recordsStream: recordsStream,
		batchSize:     batchSize,
		batchServed:   make(chan struct{}),
	}
	server := httptest.NewServer(fixture)
	t.Cleanup(server.Close)

	var stateStore StateStore = store
	client, err := NewKinesumer(&Config{
		App:             "test-app",
		ClientID:        "test-client",
		KinesisRegion:   "us-east-1",
		KinesisEndpoint: server.URL,
		StateStore:      &stateStore,
		ScanLimit:       int64(batchSize + 1),
		ScanTimeout:     time.Second,
		ScanInterval:    time.Millisecond,
		Commit:          &CommitConfig{Auto: autoCommit, Interval: time.Hour, Timeout: time.Second},
	})
	if err != nil {
		t.Fatalf("test setup: initialize Kinesumer: %v", err)
	}

	var closeOnce sync.Once
	closeClient := func() { closeOnce.Do(client.Close) }
	return client, fixture, closeClient
}

func setBackpressureTestEnv(t *testing.T, name, value string) {
	t.Helper()
	oldValue, hadValue := os.LookupEnv(name)
	if err := os.Setenv(name, value); err != nil {
		t.Fatalf("test setup: set %s: %v", name, err)
	}
	t.Cleanup(func() {
		if hadValue {
			_ = os.Setenv(name, oldValue)
		} else {
			_ = os.Unsetenv(name)
		}
	})
}

func waitForBackpressureCondition(t *testing.T, timeout time.Duration, condition func() bool) bool {
	t.Helper()
	deadline := time.NewTimer(timeout)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()

	if condition() {
		return true
	}
	for {
		select {
		case <-deadline.C:
			return false
		case <-ticker.C:
			if condition() {
				return true
			}
		}
	}
}

func TestPollingBackpressureCorrectness(t *testing.T) {
	t.Run("assignment resumes the interrupted batch", testPollingAssignmentBackpressure)
	t.Run("close releases a blocked record send", testPollingCloseBackpressure)
}

func testPollingAssignmentBackpressure(t *testing.T) {
	store := newBackpressureStateStore("second")
	client, fixture, closeClient := newBackpressureClient(t, store, "first", recordsChanBuffer+1, true)
	consumed := make(chan struct {
		records <-chan *Record
		err     error
	}, 1)
	go func() {
		records, err := client.Consume([]string{"first", "second"})
		consumed <- struct {
			records <-chan *Record
			err     error
		}{records: records, err: err}
	}()
	defer func() {
		store.releaseLookup()
		select {
		case <-consumed:
		default:
		}
		closeClient()
	}()

	select {
	case <-store.lookupStarted:
	case result := <-consumed:
		t.Fatalf("test setup: Consume returned before the second stream lookup: %v", result.err)
	case <-time.After(5 * time.Second):
		t.Fatal("test setup: second stream lookup did not start")
	}
	select {
	case <-fixture.batchServed:
	case <-time.After(5 * time.Second):
		t.Fatal("test setup: Kinesis fixture did not return the first batch")
	}
	if !waitForBackpressureCondition(t, 5*time.Second, func() bool {
		return len(client.records) == cap(client.records)
	}) {
		t.Fatal("test setup: record channel did not fill")
	}

	client.Commit()
	if got := store.savedCheckpoints(); len(got) != 0 {
		t.Errorf("automatic checkpoints must not cover the blocked batch tail: %#v", got)
	}

	store.releaseLookup()
	select {
	case <-store.lookupReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("test setup: second stream lookup did not return")
	}

	var result struct {
		records <-chan *Record
		err     error
	}
	select {
	case result = <-consumed:
	case <-time.After(2 * time.Second):
		t.Errorf("multi-stream Consume did not return while a worker was blocked sending to a full records channel")
		// Release the baseline worker so the test process can finish.
		<-client.records
		select {
		case <-consumed:
		case <-time.After(2 * time.Second):
			t.Fatal("test cleanup: baseline Consume did not finish after releasing one record slot")
		}
		return
	}
	if result.err != nil || result.records == nil {
		t.Errorf("Consume returned no usable records channel: %v", result.err)
		return
	}

	client.Commit()
	if got := store.savedCheckpoints(); len(got) != 0 {
		t.Errorf("automatic checkpoints must not cover a batch tail that has not been sent: %#v", got)
	}

	readDeadline := time.NewTimer(5 * time.Second)
	defer readDeadline.Stop()
	for i := 1; i <= recordsChanBuffer+1; i++ {
		select {
		case record, ok := <-result.records:
			if !ok || record == nil || record.SequenceNumber == nil || *record.SequenceNumber != strconv.Itoa(i) {
				t.Errorf("interrupted batch order at position %d: record=%#v open=%t", i, record, ok)
				return
			}
			if record.Stream != "first" || record.ShardID != "shard-first" {
				t.Errorf("replayed record identity changed: %#v", record)
				return
			}
		case <-readDeadline.C:
			t.Errorf("interrupted batch tail was not replayed")
			return
		}
	}

	if !waitForBackpressureCondition(t, time.Second, func() bool {
		seq, ok := client.offsets["first"].Load("shard-first")
		return ok && seq == strconv.Itoa(recordsChanBuffer+1)
	}) {
		t.Errorf("automatic checkpoint offset was not marked after the full batch was sent")
		return
	}
	client.Commit()
	checkpoints := store.savedCheckpoints()
	if len(checkpoints) != 1 || checkpoints[0].SequenceNumber != strconv.Itoa(recordsChanBuffer+1) {
		t.Errorf("automatic checkpoint did not match the fully sent batch: %#v", checkpoints)
	}
}

func testPollingCloseBackpressure(t *testing.T) {
	store := newBackpressureStateStore("")
	client, fixture, closeClient := newBackpressureClient(t, store, "first", recordsChanBuffer+1, true)
	defer closeClient()

	records, err := client.Consume([]string{"first"})
	if err != nil {
		t.Fatalf("test setup: Consume: %v", err)
	}
	select {
	case <-fixture.batchServed:
	case <-time.After(5 * time.Second):
		t.Fatal("test setup: Kinesis fixture did not return the first batch")
	}
	if !waitForBackpressureCondition(t, 5*time.Second, func() bool {
		return len(client.records) == cap(client.records)
	}) {
		t.Fatal("test setup: record channel did not fill")
	}

	closed := make(chan struct{})
	go func() {
		closeClient()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(syncTimeout + 2*time.Second):
		t.Errorf("Close did not finish while a worker was blocked sending to a full records channel")
		// A single package-internal receive is cleanup for the stuck baseline send.
		<-client.records
		select {
		case <-closed:
		case <-time.After(syncTimeout + 2*time.Second):
			t.Fatal("test cleanup: Close did not finish after releasing one record slot")
		}
		return
	}

	if got := store.savedCheckpoints(); len(got) != 0 {
		t.Errorf("Close checkpointed a batch whose final record was never sent: %#v", got)
	}
	for i := 1; i <= recordsChanBuffer; i++ {
		select {
		case record, ok := <-records:
			if !ok || record == nil || record.SequenceNumber == nil || *record.SequenceNumber != strconv.Itoa(i) {
				t.Errorf("records channel contents changed before Close: record=%#v open=%t", record, ok)
				return
			}
		case <-time.After(time.Second):
			t.Errorf("buffered records were not available after Close")
			return
		}
	}
	if record, ok := <-records; ok || record != nil {
		t.Errorf("records channel remained open after Close")
	}
}

func TestPollingManualMarkingWithBlockedBatch(t *testing.T) {
	store := newBackpressureStateStore("")
	client, fixture, closeClient := newBackpressureClient(t, store, "first", recordsChanBuffer+1, false)
	defer closeClient()

	records, err := client.Consume([]string{"first"})
	if err != nil {
		t.Fatalf("test setup: Consume: %v", err)
	}
	select {
	case <-fixture.batchServed:
	case <-time.After(5 * time.Second):
		t.Fatal("test setup: Kinesis fixture did not return the first batch")
	}
	if !waitForBackpressureCondition(t, 5*time.Second, func() bool {
		return len(client.records) == cap(client.records)
	}) {
		t.Fatal("test setup: record channel did not fill")
	}

	var record *Record
	select {
	case record = <-records:
	case <-time.After(time.Second):
		t.Fatal("test setup: first delivered record was not available")
	}
	if record == nil || record.SequenceNumber == nil || *record.SequenceNumber != "1" {
		t.Fatalf("test setup: first delivered record changed: %#v", record)
	}
	client.MarkRecord(record)
	client.Commit()
	checkpoints := store.savedCheckpoints()
	if len(checkpoints) != 1 || checkpoints[0].SequenceNumber != "1" {
		t.Errorf("manual marking must checkpoint the delivered record: %#v", checkpoints)
	}
}
