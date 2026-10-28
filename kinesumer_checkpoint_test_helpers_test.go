package kinesumer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type pollingCheckpointStore struct {
	mu           sync.Mutex
	calls        int
	rows         int
	persisted    map[string]string
	blockSeq     string
	writeStarted chan struct{}
	releaseWrite chan struct{}
}

func newPollingCheckpointStore() *pollingCheckpointStore {
	return &pollingCheckpointStore{persisted: make(map[string]string)}
}

func (s *pollingCheckpointStore) GetShards(context.Context, string) (Shards, error) {
	return nil, nil
}
func (s *pollingCheckpointStore) UpdateShards(context.Context, string, Shards) error {
	return nil
}
func (s *pollingCheckpointStore) ListAllAliveClientIDs(context.Context) ([]string, error) {
	return nil, nil
}
func (s *pollingCheckpointStore) RegisterClient(context.Context, string) error      { return nil }
func (s *pollingCheckpointStore) DeregisterClient(context.Context, string) error    { return nil }
func (s *pollingCheckpointStore) PingClientAliveness(context.Context, string) error { return nil }
func (s *pollingCheckpointStore) PruneClients(context.Context) error                { return nil }
func (s *pollingCheckpointStore) ListCheckPoints(
	_ context.Context, _ string, shardIDs []string,
) (map[string]string, error) {
	if len(shardIDs) == 0 {
		return nil, ErrEmptyShardIDs
	}
	return map[string]string{}, nil
}
func (s *pollingCheckpointStore) UpdateCheckPoints(
	_ context.Context, checkpoints []*ShardCheckPoint,
) error {
	block := s.blockSeq != ""
	for _, checkpoint := range checkpoints {
		if checkpoint.SequenceNumber != s.blockSeq {
			block = false
			break
		}
	}
	if block {
		s.writeStarted <- struct{}{}
		<-s.releaseWrite
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.rows += len(checkpoints)
	for _, checkpoint := range checkpoints {
		s.persisted[checkpoint.Stream+"/"+checkpoint.ShardID] = checkpoint.SequenceNumber
	}
	return nil
}

func (s *pollingCheckpointStore) stats() (calls, rows int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.rows
}

func (s *pollingCheckpointStore) checkpoint(stream, shard string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persisted[stream+"/"+shard]
}

type pollingKinesisFixture struct {
	server        *httptest.Server
	mu            sync.Mutex
	calls         map[string]int
	afterMark     chan string
	emitRecord    bool
	blockedReads  map[string]<-chan struct{}
	resourceInUse map[string]bool
}

func newPollingKinesisFixture(tb testing.TB, emitRecord bool) *pollingKinesisFixture {
	tb.Helper()
	fixture := &pollingKinesisFixture{
		calls:      make(map[string]int),
		afterMark:  make(chan string, 32),
		emitRecord: emitRecord,
	}
	fixture.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		switch r.Header.Get("X-Amz-Target") {
		case "Kinesis_20131202.GetShardIterator":
			var input struct {
				StreamName string
				ShardID    string `json:"ShardId"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			_, _ = fmt.Fprintf(w, `{"ShardIterator":%q}`, input.StreamName+"/"+input.ShardID)
		case "Kinesis_20131202.GetRecords":
			var input struct {
				ShardIterator string
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			fixture.mu.Lock()
			call := fixture.calls[input.ShardIterator]
			fixture.calls[input.ShardIterator] = call + 1
			fixture.mu.Unlock()
			if fixture.emitRecord && call == 0 {
				_, _ = fmt.Fprintf(w,
					`{"Records":[{"SequenceNumber":"200","Data":"eA==","PartitionKey":"p"}],"NextShardIterator":%q}`,
					input.ShardIterator,
				)
				return
			}
			if fixture.emitRecord && call == 1 {
				fixture.afterMark <- input.ShardIterator
				if release, ok := fixture.blockedReads[input.ShardIterator]; ok {
					<-release
					if fixture.resourceInUse[input.ShardIterator] {
						w.Header().Set("x-amzn-errortype", "ResourceInUseException")
						w.WriteHeader(http.StatusBadRequest)
						_, _ = fmt.Fprint(w,
							`{"__type":"ResourceInUseException","message":"ownership changed"}`)
						return
					}
				}
			}
			_, _ = fmt.Fprintf(w, `{"Records":[],"NextShardIterator":%q}`, input.ShardIterator)
		default:
			http.Error(w, "unexpected Kinesis action", http.StatusBadRequest)
		}
	}))
	tb.Cleanup(fixture.server.Close)
	return fixture
}

func (f *pollingKinesisFixture) waitForRecords(tb testing.TB, want int) {
	tb.Helper()
	deadline := time.After(3 * time.Second)
	for {
		f.mu.Lock()
		total := 0
		for _, calls := range f.calls {
			total += calls
		}
		f.mu.Unlock()
		if total >= want {
			return
		}
		select {
		case <-deadline:
			tb.Fatalf("received %d GetRecords calls, want at least %d", total, want)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func newPollingKinesumer(
	tb testing.TB,
	store StateStore,
	streamCount int,
	fixture *pollingKinesisFixture,
) *Kinesumer {
	tb.Helper()
	sess, err := session.NewSession(aws.NewConfig().
		WithRegion("us-east-1").
		WithEndpoint(fixture.server.URL).
		WithCredentials(credentials.NewStaticCredentials("key", "secret", "")))
	if err != nil {
		tb.Fatal(err)
	}

	streams := make([]string, 0, streamCount)
	shards := make(map[string]Shards, streamCount)
	offsets := make(map[string]*sync.Map, streamCount)
	checkPoints := make(map[string]*sync.Map, streamCount)
	nextIters := make(map[string]*sync.Map, streamCount)
	for i := 0; i < streamCount; i++ {
		stream := fmt.Sprintf("stream-%d", i)
		shardID := fmt.Sprintf("shard-%d", i)
		streams = append(streams, stream)
		shards[stream] = Shards{&Shard{ID: shardID}}
		offsets[stream] = &sync.Map{}
		checkPoints[stream] = &sync.Map{}
		nextIters[stream] = &sync.Map{}
		nextIters[stream].Store(shardID, aws.String(stream+"/"+shardID))
	}

	return &Kinesumer{
		client:         kinesis.New(sess),
		streams:        streams,
		shards:         shards,
		checkPoints:    checkPoints,
		offsets:        offsets,
		nextIters:      nextIters,
		records:        make(chan *Record, recordsChanBuffer),
		errors:         make(chan error, 32),
		stateStore:     store,
		scanLimit:      10,
		scanTimeout:    time.Second,
		scanInterval:   time.Millisecond,
		autoCommit:     false,
		commitTimeout:  time.Second,
		commitInterval: time.Hour,
		stop:           make(chan struct{}),
		mu:             &sync.Mutex{},
		close:          make(chan struct{}),
	}
}

func markPollingOffset(k *Kinesumer, stream, shard, sequence string) {
	k.MarkRecord(&Record{
		Stream:  stream,
		ShardID: shard,
		Record:  &kinesis.Record{SequenceNumber: aws.String(sequence)},
	})
}
