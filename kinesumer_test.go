package kinesumer

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/guregu/dynamo"
	"github.com/stretchr/testify/assert"

	"github.com/daangn/kinesumer/pkg/collection"
)

type testEnv struct {
	kinesis      *kinesis.Kinesis
	stateStoreDB *dynamo.DB
	client1      *Kinesumer
	client2      *Kinesumer
	client3      *Kinesumer
}

func newTestEnv(t *testing.T) *testEnv {
	awsCfg := aws.NewConfig()
	awsCfg.WithRegion("ap-northeast-2")
	awsCfg.WithEndpoint("http://localhost:14566")
	sess, err := session.NewSession(awsCfg)
	if err != nil {
		t.Fatal("failed to init test env:", err.Error())
	}
	var (
		kinesisClient = kinesis.New(sess, awsCfg)
		stateStoreDB  = dynamo.New(sess)
	)

	config := &Config{
		App:              "test_client",
		KinesisRegion:    "ap-northeast-2",
		KinesisEndpoint:  "http://localhost:14566",
		DynamoDBRegion:   "ap-northeast-2",
		DynamoDBTable:    "kinesumer-state-store",
		DynamoDBEndpoint: "http://localhost:14566",
		ScanLimit:        10,
		ScanTimeout:      3 * time.Second,
	}

	client1, err := NewKinesumer(config)
	if err != nil {
		t.Fatal("failed to init test env:", err.Error())
	}
	client2, err := NewKinesumer(config)
	if err != nil {
		t.Fatal("failed to init test env:", err.Error())
	}
	client3, err := NewKinesumer(config)
	if err != nil {
		t.Fatal("failed to init test env:", err.Error())
	}

	// Drain the errors.
	go func() {
		for {
			select {
			case <-client1.Errors():
			case <-client2.Errors():
			case <-client3.Errors():
			}
		}
	}()

	return &testEnv{
		kinesis:      kinesisClient,
		stateStoreDB: stateStoreDB,
		client1:      client1,
		client2:      client2,
		client3:      client3,
	}
}

func (e *testEnv) cleanUp(t *testing.T) {
	defer e.client1.Close()
	defer e.client2.Close()
	defer e.client3.Close()

	type PkSk struct {
		PK string `dynamo:"pk"`
		SK string `dynamo:"sk"`
	}
	var (
		pksks []*PkSk
		keys  []dynamo.Keyed
	)
	table := e.stateStoreDB.Table("kinesumer-state-store")
	if err := table.Scan().All(&pksks); err != nil {
		t.Fatal("failed to scan the state table:", err.Error())
	}
	for _, pksk := range pksks {
		keys = append(keys, &dynamo.Keys{pksk.PK, pksk.SK})
	}
	if _, err := table.
		Batch("pk", "sk").
		Write().
		Delete(keys...).
		Run(); err != nil {
		t.Fatal("failed to delete all test data:", err.Error())
	}
}

func (e *testEnv) produceEvents(t *testing.T) {
	_, err := e.kinesis.PutRecords(
		&kinesis.PutRecordsInput{
			Records: []*kinesis.PutRecordsRequestEntry{
				{
					Data:         []byte("raw data one"),
					PartitionKey: aws.String("pkey one"),
				},
				{
					Data:         []byte("raw data two"),
					PartitionKey: aws.String("pkey two"),
				},
			},
			StreamName: aws.String("events"),
		},
	)
	if err != nil {
		t.Fatal("failed to produce test events:", err.Error())
	}
}

func TestKinesumer_Consume(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanUp(t)

	timeout := time.After(15 * time.Second)

	streams := []string{"events"}
	records1, err := env.client1.Consume(streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}
	records2, err := env.client2.Consume(streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}
	records3, err := env.client3.Consume(streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}

	env.produceEvents(t)

	var (
		records = make(chan *Record)
		stop    = make(chan struct{})
	)

	go func() {
		for {
			select {
			case r := <-records1:
				records <- r
			case r := <-records2:
				records <- r
			case r := <-records3:
				records <- r
			case <-stop:
				close(records)
				return
			}
		}
	}()

	var recv int
	for {
		select {
		case <-records:
			if recv++; recv == 2 {
				stop <- struct{}{}
				return
			}
		case <-timeout:
			t.Errorf("%s timed out", t.Name())
			return
		}
	}
}

func TestShardsRebalancing(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanUp(t)

	var err error
	streams := []string{"events"}
	_, err = env.client1.Consume(streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}
	_, err = env.client2.Consume(streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}
	_, err = env.client3.Consume(streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}

	var (
		clientIDs = []string{
			env.client1.id,
			env.client2.id,
			env.client3.id,
		}

		clients = map[string]*Kinesumer{
			env.client1.id: env.client1,
			env.client2.id: env.client2,
			env.client3.id: env.client3,
		}
	)
	sort.Strings(clientIDs)

	time.Sleep(2*syncInterval + 10*time.Millisecond)

	expectedShardRanges1 := [][]string{
		{
			"shardId-000000000000",
			"shardId-000000000001",
		},
		{
			"shardId-000000000002",
		},
		{
			"shardId-000000000003",
			"shardId-000000000004",
		},
	}

	for i, id := range clientIDs {
		shardIDs := clients[id].shards["events"].ids()
		expected := expectedShardRanges1[i]
		if !collection.EqualsSS(shardIDs, expected) {
			t.Errorf(
				"expected %v, got %v", expected, shardIDs,
			)
		}
	}

	// Update kinesis shard count.
	_, err = env.kinesis.UpdateShardCount(
		&kinesis.UpdateShardCountInput{
			ScalingType: aws.String(
				kinesis.ScalingTypeUniformScaling,
			),
			StreamName:       aws.String("events"),
			TargetShardCount: aws.Int64(8),
		},
	)
	if err != nil {
		t.Fatal("failed to update shard count:", err.Error())
	}

	time.Sleep(2*syncInterval + 10*time.Millisecond)

	// After auto shard rebalancing.
	expectedShardRanges2 := [][]string{
		// {
		// 	"shardId-000000000000",
		// 	"shardId-000000000001",
		// 	"shardId-000000000002",
		// 	"shardId-000000000003",
		// },
		// {
		// 	"shardId-000000000004",
		// 	"shardId-000000000005",
		// 	"shardId-000000000006",
		// 	"shardId-000000000007",
		// 	"shardId-000000000008",
		// },
		// {
		// 	"shardId-000000000009",
		// 	"shardId-000000000010",
		// 	"shardId-000000000011",
		// 	"shardId-000000000012",
		// },
		{
			"shardId-000000000005",
			"shardId-000000000006",
			"shardId-000000000007",
		},
		{
			"shardId-000000000008",
			"shardId-000000000009",
		},
		{
			"shardId-000000000010",
			"shardId-000000000011",
			"shardId-000000000012",
		},
	}

	for i, id := range clientIDs {
		shardIDs := clients[id].shards["events"].ids()
		expected := expectedShardRanges2[i]
		if !collection.EqualsSS(shardIDs, expected) {
			t.Errorf(
				"expected %v, got %v", expected, shardIDs,
			)
		}
	}
}

func TestKinesumer_MarkRecordWorksFine(t *testing.T) {
	kinesumer := &Kinesumer{
		markRequestCh: make(chan *markRequest, 1),
		checkPoints: map[string]*sync.Map{
			"events": new(sync.Map),
		},
	}
	kinesumer.MarkRecord(&Record{
		Stream:  "events",
		ShardID: "shard-000000000000",
		Record: &kinesis.Record{
			SequenceNumber: aws.String("12345"),
		},
	})

	expected := &markRequest{
		stream:         "events",
		shardID:        "shard-000000000000",
		sequenceNumber: "12345",
	}
	select {
	case req := <-kinesumer.markRequestCh:
		assert.Equal(t, expected, req, "they should be equal")
	case <-time.After(1 * time.Second):
		assert.FailNow(t, "timeout")
	}
}

func TestKinesumer_MarkRecordFails(t *testing.T) {
	testCases := []struct {
		name      string
		kinesumer *Kinesumer
		input     *Record
		wantErr   error
	}{
		{
			name: "when record record is nil",
			kinesumer: &Kinesumer{
				errors: make(chan error, 1),
			},
			input:   nil,
			wantErr: errMarkNilRecord,
		},
		{
			name: "when record sequence number is empty",
			kinesumer: &Kinesumer{
				errors: make(chan error, 1),
			},
			input: &Record{
				Stream:  "foobar",
				ShardID: "shardId-000",
				Record: &kinesis.Record{
					SequenceNumber: aws.String(""),
				},
			},
			wantErr: ErrEmptySequenceNumber,
		},
		{
			name: "when unknown stream is given",
			kinesumer: &Kinesumer{
				errors: make(chan error, 1),
				checkPoints: map[string]*sync.Map{
					"foobar": {},
				},
			},
			input: &Record{
				Stream:  "foo",
				ShardID: "shardId-000",
				Record: &kinesis.Record{
					SequenceNumber: aws.String("12345"),
				},
			},
			wantErr: ErrInvalidStream,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kinesumer := tc.kinesumer
			kinesumer.MarkRecord(tc.input)

			result := <-kinesumer.errors
			assert.ErrorIs(t, result, tc.wantErr, "there should be an expected error")
		})
	}
}

func TestKinesumer_MarkRecordContextWorksFine(t *testing.T) {
	kinesumer := &Kinesumer{
		markRequestCh: make(chan *markRequest, 1),
		checkPoints: map[string]*sync.Map{
			"events": new(sync.Map),
		},
	}
	kinesumer.MarkRecordContext(
		context.Background(),
		&Record{
			Stream:  "events",
			ShardID: "shard-000000000000",
			Record: &kinesis.Record{
				SequenceNumber: aws.String("12345"),
			},
		},
	)

	expected := &markRequest{
		stream:         "events",
		shardID:        "shard-000000000000",
		sequenceNumber: "12345",
	}
	select {
	case req := <-kinesumer.markRequestCh:
		assert.Equal(t, expected, req, "they should be equal")
	case <-time.After(1 * time.Second):
		assert.FailNow(t, "timeout")
	}
}

func TestKinesumer_MarkRecordContextFails(t *testing.T) {
	testCases := []struct {
		name      string
		kinesumer *Kinesumer
		input     struct {
			ctx    context.Context
			record *Record
		}
		wantErr error
	}{
		{
			name: "when record record is nil",
			kinesumer: &Kinesumer{
				errors: make(chan error, 1),
			},
			input: struct {
				ctx    context.Context
				record *Record
			}{
				ctx:    context.Background(),
				record: nil,
			},
			wantErr: errMarkNilRecord,
		},
		{
			name: "when record sequence number is empty",
			kinesumer: &Kinesumer{
				errors: make(chan error, 1),
			},
			input: struct {
				ctx    context.Context
				record *Record
			}{
				ctx: context.Background(),
				record: &Record{
					Stream:  "foobar",
					ShardID: "shardId-000",
					Record: &kinesis.Record{
						SequenceNumber: aws.String(""),
					},
				},
			},
			wantErr: ErrEmptySequenceNumber,
		},
		{
			name: "when unknown stream is given",
			kinesumer: &Kinesumer{
				errors: make(chan error, 1),
				checkPoints: map[string]*sync.Map{
					"foobar": {},
				},
			},
			input: struct {
				ctx    context.Context
				record *Record
			}{
				ctx: context.Background(),
				record: &Record{
					Stream:  "foo",
					ShardID: "shardId-000",
					Record: &kinesis.Record{
						SequenceNumber: aws.String("12345"),
					},
				},
			},
			wantErr: ErrInvalidStream,
		},
		{
			name: "when context is canceled",
			kinesumer: &Kinesumer{
				errors: make(chan error, 1),
				checkPoints: map[string]*sync.Map{
					"foobar": {},
				},
			},
			input: struct {
				ctx    context.Context
				record *Record
			}{
				ctx: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx
				}(),
				record: &Record{
					Stream:  "foobar",
					ShardID: "shardId-000",
					Record: &kinesis.Record{
						SequenceNumber: aws.String("12345"),
					},
				},
			},
			wantErr: errMarkTimeout,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kinesumer := tc.kinesumer
			kinesumer.MarkRecordContext(tc.input.ctx, tc.input.record)

			result := <-kinesumer.errors
			assert.ErrorIs(t, result, tc.wantErr, "there should be an expected error")
		})
	}
}

func TestKinesumer_markAndCommit(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStateStore := NewMockStateStore(ctrl)
	mockStateStore.EXPECT().
		UpdateCheckPoints(gomock.Any(), []*ShardCheckPoint{
			{
				Stream:         "events",
				ShardID:        "shard-000000000000",
				SequenceNumber: "67890",
			},
		}).
		Times(1).
		Return(nil)

	kinesumer := &Kinesumer{
		markRequestCh: make(chan *markRequest),
		commit:        make(chan struct{}),
		stateStore:    mockStateStore,
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		kinesumer.markAndCommit()

		close(done)
	}()

	kinesumer.markRequestCh <- &markRequest{
		stream:         "events",
		shardID:        "shard-000000000000",
		sequenceNumber: "12345",
	}
	kinesumer.markRequestCh <- &markRequest{
		stream:         "events",
		shardID:        "shard-000000000000",
		sequenceNumber: "67890",
	}
	kinesumer.commit <- struct{}{}

	close(kinesumer.commit)

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		assert.FailNow(t, "timed out waiting for markAndCommit to finish")
	}
}

func TestKinesumer_Commit(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanUp(t)

	streams := []string{"events"}
	_, err := env.client1.Consume(streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}
	_, err = env.client2.Consume(streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}
	_, err = env.client3.Consume(streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}

	clients := map[string]*Kinesumer{
		env.client1.id: env.client1,
		env.client2.id: env.client2,
		env.client3.id: env.client3,
	}

	expectedSeqNum := "12345"
	for _, client := range clients {
		shardIDs := client.shards["events"].ids()
		for _, shardID := range shardIDs {
			env.client1.MarkRecord(&Record{
				Stream:  "events",
				ShardID: shardID,
				Record: &kinesis.Record{
					SequenceNumber: &expectedSeqNum,
				},
			})
		}
	}

	for _, client := range clients {
		client.Commit()
	}

	for _, client := range clients {
		shardIDs := client.shards["events"].ids()
		checkpoints, _ := client.stateStore.ListCheckPoints(context.Background(), "events", shardIDs)
		for _, checkpoint := range checkpoints {
			assert.EqualValues(t, expectedSeqNum, checkpoint, "sequence number should be equal")
		}
	}
}

func TestKinesumer_commitCheckPointPerStreamWorksFine(t *testing.T) {
	ctrl := gomock.NewController(t)

	input := []*ShardCheckPoint{
		{
			Stream:         "foobar",
			ShardID:        "shardId-0",
			SequenceNumber: "0",
		},
	}

	mockStateStore := NewMockStateStore(ctrl)
	mockStateStore.EXPECT().
		UpdateCheckPoints(gomock.Any(), input).
		Times(1).
		Return(nil)

	kinesumer := &Kinesumer{
		commitTimeout: 2 * time.Second,
		stateStore:    mockStateStore,
	}

	kinesumer.commitCheckPointsPerStream("foobar", input)

	select {
	case err := <-kinesumer.Errors():
		assert.NoError(t, err, "there should be no error")
	default:
	}
}

func TestKinesumer_commitCheckPointPerStreamFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	testCases := []struct {
		name         string
		newKinesumer func() *Kinesumer
		input        struct {
			stream      string
			checkpoints []*ShardCheckPoint
		}
		wantErrMsg string
	}{
		{
			name: "when state store fails to update checkpoints",
			newKinesumer: func() *Kinesumer {
				mockStateStore := NewMockStateStore(ctrl)
				mockStateStore.EXPECT().
					UpdateCheckPoints(gomock.Any(), gomock.Any()).
					Times(1).
					Return(errors.New("mock error"))
				return &Kinesumer{
					errors:     make(chan error, 1),
					stateStore: mockStateStore,
				}
			},
			input: struct {
				stream      string
				checkpoints []*ShardCheckPoint
			}{
				stream: "foobar",
				checkpoints: []*ShardCheckPoint{
					{
						Stream:         "foobar",
						ShardID:        "shardId-000",
						SequenceNumber: "0",
					},
				},
			},
			wantErrMsg: "failed to commit on stream: foobar: mock error",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kinesumer := tc.newKinesumer()
			kinesumer.commitCheckPointsPerStream(tc.input.stream, tc.input.checkpoints)
			result := <-kinesumer.errors
			assert.EqualError(t, result, tc.wantErrMsg, "there should be an expected error")
		})
	}
}

func TestOffsets_merge(t *testing.T) {
	testCases := []struct {
		name    string
		offsets *offsets
		input   struct {
			stream         string
			shardID        string
			sequenceNumber string
		}
		want *offsets
	}{
		{
			name:    "when merge new stream",
			offsets: &offsets{},
			input: struct {
				stream         string
				shardID        string
				sequenceNumber string
			}{
				stream:         "foobar",
				shardID:        "shardId-0",
				sequenceNumber: "0",
			},
			want: &offsets{
				"foobar": {
					"shardId-0": "0",
				},
			},
		},
		{
			name: "when merge new shard",
			offsets: &offsets{
				"foobar": map[string]string{
					"shardId-0": "0",
				},
			},
			input: struct {
				stream         string
				shardID        string
				sequenceNumber string
			}{
				stream:         "foobar",
				shardID:        "shardId-1",
				sequenceNumber: "1",
			},
			want: &offsets{
				"foobar": {
					"shardId-0": "0",
					"shardId-1": "1",
				},
			},
		},
		{
			name: "when merge new sequence number",
			offsets: &offsets{
				"foobar": map[string]string{
					"shardId-0": "0",
				},
			},
			input: struct {
				stream         string
				shardID        string
				sequenceNumber string
			}{
				stream:         "foobar",
				shardID:        "shardId-0",
				sequenceNumber: "1",
			},
			want: &offsets{
				"foobar": {
					"shardId-0": "1",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			o := tc.offsets
			o.merge(tc.input.stream, tc.input.shardID, tc.input.sequenceNumber)

			assert.Equal(t, tc.want, o, "they should be equal")
		})
	}
}
