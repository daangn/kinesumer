package kinesumer

import (
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/daangn/kinesumer/pkg/collection"
	"github.com/guregu/dynamo"
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
