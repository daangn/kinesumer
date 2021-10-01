package kinesumer

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/daangn/kinesumer/pkg/xrand"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	jitter = 50 * time.Millisecond

	syncInterval      = 5*time.Second + jitter
	syncTimeout       = 5*time.Second - jitter
	checkPointTimeout = 2 * time.Second

	defaultScanLimit   int64 = 2000
	defaultScanTimeout       = 2 * time.Second

	defaultTimeBuffer = 10 * time.Millisecond
	recordsChanBuffer = 20
)

// Config defines configs for the Kinesumer client.
type Config struct {
	App string // Application name.

	// Kinesis configs.
	KinesisRegion   string
	KinesisEndpoint string // Only for local server.
	// If you want to consume messages from Kinesis in a different account,
	// you need to set up the IAM role to access to target account, and pass the role arn here.
	// Reference: https://docs.aws.amazon.com/kinesisanalytics/latest/java/examples-cross.html.
	RoleARN string

	// State store configs.
	DynamoDBRegion   string
	DynamoDBTable    string
	DynamoDBEndpoint string // Only for local server.

	ScanLimit   int64
	ScanTimeout time.Duration
}

// Record represents kinesis.Record with stream name.
type Record struct {
	Stream string
	*kinesis.Record
}

// Shard holds shard id and a flag of "CLOSED" state.
type Shard struct {
	ID     string
	Closed bool
}

// Shards is a collection of Shard.
type Shards []*Shard

func (s Shards) ids() []string {
	var ids []string
	for _, shard := range s {
		ids = append(ids, shard.ID)
	}
	return ids
}

// Kinesumer implements auto re-balancing consumer group for Kinesis.
// TODO(mingrammer): export prometheus metrics.
type Kinesumer struct {
	// Unique identity of a consumer group client.
	id     string
	client *kinesis.Kinesis
	// A flag that identifies if the client is a leader.
	leader bool

	streams []string

	// A stream where consumed records will have flowed.
	records chan *Record

	errors chan error

	// A distributed key-value store for managing states.
	stateStore *stateStore

	// Shard information per stream.
	// List of all shards as cache. For only leader node.
	shardCaches map[string][]string
	// A list of shards a node is currently in charge of.
	shards map[string]Shards
	// To cache the last sequence numbers for each shard.
	checkPoints map[string]*sync.Map
	// To manage the next shard iterators for each shard.
	nextIters map[string]*sync.Map

	// Maximum count of records to scan.
	scanLimit int64
	// Records scanning maximum timeout.
	scanTimeout time.Duration

	// To wait the running consumer loops when stopping.
	wait  sync.WaitGroup
	stop  chan struct{}
	close chan struct{}
}

// NewKinesumer initializes and returns a new Kinesumer client.
func NewKinesumer(cfg *Config) (*Kinesumer, error) {
	if cfg.App == "" {
		return nil, errors.WithStack(
			errors.New("you must pass the app name"),
		)
	}

	// Make unique client id.
	id, err := os.Hostname()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	id += xrand.StringN(6) // Add suffix.

	// Initialize the state store.
	stateStore, err := newStateStore(cfg)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Initialize the AWS session to build Kinesis client.
	awsCfg := aws.NewConfig()
	awsCfg.WithRegion(cfg.KinesisRegion)
	if cfg.KinesisEndpoint != "" {
		awsCfg.WithEndpoint(cfg.KinesisEndpoint)
	}
	sess, err := session.NewSession(awsCfg)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var cfgs []*aws.Config
	if cfg.RoleARN != "" {
		cfgs = append(cfgs,
			aws.NewConfig().WithCredentials(
				stscreds.NewCredentials(
					sess, cfg.RoleARN,
				),
			),
		)
	}

	buffer := recordsChanBuffer
	kinesumer := &Kinesumer{
		id:          id,
		client:      kinesis.New(sess, cfgs...),
		stateStore:  stateStore,
		shardCaches: make(map[string][]string),
		shards:      make(map[string]Shards),
		checkPoints: make(map[string]*sync.Map),
		nextIters:   make(map[string]*sync.Map),
		scanLimit:   defaultScanLimit,
		scanTimeout: defaultScanTimeout,
		records:     make(chan *Record, buffer),
		errors:      make(chan error, 1),
		wait:        sync.WaitGroup{},
		stop:        make(chan struct{}),
		close:       make(chan struct{}),
	}

	if cfg.ScanLimit > 0 {
		kinesumer.scanLimit = cfg.ScanLimit
	}
	if cfg.ScanTimeout > 0 {
		kinesumer.scanTimeout = cfg.ScanTimeout
	}

	if err := kinesumer.init(); err != nil {
		return nil, errors.WithStack(err)
	}
	return kinesumer, nil
}

func (k *Kinesumer) init() error {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()

	// Register itself to state store.
	if err := k.stateStore.RegisterClient(ctx, k.id); err != nil {
		return errors.WithStack(err)
	}

	// The leader will be determined during the initial sync job.
	if err := k.syncShardInfo(ctx); err != nil {
		return errors.WithStack(err)
	}

	go k.loopSyncClient()
	return nil
}

func (k *Kinesumer) listShards(stream string) (Shards, error) {
	output, err := k.client.ListShards(&kinesis.ListShardsInput{
		StreamName: aws.String(stream),
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var shards []*Shard
	for _, shard := range output.Shards {
		shards = append(shards, &Shard{
			ID:     *shard.ShardId,
			Closed: shard.SequenceNumberRange.EndingSequenceNumber != nil,
		})
	}
	return shards, nil
}

// Consume consumes messages from Kinesis.
func (k *Kinesumer) Consume(
	streams []string) (<-chan *Record, error) {
	k.streams = streams

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()

	if err := k.syncShardInfo(ctx); err != nil {
		return nil, errors.WithStack(err)
	}
	return k.records, nil
}

func (k *Kinesumer) start() {
	k.stop = make(chan struct{})

	for stream, shardsPerStream := range k.shards {
		for _, shard := range shardsPerStream {
			k.wait.Add(1)
			go k.consume(stream, shard)
		}
	}
}

func (k *Kinesumer) pause() {
	close(k.stop)

	k.wait.Wait()
}

func (k *Kinesumer) consume(stream string, shard *Shard) {
	defer k.wait.Done()

	for {
		select {
		case <-k.stop:
			return
		case <-k.close:
			return
		default:
			time.Sleep(defaultTimeBuffer) // Time buffer to prevent high stress.
			if closed := k.consumeOnce(stream, shard); closed {
				return // Close consume loop if shard is CLOSED and has no data.
			}
		}
	}
}

// It returns a flag whether if shard is CLOSED state and has no remaining data.
func (k *Kinesumer) consumeOnce(stream string, shard *Shard) bool {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, k.scanTimeout)
	defer cancel()

	shardIter, err := k.getNextShardIterator(ctx, stream, shard.ID)
	if err != nil {
		k.errors <- errors.WithStack(err)
		var riue *kinesis.ResourceInUseException
		if errors.As(err, &riue) {
			return true
		}
		return false
	}

	output, err := k.client.GetRecordsWithContext(ctx, &kinesis.GetRecordsInput{
		Limit:         aws.Int64(k.scanLimit),
		ShardIterator: shardIter,
	})
	if err != nil {
		k.errors <- errors.WithStack(err)
		var riue *kinesis.ResourceInUseException
		if errors.As(err, &riue) {
			return true
		}
		var eie *kinesis.ExpiredIteratorException
		if errors.As(err, &eie) {
			k.nextIters[stream].Delete(shard.ID) // Delete expired next iterator cache.
		}
		return false
	}
	defer k.nextIters[stream].Store(shard.ID, output.NextShardIterator) // Update iter.

	n := len(output.Records)
	// We no longer care about shards that have no records left and are in the "CLOSED" state.
	if n == 0 {
		return shard.Closed
	}

	var lastSequence string
	for i, record := range output.Records {
		k.records <- &Record{
			Stream: stream,
			Record: record,
		}
		if i == n-1 {
			lastSequence = *record.SequenceNumber
		}
	}

	// Check point the sequence number.
	ctx = context.Background()
	ctx, cancel = context.WithTimeout(ctx, checkPointTimeout)
	defer cancel()

	if err := k.stateStore.UpdateCheckPoint(ctx, stream, shard.ID, lastSequence); err != nil {
		log.Err(err).
			Str("stream", stream).
			Str("shard id", shard.ID).
			Str("missed sequence number", lastSequence).
			Msg("kinesumer: failed to UpdateCheckPoint")
	}
	k.checkPoints[stream].Store(shard.ID, lastSequence)
	return false
}

func (k *Kinesumer) getNextShardIterator(
	ctx context.Context, stream, shardID string) (*string, error) {
	if iter, ok := k.nextIters[stream].Load(shardID); ok {
		return iter.(*string), nil
	}

	input := &kinesis.GetShardIteratorInput{
		StreamName: aws.String(stream),
		ShardId:    aws.String(shardID),
	}
	if seq, ok := k.checkPoints[stream].Load(shardID); ok {
		input.SetShardIteratorType(kinesis.ShardIteratorTypeAfterSequenceNumber)
		input.SetStartingSequenceNumber(seq.(string))
	} else {
		input.SetShardIteratorType(kinesis.ShardIteratorTypeLatest)
	}

	output, err := k.client.GetShardIteratorWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	k.nextIters[stream].Store(shardID, output.ShardIterator)
	return output.ShardIterator, nil
}

// Refresh refreshes the consuming streams.
func (k *Kinesumer) Refresh(streams []string) {
	k.streams = streams
}

// Errors returns error channel.
func (k *Kinesumer) Errors() <-chan error {
	return k.errors
}

// Close stops the consuming and sync jobs.
func (k *Kinesumer) Close() {
	log.Info().
		Msg("kinesumer: closing the kinesumer")
	close(k.close)

	k.wait.Wait()

	// Client should drain the remaining records.
	close(k.records)

	// Drain the remaining errors.
	close(k.errors)
	for range k.errors {
		// Do nothing with errors.
	}

	// Wait last sync jobs.
	time.Sleep(syncTimeout)
	log.Info().
		Msg("kinesumer: shutdown successfully")
}
