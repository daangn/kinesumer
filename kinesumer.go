package kinesumer

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/daangn/kinesumer/pkg/xrand"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	jitter = 50 * time.Millisecond

	syncInterval = 5*time.Second + jitter
	syncTimeout  = 5*time.Second - jitter

	defaultCommitTimeout  = 2 * time.Second
	defaultCommitInterval = 5 * time.Second

	defaultScanLimit int64 = 2000

	defaultScanTimeout  = 2 * time.Second
	defaultScanInterval = 10 * time.Millisecond

	recordsChanBuffer = 20
)

// Error codes.
var (
	ErrEmptySequenceNumber    = errors.New("kinesumer: sequence number can't be empty")
	ErrInvalidStream          = errors.New("kinesumer: invalid stream")
	errEmptyCommitCheckpoints = errors.New("kinesumer: commit checkpoints can't be empty")
	errMarkNilRecord          = errors.New("kinesumer: nil record can't be marked")
)

// WithSessionTraceFunc is a function that returns a new session with tracing.
type WithSessionTraceFunc func(*session.Session) (*session.Session, error)

// Config defines configs for the Kinesumer client.
type Config struct {
	App      string // Application name.
	Region   string // Region name. (optional)
	ClientID string // Consumer group client id. (optional)

	// Kinesis configs.
	KinesisRegion   string
	KinesisEndpoint string // Only for local server.
	// If you want to consume messages from Kinesis in a different account,
	// you need to set up the IAM role to access to target account, and pass the role arn here.
	// Reference: https://docs.aws.amazon.com/kinesisanalytics/latest/java/examples-cross.html.
	RoleARN                       string
	KinesisClientSessionTraceFunc WithSessionTraceFunc

	// State store configs.
	StateStore                     *StateStore
	DynamoDBRegion                 string
	DynamoDBTable                  string
	DynamoDBEndpoint               string // Only for local server.
	DynamoDBClientSessionTraceFunc WithSessionTraceFunc

	// These configs are not used in EFO mode.
	ScanLimit    int64
	ScanTimeout  time.Duration
	ScanInterval time.Duration

	EFOMode bool // On/off the Enhanced Fan-Out feature.

	// This config is used for how to manage sequence number.
	Commit *CommitConfig
}

// CommitConfig holds options for how to offset handled.
type CommitConfig struct {
	// Whether to auto-commit updated sequence number. (default is true)
	Auto bool

	// How frequently to commit updated sequence numbers. (default is 5s)
	Interval time.Duration

	// A Timeout config for commit per stream. (default is 2s)
	Timeout time.Duration
}

// NewDefaultCommitConfig returns a new default offset management configuration.
func NewDefaultCommitConfig() *CommitConfig {
	return &CommitConfig{
		Auto:     true,
		Interval: defaultCommitInterval,
		Timeout:  defaultCommitTimeout,
	}
}

// Record represents kinesis.Record with stream name.
type Record struct {
	Stream  string
	ShardID string
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

type efoMeta struct {
	streamARN    string
	consumerARN  string
	consumerName string
}

// Kinesumer implements auto re-balancing consumer group for Kinesis.
// TODO(mingrammer): export prometheus metrics.
type Kinesumer struct {
	// Unique identity of a consumer group client.
	id     string
	client *kinesis.Kinesis

	app string
	rgn string

	// A flag that identifies if the client is a leader.
	leader bool

	streams []string

	efoMode bool
	efoMeta map[string]*efoMeta

	// A stream where consumed records will have flowed.
	records chan *Record

	errors chan error

	// A distributed key-value store for managing states.
	stateStore StateStore

	// Shard information per stream.
	// List of all shards as cache. For only leader node.
	shardCaches map[string][]string
	// A list of shards a node is currently in charge of.
	shards map[string]Shards
	// To cache the last sequence numbers for each shard.
	checkPoints map[string]*sync.Map
	// offsets holds uncommitted sequence numbers.
	offsets map[string]*sync.Map
	// To manage the next shard iterators for each shard.
	nextIters map[string]*sync.Map

	// Maximum count of records to scan.
	scanLimit int64
	// Records scanning maximum timeout.
	scanTimeout time.Duration
	// Scan the records at this interval.
	scanInterval time.Duration

	started chan struct{}

	// commit options.
	autoCommit     bool
	commitTimeout  time.Duration
	commitInterval time.Duration

	// To wait the running consumer loops when stopping.
	wait sync.WaitGroup
	stop chan struct{}
	// Lock for pausing and starting.
	mu    *sync.Mutex
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

	if cfg.ClientID != "" {
		id = cfg.ClientID
	}

	// Initialize the state store.
	var stateStore StateStore
	if cfg.StateStore == nil {
		s, err := newStateStore(cfg)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		stateStore = s
	} else {
		stateStore = *cfg.StateStore
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
	if cfg.KinesisClientSessionTraceFunc != nil {
		sess, err = cfg.KinesisClientSessionTraceFunc(sess)
		if err != nil {
			return nil, errors.WithStack(err)
		}
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

	if cfg.Commit == nil {
		cfg.Commit = NewDefaultCommitConfig()
	}

	buffer := recordsChanBuffer
	kinesumer := &Kinesumer{
		id:           id,
		client:       kinesis.New(sess, cfgs...),
		app:          cfg.App,
		rgn:          cfg.Region,
		efoMode:      cfg.EFOMode,
		records:      make(chan *Record, buffer),
		errors:       make(chan error, 1),
		stateStore:   stateStore,
		shardCaches:  make(map[string][]string),
		shards:       make(map[string]Shards),
		checkPoints:  make(map[string]*sync.Map),
		offsets:      make(map[string]*sync.Map),
		nextIters:    make(map[string]*sync.Map),
		scanLimit:    defaultScanLimit,
		scanTimeout:  defaultScanTimeout,
		scanInterval: defaultScanInterval,
		started:      make(chan struct{}),
		wait:         sync.WaitGroup{},
		stop:         make(chan struct{}),
		mu:           &sync.Mutex{},
		close:        make(chan struct{}),
	}

	if cfg.ScanLimit > 0 {
		kinesumer.scanLimit = cfg.ScanLimit
	}
	if cfg.ScanTimeout > 0 {
		kinesumer.scanTimeout = cfg.ScanTimeout
	}
	if cfg.ScanInterval > 0 {
		kinesumer.scanInterval = cfg.ScanInterval
	}

	if kinesumer.efoMode {
		kinesumer.efoMeta = make(map[string]*efoMeta)
	}

	kinesumer.autoCommit = cfg.Commit.Auto
	kinesumer.commitInterval = cfg.Commit.Interval
	kinesumer.commitTimeout = cfg.Commit.Timeout

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
		// TODO(mingrammer): handle CLOSED shards.
		if shard.SequenceNumberRange.EndingSequenceNumber == nil {
			shards = append(shards, &Shard{
				ID:     *shard.ShardId,
				Closed: shard.SequenceNumberRange.EndingSequenceNumber != nil,
			})
		}
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

	// In EFO mode, client should register itself to Kinesis stream.
	if k.efoMode {
		if err := k.registerConsumers(); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	if err := k.syncShardInfo(ctx); err != nil {
		return nil, errors.WithStack(err)
	}

	go k.loopSyncClient()

	close(k.started)
	return k.records, nil
}

func (k *Kinesumer) registerConsumers() error {
	consumerName := k.app
	if k.rgn != "" {
		consumerName += "-" + k.rgn
	}

	waitForActive := func(efoMeta *efoMeta) error {
		var (
			attemptCount    = 0
			maxAttemptCount = 10
			attemptDelay    = time.Second
		)

		for attemptCount < maxAttemptCount {
			attemptCount++

			dOutput, err := k.client.DescribeStreamConsumer(
				&kinesis.DescribeStreamConsumerInput{
					ConsumerARN:  &efoMeta.consumerARN,
					ConsumerName: &efoMeta.consumerName,
					StreamARN:    &efoMeta.streamARN,
				},
			)
			if err != nil {
				return errors.WithStack(err)
			}
			if *dOutput.ConsumerDescription.ConsumerStatus == kinesis.ConsumerStatusActive {
				return nil
			}

			time.Sleep(attemptDelay)
		}
		return nil
	}

	for _, stream := range k.streams {
		dOutput, err := k.client.DescribeStream(
			&kinesis.DescribeStreamInput{
				StreamName: aws.String(stream),
			},
		)
		if err != nil {
			return errors.WithStack(err)
		}

		streamARN := dOutput.StreamDescription.StreamARN
		rOutput, err := k.client.RegisterStreamConsumer(
			&kinesis.RegisterStreamConsumerInput{
				ConsumerName: aws.String(consumerName),
				StreamARN:    streamARN,
			},
		)

		// In case of that consumer is already registered.
		var awsErr awserr.Error
		if errors.As(err, &awsErr) {
			if awsErr.Code() != "ResourceInUseException" {
				return errors.WithStack(err)
			}
			lOutput, err := k.client.ListStreamConsumers(
				&kinesis.ListStreamConsumersInput{
					MaxResults: aws.Int64(20),
					StreamARN:  streamARN,
				},
			)
			if err != nil {
				return errors.WithStack(err)
			}

			var consumer *kinesis.Consumer
			for _, c := range lOutput.Consumers {
				if *c.ConsumerName == consumerName {
					consumer = c
					break
				}
			}
			k.efoMeta[stream] = &efoMeta{
				consumerARN:  *consumer.ConsumerARN,
				consumerName: *consumer.ConsumerName,
				streamARN:    *streamARN,
			}
			continue
		} else if err != nil {
			return errors.WithStack(err)
		}
		k.efoMeta[stream] = &efoMeta{
			consumerARN:  *rOutput.Consumer.ConsumerARN,
			consumerName: *rOutput.Consumer.ConsumerName,
			streamARN:    *streamARN,
		}
	}
	for _, efoMeta := range k.efoMeta {
		if err := waitForActive(efoMeta); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (k *Kinesumer) deregisterConsumers() {
	for _, meta := range k.efoMeta {
		_, err := k.client.DeregisterStreamConsumer(
			&kinesis.DeregisterStreamConsumerInput{
				ConsumerARN:  aws.String(meta.consumerARN),
				ConsumerName: aws.String(meta.consumerName),
				StreamARN:    aws.String(meta.streamARN),
			},
		)
		if err != nil {
			log.Warn().
				Msg("kinesumer: failed to deregister")
		}
	}
}

func (k *Kinesumer) start() {
	k.stop = make(chan struct{})

	if k.efoMode {
		k.consumeEFOMode()
	} else {
		k.consumePolling()
	}

	if k.autoCommit {
		go k.commitPeriodically()
	}
}

func (k *Kinesumer) pause() {
	close(k.stop)

	k.wait.Wait()
}

/*

Dedicated consumer with EFO.

*/

func (k *Kinesumer) consumeEFOMode() {
	for stream, shardsPerStream := range k.shards {
		for _, shard := range shardsPerStream {
			k.wait.Add(1)
			go k.consumePipe(stream, shard)
		}
	}
}

func (k *Kinesumer) consumePipe(stream string, shard *Shard) {
	defer k.wait.Done()

	streamEvents := make(chan kinesis.SubscribeToShardEventStreamEvent)

	go k.subscribeToShard(streamEvents, stream, shard)

	for {
		select {
		case e, ok := <-streamEvents:
			if !ok {
				k.Commit()
				k.cleanupOffsets(stream, shard)
				return
			}
			if se, ok := e.(*kinesis.SubscribeToShardEvent); ok {
				n := len(se.Records)
				if n == 0 {
					continue
				}

				for i, record := range se.Records {
					r := &Record{
						Stream:  stream,
						ShardID: shard.ID,
						Record:  record,
					}
					k.records <- r

					if k.autoCommit && i == n-1 {
						k.MarkRecord(r)
					}
				}
			}
		}
	}
}

func (k *Kinesumer) subscribeToShard(streamEvents chan kinesis.SubscribeToShardEventStreamEvent, stream string, shard *Shard) {
	defer close(streamEvents)

	for {
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)

		input := &kinesis.SubscribeToShardInput{
			ConsumerARN: aws.String(k.efoMeta[stream].consumerARN),
			ShardId:     aws.String(shard.ID),
			StartingPosition: &kinesis.StartingPosition{
				Type: aws.String(kinesis.ShardIteratorTypeLatest),
			},
		}

		if seq, ok := k.checkPoints[stream].Load(shard.ID); ok {
			input.StartingPosition.SetType(kinesis.ShardIteratorTypeAfterSequenceNumber)
			input.StartingPosition.SetSequenceNumber(seq.(string))
		}

		output, err := k.client.SubscribeToShardWithContext(ctx, input)
		if err != nil {
			k.sendOrDiscardError(errors.WithStack(err))
			cancel()
			continue
		}

		open := true
		for open {
			select {
			case <-k.stop:
				output.GetEventStream().Close()
				cancel()
				return
			case <-k.close:
				output.GetEventStream().Close()
				cancel()
				return
			case e, ok := <-output.GetEventStream().Events():
				if !ok {
					cancel()
					open = false
				}
				streamEvents <- e
			}
		}
	}
}

/*

Shared consumer with polling.

*/

func (k *Kinesumer) consumePolling() {
	for stream, shardsPerStream := range k.shards {
		for _, shard := range shardsPerStream {
			k.wait.Add(1)
			go k.consumeLoop(stream, shard)
		}
	}
}

func (k *Kinesumer) consumeLoop(stream string, shard *Shard) {
	defer k.wait.Done()

	for {
		select {
		case <-k.stop:
			k.Commit()
			return
		case <-k.close:
			k.Commit()
			return
		default:
			time.Sleep(k.scanInterval)
			records, closed := k.consumeOnce(stream, shard)
			if closed {
				k.cleanupOffsets(stream, shard)
				return // Close consume loop if shard is CLOSED and has no data.
			}

			n := len(records)
			if n == 0 {
				continue
			}

			for i, record := range records {
				r := &Record{
					Stream:  stream,
					ShardID: shard.ID,
					Record:  record,
				}
				k.records <- r

				if k.autoCommit && i == n-1 {
					k.MarkRecord(r)
				}
			}
		}
	}
}

// It returns records & flag which is whether if shard is CLOSED state and has no remaining data.
func (k *Kinesumer) consumeOnce(stream string, shard *Shard) ([]*kinesis.Record, bool) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, k.scanTimeout)
	defer cancel()

	shardIter, err := k.getNextShardIterator(ctx, stream, shard.ID)
	if err != nil {
		k.sendOrDiscardError(errors.WithStack(err))

		var riue *kinesis.ResourceInUseException
		return nil, errors.As(err, &riue)
	}

	output, err := k.client.GetRecordsWithContext(ctx, &kinesis.GetRecordsInput{
		Limit:         aws.Int64(k.scanLimit),
		ShardIterator: shardIter,
	})
	if err != nil {
		k.sendOrDiscardError(errors.WithStack(err))

		var riue *kinesis.ResourceInUseException
		if errors.As(err, &riue) {
			return nil, true
		}
		var eie *kinesis.ExpiredIteratorException
		if errors.As(err, &eie) {
			k.nextIters[stream].Delete(shard.ID) // Delete expired next iterator cache.
		}
		return nil, false
	}
	defer k.nextIters[stream].Store(shard.ID, output.NextShardIterator) // Update iter.

	n := len(output.Records)
	// We no longer care about shards that have no records left and are in the "CLOSED" state.
	if n == 0 {
		return nil, shard.Closed
	}

	return output.Records, false
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

func (k *Kinesumer) commitPeriodically() {
	var checkPointTicker = time.NewTicker(k.commitInterval)

	for {
		select {
		case <-k.stop:
			return
		case <-k.close:
			return
		case <-checkPointTicker.C:
			k.Commit()
		}
	}
}

// MarkRecord marks the provided record as consumed.
func (k *Kinesumer) MarkRecord(record *Record) {
	if record == nil {
		k.sendOrDiscardError(errMarkNilRecord)
		return
	}

	seqNum := *record.SequenceNumber
	if seqNum == "" {
		// sequence number can't be empty.
		k.sendOrDiscardError(ErrEmptySequenceNumber)
		return
	}
	if _, ok := k.checkPoints[record.Stream]; !ok {
		k.sendOrDiscardError(ErrInvalidStream)
		return
	}
	k.offsets[record.Stream].Store(record.ShardID, seqNum)
}

// Commit updates check point using current checkpoints.
func (k *Kinesumer) Commit() {
	var wg sync.WaitGroup
	for stream := range k.shards {
		wg.Add(1)

		var checkpoints []*ShardCheckPoint
		k.offsets[stream].Range(func(shardID, seqNum interface{}) bool {
			checkpoints = append(checkpoints, &ShardCheckPoint{
				Stream:         stream,
				ShardID:        shardID.(string),
				SequenceNumber: seqNum.(string),
				UpdatedAt:      time.Now(),
			})
			return true
		})

		go func(stream string, checkpoints []*ShardCheckPoint) {
			defer wg.Done()
			k.commitCheckPointsPerStream(stream, checkpoints)
		}(stream, checkpoints)
	}
	wg.Wait()
}

// commitCheckPointsPerStream updates checkpoints using sequence number.
func (k *Kinesumer) commitCheckPointsPerStream(stream string, checkpoints []*ShardCheckPoint) {
	if len(checkpoints) == 0 {
		return
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), k.commitTimeout)
	defer cancel()

	if err := k.stateStore.UpdateCheckPoints(timeoutCtx, checkpoints); err != nil {
		k.sendOrDiscardError(errors.Wrapf(err, "failed to commit on stream: %s", stream))
		return
	}
}

// cleanupOffsets remove uninterested stream's shard.
// TODO(proost): how to remove unused stream?
func (k *Kinesumer) cleanupOffsets(stream string, shard *Shard) {
	if shard == nil {
		return
	}
	if offsets, ok := k.offsets[stream]; ok {
		offsets.Delete(shard.ID)
	}
}

// Refresh refreshes the consuming streams.
func (k *Kinesumer) Refresh(streams []string) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.pause()
	// TODO(mingrammer): Deregister the EFO consumers.

	k.streams = streams

	if k.efoMode {
		k.registerConsumers()
	}
	k.start()
}

// Errors returns error channel.
func (k *Kinesumer) Errors() <-chan error {
	return k.errors
}

func (k *Kinesumer) sendOrDiscardError(err error) {
	select {
	case k.errors <- err:
	default:
		// if there are no error listeners, error is discarded.
	}
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

	// TODO(mingrammer): Deregister the EFO consumers.

	// Wait last sync jobs.
	time.Sleep(syncTimeout)
	log.Info().
		Msg("kinesumer: shutdown successfully")
}
