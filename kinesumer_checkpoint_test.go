package kinesumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPollingRefreshAndCloseCheckpointCounts(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		streamCount int
		wantCalls   int
		wantRows    int
	}{
		{name: "one worker", streamCount: 1, wantCalls: 1, wantRows: 1},
		{name: "two streams", streamCount: 2, wantCalls: 2, wantRows: 2},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := newPollingCheckpointStore()
			fixture := newPollingKinesisFixture(t, false)
			k := newPollingKinesumer(t, store, testCase.streamCount, fixture)
			for stream, shards := range k.shards {
				markPollingOffset(k, stream, shards[0].ID, "100")
			}
			k.start()
			fixture.waitForRecords(t, testCase.streamCount)

			beforeCalls, beforeRows := store.stats()
			k.Refresh(k.streams)
			calls, rows := store.stats()
			if got := calls - beforeCalls; got != testCase.wantCalls {
				t.Fatalf("Refresh made %d checkpoint writes, want %d", got, testCase.wantCalls)
			}
			if got := rows - beforeRows; got != testCase.wantRows {
				t.Fatalf("Refresh wrote %d checkpoint rows, want %d", got, testCase.wantRows)
			}

			beforeCalls, beforeRows = calls, rows
			k.Close()
			calls, rows = store.stats()
			if got := calls - beforeCalls; got != testCase.wantCalls {
				t.Fatalf("Close made %d checkpoint writes, want %d", got, testCase.wantCalls)
			}
			if got := rows - beforeRows; got != testCase.wantRows {
				t.Fatalf("Close wrote %d checkpoint rows, want %d", got, testCase.wantRows)
			}
		})
	}
}

func TestPollingFinalMarkSurvivesOlderPeriodicCommit(t *testing.T) {
	for _, operation := range []string{"pause", "close"} {
		t.Run(operation, func(t *testing.T) {
			store := newPollingCheckpointStore()
			store.blockSeq = "100"
			store.writeStarted = make(chan struct{}, 8)
			store.releaseWrite = make(chan struct{})
			fixture := newPollingKinesisFixture(t, true)
			k := newPollingKinesumer(t, store, 2, fixture)
			k.autoCommit = true
			k.commitInterval = defaultCommitInterval
			for stream, shards := range k.shards {
				markPollingOffset(k, stream, shards[0].ID, "100")
			}

			periodicDone := make(chan struct{})
			var releaseOnce sync.Once
			releaseOld := func() { releaseOnce.Do(func() { close(store.releaseWrite) }) }
			var operationDone chan struct{}
			t.Cleanup(func() {
				releaseOld()
				if operationDone != nil {
					select {
					case <-operationDone:
					case <-time.After(8 * time.Second):
					}
				} else if !isKinesumerClosedForTest(k) {
					k.Close()
				}
				select {
				case <-periodicDone:
				case <-time.After(3 * time.Second):
				}
			})
			k.wait.Add(1)
			go func() {
				defer k.wait.Done()
				k.commitPeriodically()
				close(periodicDone)
			}()
			for range k.shards {
				select {
				case <-store.writeStarted:
				case <-time.After(defaultCommitInterval + 3*time.Second):
					t.Fatal("periodic commit did not start both old stream writes")
				}
			}

			for stream, shards := range k.shards {
				stream, shard := stream, shards[0]
				k.wait.Add(1)
				go k.consumeLoop(stream, shard)
			}
			for range k.shards {
				select {
				case <-fixture.afterMark:
				case <-time.After(3 * time.Second):
					t.Fatal("worker did not mark sequence 200 before its next poll")
				}
			}
			operationDone = make(chan struct{})
			stop := k.stop
			go func() {
				if operation == "pause" {
					k.pause()
				} else {
					k.Close()
				}
				close(operationDone)
			}()
			select {
			case <-stop:
			case <-time.After(3 * time.Second):
				releaseOld()
				t.Fatal("lifecycle operation did not signal polling workers")
			}
			releaseOld()
			select {
			case <-operationDone:
			case <-time.After(8 * time.Second):
				t.Fatal("lifecycle operation did not finish after releasing the older write")
			}
			select {
			case <-periodicDone:
			case <-time.After(3 * time.Second):
				t.Fatal("periodic commit loop did not stop")
			}

			for stream, shards := range k.shards {
				if got := store.checkpoint(stream, shards[0].ID); got != "200" {
					t.Errorf("%s/%s persisted %q, want final worker mark 200", stream, shards[0].ID, got)
				}
			}
			if operation == "pause" {
				close(k.records)
				close(k.errors)
			}
		})
	}
}

type blockingShardStore struct {
	*pollingCheckpointStore
	started chan struct{}
	release chan struct{}
}

func (s *blockingShardStore) GetShards(context.Context, string) (Shards, error) {
	s.started <- struct{}{}
	<-s.release
	return Shards{&Shard{ID: "new-shard"}}, nil
}

func TestLifecycleMethodsDoNotRestartAfterClose(t *testing.T) {
	store := &blockingShardStore{
		pollingCheckpointStore: newPollingCheckpointStore(),
		started:                make(chan struct{}, 1),
		release:                make(chan struct{}),
	}
	fixture := newPollingKinesisFixture(t, false)
	k := newPollingKinesumer(t, store, 1, fixture)
	k.shards["stream-0"] = Shards{&Shard{ID: "old-shard"}}
	t.Cleanup(func() {
		select {
		case <-store.release:
		default:
			close(store.release)
		}
	})

	syncDone := make(chan error, 1)
	go func() {
		syncDone <- k.syncShardInfoForStream(context.Background(), "stream-0", 0, 1)
	}()
	select {
	case <-store.started:
	case <-time.After(3 * time.Second):
		t.Fatal("background shard sync did not reach the controlled state-store read")
	}

	closeDone := make(chan struct{})
	go func() {
		k.Close()
		close(closeDone)
	}()
	select {
	case <-k.close:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not publish its shutdown signal")
	}
	close(store.release)
	select {
	case err := <-syncDone:
		if err != nil {
			t.Fatalf("syncShardInfoForStream after Close returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("queued shard sync did not finish after Close")
	}

	k.Refresh([]string{"stream-0"})
	if got := k.shards["stream-0"][0].ID; got != "old-shard" {
		t.Fatalf("shard reconfiguration after Close changed the shard to %q", got)
	}
	if calls, _ := store.stats(); calls != 0 {
		t.Fatalf("lifecycle method after Close wrote %d checkpoints", calls)
	}
	select {
	case <-closeDone:
	case <-time.After(8 * time.Second):
		t.Fatal("Close did not finish")
	}
}

type reassignmentCheckpointStore struct {
	*pollingCheckpointStore
}

func (s *reassignmentCheckpointStore) GetShards(context.Context, string) (Shards, error) {
	return Shards{&Shard{ID: "shard-0"}}, nil
}

func TestCloseDoesNotOverwriteCheckpointAfterEmptyAssignment(t *testing.T) {
	store := &reassignmentCheckpointStore{pollingCheckpointStore: newPollingCheckpointStore()}
	fixture := newPollingKinesisFixture(t, true)
	k := newPollingKinesumer(t, store, 1, fixture)
	k.autoCommit = true
	k.commitInterval = time.Hour
	t.Cleanup(func() {
		if !isKinesumerClosedForTest(k) {
			k.Close()
		}
	})
	if err := store.UpdateCheckPoints(context.Background(), []*ShardCheckPoint{{
		Stream: "stream-0", ShardID: "shard-0", SequenceNumber: "100",
	}}); err != nil {
		t.Fatal(err)
	}
	k.start()
	select {
	case <-fixture.afterMark:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not automatically mark sequence 200")
	}

	err := k.syncShardInfoForStream(context.Background(), "stream-0", 1, 2)
	if !errors.Is(err, ErrEmptyShardIDs) {
		t.Fatalf("empty shard assignment returned %v, want ErrEmptyShardIDs", err)
	}
	if err := store.UpdateCheckPoints(context.Background(), []*ShardCheckPoint{{
		Stream: "stream-0", ShardID: "shard-0", SequenceNumber: "300",
	}}); err != nil {
		t.Fatal(err)
	}

	k.Close()
	if got := store.checkpoint("stream-0", "shard-0"); got != "300" {
		t.Fatalf("old client's Close overwrote the new owner's checkpoint with %q", got)
	}
}

func TestPollingLifecycleCommitSkipsMissingOffsets(t *testing.T) {
	store := newPollingCheckpointStore()
	fixture := newPollingKinesisFixture(t, false)
	k := newPollingKinesumer(t, store, 0, fixture)
	k.shards["stream-0"] = Shards{&Shard{ID: "shard-0"}}

	k.commitPollingShard("stream-0", "shard-0")
	if calls, rows := store.stats(); calls != 0 || rows != 0 {
		t.Fatalf("lifecycle flush wrote %d rows in %d calls without offsets", rows, calls)
	}
}

type handoffCheckpointStore struct {
	*pollingCheckpointStore
	gateMu  sync.Mutex
	gate    bool
	started chan []*ShardCheckPoint
	release chan struct{}
	once    sync.Once
}

func (s *handoffCheckpointStore) blockSubsequentWrites() {
	s.gateMu.Lock()
	s.gate = true
	s.gateMu.Unlock()
}

func (s *handoffCheckpointStore) allowWrites() {
	s.gateMu.Lock()
	s.gate = false
	s.gateMu.Unlock()
}

func (s *handoffCheckpointStore) releaseWrite() {
	s.once.Do(func() { close(s.release) })
}

func (s *handoffCheckpointStore) UpdateCheckPoints(
	ctx context.Context, checkpoints []*ShardCheckPoint,
) error {
	s.gateMu.Lock()
	block := s.gate
	s.gateMu.Unlock()
	if block {
		copyOfCheckpoints := make([]*ShardCheckPoint, len(checkpoints))
		for i, checkpoint := range checkpoints {
			copy := *checkpoint
			copyOfCheckpoints[i] = &copy
		}
		s.started <- copyOfCheckpoints
		<-s.release
	}
	return s.pollingCheckpointStore.UpdateCheckPoints(ctx, checkpoints)
}

type handoffReassignmentStore struct {
	*handoffCheckpointStore
}

func (s *handoffReassignmentStore) GetShards(context.Context, string) (Shards, error) {
	return Shards{&Shard{ID: "shard-a"}, &Shard{ID: "shard-b"}}, nil
}

func TestPollingLifecycleWaitsForWorkerCheckpointWrites(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		rebalance bool
	}{
		{name: "close"},
		{name: "refresh"},
		{name: "shard sync", rebalance: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			baseStore := newPollingCheckpointStore()
			gatedStore := &handoffCheckpointStore{
				pollingCheckpointStore: baseStore,
				started:                make(chan []*ShardCheckPoint, 4),
				release:                make(chan struct{}),
			}
			var stateStore StateStore = gatedStore
			if testCase.rebalance {
				stateStore = &handoffReassignmentStore{handoffCheckpointStore: gatedStore}
			}
			for _, shardID := range []string{"shard-a", "shard-b"} {
				if err := baseStore.UpdateCheckPoints(context.Background(), []*ShardCheckPoint{{
					Stream: "stream-0", ShardID: shardID, SequenceNumber: "100",
				}}); err != nil {
					t.Fatal(err)
				}
			}
			fixture := newPollingKinesisFixture(t, true)
			releaseA := make(chan struct{})
			releaseB := make(chan struct{})
			fixture.blockedReads = map[string]<-chan struct{}{
				"stream-0/shard-a": releaseA,
				"stream-0/shard-b": releaseB,
			}
			k := newPollingKinesumer(t, stateStore, 1, fixture)
			k.autoCommit = true
			k.commitInterval = time.Hour
			k.shards["stream-0"] = Shards{&Shard{ID: "shard-a"}, &Shard{ID: "shard-b"}}
			k.offsets["stream-0"].Delete("shard-0")
			for _, shardID := range []string{"shard-a", "shard-b"} {
				k.checkPoints["stream-0"].Store(shardID, "100")
				k.offsets["stream-0"].Store(shardID, "100")
				iterator := "stream-0/" + shardID
				k.nextIters["stream-0"].Store(shardID, &iterator)
			}
			operationDone := make(chan struct{})
			var operationErr chan error
			var releaseAOnce, releaseBOnce, releaseWriteOnce sync.Once
			releaseARequest := func() { releaseAOnce.Do(func() { close(releaseA) }) }
			releaseBRequest := func() { releaseBOnce.Do(func() { close(releaseB) }) }
			releaseWrite := func() { releaseWriteOnce.Do(func() { gatedStore.releaseWrite() }) }
			t.Cleanup(func() {
				releaseARequest()
				releaseBRequest()
				releaseWrite()
				select {
				case <-operationDone:
				case <-time.After(10 * time.Second):
				}
				if !isKinesumerClosedForTest(k) {
					k.Close()
				}
			})

			for _, shard := range k.shards["stream-0"] {
				shard := shard
				k.wait.Add(1)
				go k.consumeLoop("stream-0", shard)
			}
			seen := make(map[string]bool, 2)
			for len(seen) < 2 {
				select {
				case iterator := <-fixture.afterMark:
					seen[iterator] = true
				case <-time.After(3 * time.Second):
					t.Fatal("workers did not automatically mark sequence 200")
				}
			}

			gatedStore.blockSubsequentWrites()
			switch testCase.name {
			case "close":
				go func() { k.Close(); close(operationDone) }()
			case "refresh":
				go func() { k.Refresh(k.streams); close(operationDone) }()
			case "shard sync":
				operationErr = make(chan error, 1)
				go func() {
					operationErr <- k.syncShardInfoForStream(context.Background(), "stream-0", 0, 2)
					close(operationDone)
				}()
			}
			select {
			case <-k.stop:
			case <-k.close:
			case <-time.After(3 * time.Second):
				t.Fatal("lifecycle operation did not stop polling workers")
			}
			releaseARequest()
			releaseBRequest()

			var firstWrite []*ShardCheckPoint
			select {
			case firstWrite = <-gatedStore.started:
			case <-operationDone:
				t.Fatal("lifecycle operation returned before its checkpoint write")
			case <-time.After(8 * time.Second):
				t.Fatal("checkpoint write did not reach the controlled state")
			}
			if len(firstWrite) == 0 {
				t.Fatal("lifecycle submitted no worker checkpoint rows")
			}
			for _, checkpoint := range firstWrite {
				if checkpoint.SequenceNumber != "200" || (checkpoint.ShardID != "shard-a" && checkpoint.ShardID != "shard-b") {
					t.Fatalf("lifecycle checkpoint rows = %v, want marked worker rows at 200", firstWrite)
				}
			}
			select {
			case <-operationDone:
				t.Fatal("lifecycle returned while a checkpoint write was blocked")
			default:
			}

			gatedStore.releaseWrite()
			select {
			case <-operationDone:
			case <-time.After(10 * time.Second):
				t.Fatal("lifecycle operation did not finish after checkpoint writes")
			}
			if testCase.rebalance {
				if err := <-operationErr; err != nil {
					t.Fatalf("shard synchronization returned %v", err)
				}
				shards := k.shards["stream-0"]
				if len(shards) != 1 || shards[0].ID != "shard-a" {
					t.Fatalf("rebalance retained shards %v, want only shard-a", shards.ids())
				}
			}
			gatedStore.allowWrites()
			for _, shardID := range []string{"shard-a", "shard-b"} {
				if got := baseStore.checkpoint("stream-0", shardID); got != "200" {
					t.Fatalf("%s left %s checkpoint at %q, want 200", testCase.name, shardID, got)
				}
			}
			if testCase.name == "refresh" {
				return
			}
			if err := baseStore.UpdateCheckPoints(context.Background(), []*ShardCheckPoint{{
				Stream: "stream-0", ShardID: "shard-b", SequenceNumber: "300",
			}}); err != nil {
				t.Fatal(err)
			}
			if testCase.rebalance {
				k.Close()
			}
			if got := baseStore.checkpoint("stream-0", "shard-b"); got != "300" {
				t.Fatalf("successor checkpoint was overwritten with %q", got)
			}
		})
	}
}

func TestPollingTerminalCleanupExcludesOldOwnerOffsets(t *testing.T) {
	cases := []struct {
		name       string
		operation  string
		manualMark bool
	}{
		{name: "refresh next owner", operation: "refresh"},
		{name: "close next owner", operation: "close"},
		{name: "refresh manual mark", operation: "refresh", manualMark: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			baseStore := newPollingCheckpointStore()
			store := &handoffCheckpointStore{
				pollingCheckpointStore: baseStore,
				started:                make(chan []*ShardCheckPoint, 2),
				release:                make(chan struct{}),
			}
			for _, shardID := range []string{"shard-a", "shard-b"} {
				if err := baseStore.UpdateCheckPoints(context.Background(), []*ShardCheckPoint{{
					Stream: "stream-0", ShardID: shardID, SequenceNumber: "100",
				}}); err != nil {
					t.Fatal(err)
				}
			}
			fixture := newPollingKinesisFixture(t, true)
			releaseA := make(chan struct{})
			releaseB := make(chan struct{})
			fixture.blockedReads = map[string]<-chan struct{}{
				"stream-0/shard-a": releaseA,
				"stream-0/shard-b": releaseB,
			}
			fixture.resourceInUse = map[string]bool{"stream-0/shard-b": true}
			k := newPollingKinesumer(t, store, 1, fixture)
			k.autoCommit = true
			k.commitInterval = time.Hour
			k.shards["stream-0"] = Shards{
				&Shard{ID: "shard-a"},
				&Shard{ID: "shard-b"},
			}
			k.offsets["stream-0"].Delete("shard-0")
			for _, shardID := range []string{"shard-a", "shard-b"} {
				k.checkPoints["stream-0"].Store(shardID, "100")
				k.offsets["stream-0"].Store(shardID, "100")
				iterator := "stream-0/" + shardID
				k.nextIters["stream-0"].Store(shardID, &iterator)
			}
			var releaseAOnce, releaseBOnce sync.Once
			releaseARequest := func() { releaseAOnce.Do(func() { close(releaseA) }) }
			releaseBRequest := func() { releaseBOnce.Do(func() { close(releaseB) }) }
			var operationDone chan struct{}
			t.Cleanup(func() {
				releaseARequest()
				releaseBRequest()
				store.releaseWrite()
				if operationDone != nil {
					select {
					case <-operationDone:
					case <-time.After(8 * time.Second):
					}
				}
				if !isKinesumerClosedForTest(k) {
					k.Close()
				}
			})

			workerADone := make(chan struct{})
			workerBDone := make(chan struct{})
			k.wait.Add(1)
			go func() {
				k.consumeLoop("stream-0", k.shards["stream-0"][0])
				close(workerADone)
			}()
			k.wait.Add(1)
			go func() {
				k.consumeLoop("stream-0", k.shards["stream-0"][1])
				close(workerBDone)
			}()
			seen := make(map[string]bool, 2)
			for len(seen) < 2 {
				select {
				case iterator := <-fixture.afterMark:
					seen[iterator] = true
				case <-time.After(3 * time.Second):
					t.Fatal("workers did not mark sequence 200 before blocking")
				}
			}

			operationDone = make(chan struct{})
			if testCase.operation == "refresh" {
				go func() { k.Refresh(k.streams); close(operationDone) }()
				select {
				case <-k.stop:
				case <-time.After(3 * time.Second):
					t.Fatal("Refresh did not signal workers")
				}
			} else {
				go func() { k.Close(); close(operationDone) }()
				select {
				case <-k.stop:
				case <-k.close:
				case <-time.After(3 * time.Second):
					t.Fatal("Close did not signal workers")
				}
			}
			releaseBRequest()
			select {
			case <-workerBDone:
			case <-time.After(3 * time.Second):
				t.Fatal("terminal worker B did not finish")
			}

			if !testCase.manualMark {
				newOwnerCheckpoint := []*ShardCheckPoint{{
					Stream: "stream-0", ShardID: "shard-b", SequenceNumber: "300",
				}}
				if err := baseStore.UpdateCheckPoints(context.Background(), newOwnerCheckpoint); err != nil {
					t.Fatal(err)
				}
				k.Commit()
				if got := baseStore.checkpoint("stream-0", "shard-b"); got != "300" {
					t.Fatalf("Commit overwrote the next owner's checkpoint with %q", got)
				}
			}

			store.blockSubsequentWrites()
			releaseARequest()
			var finalWrite []*ShardCheckPoint
			select {
			case finalWrite = <-store.started:
			case <-operationDone:
			case <-time.After(8 * time.Second):
				t.Fatal("lifecycle operation neither flushed nor returned")
			}
			if finalWrite == nil {
				t.Fatal("lifecycle operation did not submit the surviving shard checkpoint")
			}
			var sawA, sawB bool
			for _, checkpoint := range finalWrite {
				sawA = sawA || checkpoint.ShardID == "shard-a"
				sawB = sawB || checkpoint.ShardID == "shard-b"
			}
			if !sawA || sawB {
				t.Fatalf("lifecycle write rows = %v, want shard-a only", finalWrite)
			}
			if testCase.manualMark {
				markPollingOffset(k, "stream-0", "shard-b", "300")
			}
			store.releaseWrite()
			select {
			case <-operationDone:
			case <-time.After(8 * time.Second):
				t.Fatal("lifecycle operation did not finish after the blocked write")
			}
			store.allowWrites()

			if testCase.manualMark {
				k.Commit()
			}
			if got := baseStore.checkpoint("stream-0", "shard-b"); got != "300" {
				t.Fatalf("%s overwrote or lost the final checkpoint: %q (write rows=%v)",
					testCase.operation, got, finalWrite)
			}
			if testCase.operation == "refresh" {
				k.Close()
			}
		})
	}
}

func isKinesumerClosedForTest(k *Kinesumer) bool {
	select {
	case <-k.close:
		return true
	default:
		return false
	}
}
