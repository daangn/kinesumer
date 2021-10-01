package kinesumer

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/daangn/kinesumer/pkg/collection"
)

func (k *Kinesumer) loopSyncClient() {
	ticker := time.NewTicker(syncInterval)
	for {
		select {
		case <-ticker.C:
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, syncTimeout)

			if err := k.pingAliveness(ctx); err != nil {
				log.Err(err).
					Msg("kinesumer: failed to pingAliveness")
			}
			if err := k.syncShardInfo(ctx); err != nil {
				log.Err(err).
					Msg("kinesumer: failed to syncShardInfo")
			}

			if k.leader {
				if err := k.doLeadershipSyncShardIDs(ctx); err != nil {
					log.Err(err).
						Msg("kinesumer: failed to doLeadershipSyncShardIDs")
				}
				if err := k.doLeadershipPruneClients(ctx); err != nil {
					log.Err(err).
						Msg("kinesumer: failed to doLeadershipPruneClients")
				}
			}
			cancel()
		case <-k.close:
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, syncTimeout)

			if err := k.stateStore.DeregisterClient(ctx, k.id); err != nil {
				log.Err(err).
					Msg("kinesumer: failed to DeregisterClient")
			}
			cancel()
			return
		}
	}
}

func (k *Kinesumer) pingAliveness(ctx context.Context) error {
	if err := k.stateStore.PingClientAliveness(ctx, k.id); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (k *Kinesumer) syncShardInfo(ctx context.Context) error {
	clientIDs, err := k.stateStore.ListAllAliveClientIDs(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	var idx int
	for i, id := range clientIDs {
		if id == k.id {
			idx = i
			break
		}
	}
	// Simple leader selection: take first (order by client id).
	k.leader = idx == 0

	// Update shard information.
	numOfClient := len(clientIDs)
	for _, stream := range k.streams {
		if err := k.syncShardInfoForStream(ctx, stream, idx, numOfClient); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (k *Kinesumer) syncShardInfoForStream(
	ctx context.Context, stream string, idx, numOfClient int,
) error {
	shards, err := k.stateStore.GetShards(ctx, stream)
	if errors.Is(err, ErrNoShardCache) {
		// If there are no cache, fetch shards from Kinesis directly.
		shards, err = k.listShards(stream)
		if err != nil {
			return errors.WithStack(err)
		}
	} else if err != nil {
		return errors.WithStack(err)
	}

	numShards := len(shards)

	// Assign a partial range of shard list to client.
	r := float64(numShards) / float64(numOfClient)
	splitStartIdx := int(math.Round(float64(idx) * r))
	splitEndIdx := int(math.Round(float64(idx+1) * r))
	newShards := shards[splitStartIdx:splitEndIdx]

	if collection.EqualsSS(k.shards[stream].ids(), newShards.ids()) {
		return nil
	}

	// Update client shard ids.
	k.pause() // Pause the current consuming jobs before update shards.
	k.shards[stream] = newShards
	defer k.start() // Re-start the consuming jobs with updated shards.

	// Sync next iterators map.
	if _, ok := k.nextIters[stream]; !ok {
		k.nextIters[stream] = &sync.Map{}
	}

	// Delete uninterested shard ids.
	shardIDs := k.shards[stream].ids()
	k.nextIters[stream].Range(func(key, _ interface{}) bool {
		if !collection.ContainsS(shardIDs, key.(string)) {
			k.nextIters[stream].Delete(key)
		}
		return true
	})

	// Sync shard check points.
	seqMap, err := k.stateStore.ListCheckPoints(ctx, stream, shardIDs)
	if err != nil {
		return errors.WithStack(err)
	}

	if _, ok := k.checkPoints[stream]; !ok {
		k.checkPoints[stream] = &sync.Map{}
	}

	// Delete uninterested shard ids.
	k.checkPoints[stream].Range(func(key, _ interface{}) bool {
		if _, ok := seqMap[key.(string)]; !ok {
			k.checkPoints[stream].Delete(key)
		}
		return true
	})
	for id, seq := range seqMap {
		k.checkPoints[stream].Store(id, seq)
	}
	log.Info().
		Str("stream", stream).
		Msgf("shard id range: %v", shardIDs)
	return nil
}
