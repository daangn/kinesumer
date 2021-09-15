package kinesumer

import (
	"context"
	"time"

	"github.com/daangn/kinesumer/pkg/collection"
	"github.com/pkg/errors"
)

const (
	outdatedGap = 10 * time.Second
)

func (k *Kinesumer) doLeadershipSyncShardIDs(ctx context.Context) error {
	for _, stream := range k.streams {
		shardIDs, err := k.listShardIDs(stream)
		if err != nil {
			return errors.WithStack(err)
		}
		if collection.EqualsSS(k.streamToShardCaches[stream], shardIDs) {
			return nil
		}
		if err := k.stateStore.UpdateShardIDs(ctx, stream, shardIDs); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (k *Kinesumer) doLeadershipPruneClients(ctx context.Context) error {
	if err := k.stateStore.PruneClients(ctx); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
