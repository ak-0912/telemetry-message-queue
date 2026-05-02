package application

import (
	"context"

	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
)

type CommitOffsetUsecase struct {
	Partitions     domain.PartitionStore
	PartitionCount int
	Offsets        domain.OffsetStore
}

func (u *CommitOffsetUsecase) Commit(ctx context.Context, group, topic string, partition int32, offset int64) error {
	if err := u.Partitions.EnsureTopic(topic, u.PartitionCount); err != nil {
		return err
	}
	return u.Offsets.Commit(ctx, group, topic, partition, offset)
}

// EnsureConsumerTopic ensures the partition log exists and records group interest in topic for retention watermarks.
func (u *CommitOffsetUsecase) EnsureConsumerTopic(ctx context.Context, group, topic string) error {
	if err := u.Partitions.EnsureTopic(topic, u.PartitionCount); err != nil {
		return err
	}
	return u.Offsets.EnsureConsumerTopic(ctx, group, topic)
}
