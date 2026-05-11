package application

import (
	"context"

	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
)

// DefaultFetchMax is used when the client sends max=0.
const DefaultFetchMax int32 = 200

// FetchUsecase reads messages from a partition and updates consumer lag metrics.
type FetchUsecase struct {
	Partitions     domain.PartitionStore
	PartitionCount int
	Offsets        domain.OffsetStore
	Metrics        FetchMetrics
}

// FetchMetrics is the subset of metrics the fetch path needs.
type FetchMetrics interface {
	IncFetched(group string, partition int32, n int64)
	SetLag(group, topic string, partition int32, lag float64)
}

// Fetch reads up to max messages from a partition starting at offset and
// updates the consumer lag gauge for the group.
func (u *FetchUsecase) Fetch(ctx context.Context, group, topic string, partition int32, offset int64, max int32) ([]domain.Message, error) {
	if max <= 0 {
		max = DefaultFetchMax
	}
	if err := u.Partitions.EnsureTopic(topic, u.PartitionCount); err != nil {
		return nil, err
	}
	msgs, err := u.Partitions.Fetch(ctx, topic, partition, offset, max)
	if err != nil {
		return nil, err
	}
	if u.Metrics != nil {
		u.Metrics.IncFetched(group, partition, int64(len(msgs)))
		hw := u.Partitions.HighWatermark(ctx, topic, partition)
		committed := u.Offsets.Get(ctx, group, topic, partition)
		lag := float64(hw - committed)
		if lag < 0 {
			lag = 0
		}
		u.Metrics.SetLag(group, topic, partition, lag)
	}
	return msgs, nil
}
