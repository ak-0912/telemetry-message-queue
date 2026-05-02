package application

import (
	"context"
	"time"

	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
)

type PublishUsecase struct {
	Partitions     domain.PartitionStore
	PartitionCount int
	Metrics        PublishMetrics
}

type PublishMetrics interface {
	ObservePublishLatency(topic string, partition int32, d time.Duration)
	IncPublished(topic string, partition int32, n int64)
}

func (u *PublishUsecase) Publish(ctx context.Context, topic, key string, payload []byte) (partition int32, offset int64, err error) {
	start := time.Now()
	defer func() {
		if u.Metrics != nil {
			u.Metrics.ObservePublishLatency(topic, partition, time.Since(start))
		}
	}()
	if err := u.Partitions.EnsureTopic(topic, u.PartitionCount); err != nil {
		return 0, 0, err
	}
	partition = int32(PartitionFor(key, u.PartitionCount))
	offset, err = u.Partitions.Append(ctx, topic, partition, key, payload)
	if err != nil {
		return 0, 0, err
	}
	if u.Metrics != nil {
		u.Metrics.IncPublished(topic, partition, 1)
	}
	return partition, offset, nil
}
