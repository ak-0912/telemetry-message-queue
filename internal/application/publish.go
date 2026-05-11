// Package application contains the use-case orchestrators that sit between
// the transport layer (gRPC) and the domain/infrastructure layer.
// Each use case is a thin struct that composes domain ports.
package application

import (
	"context"
	"time"

	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
)

// PublishUsecase routes a message to the correct partition (FNV32a on key),
// appends it to the store, and records publish metrics.
type PublishUsecase struct {
	Partitions     domain.PartitionStore
	PartitionCount int
	Metrics        PublishMetrics
}

// PublishMetrics is the subset of metrics the publish path needs.
type PublishMetrics interface {
	ObservePublishLatency(topic string, partition int32, d time.Duration)
	IncPublished(topic string, partition int32, n int64)
}

// Publish routes the message to a partition and appends it.
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
