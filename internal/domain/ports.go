package domain

import "context"

// PartitionStore persists and serves partition logs.
type PartitionStore interface {
	Append(ctx context.Context, topic string, partition int32, key string, payload []byte) (offset int64, err error)
	Fetch(ctx context.Context, topic string, partition int32, offset int64, max int32) ([]Message, error)
	HighWatermark(ctx context.Context, topic string, partition int32) int64
	PartitionCount(topic string) int
	EnsureTopic(topic string, partitionCount int) error
	// OldestOffset returns the offset of the earliest retained message, or nextOffset if empty.
	OldestOffset(topic string, partition int32) int64
	RunRetentionGC(ctx context.Context)
}

// OffsetStore persists committed consumer offsets (next offset to fetch).
type OffsetStore interface {
	Get(ctx context.Context, group, topic string, partition int32) int64
	Commit(ctx context.Context, group, topic string, partition int32, offset int64) error
	// EnsureConsumerTopic records that group consumes topic so MinCommittedNextOffset includes it before the first commit.
	EnsureConsumerTopic(ctx context.Context, group, topic string) error
	MinCommittedNextOffset(topic string, partition int32) int64
}

// GroupCoordinator manages consumer group membership and assignments.
type GroupCoordinator interface {
	Join(ctx context.Context, group, topic, memberID string) (generationID string, err error)
	Heartbeat(ctx context.Context, group, memberID, generationID string) (rebalanceNeeded bool, currentGeneration string, err error)
	Assignment(ctx context.Context, group, memberID, generationID string) ([]PartitionAssignment, error)
	Leave(ctx context.Context, group, memberID string) error
	CurrentGeneration(group string) string
}
