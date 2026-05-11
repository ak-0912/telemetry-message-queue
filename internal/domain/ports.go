// Package domain defines the core types and port interfaces for the message queue.
//
// Types here are pure value objects with no infrastructure dependencies.
// Interfaces (ports) describe the capabilities the application layer needs;
// concrete implementations live in internal/infrastructure.
package domain

import "context"

// PartitionStore persists and serves partition logs.
// Each topic is split into a fixed number of partitions; messages within a
// partition are strictly ordered by monotonically increasing offset.
type PartitionStore interface {
	// Append writes a message to the given partition and returns its assigned offset.
	Append(ctx context.Context, topic string, partition int32, key string, payload []byte) (offset int64, err error)
	// Fetch returns up to max messages starting from offset (inclusive).
	Fetch(ctx context.Context, topic string, partition int32, offset int64, max int32) ([]Message, error)
	// HighWatermark returns the next offset that will be assigned (one past the latest message).
	HighWatermark(ctx context.Context, topic string, partition int32) int64
	// PartitionCount returns the number of partitions for a topic, or 0 if unknown.
	PartitionCount(topic string) int
	// EnsureTopic idempotently creates a topic with the given partition count.
	EnsureTopic(topic string, partitionCount int) error
	// OldestOffset returns the offset of the earliest retained message, or HighWatermark if empty.
	OldestOffset(topic string, partition int32) int64
	// RunRetentionGC trims messages that exceed age or size limits.
	RunRetentionGC(ctx context.Context)
}

// OffsetStore persists committed consumer offsets.
// The committed value is the next offset to fetch (exclusive lower bound of
// the unconsumed portion of the stream).
type OffsetStore interface {
	// Get returns the committed next-fetch offset, or 0 if never committed.
	Get(ctx context.Context, group, topic string, partition int32) int64
	// Commit persists a new next-fetch offset. Returns ErrOffsetRegression if
	// the new value is lower than the current one.
	Commit(ctx context.Context, group, topic string, partition int32, offset int64) error
	// EnsureConsumerTopic records that group consumes topic so that
	// MinCommittedNextOffset includes it before the first Commit call.
	EnsureConsumerTopic(ctx context.Context, group, topic string) error
	// MinCommittedNextOffset returns the lowest committed offset across all
	// groups registered for the topic/partition. Returns 0 if no group is registered.
	MinCommittedNextOffset(topic string, partition int32) int64
}

// GroupCoordinator manages consumer group membership, partition assignment,
// and heartbeat-based liveness detection.
type GroupCoordinator interface {
	// Join adds (or re-registers) a member in a consumer group and returns the
	// current generation ID. A new generation is created only when membership changes.
	Join(ctx context.Context, group, topic, memberID string) (generationID string, err error)
	// Heartbeat refreshes a member's liveness timestamp and reports whether the
	// member should rejoin (rebalanceNeeded) due to a generation change.
	Heartbeat(ctx context.Context, group, memberID, generationID string) (rebalanceNeeded bool, currentGeneration string, err error)
	// Assignment returns the partitions assigned to a member for the given generation.
	Assignment(ctx context.Context, group, memberID, generationID string) ([]PartitionAssignment, error)
	// Leave removes a member from the group and triggers a rebalance if others remain.
	Leave(ctx context.Context, group, memberID string) error
	// CurrentGeneration returns the latest generation ID, or "" if the group does not exist.
	CurrentGeneration(group string) string
}
