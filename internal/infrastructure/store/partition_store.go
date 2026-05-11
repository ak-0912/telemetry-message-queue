// Package store implements the domain.PartitionStore interface with in-memory
// partition logs backed by per-partition append-only WAL files.
package store

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
	mqv1 "github.com/cisco-interview/telemetry-message-queue/proto/mq/v1"
)

// MemoryPartitionStore keeps an ordered message slice per partition in memory
// and persists every append to a length-prefixed protobuf WAL on disk.
// On startup the WAL is replayed to rebuild the in-memory state.
type MemoryPartitionStore struct {
	mu sync.RWMutex

	dataDir           string
	defaultPartitions int
	retention         time.Duration
	maxPerPartition   int
	offsets           domain.OffsetStore

	topics map[string]*topicPartitions
}

type topicPartitions struct {
	logs []*partitionLog
}

type partitionLog struct {
	mu         sync.Mutex
	topic      string
	id         int32
	messages   []domain.Message
	nextOffset int64
	wal        *WalWriter
	walPath    string
}

// NewMemoryPartitionStore creates a store rooted at dataDir. The offsets store
// is consulted during retention GC to avoid trimming unconsumed messages.
func NewMemoryPartitionStore(dataDir string, defaultPartitions int, retention time.Duration, maxPerPartition int, offsets domain.OffsetStore) *MemoryPartitionStore {
	return &MemoryPartitionStore{
		dataDir:           dataDir,
		defaultPartitions: defaultPartitions,
		retention:         retention,
		maxPerPartition:   maxPerPartition,
		offsets:           offsets,
		topics:            make(map[string]*topicPartitions),
	}
}

// EnsureTopic idempotently creates partition logs (and their WAL files) for
// the given topic. If the topic already exists, this is a no-op.
func (s *MemoryPartitionStore) EnsureTopic(topic string, partitionCount int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[topic]; ok {
		return nil
	}
	if partitionCount <= 0 {
		partitionCount = s.defaultPartitions
	}
	tp := &topicPartitions{logs: make([]*partitionLog, partitionCount)}
	for i := 0; i < partitionCount; i++ {
		log, err := s.openPartition(topic, int32(i))
		if err != nil {
			return err
		}
		tp.logs[i] = log
	}
	s.topics[topic] = tp
	return nil
}

func (s *MemoryPartitionStore) PartitionCount(topic string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tp, ok := s.topics[topic]
	if !ok {
		return 0
	}
	return len(tp.logs)
}

// KnownTopics returns topic names that have been initialized.
func (s *MemoryPartitionStore) KnownTopics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.topics))
	for t := range s.topics {
		out = append(out, t)
	}
	return out
}

// walPath returns the on-disk path for a partition WAL, sanitising topic
// names that contain slashes.
func (s *MemoryPartitionStore) walPath(topic string, partition int32) string {
	t := strings.ReplaceAll(topic, "/", "_")
	return fmt.Sprintf("%s/partitions/%s/%d.wal", s.dataDir, t, partition)
}

// openPartition opens (or creates) the WAL file and replays any existing
// records to rebuild the in-memory message slice.
func (s *MemoryPartitionStore) openPartition(topic string, id int32) (*partitionLog, error) {
	path := s.walPath(topic, id)
	w, err := OpenWal(path)
	if err != nil {
		return nil, err
	}
	pl := &partitionLog{
		topic:   topic,
		id:      id,
		wal:     w,
		walPath: path,
	}
	if err := ReplayWal(path, func(m *mqv1.Message) error {
		dm := domain.Message{
			Partition:   m.GetPartition(),
			Offset:      m.GetOffset(),
			Key:         m.GetKey(),
			Payload:     append([]byte(nil), m.GetPayload()...),
			PublishedAt: time.Unix(0, m.GetPublishedAt()),
		}
		pl.messages = append(pl.messages, dm)
		if m.GetOffset()+1 > pl.nextOffset {
			pl.nextOffset = m.GetOffset() + 1
		}
		return nil
	}); err != nil {
		_ = w.Close()
		return nil, err
	}
	return pl, nil
}

// getLog returns the partitionLog for the given topic/partition or an
// ErrUnknownTopic / ErrInvalidPartition sentinel error.
func (s *MemoryPartitionStore) getLog(topic string, partition int32) (*partitionLog, error) {
	s.mu.RLock()
	tp, ok := s.topics[topic]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("topic %q: %w", topic, ErrUnknownTopic)
	}
	if partition < 0 || int(partition) >= len(tp.logs) {
		return nil, fmt.Errorf("partition %d: %w", partition, ErrInvalidPartition)
	}
	return tp.logs[partition], nil
}

// Append writes a message to the partition log and WAL. On WAL failure the
// offset is rolled back so the in-memory state stays consistent.
func (s *MemoryPartitionStore) Append(ctx context.Context, topic string, partition int32, key string, payload []byte) (int64, error) {
	_ = ctx
	pl, err := s.getLog(topic, partition)
	if err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	pl.mu.Lock()
	defer pl.mu.Unlock()
	off := pl.nextOffset
	pl.nextOffset++
	dm := domain.Message{
		Partition:   partition,
		Offset:      off,
		Key:         key,
		Payload:     append([]byte(nil), payload...),
		PublishedAt: now,
	}
	pm := &mqv1.Message{
		Partition:   partition,
		Offset:      off,
		Key:         key,
		Payload:     payload,
		PublishedAt: now.UnixNano(),
	}
	if err := pl.wal.Append(pm); err != nil {
		pl.nextOffset--
		return 0, err
	}
	pl.messages = append(pl.messages, dm)
	return off, nil
}

// Fetch returns up to max messages starting from offset (inclusive). Returned
// payloads are deep-copied so the caller cannot mutate the log.
func (s *MemoryPartitionStore) Fetch(ctx context.Context, topic string, partition int32, offset int64, max int32) ([]domain.Message, error) {
	_ = ctx
	pl, err := s.getLog(topic, partition)
	if err != nil {
		return nil, err
	}
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if max <= 0 {
		return nil, nil
	}
	start := sort.Search(len(pl.messages), func(i int) bool {
		return pl.messages[i].Offset >= offset
	})
	if start >= len(pl.messages) {
		return nil, nil
	}
	end := start + int(max)
	if end > len(pl.messages) {
		end = len(pl.messages)
	}
	out := make([]domain.Message, end-start)
	copy(out, pl.messages[start:end])
	// Return deep copy of payloads
	for i := range out {
		out[i].Payload = append([]byte(nil), out[i].Payload...)
	}
	return out, nil
}

func (s *MemoryPartitionStore) HighWatermark(ctx context.Context, topic string, partition int32) int64 {
	_ = ctx
	pl, err := s.getLog(topic, partition)
	if err != nil {
		return 0
	}
	pl.mu.Lock()
	defer pl.mu.Unlock()
	return pl.nextOffset
}

func (s *MemoryPartitionStore) OldestOffset(topic string, partition int32) int64 {
	pl, err := s.getLog(topic, partition)
	if err != nil {
		return 0
	}
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if len(pl.messages) == 0 {
		return pl.nextOffset
	}
	return pl.messages[0].Offset
}

func (s *MemoryPartitionStore) MessageCount(topic string, partition int32) int {
	pl, err := s.getLog(topic, partition)
	if err != nil {
		return 0
	}
	pl.mu.Lock()
	defer pl.mu.Unlock()
	return len(pl.messages)
}

// RunRetentionGC walks every partition, trims messages that are both older
// than the retention window and past the commit watermark, and rewrites the
// WAL for any partition that was trimmed.
func (s *MemoryPartitionStore) RunRetentionGC(ctx context.Context) {
	s.mu.RLock()
	topics := make(map[string]*topicPartitions, len(s.topics))
	for k, v := range s.topics {
		topics[k] = v
	}
	ret := s.retention
	maxSz := s.maxPerPartition
	offsets := s.offsets
	s.mu.RUnlock()

	cutoff := time.Now().Add(-ret)
	for topic, tp := range topics {
		for i, pl := range tp.logs {
			if pl == nil {
				continue
			}
			s.trimPartition(ctx, pl, topic, int32(i), cutoff, maxSz, offsets)
		}
	}
}

// trimPartition drops messages from the head of the log that are both
// expired (older than cutoff) and already consumed (offset < minCommitWatermark).
// A second pass enforces the per-partition size cap.
func (s *MemoryPartitionStore) trimPartition(ctx context.Context, pl *partitionLog, topic string, partition int32, cutoff time.Time, maxSz int, offsets domain.OffsetStore) {
	_ = ctx
	minNext := int64(1<<62 - 1)
	if offsets != nil {
		minNext = offsets.MinCommittedNextOffset(topic, partition)
	}
	pl.mu.Lock()
	defer pl.mu.Unlock()
	before := len(pl.messages)
	trim := func() {
		for len(pl.messages) > 0 {
			head := pl.messages[0]
			aged := !head.PublishedAt.After(cutoff)
			beforeCommit := head.Offset < minNext
			if aged && beforeCommit {
				pl.messages = pl.messages[1:]
				continue
			}
			break
		}
		for len(pl.messages) > maxSz && maxSz > 0 {
			head := pl.messages[0]
			if head.Offset < minNext {
				pl.messages = pl.messages[1:]
				continue
			}
			break
		}
	}
	trim()
	if len(pl.messages) < before {
		_ = rewritePartitionWAL(pl)
	}
}

// rewritePartitionWAL replaces the WAL file with a fresh one containing only
// the messages still in memory. Must be called with pl.mu held.
func rewritePartitionWAL(pl *partitionLog) error {
	if pl.wal != nil {
		_ = pl.wal.Close()
	}
	if err := os.Remove(pl.walPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	w, err := OpenWal(pl.walPath)
	if err != nil {
		return err
	}
	pl.wal = w
	for _, dm := range pl.messages {
		pm := &mqv1.Message{
			Partition:   dm.Partition,
			Offset:      dm.Offset,
			Key:         dm.Key,
			Payload:     dm.Payload,
			PublishedAt: dm.PublishedAt.UnixNano(),
		}
		if err := pl.wal.Append(pm); err != nil {
			return err
		}
	}
	return nil
}

var _ domain.PartitionStore = (*MemoryPartitionStore)(nil)
