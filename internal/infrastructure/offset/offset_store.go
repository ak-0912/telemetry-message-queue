package offset

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
)

// ErrOffsetRegression is returned when a commit moves the offset backwards.
var ErrOffsetRegression = errors.New("offset regression")

// FileOffsetStore keeps committed offsets in memory and flushes each group to JSON on commit.
// Committed value is the next offset to fetch (exclusive lower bound of unconsumed stream).
type FileOffsetStore struct {
	mu      sync.RWMutex
	dataDir string
	byGroup map[string]*groupOffsets // group -> topic -> partition -> next offset
}

type groupOffsets struct {
	mu     sync.Mutex
	Topics map[string]map[string]int64 `json:"topics"` // topic -> partition string -> offset
	path   string
}

func NewFileOffsetStore(dataDir string) (*FileOffsetStore, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	s := &FileOffsetStore{
		dataDir: dataDir,
		byGroup: make(map[string]*groupOffsets),
	}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) != ".json" {
			continue
		}
		group := name[:len(name)-len(".json")]
		if err := s.loadGroup(group); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *FileOffsetStore) loadGroup(group string) error {
	path := filepath.Join(s.dataDir, group+".json")
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			g := &groupOffsets{Topics: make(map[string]map[string]int64), path: path}
			s.byGroup[group] = g
			return nil
		}
		return err
	}
	var payload struct {
		Topics map[string]map[string]int64 `json:"topics"`
	}
	if err := json.Unmarshal(b, &payload); err != nil {
		return err
	}
	if payload.Topics == nil {
		payload.Topics = make(map[string]map[string]int64)
	}
	g := &groupOffsets{Topics: payload.Topics, path: path}
	s.byGroup[group] = g
	return nil
}

func (s *FileOffsetStore) group(group string) *groupOffsets {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.byGroup[group]
	if !ok {
		g = &groupOffsets{
			Topics: make(map[string]map[string]int64),
			path:   filepath.Join(s.dataDir, group+".json"),
		}
		s.byGroup[group] = g
	}
	return g
}

func (s *FileOffsetStore) Get(ctx context.Context, group, topic string, partition int32) int64 {
	_ = ctx
	s.mu.RLock()
	g, ok := s.byGroup[group]
	s.mu.RUnlock()
	if !ok {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	pm, ok := g.Topics[topic]
	if !ok {
		return 0
	}
	key := strconv.Itoa(int(partition))
	return pm[key]
}

func (s *FileOffsetStore) Commit(ctx context.Context, group, topic string, partition int32, offset int64) error {
	_ = ctx
	g := s.group(group)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.Topics[topic] == nil {
		g.Topics[topic] = make(map[string]int64)
	}
	key := strconv.Itoa(int(partition))
	prev := g.Topics[topic][key]
	if offset < prev {
		return fmt.Errorf("partition %s topic %s current %d new %d: %w", key, topic, prev, offset, ErrOffsetRegression)
	}
	g.Topics[topic][key] = offset
	return g.flushLocked()
}

// EnsureConsumerTopic creates an empty topic map for the group if needed and persists it.
func (s *FileOffsetStore) EnsureConsumerTopic(ctx context.Context, group, topic string) error {
	_ = ctx
	g := s.group(group)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.Topics[topic] == nil {
		g.Topics[topic] = make(map[string]int64)
		return g.flushLocked()
	}
	return nil
}

func (g *groupOffsets) flushLocked() error {
	payload := struct {
		Topics map[string]map[string]int64 `json:"topics"`
	}{Topics: g.Topics}
	b, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	tmp := g.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, g.path)
}

// MinCommittedNextOffset returns the minimum next-fetch offset across consumer groups that have
// registered topic (Commit or EnsureConsumerTopic). Groups with no topics[topic] entry are ignored
// so unrelated groups do not force a watermark of 0. If no group has registered for topic, returns 0.
func (s *FileOffsetStore) MinCommittedNextOffset(topic string, partition int32) int64 {
	key := strconv.Itoa(int(partition))
	s.mu.RLock()
	groups := make([]*groupOffsets, 0, len(s.byGroup))
	for _, g := range s.byGroup {
		groups = append(groups, g)
	}
	s.mu.RUnlock()

	var min int64 = -1
	participating := 0
	for _, g := range groups {
		g.mu.Lock()
		pm, ok := g.Topics[topic]
		if !ok {
			g.mu.Unlock()
			continue
		}
		participating++
		v := pm[key]
		g.mu.Unlock()
		if min < 0 || v < min {
			min = v
		}
	}
	if participating == 0 || min < 0 {
		return 0
	}
	return min
}

var _ domain.OffsetStore = (*FileOffsetStore)(nil)
