package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
)

const maxGroupMembers = 10

// GroupCoordinator implements range assignment and heartbeat-driven rebalancing.
type GroupCoordinator struct {
	mu sync.Mutex

	partitionCount int
	heartbeatTTL   time.Duration
	offsets        domain.OffsetStore
	onRebalance    func()

	groups map[string]*groupState // group name
}

type groupState struct {
	topic       string
	generation  int64
	members     map[string]time.Time // memberID -> last heartbeat
	assignments map[string][]int32   // memberID -> partitions
}

// NewGroupCoordinator creates a coordinator. partitionCount is used for assignments.
// onRebalance is optional; called after each rebalance (assignment recompute).
func NewGroupCoordinator(partitionCount int, heartbeatTimeoutSec int, offsets domain.OffsetStore, onRebalance func()) *GroupCoordinator {
	return &GroupCoordinator{
		partitionCount: partitionCount,
		heartbeatTTL:   time.Duration(heartbeatTimeoutSec) * time.Second,
		offsets:        offsets,
		onRebalance:    onRebalance,
		groups:         make(map[string]*groupState),
	}
}

func (c *GroupCoordinator) Join(ctx context.Context, group, topic, memberID string) (string, error) {
	_ = ctx
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.groups[group]
	if !ok {
		st = &groupState{
			topic:       topic,
			members:     make(map[string]time.Time),
			assignments: make(map[string][]int32),
		}
		c.groups[group] = st
	} else if st.topic != "" && st.topic != topic {
		return "", fmt.Errorf("group %q already joined topic %q", group, st.topic)
	} else if st.topic == "" {
		st.topic = topic
	}
	if len(st.members) >= maxGroupMembers {
		if _, exists := st.members[memberID]; !exists {
			return "", fmt.Errorf("group %q is full (max %d members)", group, maxGroupMembers)
		}
	}
	st.members[memberID] = time.Now()
	st.generation++
	c.rebalanceLocked(st)
	return strconv.FormatInt(st.generation, 10), nil
}

func (c *GroupCoordinator) Heartbeat(ctx context.Context, group, memberID, generationID string) (rebalanceNeeded bool, currentGeneration string, err error) {
	_ = ctx
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.groups[group]
	if !ok {
		return false, "", fmt.Errorf("unknown group %q", group)
	}
	if _, ok := st.members[memberID]; !ok {
		return false, "", fmt.Errorf("unknown member %q", memberID)
	}
	st.members[memberID] = time.Now()
	c.evictStaleLocked(st)
	genStr := strconv.FormatInt(st.generation, 10)
	rebalanceNeeded = generationID != genStr
	return rebalanceNeeded, genStr, nil
}

func (c *GroupCoordinator) Assignment(ctx context.Context, group, memberID, generationID string) ([]domain.PartitionAssignment, error) {
	_ = ctx
	c.mu.Lock()
	st, ok := c.groups[group]
	if !ok {
		c.mu.Unlock()
		return nil, fmt.Errorf("unknown group %q", group)
	}
	genStr := strconv.FormatInt(st.generation, 10)
	if generationID != genStr {
		c.mu.Unlock()
		return nil, fmt.Errorf("stale generation: have %s want %s", generationID, genStr)
	}
	parts, ok := st.assignments[memberID]
	topic := st.topic
	c.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no assignment for member %q", memberID)
	}
	out := make([]domain.PartitionAssignment, 0, len(parts))
	for _, p := range parts {
		off := c.offsets.Get(ctx, group, topic, p)
		out = append(out, domain.PartitionAssignment{Partition: p, StartOffset: off})
	}
	return out, nil
}

func (c *GroupCoordinator) Leave(ctx context.Context, group, memberID string) error {
	_ = ctx
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.groups[group]
	if !ok {
		return nil
	}
	delete(st.members, memberID)
	delete(st.assignments, memberID)
	if len(st.members) == 0 {
		delete(c.groups, group)
		return nil
	}
	st.generation++
	c.rebalanceLocked(st)
	return nil
}

func (c *GroupCoordinator) CurrentGeneration(group string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.groups[group]
	if !ok {
		return ""
	}
	return strconv.FormatInt(st.generation, 10)
}

func (c *GroupCoordinator) evictStaleLocked(st *groupState) {
	cutoff := time.Now().Add(-c.heartbeatTTL)
	changed := false
	for id, t := range st.members {
		if t.Before(cutoff) {
			delete(st.members, id)
			delete(st.assignments, id)
			changed = true
		}
	}
	if changed {
		st.generation++
		c.rebalanceLocked(st)
	}
}

func (c *GroupCoordinator) rebalanceLocked(st *groupState) {
	ids := make([]string, 0, len(st.members))
	for id := range st.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	st.assignments = rangeAssign(ids, c.partitionCount)
	if c.onRebalance != nil {
		c.onRebalance()
	}
}

func rangeAssign(memberIDs []string, partitionCount int) map[string][]int32 {
	out := make(map[string][]int32)
	n := len(memberIDs)
	if n == 0 {
		return out
	}
	for i, m := range memberIDs {
		start := i * partitionCount / n
		end := (i + 1) * partitionCount / n
		for p := start; p < end; p++ {
			out[m] = append(out[m], int32(p))
		}
	}
	return out
}

// Run sweeps stale members periodically.
func (c *GroupCoordinator) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.mu.Lock()
			for _, st := range c.groups {
				c.evictStaleLocked(st)
			}
			c.mu.Unlock()
		}
	}
}

var _ domain.GroupCoordinator = (*GroupCoordinator)(nil)
