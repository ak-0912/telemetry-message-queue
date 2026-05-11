// Package coordinator implements consumer group membership, range-based
// partition assignment, and heartbeat-driven liveness detection.
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

const defaultMaxGroupMembers = 10

// GroupCoordinator assigns partitions to consumer group members using a
// contiguous-range strategy (similar to Kafka's RangeAssignor). Membership
// is tracked via heartbeats; stale members are evicted after heartbeatTTL.
type GroupCoordinator struct {
	mu sync.Mutex

	partitionCount  int
	heartbeatTTL    time.Duration
	maxGroupMembers int
	offsets         domain.OffsetStore
	onRebalance     func()

	groups map[string]*groupState // group name
}

type groupState struct {
	topic       string
	generation  int64
	members     map[string]time.Time // memberID -> last heartbeat
	assignments map[string][]int32   // memberID -> partitions
}

// NewGroupCoordinator creates a coordinator with the given partition count.
// heartbeatTimeoutSec controls how long a member can go without a heartbeat
// before being evicted. onRebalance, if non-nil, is called after every
// assignment recompute (useful for incrementing a metrics counter).
func NewGroupCoordinator(partitionCount int, heartbeatTimeoutSec int, maxGroupMembers int, offsets domain.OffsetStore, onRebalance func()) *GroupCoordinator {
	if maxGroupMembers <= 0 {
		maxGroupMembers = defaultMaxGroupMembers
	}
	return &GroupCoordinator{
		partitionCount:  partitionCount,
		heartbeatTTL:    time.Duration(heartbeatTimeoutSec) * time.Second,
		maxGroupMembers: maxGroupMembers,
		offsets:         offsets,
		onRebalance:     onRebalance,
		groups:          make(map[string]*groupState),
	}
}

// Join adds a member to the group. If the member is already present (a rejoin),
// only the heartbeat timestamp is refreshed — the generation is not bumped and
// no rebalance occurs. This prevents a rejoin storm where two members
// alternately trigger rebalances in a tight loop.
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
	_, alreadyMember := st.members[memberID]
	if len(st.members) >= c.maxGroupMembers && !alreadyMember {
		return "", fmt.Errorf("group %q is full (max %d members)", group, c.maxGroupMembers)
	}
	st.members[memberID] = time.Now()
	if !alreadyMember {
		st.generation++
		c.rebalanceLocked(st)
	}
	return strconv.FormatInt(st.generation, 10), nil
}

// Heartbeat refreshes the member's liveness timestamp and evicts any stale
// members whose last heartbeat exceeds heartbeatTTL. If eviction changes the
// generation, rebalanceNeeded is true for all members whose cached generation
// no longer matches.
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

// Assignment returns the partitions assigned to a member for the given
// generation. The coordinator lock is released before reading committed
// offsets from the OffsetStore so that slow I/O does not block other RPCs.
func (c *GroupCoordinator) Assignment(ctx context.Context, group, memberID, generationID string) ([]domain.PartitionAssignment, error) {
	_ = ctx

	// Hold the lock only long enough to snapshot the assignment slice and topic.
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

// Leave removes a member from the group. If other members remain, a new
// generation is created and partitions are reassigned.
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

// evictStaleLocked removes members whose last heartbeat is older than
// heartbeatTTL and triggers a rebalance if membership changed.
// Must be called with c.mu held.
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

// rebalanceLocked recomputes partition assignments using a contiguous-range
// strategy. Must be called with c.mu held.
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

// rangeAssign distributes partitions [0, partitionCount) across sorted member
// IDs in contiguous slices. Each member gets floor or ceil(partitionCount/n).
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
