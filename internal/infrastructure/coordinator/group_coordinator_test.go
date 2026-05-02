package coordinator

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	offsetstore "github.com/cisco-interview/telemetry-message-queue/internal/infrastructure/offset"
)

func TestRangeAssign_coversAllPartitions_noOverlap(t *testing.T) {
	t.Parallel()
	partitions := 256
	members := []string{"m0", "m1", "m2"}
	out := rangeAssign(members, partitions)
	seen := make(map[int32]struct{})
	for _, m := range members {
		for _, p := range out[m] {
			if _, dup := seen[p]; dup {
				t.Fatalf("partition %d assigned twice", p)
			}
			seen[p] = struct{}{}
		}
		s := append([]int32(nil), out[m]...)
		slices.Sort(s)
		for i := 1; i < len(s); i++ {
			if s[i] != s[i-1]+1 {
				t.Fatalf("member %s partitions not contiguous: %v", m, s)
			}
		}
	}
	if len(seen) != partitions {
		t.Fatalf("covered %d want %d", len(seen), partitions)
	}
}

func TestGroupCoordinator_twoMembers_rangeSplit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	off, err := offsetstore.NewFileOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	c := NewGroupCoordinator(6, 30, off, nil)

	gen1, err := c.Join(ctx, "g", "topic", "b")
	if err != nil {
		t.Fatal(err)
	}
	if gen1 != "1" {
		t.Fatalf("gen1 %s", gen1)
	}
	gen2, err := c.Join(ctx, "g", "topic", "a")
	if err != nil {
		t.Fatal(err)
	}
	if gen2 != "2" {
		t.Fatalf("gen2 %s", gen2)
	}

	aAssign, err := c.Assignment(ctx, "g", "a", gen2)
	if err != nil {
		t.Fatal(err)
	}
	bAssign, err := c.Assignment(ctx, "g", "b", gen2)
	if err != nil {
		t.Fatal(err)
	}
	var all []int32
	for _, x := range aAssign {
		all = append(all, x.Partition)
	}
	for _, x := range bAssign {
		all = append(all, x.Partition)
	}
	slices.Sort(all)
	want := []int32{0, 1, 2, 3, 4, 5}
	if !slices.Equal(all, want) {
		t.Fatalf("partitions %v want %v", all, want)
	}
	// Sorted member order: a, b → a gets first half [0,1,2], b gets [3,4,5]
	if len(aAssign) != 3 || len(bAssign) != 3 {
		t.Fatalf("a=%d b=%d parts", len(aAssign), len(bAssign))
	}
}

func TestGroupCoordinator_heartbeatStaleGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	off, err := offsetstore.NewFileOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := NewGroupCoordinator(4, 30, off, nil)
	gen1, err := c.Join(ctx, "g", "topic", "m1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Join(ctx, "g", "topic", "m2")
	if err != nil {
		t.Fatal(err)
	}
	need, cur, err := c.Heartbeat(ctx, "g", "m1", gen1)
	if err != nil {
		t.Fatal(err)
	}
	if !need {
		t.Fatal("want rebalanceNeeded after second join")
	}
	if cur != "2" {
		t.Fatalf("current gen %s want 2", cur)
	}
}

func TestGroupCoordinator_groupFull(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	off, err := offsetstore.NewFileOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := NewGroupCoordinator(4, 30, off, nil)
	for i := range maxGroupMembers {
		_, err := c.Join(ctx, "g", "topic", "m"+strconv.Itoa(i))
		if err != nil {
			t.Fatalf("join %d: %v", i, err)
		}
	}
	_, err = c.Join(ctx, "g", "topic", "overflow")
	if err == nil {
		t.Fatal("want error when group full")
	}
}

func TestGroupCoordinator_Join_topicMismatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	off, err := offsetstore.NewFileOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := NewGroupCoordinator(4, 30, off, nil)
	if _, err := c.Join(ctx, "g", "t1", "m1"); err != nil {
		t.Fatal(err)
	}
	_, err = c.Join(ctx, "g", "t2", "m2")
	if err == nil {
		t.Fatal("want topic mismatch error")
	}
}

func TestGroupCoordinator_CurrentGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	off, err := offsetstore.NewFileOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := NewGroupCoordinator(4, 30, off, nil)
	if g := c.CurrentGeneration("missing"); g != "" {
		t.Fatalf("got %q", g)
	}
	gen, err := c.Join(ctx, "g", "t", "m")
	if err != nil {
		t.Fatal(err)
	}
	if c.CurrentGeneration("g") != gen {
		t.Fatalf("want %s got %s", gen, c.CurrentGeneration("g"))
	}
}

func TestGroupCoordinator_Heartbeat_unknownGroup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	off, err := offsetstore.NewFileOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := NewGroupCoordinator(4, 30, off, nil)
	_, _, err = c.Heartbeat(ctx, "none", "m", "1")
	if err == nil {
		t.Fatal("want error")
	}
}

func TestGroupCoordinator_Assignment_unknownGroup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	off, err := offsetstore.NewFileOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := NewGroupCoordinator(4, 30, off, nil)
	_, err = c.Assignment(ctx, "none", "m", "1")
	if err == nil {
		t.Fatal("want error")
	}
}

func TestGroupCoordinator_Run_stopsOnCancel(t *testing.T) {
	t.Parallel()
	off, err := offsetstore.NewFileOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := NewGroupCoordinator(2, 30, off, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx, 20*time.Millisecond)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestGroupCoordinator_evictStaleMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	off, err := offsetstore.NewFileOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := NewGroupCoordinator(4, 1, off, nil) // 1s heartbeat timeout
	_, err = c.Join(ctx, "g", "topic", "alive")
	if err != nil {
		t.Fatal(err)
	}
	genDead, err := c.Join(ctx, "g", "topic", "dies")
	if err != nil {
		t.Fatal(err)
	}
	// Force stale heartbeat for "dies" without waiting 1s: manipulate would need export — instead sleep.
	time.Sleep(1200 * time.Millisecond)
	need, gen, err := c.Heartbeat(ctx, "g", "alive", genDead)
	if err != nil {
		t.Fatal(err)
	}
	if !need {
		t.Fatal("want rebalance after stale eviction")
	}
	assign, err := c.Assignment(ctx, "g", "alive", gen)
	if err != nil {
		t.Fatal(err)
	}
	if len(assign) != 4 {
		t.Fatalf("alive should own all 4 partitions, got %d", len(assign))
	}
}
