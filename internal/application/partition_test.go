package application

import (
	"hash/fnv"
	"testing"
)

func TestPartitionFor_deterministic(t *testing.T) {
	key := "GPU-5fd4f087-86f3-7a43-b711-4771313afc50"
	count := 256
	p1 := PartitionFor(key, count)
	p2 := PartitionFor(key, count)
	if p1 != p2 {
		t.Fatalf("expected stable partition, got %d vs %d", p1, p2)
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	want := int(h.Sum32() % uint32(count))
	if p1 != want {
		t.Fatalf("partition %d want %d", p1, want)
	}
	if p1 < 0 || p1 >= count {
		t.Fatalf("out of range: %d", p1)
	}
}

func TestPartitionFor_differentKeys(t *testing.T) {
	count := 256
	a := PartitionFor("GPU-aaaa", count)
	b := PartitionFor("GPU-bbbb", count)
	// Extremely unlikely collision for distinct keys; just ensure function runs
	if a == b {
		t.Logf("collision a=b=%d (unlikely)", a)
	}
}
