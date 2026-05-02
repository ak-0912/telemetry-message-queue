package offset

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileOffsetStore_commitGet_roundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	s, err := NewFileOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if v := s.Get(ctx, "g1", "topic-a", 3); v != 0 {
		t.Fatalf("initial %d", v)
	}
	if err := s.Commit(ctx, "g1", "topic-a", 3, 42); err != nil {
		t.Fatal(err)
	}
	if v := s.Get(ctx, "g1", "topic-a", 3); v != 42 {
		t.Fatalf("get %d want 42", v)
	}

	path := filepath.Join(dir, "g1.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}

	s2, err := NewFileOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if v := s2.Get(ctx, "g1", "topic-a", 3); v != 42 {
		t.Fatalf("reload get %d want 42", v)
	}
}

func TestFileOffsetStore_commitMonotonic_regression(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := NewFileOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "g", "t", 0, 10); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "g", "t", 0, 10); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "g", "t", 0, 11); err != nil {
		t.Fatal(err)
	}
	err = s.Commit(ctx, "g", "t", 0, 5)
	if err == nil {
		t.Fatal("want regression error")
	}
	if !errors.Is(err, ErrOffsetRegression) {
		t.Fatalf("want ErrOffsetRegression: %v", err)
	}
}

func TestFileOffsetStore_minCommittedNextOffset(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := NewFileOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Commit(ctx, "ga", "topic-x", 0, 100)
	_ = s.Commit(ctx, "gb", "topic-x", 0, 50)
	if m := s.MinCommittedNextOffset("topic-x", 0); m != 50 {
		t.Fatalf("min %d want 50", m)
	}
}

func TestFileOffsetStore_minCommittedNextOffset_ignoresGroupsWithoutTopic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := NewFileOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "ga", "topic-x", 0, 100); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "gb", "other-topic", 0, 1); err != nil {
		t.Fatal(err)
	}
	if m := s.MinCommittedNextOffset("topic-x", 0); m != 100 {
		t.Fatalf("min %d want 100 (gb must not contribute 0 for unrelated topic)", m)
	}
}

func TestFileOffsetStore_minCommittedNextOffset_noRegisteredGroupReturnsZero(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := NewFileOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "gb", "other-topic", 0, 99); err != nil {
		t.Fatal(err)
	}
	if m := s.MinCommittedNextOffset("topic-x", 0); m != 0 {
		t.Fatalf("min %d want 0 when no group tracks topic-x", m)
	}
}

func TestFileOffsetStore_ensureConsumerTopic_includesGroupInMinBeforeCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := NewFileOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "ga", "topic-x", 0, 100); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureConsumerTopic(ctx, "gb", "topic-x"); err != nil {
		t.Fatal(err)
	}
	if m := s.MinCommittedNextOffset("topic-x", 0); m != 0 {
		t.Fatalf("min %d want 0 (gb registered at start offset)", m)
	}
}
