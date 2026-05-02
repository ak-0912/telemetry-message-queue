package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	offsetstore "github.com/cisco-interview/telemetry-message-queue/internal/infrastructure/offset"
)

func TestMemoryPartitionStore_appendFetch_ordering(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryPartitionStore(t.TempDir(), 4, time.Hour, 1000, nil)
	if err := s.EnsureTopic("gpu-telemetry", 4); err != nil {
		t.Fatal(err)
	}

	off0, err := s.Append(ctx, "gpu-telemetry", 1, "key-a", []byte("m0"))
	if err != nil {
		t.Fatal(err)
	}
	off1, err := s.Append(ctx, "gpu-telemetry", 1, "key-a", []byte("m1"))
	if err != nil {
		t.Fatal(err)
	}
	if off0 != 0 || off1 != 1 {
		t.Fatalf("offsets %d,%d want 0,1", off0, off1)
	}

	msgs, err := s.Fetch(ctx, "gpu-telemetry", 1, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("len %d", len(msgs))
	}
	if string(msgs[0].Payload) != "m0" || string(msgs[1].Payload) != "m1" {
		t.Fatalf("payloads %q %q", msgs[0].Payload, msgs[1].Payload)
	}

	msgs2, err := s.Fetch(ctx, "gpu-telemetry", 1, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs2) != 1 || msgs2[0].Offset != 1 {
		t.Fatalf("fetch from 1: %+v", msgs2)
	}

	if hw := s.HighWatermark(ctx, "gpu-telemetry", 1); hw != 2 {
		t.Fatalf("hw %d want 2", hw)
	}
}

func TestMemoryPartitionStore_unknownTopic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryPartitionStore(t.TempDir(), 2, time.Hour, 1000, nil)
	_, err := s.Fetch(ctx, "missing", 0, 0, 10)
	if err == nil {
		t.Fatal("want error")
	}
	if !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("want ErrUnknownTopic, got %v", err)
	}
}

func TestMemoryPartitionStore_invalidPartition(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryPartitionStore(t.TempDir(), 2, time.Hour, 1000, nil)
	if err := s.EnsureTopic("t", 2); err != nil {
		t.Fatal(err)
	}
	_, err := s.Fetch(ctx, "t", 9, 0, 10)
	if err == nil {
		t.Fatal("want error")
	}
	if !errors.Is(err, ErrInvalidPartition) {
		t.Fatalf("want ErrInvalidPartition, got %v", err)
	}
}

func TestMemoryPartitionStore_walReplay_secondInstance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	s1 := NewMemoryPartitionStore(dir, 2, time.Hour, 1000, nil)
	if err := s1.EnsureTopic("t", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Append(ctx, "t", 0, "k", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Append(ctx, "t", 0, "k", []byte("two")); err != nil {
		t.Fatal(err)
	}

	s2 := NewMemoryPartitionStore(dir, 2, time.Hour, 1000, nil)
	if err := s2.EnsureTopic("t", 2); err != nil {
		t.Fatal(err)
	}
	msgs, err := s2.Fetch(ctx, "t", 0, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("replayed len %d", len(msgs))
	}
	if string(msgs[0].Payload) != "one" || string(msgs[1].Payload) != "two" {
		t.Fatalf("payloads %q %q", msgs[0].Payload, msgs[1].Payload)
	}
	if hw := s2.HighWatermark(ctx, "t", 0); hw != 2 {
		t.Fatalf("hw after replay %d", hw)
	}
}

func TestMemoryPartitionStore_fetchMax_respected(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryPartitionStore(t.TempDir(), 1, time.Hour, 1000, nil)
	if err := s.EnsureTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := s.Append(ctx, "t", 0, "k", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	msgs, err := s.Fetch(ctx, "t", 0, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("len %d want 2", len(msgs))
	}
}

func TestMemoryPartitionStore_retentionGC_maxSize_doesNotAgeTrimPastCommitWatermark(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	off, err := offsetstore.NewFileOffsetStore(filepath.Join(dir, "offsets"))
	if err != nil {
		t.Fatal(err)
	}
	if err := off.EnsureConsumerTopic(ctx, "ga", "t"); err != nil {
		t.Fatal(err)
	}
	if err := off.Commit(ctx, "gb", "other", 0, 100); err != nil {
		t.Fatal(err)
	}
	s := NewMemoryPartitionStore(filepath.Join(dir, "data"), 1, 10*time.Millisecond, 1, off)
	if err := s.EnsureTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, "t", 0, "k", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, "t", 0, "k", []byte("b")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	s.RunRetentionGC(ctx)
	if n := s.MessageCount("t", 0); n != 2 {
		t.Fatalf("uncommitted messages must be retained under max-size pressure; count=%d", n)
	}
}

func TestMemoryPartitionStore_retentionGC_trimsOldMessages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryPartitionStore(t.TempDir(), 1, 10*time.Millisecond, 1000, nil)
	if err := s.EnsureTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, "t", 0, "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if s.MessageCount("t", 0) != 1 {
		t.Fatal()
	}
	time.Sleep(25 * time.Millisecond)
	s.RunRetentionGC(ctx)
	if s.MessageCount("t", 0) != 0 {
		t.Fatalf("expected GC, count=%d", s.MessageCount("t", 0))
	}
}

func TestMemoryPartitionStore_oldestOffset(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryPartitionStore(t.TempDir(), 1, time.Hour, 1000, nil)
	if err := s.EnsureTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	if o := s.OldestOffset("t", 0); o != 0 {
		t.Fatalf("empty log want next offset 0, got %d", o)
	}
	if _, err := s.Append(ctx, "t", 0, "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if o := s.OldestOffset("t", 0); o != 0 {
		t.Fatalf("want first msg offset 0, got %d", o)
	}
}

func TestMemoryPartitionStore_highWatermark_unknownTopic(t *testing.T) {
	t.Parallel()
	s := NewMemoryPartitionStore(t.TempDir(), 2, time.Hour, 1000, nil)
	if n := s.HighWatermark(context.Background(), "nope", 0); n != 0 {
		t.Fatalf("got %d", n)
	}
}
