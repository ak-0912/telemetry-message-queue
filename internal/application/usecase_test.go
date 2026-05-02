package application

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
	"github.com/cisco-interview/telemetry-message-queue/internal/infrastructure/offset"
	"github.com/cisco-interview/telemetry-message-queue/internal/infrastructure/store"
	"github.com/cisco-interview/telemetry-message-queue/pkg/metrics"
)

func TestPublishUsecase_sameKey_samePartition(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	partCount := 256
	s := store.NewMemoryPartitionStore(t.TempDir(), partCount, time.Hour, 1000, nil)
	uc := &PublishUsecase{Partitions: s, PartitionCount: partCount, Metrics: nil}
	key := "GPU-5fd4f087-86f3-7a43-b711-4771313afc50"

	p0, off0, err := uc.Publish(ctx, "gpu-telemetry", key, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	p1, off1, err := uc.Publish(ctx, "gpu-telemetry", key, []byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	if p0 != p1 {
		t.Fatalf("partition drift %d vs %d", p0, p1)
	}
	if off0 != 0 || off1 != 1 {
		t.Fatalf("offsets %d,%d want 0,1", off0, off1)
	}
	wantPart := int32(PartitionFor(key, partCount))
	if p0 != wantPart {
		t.Fatalf("partition %d want %d", p0, wantPart)
	}
}

func TestFetchUsecase_fetchAfterCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	offDir := t.TempDir()
	off, err := offset.NewFileOffsetStore(offDir)
	if err != nil {
		t.Fatal(err)
	}
	partCount := 32
	storeDir := t.TempDir()
	s := store.NewMemoryPartitionStore(storeDir, partCount, time.Hour, 1000, off)
	pub := &PublishUsecase{Partitions: s, PartitionCount: partCount, Metrics: nil}
	fetch := &FetchUsecase{Partitions: s, PartitionCount: partCount, Offsets: off, Metrics: nil}

	topic := "gpu-telemetry"
	group := "telemetry-collector"
	part := int32(3)
	var key string
	for i := 0; i < 100000; i++ {
		k := "k-" + strconv.Itoa(i)
		if PartitionFor(k, partCount) == int(part) {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("could not find key for partition 3")
	}

	if _, _, err := pub.Publish(ctx, topic, key, []byte("m0")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := pub.Publish(ctx, topic, key, []byte("m1")); err != nil {
		t.Fatal(err)
	}

	msgs, err := fetch.Fetch(ctx, group, topic, part, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("want 2 msgs, got %d", len(msgs))
	}
	if string(msgs[0].Payload) != "m0" || string(msgs[1].Payload) != "m1" {
		t.Fatalf("payloads %q %q", msgs[0].Payload, msgs[1].Payload)
	}

	commit := &CommitOffsetUsecase{Partitions: s, PartitionCount: partCount, Offsets: off}
	next := msgs[1].Offset + 1
	if err := commit.Commit(ctx, group, topic, part, next); err != nil {
		t.Fatal(err)
	}
	msgs2, err := fetch.Fetch(ctx, group, topic, part, next, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs2) != 0 {
		t.Fatalf("expected empty after commit, got %d", len(msgs2))
	}
}

func TestPublishUsecase_metricsOnEnsureTopicFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, m := metrics.NewRegistry()
	uc := &PublishUsecase{Partitions: brokenPartitionStore{}, PartitionCount: 4, Metrics: m}
	_, _, err := uc.Publish(ctx, "t", "k", []byte("x"))
	if !errors.Is(err, errBoom) {
		t.Fatal(err)
	}
}

func TestPublishUsecase_withMetrics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, m := metrics.NewRegistry()
	partCount := 16
	s := store.NewMemoryPartitionStore(t.TempDir(), partCount, time.Hour, 1000, nil)
	uc := &PublishUsecase{Partitions: s, PartitionCount: partCount, Metrics: m}
	if _, _, err := uc.Publish(ctx, "gpu-telemetry", "key", []byte("z")); err != nil {
		t.Fatal(err)
	}
}

func TestFetchUsecase_withMetrics_andMaxDefault(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	offDir := t.TempDir()
	off, err := offset.NewFileOffsetStore(offDir)
	if err != nil {
		t.Fatal(err)
	}
	partCount := 4
	part := int32(0)
	var key string
	for i := 0; i < 1000; i++ {
		k := "k-" + strconv.Itoa(i)
		if PartitionFor(k, partCount) == int(part) {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no key for partition 0")
	}
	s := store.NewMemoryPartitionStore(t.TempDir(), partCount, time.Hour, 1000, off)
	_, m := metrics.NewRegistry()
	pub := &PublishUsecase{Partitions: s, PartitionCount: partCount, Metrics: m}
	fetch := &FetchUsecase{Partitions: s, PartitionCount: partCount, Offsets: off, Metrics: m}
	if _, _, err := pub.Publish(ctx, "t", key, []byte("a")); err != nil {
		t.Fatal(err)
	}
	// max <= 0 triggers DefaultFetchMax inside FetchUsecase
	msgs, err := fetch.Fetch(ctx, "g", "t", part, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatal(len(msgs))
	}
}

func TestPublishUsecase_ensureTopicError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	uc := &PublishUsecase{
		Partitions:     brokenPartitionStore{},
		PartitionCount: 4,
		Metrics:        nil,
	}
	_, _, err := uc.Publish(ctx, "t", "k", []byte("x"))
	if !errors.Is(err, errBoom) {
		t.Fatalf("%v", err)
	}
}

type brokenPartitionStore struct{}

func (brokenPartitionStore) EnsureTopic(string, int) error { return errBoom }
func (brokenPartitionStore) Append(context.Context, string, int32, string, []byte) (int64, error) {
	return 0, errBoom
}
func (brokenPartitionStore) Fetch(context.Context, string, int32, int64, int32) ([]domain.Message, error) {
	return nil, errBoom
}
func (brokenPartitionStore) HighWatermark(context.Context, string, int32) int64 { return 0 }
func (brokenPartitionStore) PartitionCount(string) int                          { return 0 }
func (brokenPartitionStore) OldestOffset(string, int32) int64                   { return 0 }
func (brokenPartitionStore) RunRetentionGC(context.Context)                     {}

var errBoom = errors.New("boom")

func TestCommitUsecase_ensureTopicError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	uc := &CommitOffsetUsecase{
		Partitions:     brokenPartitionStore{},
		PartitionCount: 4,
		Offsets:        noopOffset{},
	}
	err := uc.Commit(ctx, "g", "t", 0, 1)
	if !errors.Is(err, errBoom) {
		t.Fatalf("%v", err)
	}
}

type noopOffset struct{}

func (noopOffset) Get(context.Context, string, string, int32) int64 { return 0 }
func (noopOffset) Commit(context.Context, string, string, int32, int64) error {
	return nil
}
func (noopOffset) EnsureConsumerTopic(context.Context, string, string) error { return nil }
func (noopOffset) MinCommittedNextOffset(string, int32) int64 { return 0 }

func TestFetchUsecase_fetchError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	off, err := offset.NewFileOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := store.NewMemoryPartitionStore(t.TempDir(), 2, time.Hour, 1000, off)
	fetch := &FetchUsecase{Partitions: s, PartitionCount: 2, Offsets: off, Metrics: nil}
	if err := s.EnsureTopic("t", 2); err != nil {
		t.Fatal(err)
	}
	_, err = fetch.Fetch(ctx, "g", "t", 9, 0, 10)
	if err == nil {
		t.Fatal("want invalid partition error")
	}
}
