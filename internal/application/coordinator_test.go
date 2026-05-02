package application

import (
	"context"
	"testing"

	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
)

type stubCoordinator struct {
	joinGen   string
	joinErr   error
	hbNeed    bool
	hbErr     error
	assign    []domain.PartitionAssignment
	assignErr error
	leaveErr  error
}

func (s *stubCoordinator) Join(ctx context.Context, group, topic, memberID string) (string, error) {
	_ = ctx
	_ = group
	_ = topic
	_ = memberID
	return s.joinGen, s.joinErr
}

func (s *stubCoordinator) Heartbeat(ctx context.Context, group, memberID, generationID string) (bool, string, error) {
	_ = ctx
	_ = group
	_ = memberID
	_ = generationID
	return s.hbNeed, "9", s.hbErr
}

func (s *stubCoordinator) Assignment(ctx context.Context, group, memberID, generationID string) ([]domain.PartitionAssignment, error) {
	_ = ctx
	_ = group
	_ = memberID
	_ = generationID
	return s.assign, s.assignErr
}

func (s *stubCoordinator) Leave(ctx context.Context, group, memberID string) error {
	_ = ctx
	_ = group
	_ = memberID
	return s.leaveErr
}

func (s *stubCoordinator) CurrentGeneration(group string) string {
	_ = group
	return ""
}

func TestCoordinatorUsecase_delegates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := &stubCoordinator{
		joinGen: "1",
		hbNeed:  true,
		assign:  []domain.PartitionAssignment{{Partition: 2, StartOffset: 3}},
	}
	uc := &CoordinatorUsecase{Coordinator: st}

	g, err := uc.Join(ctx, "g", "topic", "m")
	if err != nil || g != "1" {
		t.Fatalf("%v %q", err, g)
	}
	need, err := uc.Heartbeat(ctx, "g", "m", "0")
	if err != nil || !need {
		t.Fatalf("%v %v", err, need)
	}
	a, err := uc.Assignment(ctx, "g", "m", "1")
	if err != nil || len(a) != 1 || a[0].Partition != 2 {
		t.Fatalf("%v %+v", err, a)
	}
	if err := uc.Leave(ctx, "g", "m"); err != nil {
		t.Fatal(err)
	}
}

var _ domain.GroupCoordinator = (*stubCoordinator)(nil)
