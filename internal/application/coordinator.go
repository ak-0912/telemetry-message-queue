package application

import (
	"context"

	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
)

type CoordinatorUsecase struct {
	Coordinator domain.GroupCoordinator
}

func (u *CoordinatorUsecase) Join(ctx context.Context, group, topic, memberID string) (generationID string, err error) {
	return u.Coordinator.Join(ctx, group, topic, memberID)
}

func (u *CoordinatorUsecase) Heartbeat(ctx context.Context, group, memberID, generationID string) (rebalanceNeeded bool, err error) {
	need, _, err := u.Coordinator.Heartbeat(ctx, group, memberID, generationID)
	return need, err
}

func (u *CoordinatorUsecase) Assignment(ctx context.Context, group, memberID, generationID string) ([]domain.PartitionAssignment, error) {
	return u.Coordinator.Assignment(ctx, group, memberID, generationID)
}

func (u *CoordinatorUsecase) Leave(ctx context.Context, group, memberID string) error {
	return u.Coordinator.Leave(ctx, group, memberID)
}
