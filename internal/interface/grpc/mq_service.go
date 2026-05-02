package grpcsvc

import (
	"context"
	"errors"
	"log/slog"

	"github.com/cisco-interview/telemetry-message-queue/internal/application"
	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
	offsetstore "github.com/cisco-interview/telemetry-message-queue/internal/infrastructure/offset"
	"github.com/cisco-interview/telemetry-message-queue/internal/infrastructure/store"
	mqv1 "github.com/cisco-interview/telemetry-message-queue/proto/mq/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MessageQueueServer implements mqv1.MessageQueueServiceServer.
type MessageQueueServer struct {
	mqv1.UnimplementedMessageQueueServiceServer
	Log *slog.Logger

	Partitions     domain.PartitionStore
	PartitionCount int

	PublishUC *application.PublishUsecase
	FetchUC   *application.FetchUsecase
	CommitUC  *application.CommitOffsetUsecase
	CoordUC   *application.CoordinatorUsecase

	FetchDefaultMax int32
}

func (s *MessageQueueServer) Publish(ctx context.Context, req *mqv1.PublishRequest) (*mqv1.PublishResponse, error) {
	if req.GetTopic() == "" || req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "topic and key required")
	}
	part, off, err := s.PublishUC.Publish(ctx, req.GetTopic(), req.GetKey(), req.GetPayload())
	if err != nil {
		s.Log.Error("publish failed", "component", "grpc", "topic", req.GetTopic(), "err", err)
		return nil, mapRPCError(err)
	}
	return &mqv1.PublishResponse{Partition: part, Offset: off}, nil
}

func (s *MessageQueueServer) PublishBatch(ctx context.Context, req *mqv1.PublishBatchReq) (*mqv1.PublishBatchResp, error) {
	out := &mqv1.PublishBatchResp{}
	for _, m := range req.GetMessages() {
		if m.GetTopic() == "" || m.GetKey() == "" {
			return nil, status.Error(codes.InvalidArgument, "topic and key required for each message")
		}
		part, off, err := s.PublishUC.Publish(ctx, m.GetTopic(), m.GetKey(), m.GetPayload())
		if err != nil {
			return nil, mapRPCError(err)
		}
		out.Results = append(out.Results, &mqv1.PublishResponse{Partition: part, Offset: off})
	}
	return out, nil
}

func (s *MessageQueueServer) Fetch(ctx context.Context, req *mqv1.FetchRequest) (*mqv1.FetchResponse, error) {
	if req.GetGroup() == "" || req.GetTopic() == "" {
		return nil, status.Error(codes.InvalidArgument, "group and topic required")
	}
	max := req.GetMax()
	if max <= 0 {
		max = s.FetchDefaultMax
		if max <= 0 {
			max = application.DefaultFetchMax
		}
	}
	msgs, err := s.FetchUC.Fetch(ctx, req.GetGroup(), req.GetTopic(), req.GetPartition(), req.GetOffset(), max)
	if err != nil {
		return nil, mapRPCError(err)
	}
	resp := &mqv1.FetchResponse{Messages: make([]*mqv1.Message, 0, len(msgs))}
	for _, m := range msgs {
		resp.Messages = append(resp.Messages, domainMessageToProto(m))
	}
	return resp, nil
}

func (s *MessageQueueServer) CommitOffset(ctx context.Context, req *mqv1.CommitRequest) (*mqv1.CommitResponse, error) {
	if req.GetGroup() == "" || req.GetTopic() == "" {
		return nil, status.Error(codes.InvalidArgument, "group and topic required")
	}
	if err := s.CommitUC.Commit(ctx, req.GetGroup(), req.GetTopic(), req.GetPartition(), req.GetOffset()); err != nil {
		return nil, mapRPCError(err)
	}
	return &mqv1.CommitResponse{Ok: true}, nil
}

func (s *MessageQueueServer) JoinGroup(ctx context.Context, req *mqv1.JoinRequest) (*mqv1.JoinResponse, error) {
	if req.GetGroup() == "" || req.GetTopic() == "" || req.GetMemberId() == "" {
		return nil, status.Error(codes.InvalidArgument, "group, topic, member_id required")
	}
	if s.Partitions != nil {
		if err := s.Partitions.EnsureTopic(req.GetTopic(), s.PartitionCount); err != nil {
			return nil, mapRPCError(err)
		}
	}
	gen, err := s.CoordUC.Join(ctx, req.GetGroup(), req.GetTopic(), req.GetMemberId())
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if s.CommitUC != nil {
		if err := s.CommitUC.EnsureConsumerTopic(ctx, req.GetGroup(), req.GetTopic()); err != nil {
			return nil, mapRPCError(err)
		}
	}
	s.Log.Info("consumer joined", "component", "grpc", "group", req.GetGroup(), "member_id", req.GetMemberId(), "generation", gen)
	return &mqv1.JoinResponse{GenerationId: gen}, nil
}

func (s *MessageQueueServer) Heartbeat(ctx context.Context, req *mqv1.HeartbeatRequest) (*mqv1.HeartbeatResponse, error) {
	if req.GetGroup() == "" || req.GetMemberId() == "" || req.GetGenerationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "group, member_id, generation_id required")
	}
	need, err := s.CoordUC.Heartbeat(ctx, req.GetGroup(), req.GetMemberId(), req.GetGenerationId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &mqv1.HeartbeatResponse{RebalanceNeeded: need}, nil
}

func (s *MessageQueueServer) GetAssignment(ctx context.Context, req *mqv1.AssignRequest) (*mqv1.AssignResponse, error) {
	if req.GetGroup() == "" || req.GetMemberId() == "" || req.GetGenerationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "group, member_id, generation_id required")
	}
	assigns, err := s.CoordUC.Assignment(ctx, req.GetGroup(), req.GetMemberId(), req.GetGenerationId())
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	out := &mqv1.AssignResponse{}
	for _, a := range assigns {
		out.Assignments = append(out.Assignments, &mqv1.PartitionAssignment{
			Partition:   a.Partition,
			StartOffset: a.StartOffset,
		})
	}
	return out, nil
}

func (s *MessageQueueServer) LeaveGroup(ctx context.Context, req *mqv1.LeaveRequest) (*mqv1.LeaveResponse, error) {
	if req.GetGroup() == "" || req.GetMemberId() == "" {
		return nil, status.Error(codes.InvalidArgument, "group and member_id required")
	}
	if err := s.CoordUC.Leave(ctx, req.GetGroup(), req.GetMemberId()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &mqv1.LeaveResponse{Ok: true}, nil
}

func domainMessageToProto(m domain.Message) *mqv1.Message {
	return &mqv1.Message{
		Partition:   m.Partition,
		Offset:      m.Offset,
		Key:         m.Key,
		Payload:     m.Payload,
		PublishedAt: m.PublishedAt.UnixNano(),
	}
}

var _ mqv1.MessageQueueServiceServer = (*MessageQueueServer)(nil)

func mapRPCError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrUnknownTopic):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrInvalidPartition):
		return status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, offsetstore.ErrOffsetRegression):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
