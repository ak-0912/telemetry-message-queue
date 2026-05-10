package grpcsvc

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/cisco-interview/telemetry-message-queue/internal/application"
	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
	"github.com/cisco-interview/telemetry-message-queue/internal/infrastructure/coordinator"
	offsetstore "github.com/cisco-interview/telemetry-message-queue/internal/infrastructure/offset"
	"github.com/cisco-interview/telemetry-message-queue/internal/infrastructure/store"
	"github.com/cisco-interview/telemetry-message-queue/pkg/metrics"
	mqv1 "github.com/cisco-interview/telemetry-message-queue/proto/mq/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testServer(t *testing.T) *MessageQueueServer {
	t.Helper()
	dir := t.TempDir()
	off, err := offsetstore.NewFileOffsetStore(filepath.Join(dir, "offsets"))
	if err != nil {
		t.Fatal(err)
	}
	partCount := 8
	part := store.NewMemoryPartitionStore(filepath.Join(dir, "data"), partCount, time.Hour, 1000, off)
	if err := part.EnsureTopic(domain.DefaultTopic, partCount); err != nil {
		t.Fatal(err)
	}
	_, met := metrics.NewRegistry()
	coord := coordinator.NewGroupCoordinator(partCount, 30, 10, off, met.IncRebalance)
	pub := &application.PublishUsecase{Partitions: part, PartitionCount: partCount, Metrics: met}
	fetch := &application.FetchUsecase{Partitions: part, PartitionCount: partCount, Offsets: off, Metrics: met}
	commit := &application.CommitOffsetUsecase{Partitions: part, PartitionCount: partCount, Offsets: off}
	coordUC := &application.CoordinatorUsecase{Coordinator: coord}

	return &MessageQueueServer{
		Log:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		Partitions:      part,
		PartitionCount:  partCount,
		PublishUC:       pub,
		FetchUC:         fetch,
		CommitUC:        commit,
		CoordUC:         coordUC,
		FetchDefaultMax: 50,
	}
}

func TestMessageQueueServer_Publish_Fetch_Commit_flow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv := testServer(t)

	_, err := srv.Publish(ctx, &mqv1.PublishRequest{})
	if err == nil {
		t.Fatal("want validation error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal(status.Code(err))
	}

	resp, err := srv.Publish(ctx, &mqv1.PublishRequest{
		Topic: domain.DefaultTopic, Key: "GPU-1", Payload: []byte(`{"x":1}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	part := resp.GetPartition()
	off := resp.GetOffset()

	batch, err := srv.PublishBatch(ctx, &mqv1.PublishBatchReq{
		Messages: []*mqv1.PublishRequest{
			{Topic: domain.DefaultTopic, Key: "GPU-2", Payload: []byte(`2`)},
		},
	})
	if err != nil || len(batch.GetResults()) != 1 {
		t.Fatal(err)
	}

	fetchResp, err := srv.Fetch(ctx, &mqv1.FetchRequest{
		Group: "g", Topic: domain.DefaultTopic, Partition: part, Offset: off, Max: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fetchResp.GetMessages()) != 1 {
		t.Fatal(len(fetchResp.GetMessages()))
	}

	_, err = srv.CommitOffset(ctx, &mqv1.CommitRequest{})
	if err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	_, err = srv.CommitOffset(ctx, &mqv1.CommitRequest{
		Group: "g", Topic: domain.DefaultTopic, Partition: part, Offset: off + 1,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMessageQueueServer_consumerGroup_RPCs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv := testServer(t)

	_, err := srv.JoinGroup(ctx, &mqv1.JoinRequest{})
	if err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}

	j, err := srv.JoinGroup(ctx, &mqv1.JoinRequest{
		Group: "telemetry-collector", Topic: domain.DefaultTopic, MemberId: "m1",
	})
	if err != nil {
		t.Fatal(err)
	}
	gen := j.GetGenerationId()

	_, err = srv.Heartbeat(ctx, &mqv1.HeartbeatRequest{})
	if err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	hb, err := srv.Heartbeat(ctx, &mqv1.HeartbeatRequest{
		Group: "telemetry-collector", MemberId: "m1", GenerationId: gen,
	})
	if err != nil || hb.GetRebalanceNeeded() {
		t.Fatal(err)
	}

	as, err := srv.GetAssignment(ctx, &mqv1.AssignRequest{
		Group: "telemetry-collector", MemberId: "m1", GenerationId: gen,
	})
	if err != nil || len(as.GetAssignments()) == 0 {
		t.Fatal(err)
	}

	_, err = srv.GetAssignment(ctx, &mqv1.AssignRequest{
		Group: "telemetry-collector", MemberId: "m1", GenerationId: "0",
	})
	if err == nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}

	_, err = srv.LeaveGroup(ctx, &mqv1.LeaveRequest{})
	if err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	_, err = srv.LeaveGroup(ctx, &mqv1.LeaveRequest{
		Group: "telemetry-collector", MemberId: "m1",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMessageQueueServer_commitOffset_regression(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv := testServer(t)
	resp, err := srv.Publish(ctx, &mqv1.PublishRequest{
		Topic: domain.DefaultTopic, Key: "k", Payload: []byte("x"),
	})
	if err != nil {
		t.Fatal(err)
	}
	part := resp.GetPartition()
	_, err = srv.CommitOffset(ctx, &mqv1.CommitRequest{
		Group: "g", Topic: domain.DefaultTopic, Partition: part, Offset: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = srv.CommitOffset(ctx, &mqv1.CommitRequest{
		Group: "g", Topic: domain.DefaultTopic, Partition: part, Offset: 2,
	})
	if err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("%v", err)
	}
}

func TestMessageQueueServer_Fetch_defaultMax(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv := testServer(t)
	srv.FetchDefaultMax = 0
	_, _ = srv.Publish(ctx, &mqv1.PublishRequest{
		Topic: domain.DefaultTopic, Key: "k", Payload: []byte("x"),
	})
	_, err := srv.Fetch(ctx, &mqv1.FetchRequest{
		Group: "g", Topic: domain.DefaultTopic, Partition: 0, Offset: 0, Max: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
}
