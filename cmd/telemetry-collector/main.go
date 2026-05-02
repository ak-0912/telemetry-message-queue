// Command telemetry-collector joins a consumer group, consumes assigned partitions, and commits offsets (demo / E2E helper).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
	mqv1 "github.com/cisco-interview/telemetry-message-queue/proto/mq/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "message queue gRPC address")
	group := flag.String("group", "telemetry-collector", "consumer group")
	topic := flag.String("topic", domain.DefaultTopic, "topic")
	member := flag.String("member", hostname(), "member id")
	maxFetch := flag.Int("max", 200, "max messages per fetch")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	cli := mqv1.NewMessageQueueServiceClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		cancel()
	}()

	join, err := cli.JoinGroup(ctx, &mqv1.JoinRequest{Group: *group, Topic: *topic, MemberId: *member})
	if err != nil {
		log.Fatalf("JoinGroup: %v", err)
	}
	gen := join.GetGenerationId()
	log.Printf("joined group=%s member=%s generation=%s", *group, *member, gen)

	go heartbeatLoop(ctx, cli, *group, *member, &gen)

	assign, err := cli.GetAssignment(ctx, &mqv1.AssignRequest{Group: *group, MemberId: *member, GenerationId: gen})
	if err != nil {
		log.Fatalf("GetAssignment: %v", err)
	}

	var wg sync.WaitGroup
	for _, a := range assign.GetAssignments() {
		a := a
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := consumePartition(ctx, cli, *group, *topic, int32(*maxFetch), a); err != nil && ctx.Err() == nil {
				log.Printf("partition %d: %v", a.GetPartition(), err)
			}
		}()
	}
	wg.Wait()

	_, _ = cli.LeaveGroup(context.Background(), &mqv1.LeaveRequest{Group: *group, MemberId: *member})
}

func heartbeatLoop(ctx context.Context, cli mqv1.MessageQueueServiceClient, group, member string, gen *string) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			resp, err := cli.Heartbeat(ctx, &mqv1.HeartbeatRequest{Group: group, MemberId: member, GenerationId: *gen})
			if err != nil {
				log.Printf("heartbeat: %v", err)
				continue
			}
			if resp.GetRebalanceNeeded() {
				log.Printf("rebalance needed: call JoinGroup/GetAssignment again to refresh (demo uses fixed generation)")
			}
		}
	}
}

func consumePartition(ctx context.Context, cli mqv1.MessageQueueServiceClient, group, topic string, max int32, a *mqv1.PartitionAssignment) error {
	part := a.GetPartition()
	off := a.GetStartOffset()
	log.Printf("partition %d start offset %d", part, off)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		resp, err := cli.Fetch(ctx, &mqv1.FetchRequest{
			Group: group, Topic: topic, Partition: part, Offset: off, Max: max,
		})
		if err != nil {
			return fmt.Errorf("fetch: %w", err)
		}
		msgs := resp.GetMessages()
		if len(msgs) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		last := msgs[len(msgs)-1].GetOffset()
		next := last + 1
		if _, err := cli.CommitOffset(ctx, &mqv1.CommitRequest{
			Group: group, Topic: topic, Partition: part, Offset: next,
		}); err != nil {
			return fmt.Errorf("commit offset: %w", err)
		}
		off = next
	}
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "collector"
	}
	return h
}
