// Command csv-streamer publishes DCGM CSV rows to the message queue (topic gpu-telemetry, key = uuid column).
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
	mqv1 "github.com/cisco-interview/telemetry-message-queue/proto/mq/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	csvPath := flag.String("csv", "dcgm_metrics_20250718_134233.csv", "path to DCGM CSV")
	addr := flag.String("addr", "localhost:50051", "message queue gRPC address")
	topic := flag.String("topic", domain.DefaultTopic, "topic name")
	flag.Parse()

	f, err := os.Open(*csvPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.LazyQuotes = true
	header, err := r.Read()
	if err != nil {
		log.Fatal(err)
	}
	col := map[string]int{}
	for i, h := range header {
		col[strings.TrimSpace(h)] = i
	}
	uuidIdx, ok := col["uuid"]
	if !ok {
		log.Fatal("csv missing uuid column")
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	client := mqv1.NewMessageQueueServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	var n int
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if len(rec) <= uuidIdx {
			continue
		}
		key := strings.Trim(rec[uuidIdx], `"`)
		if key == "" {
			continue
		}
		row := map[string]string{}
		for name, idx := range col {
			if idx < len(rec) {
				row[name] = strings.Trim(rec[idx], `"`)
			}
		}
		payload, err := json.Marshal(row)
		if err != nil {
			log.Fatal(err)
		}
		_, err = client.Publish(ctx, &mqv1.PublishRequest{
			Topic:   *topic,
			Key:     key,
			Payload: payload,
		})
		if err != nil {
			log.Fatalf("publish row %d: %v", n, err)
		}
		n++
		if n%500 == 0 {
			fmt.Fprintf(os.Stderr, "published %d rows\n", n)
		}
	}
	fmt.Printf("published %d rows to topic %q\n", n, *topic)
}
