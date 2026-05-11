# Message Queue Service — Requirements

## 1. Overview

A custom, self-contained message queue built in Go, exposed over gRPC, deployable on Kubernetes.
It connects Telemetry Streamers (producers) with Telemetry Collectors (consumers) with partition-based
parallelism, consumer group semantics, and at-least-once delivery guarantees.

---

## 2. Functional Requirements

### 2.1 Topic Management
- Support named topics (initial topic: `gpu-telemetry`).
- Each topic is divided into a configurable number of partitions (default: `256`).
- Partitions are append-only, ordered logs.

### 2.2 Publishing (Producer)
- Streamer calls `Publish(topic, key, payload)`.
- Partition is assigned deterministically: `fnv32a(key) % numPartitions`.
- Partition key = `uuid` field from CSV (globally unique per GPU).
- Messages include: `offset`, `key`, `payload` (JSON bytes), `published_at` timestamp.
- Publish must be acknowledged (at-least-once delivery).
- Support batch publish for throughput.

### 2.3 Consuming (Consumer)
- Collectors join a named consumer group: `telemetry-collector`.
- Consumer group coordinator assigns partitions to active group members.
- Collector fetches messages from assigned partitions starting at committed offset.
- Collector commits offset only after successful DB write (at-least-once guarantee).
- Support configurable fetch batch size (default: `200` messages).

### 2.4 Consumer Group Coordination
- `JoinGroup`: collector registers itself with group + topic.
- `GetAssignment`: returns list of assigned partitions and starting offsets.
- `Heartbeat`: collector sends heartbeat every `5s`; timeout = `15s`.
- On heartbeat timeout or explicit leave: coordinator rebalances partitions.
- Rebalance strategy: **range assignment** (partition range split evenly across members).
- New owner resumes from last committed offset for the partition.

### 2.5 Offset Management
- Per `(group, topic, partition)` committed offset stored persistently.
- `CommitOffset(group, topic, partition, offset)`.
- On collector restart, resume from last committed offset (no data loss).

### 2.6 Retention
- Messages retained for configurable duration (default: `24h`) or configurable max size per partition.
- Expired messages are garbage-collected in background.

---

## 3. Non-Functional Requirements

| Property | Requirement |
|---|---|
| Max streamers | 10 instances |
| Max collectors | 10 instances |
| Partitions | 256 (covers 247 GPUs + headroom) |
| Throughput | ≥ 10,000 messages/sec (aggregate) |
| Latency | p99 publish-to-fetch < 100ms |
| Delivery | At-least-once |
| Ordering | Per-partition ordering guaranteed |
| Availability | Single replica acceptable for this exercise |
| Language | Go 1.22+ |
| Transport | gRPC (protobuf) |
| Storage | In-memory + append-only WAL file per partition |
| Observability | Prometheus metrics + structured JSON logs |

---

## 4. Architecture Decision: DDD for Message Queue?

**Verdict: Apply lightweight DDD — not full DDD.**

The message queue is primarily infrastructure. However, it has enough internal logic to benefit
from a clean separation of concerns using DDD-inspired layers, without the full ceremony of
aggregates, value objects, and domain events.

### What applies from DDD/Clean Architecture:

| Layer | What goes here |
|---|---|
| `domain/` | Core types: `Message`, `Topic`, `Partition`, `Offset`, `ConsumerGroup` — plain structs + interfaces only, no framework deps |
| `application/` | Use-cases: `PublishUsecase`, `FetchUsecase`, `CommitOffsetUsecase`, `CoordinatorUsecase` |
| `infrastructure/` | In-memory store, WAL file writer, offset persistence, background retention GC |
| `interface/` | gRPC server handlers — translate proto ↔ domain types |

### What does NOT apply:
- No domain aggregates (no root aggregate needed).
- No domain events (internal state changes are direct).
- No repository pattern (storage is simple enough to be a direct interface).

---

## 5. Project Structure

```
message-queue/
├── cmd/
│   └── server/
│       └── main.go                   # wiring: DI, gRPC server start
│
├── internal/
│   ├── domain/
│   │   ├── message.go                # Message, Offset types
│   │   ├── topic.go                  # Topic, Partition types
│   │   ├── consumer_group.go         # ConsumerGroup, Member, Assignment types
│   │   └── ports.go                  # interfaces: PartitionStore, OffsetStore, GroupStore
│   │
│   ├── application/
│   │   ├── publish.go                # PublishUsecase
│   │   ├── fetch.go                  # FetchUsecase
│   │   ├── commit_offset.go          # CommitOffsetUsecase
│   │   └── coordinator.go            # CoordinatorUsecase (join, heartbeat, rebalance)
│   │
│   ├── infrastructure/
│   │   ├── store/
│   │   │   ├── partition_store.go    # in-memory ring buffer per partition
│   │   │   └── wal.go                # append-only WAL file per partition
│   │   ├── offset/
│   │   │   └── offset_store.go       # committed offsets (in-memory + file flush)
│   │   └── coordinator/
│   │       └── group_coordinator.go  # consumer group state, heartbeat tracker, rebalancer
│   │
│   └── interface/
│       └── grpc/
│           ├── server.go             # gRPC server setup
│           ├── publish_handler.go    # Publish RPC
│           ├── fetch_handler.go      # Fetch RPC
│           ├── offset_handler.go     # CommitOffset RPC
│           └── group_handler.go      # JoinGroup, Heartbeat, GetAssignment RPCs
│
├── proto/
│   └── mq/
│       └── v1/
│           └── mq.proto              # protobuf definitions
│
├── pkg/
│   ├── config/
│   │   └── config.go                 # env-based config (viper)
│   └── metrics/
│       └── prometheus.go             # publish_count, fetch_count, rebalance_count, lag
│
├── helm/
│   └── message-queue/
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/
│           ├── deployment.yaml
│           ├── service.yaml
│           ├── pvc.yaml
│           ├── hpa.yaml
│           └── _helpers.tpl
│
├── Dockerfile
├── go.mod
└── README.md
```

---

## 6. gRPC API — Proto Definition

```proto
syntax = "proto3";
package mq.v1;
option go_package = "github.com/your-org/message-queue/proto/mq/v1";

service MessageQueueService {
  rpc Publish         (PublishRequest)    returns (PublishResponse);
  rpc PublishBatch    (PublishBatchReq)   returns (PublishBatchResp);
  rpc Fetch           (FetchRequest)      returns (FetchResponse);
  rpc CommitOffset    (CommitRequest)     returns (CommitResponse);
  rpc JoinGroup       (JoinRequest)       returns (JoinResponse);
  rpc Heartbeat       (HeartbeatRequest)  returns (HeartbeatResponse);
  rpc GetAssignment   (AssignRequest)     returns (AssignResponse);
  rpc LeaveGroup      (LeaveRequest)      returns (LeaveResponse);
}

message PublishRequest {
  string topic   = 1;
  string key     = 2;   // GPU UUID — used for partition routing
  bytes  payload = 3;   // JSON-encoded TelemetryEvent
}
message PublishResponse {
  int32 partition = 1;
  int64 offset    = 2;
}

message PublishBatchReq  { repeated PublishRequest messages = 1; }
message PublishBatchResp { repeated PublishResponse results  = 1; }

message FetchRequest {
  string group     = 1;
  string topic     = 2;
  int32  partition = 3;
  int64  offset    = 4;
  int32  max       = 5;  // max messages to return
}
message FetchResponse { repeated Message messages = 1; }

message Message {
  int32  partition    = 1;
  int64  offset       = 2;
  string key          = 3;
  bytes  payload      = 4;
  int64  published_at = 5;  // Unix nano
}

message CommitRequest  { string group = 1; string topic = 2; int32 partition = 3; int64 offset = 4; }
message CommitResponse { bool   ok    = 1; }

message JoinRequest    { string group = 1; string topic = 2; string member_id = 3; }
message JoinResponse   { string generation_id = 1; }

message HeartbeatRequest  { string group = 1; string member_id = 2; string generation_id = 3; }
message HeartbeatResponse { bool rebalance_needed = 1; }

message AssignRequest   { string group = 1; string member_id = 2; string generation_id = 3; }
message AssignResponse  { repeated PartitionAssignment assignments = 1; }
message PartitionAssignment { int32 partition = 1; int64 start_offset = 2; }

message LeaveRequest   { string group = 1; string member_id = 2; }
message LeaveResponse  { bool ok = 1; }
```

---

## 7. Partitioning

- Partition count: **256** (configurable via env `MQ_PARTITION_COUNT`).
- Partition key: `uuid` from CSV (`Message.key` field in proto).
- Hash function: `fnv32a(key) % partitionCount` — consistent, fast, no external dependency.
- Ordering: all events for one GPU UUID land in the same partition — per-GPU ordering guaranteed.
- With 10 collector workers: each worker owns ~25–26 partitions.

```go
// Partition routing (inside Publish usecase)
func partitionFor(key string, count int) int {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32() % uint32(count))
}
```

---

## 8. Storage Design

### Partition Store (in-memory + WAL)
- Each partition is a `[]Message` ring buffer in memory (configurable max size).
- Every append is also written to a WAL file: `data/partitions/<partition_id>.wal`.
- WAL format: length-prefixed protobuf records.
- On startup: replay WAL to rebuild in-memory state.

### Offset Store
- Map: `group:topic:partition → offset` held in memory.
- Flushed to `data/offsets/<group>.json` on every commit.
- Loaded from file on startup.

---

## 9. Observability

### Prometheus Metrics
| Metric | Type | Description |
|---|---|---|
| `mq_messages_published_total` | Counter | Total messages published, by topic/partition |
| `mq_messages_fetched_total` | Counter | Total messages fetched, by group/partition |
| `mq_consumer_lag` | Gauge | Offset lag per group/partition |
| `mq_rebalance_total` | Counter | Number of group rebalances |
| `mq_partition_size` | Gauge | Current messages in each partition |
| `mq_publish_latency_seconds` | Histogram | Publish handler latency |

### Logging
- Structured JSON logs via `slog` (Go 1.22 stdlib).
- Fields: `level`, `time`, `component`, `topic`, `partition`, `group`, `member_id`, `msg`.

### Health Endpoints
- gRPC health check: `grpc.health.v1.Health/Check`.
- HTTP `/healthz` (liveness) and `/readyz` (readiness) on port `8080`.

---

## 10. Configuration (Environment Variables)

| Variable | Default | Description |
|---|---|---|
| `MQ_GRPC_PORT` | `50051` | gRPC server port |
| `MQ_HTTP_PORT` | `8080` | Health + metrics HTTP port |
| `MQ_PARTITION_COUNT` | `256` | Number of partitions per topic |
| `MQ_DATA_DIR` | `/data/mq` | WAL and offset files root |
| `MQ_RETENTION_HOURS` | `24` | Message retention in hours |
| `MQ_HEARTBEAT_TIMEOUT_SEC` | `15` | Consumer heartbeat timeout |
| `MQ_MAX_PARTITION_SIZE` | `100000` | Max messages in memory per partition |
| `LOG_LEVEL` | `info` | Log level (debug/info/warn/error) |

---

## 11. Dockerfile

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o mq-server ./cmd/server

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /app/mq-server /mq-server
EXPOSE 50051 8080
ENTRYPOINT ["/mq-server"]
```

---

## 12. Kubernetes Deployment

### deployment.yaml
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: message-queue
  labels:
    app: message-queue
spec:
  replicas: 1
  selector:
    matchLabels:
      app: message-queue
  template:
    metadata:
      labels:
        app: message-queue
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: message-queue
          image: your-org/message-queue:latest
          ports:
            - containerPort: 50051  # gRPC
            - containerPort: 8080   # health + metrics
          env:
            - name: MQ_GRPC_PORT
              value: "50051"
            - name: MQ_HTTP_PORT
              value: "8080"
            - name: MQ_PARTITION_COUNT
              value: "256"
            - name: MQ_DATA_DIR
              value: "/data/mq"
            - name: MQ_RETENTION_HOURS
              value: "24"
            - name: MQ_HEARTBEAT_TIMEOUT_SEC
              value: "15"
            - name: LOG_LEVEL
              value: "info"
          volumeMounts:
            - name: mq-data
              mountPath: /data/mq
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8080
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /readyz
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 10
          resources:
            requests:
              cpu: "250m"
              memory: "256Mi"
            limits:
              cpu: "1000m"
              memory: "1Gi"
      volumes:
        - name: mq-data
          persistentVolumeClaim:
            claimName: mq-data-pvc
```

### service.yaml
```yaml
apiVersion: v1
kind: Service
metadata:
  name: message-queue
spec:
  selector:
    app: message-queue
  ports:
    - name: grpc
      port: 50051
      targetPort: 50051
    - name: http
      port: 8080
      targetPort: 8080
  type: ClusterIP
```

### pvc.yaml
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mq-data-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

---

## 13. Helm Chart values.yaml

```yaml
replicaCount: 1

image:
  repository: your-org/message-queue
  tag: latest
  pullPolicy: IfNotPresent

service:
  grpcPort: 50051
  httpPort: 8080

config:
  partitionCount: 256
  dataDir: /data/mq
  retentionHours: 24
  heartbeatTimeoutSec: 15
  maxPartitionSize: 100000
  logLevel: info

resources:
  requests:
    cpu: 250m
    memory: 256Mi
  limits:
    cpu: 1000m
    memory: 1Gi

persistence:
  enabled: true
  size: 10Gi
  storageClass: ""

metrics:
  enabled: true
  path: /metrics
  port: 8080
```

---

## 14. Dependency Summary

```
github.com/your-org/message-queue

go 1.22

require (
    google.golang.org/grpc              v1.63.0
    google.golang.org/protobuf          v1.34.0
    github.com/spf13/viper              v1.18.0
    github.com/prometheus/client_golang v1.19.0
    golang.org/x/net                    v0.24.0
)
```

---

## 15. Acceptance Criteria

- [ ] gRPC server starts and all 8 RPC methods respond correctly.
- [ ] `Publish` routes message to correct partition via `fnv32a(uuid) % 256`.
- [ ] `Fetch` returns messages in order from given offset.
- [ ] `CommitOffset` persists to disk; survives pod restart at correct offset.
- [ ] Consumer group rebalances correctly when a collector instance dies (heartbeat timeout).
- [ ] WAL replay on startup restores all unflushed partition messages.
- [ ] Prometheus metrics endpoint returns all defined metrics.
- [ ] Liveness and readiness probes pass.
- [ ] Helm chart deploys cleanly to a local K8s cluster (minikube/kind).
- [ ] Streamer → MQ → Collector end-to-end flow works with real CSV data.
