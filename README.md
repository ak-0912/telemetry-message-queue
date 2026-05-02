# telemetry-message-queue

Custom Go message queue with gRPC, partition logs (FNV routing by GPU UUID), consumer groups with range assignment, at-least-once delivery, WAL + offset persistence, Prometheus metrics, and Kubernetes/Helm packaging.

The full product specification lives in [message-queue-requirements.md](message-queue-requirements.md).

---

## Implementation overview

### Architecture

The service follows a small layered layout:

| Layer | Path | Role |
|-------|------|------|
| Domain | `internal/domain/` | Core types (`Message`, assignments) and interfaces (`PartitionStore`, `OffsetStore`, `GroupCoordinator`). |
| Application | `internal/application/` | Use cases: publish (partition routing), fetch, commit offset, coordinator (join/heartbeat/assignment). |
| Infrastructure | `internal/infrastructure/` | In-memory partition logs + per-partition WAL (`store/`), JSON offset files (`offset/`), consumer group coordinator (`coordinator/`). |
| Interface | `internal/interface/grpc/` | gRPC handlers; maps domain errors to status codes. |
| API | `proto/mq/v1/` | Protobuf service and messages (generate with Buf / `go generate`). |
| Config & metrics | `pkg/config/`, `pkg/metrics/` | Viper-style env config; Prometheus registry and collectors. |

### Data flow

1. **Publish** — The producer supplies `topic`, partition key (`key`, e.g. GPU `uuid` from CSV), and JSON `payload`. The server computes `partition = fnv32a(key) % MQ_PARTITION_COUNT`, appends to that partition’s log, syncs a length-prefixed protobuf record to the partition WAL, and returns `(partition, offset)`.

2. **Consume** — Clients **JoinGroup** (with `topic` and `member_id`), then **GetAssignment** for partition ranges and **start offsets** (next offset to read per partition, from the committed offset store). **Fetch** reads from `(topic, partition, offset)` up to `max` messages. After a successful “sink” (e.g. DB write), **CommitOffset** stores the **next** offset to fetch; commits are monotonic and flushed to `data/offsets/<group>.json`.

3. **Consumer groups** — The coordinator assigns partitions with **range** strategy (sorted `member_id`, contiguous partition ranges). **Heartbeat** refreshes membership; members who miss heartbeats within `MQ_HEARTBEAT_TIMEOUT_SEC` are evicted and the group **generation** increments so clients can detect **rebalance** (demo collector logs this; production clients should re-join and refresh assignments).

4. **Retention** — A background task drops messages older than `MQ_RETENTION_HOURS` (and respects max length per partition), then **rewrites** the partition WAL so restarts do not resurrect deleted records.

5. **Restart** — On startup, each partition WAL is **replayed** into memory; offset JSON files reload committed positions.

### On-disk layout

Under `MQ_DATA_DIR` (default `/data/mq`):

- `partitions/<sanitized_topic>/<N>.wal` — append-only log for partition `N`.
- `offsets/<group>.json` — committed offsets per topic and partition for that group.

### Observability

- **HTTP** (default `8080`): `/healthz`, `/readyz`, `/metrics`.
- **gRPC**: standard `grpc.health.v1.Health` service.
- **Logs**: structured JSON via `slog` (`LOG_LEVEL`).

---

## Commands (`cmd/`)

All binaries are also produced by `make build` under `bin/`.

### `cmd/server` → `mq-server`

The main message queue process: gRPC API, HTTP health/metrics, background retention sweep, coordinator sweep, partition size metrics refresh.

- **Configuration**: environment variables only (see [Configuration](#configuration-environment) below). No CLI flags.
- **Listens**: gRPC `MQ_GRPC_PORT`, HTTP `MQ_HTTP_PORT`.
- **Ensures** default topic `gpu-telemetry` exists at startup so clients can connect before any publish.

```bash
go run ./cmd/server
# or
./bin/mq-server
```

### `cmd/csv-streamer`

Streams a DCGM-style CSV and publishes one message per **data** row. Each row is JSON-encoded (all columns as strings); the **`uuid`** column is used as the gRPC **`key`** (partition routing). Header row is required.

| Flag | Default | Description |
|------|---------|-------------|
| `-csv` | `dcgm_metrics_20250718_134233.csv` | Input CSV path. |
| `-addr` | `localhost:50051` | gRPC server address. |
| `-topic` | `gpu-telemetry` | Topic name. |

```bash
go run ./cmd/csv-streamer -csv ./dcgm_metrics_20250718_134233.csv -addr localhost:50051 -topic gpu-telemetry
./bin/csv-streamer -h
```

### `cmd/telemetry-collector`

Demo **consumer**: joins a group, starts a **heartbeat** ticker (every 5s), **GetAssignment**, then runs one goroutine per assigned partition that **Fetch**es in a loop and **CommitOffset**s to `lastOffset + 1` after each non-empty batch. On SIGINT/SIGTERM it cancels context and **LeaveGroup**. It does **not** automatically handle **rebalance** beyond logging `rebalance_needed`; production workers should re-join and refresh assignments when the generation changes.

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `localhost:50051` | gRPC server address. |
| `-group` | `telemetry-collector` | Consumer group id. |
| `-topic` | `gpu-telemetry` | Topic to consume. |
| `-member` | host name | Unique member id in the group. |
| `-max` | `200` | Max messages per **Fetch** (capped server-side by defaults too). |

```bash
go run ./cmd/telemetry-collector -addr localhost:50051 -group telemetry-collector -topic gpu-telemetry -member worker-1
./bin/telemetry-collector -h
```

---

## Requirements

- Go 1.22+
- Buf (optional, for `go generate`): `go install github.com/bufbuild/buf/cmd/buf@latest`

## Generate protobuf code

```bash
go generate ./...
# or: buf generate
```

## Build

Runs `go vet`, staticcheck (`lint`), then builds all binaries.

```bash
make build          # vet + lint + compile
make proto          # regenerate protobuf (Buf via go generate)
make run            # background server; PID in .mq-server.pid
make stop           # kill server from PID file
make test
make test-coverage  # ./internal/... + ./pkg/... coverage; fails if below 80% (override e.g. COVERAGE_MIN=75)
```

Outputs: `bin/mq-server`, `bin/csv-streamer`, `bin/telemetry-collector`.

## Run server locally

Foreground:

```bash
MQ_DATA_DIR=./data/mq MQ_GRPC_PORT=50051 MQ_HTTP_PORT=8080 go run ./cmd/server
```

Background (Makefile):

```bash
make run    # default grpc 50051, http 8080
make stop
```

- gRPC: `:50051`
- HTTP: `http://localhost:8080/healthz`, `/readyz`, `/metrics`

## End-to-end with sample CSV

Terminal 1 — server (as above).

Terminal 2 — publish DCGM CSV (`uuid` column is the partition key):

```bash
go run ./cmd/csv-streamer -csv dcgm_metrics_20250718_134233.csv -addr localhost:50051
```

Terminal 3 — consumer (commits after each batch):

```bash
go run ./cmd/telemetry-collector -addr localhost:50051 -group telemetry-collector -topic gpu-telemetry
```

## Configuration (environment)

| Variable | Default |
|----------|---------|
| `MQ_GRPC_PORT` | `50051` |
| `MQ_HTTP_PORT` | `8080` |
| `MQ_PARTITION_COUNT` | `256` |
| `MQ_DATA_DIR` | `/data/mq` |
| `MQ_RETENTION_HOURS` | `24` |
| `MQ_HEARTBEAT_TIMEOUT_SEC` | `15` |
| `MQ_MAX_PARTITION_SIZE` | `100000` |
| `MQ_FETCH_BATCH_DEFAULT` | `200` |
| `LOG_LEVEL` | `info` |

## Docker

```bash
docker build -t message-queue:local .
docker run --rm -p 50051:50051 -p 8080:8080 -v mqdata:/data/mq message-queue:local
```

## Helm

```bash
helm upgrade --install mq ./helm/message-queue -n default --create-namespace
```

Set `image.repository` / `image.tag` in `values.yaml` to your registry.

## Repository layout

```
cmd/
  server/                 # mq-server — production entrypoint
  csv-streamer/           # CSV → Publish
  telemetry-collector/    # demo consumer
internal/
  domain/                 # types + ports
  application/            # use cases
  infrastructure/         # store, WAL, offsets, coordinator
  interface/grpc/         # gRPC service implementation
proto/mq/v1/              # .proto + generated Go
pkg/config/               # env config
pkg/metrics/              # Prometheus
helm/message-queue/       # Kubernetes chart
```

---

See [message-queue-requirements.md](message-queue-requirements.md) for RPC definitions, non-functional targets, and acceptance criteria.
