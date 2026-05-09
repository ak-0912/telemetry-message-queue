# Message queue (MQ) — implementation overview

This document describes how the telemetry message queue service is structured in code and how data flows through it. For product requirements, see `message-queue-requirements.md`.

## Purpose

A Go **gRPC** service that provides Kafka-like semantics at a smaller scale: **topics**, **partitions**, **offsets**, **consumer groups**, **range assignment**, and **heartbeats**. Producers publish keyed messages; consumers join a group, receive partition assignments, fetch from committed offsets, and commit after processing.

## API definition

The RPC surface is defined in `proto/mq/v1/mq.proto` as `MessageQueueService`:

| RPC | Role |
| --- | --- |
| `Publish` / `PublishBatch` | Append message(s) to a topic; response includes `partition` and `offset`. |
| `Fetch` | Read up to `max` messages for `(group, topic, partition)` from `offset`. |
| `CommitOffset` | Persist consumer progress for `(group, topic, partition)`. |
| `JoinGroup` | Register a consumer member; returns `generation_id`. |
| `Heartbeat` | Liveness; returns `rebalance_needed` if generation is stale. |
| `GetAssignment` | Partition list and `start_offset` per partition for the current generation. |
| `LeaveGroup` | Remove member and trigger rebalance if others remain. |

## Entry point and wiring

`cmd/server/main.go`:

1. Loads configuration (`pkg/config`).
2. Opens **FileOffsetStore** at `{dataDir}/offsets`.
3. Constructs **MemoryPartitionStore** with partition count, retention, max messages per partition, and the offset store (for retention watermarks).
4. Ensures the default topic exists (`internal/domain`).
5. Creates **GroupCoordinator**, Prometheus registry, and use cases: publish, fetch, commit, coordinator.
6. Registers the gRPC server and standard health service.
7. Starts HTTP for `/healthz`, `/readyz`, and `/metrics`.
8. Background goroutines: coordinator sweep (`Run`), retention GC (`RunRetentionGC`), partition size gauges.

## Layering

- **`internal/interface/grpc`** — `MessageQueueServer` validates requests, delegates to use cases, maps errors to gRPC status codes, converts domain messages to protobuf.
- **`internal/application`** — orchestration: publish (hash + append), fetch (read slice + metrics), commit, coordinator calls.
- **`internal/domain`** — types and interfaces (messages, topics, ports).
- **`internal/infrastructure/store`** — in-memory partition logs + WAL append/replay/rewrite.
- **`internal/infrastructure/offset`** — committed offsets in memory, JSON flush per group.
- **`internal/infrastructure/coordinator`** — group membership, generation, range assignment, stale eviction.

## Publishing

`application/publish.go`:

1. `EnsureTopic(topic, partitionCount)`.
2. Partition index: `PartitionFor(key, count)` in `application/partition.go` — **FNV-1a** hash of the key modulo partition count.
3. `PartitionStore.Append` returns the monotonic offset for that partition.

## Fetching

`application/fetch.go`:

- Default batch cap `DefaultFetchMax` (200) when the client sends `max <= 0` (gRPC layer applies this using config when set).
- Reads from the partition log from `offset` upward.
- Lag metric (when metrics are wired): high watermark minus committed offset for the same `(group, topic, partition)`.

## Committed offsets

`application/commit_offset.go` and `internal/infrastructure/offset`:

- Commit stores the **next offset to read** (exclusive lower bound of unconsumed data).
- **Regression** (committing backwards) is rejected (`ErrOffsetRegression` → `InvalidArgument` on gRPC).
- `EnsureConsumerTopic` on join ties the consumer group to a topic for retention bookkeeping.

## Partition storage and WAL

`internal/infrastructure/store/partition_store.go`:

- Per topic: fixed number of **partition logs**; each has an in-memory `[]Message` and a WAL file under `{dataDir}/partitions/{topic}/{partition}.wal`.
- **Startup**: `ReplayWal` rebuilds messages and `nextOffset`.
- **Append**: length-prefixed protobuf `mq.v1.Message` (`wal.go`), then append to the slice.
- **Fetch**: scan messages with `offset >= requested`, copy payloads for callers.

### Retention

`RunRetentionGC` (periodic from `main`):

- Drops messages older than retention **and** safely before consumer progress (`MinCommittedNextOffset`), or enforces **max messages per partition** without deleting ahead of the minimum committed next offset.
- After trimming, the WAL may be **rewritten** to match remaining messages.

## Consumer group coordinator

`internal/infrastructure/coordinator/group_coordinator.go`:

- **Join**: adds member, increments **generation**, runs **rebalance**. A group is tied to a single topic; duplicate topic mismatch is an error; cap on distinct members (`maxGroupMembers`).
- **Range assignment**: members sorted by ID; each gets a contiguous range of partition indices (`partitionCount / n` style split).
- **Heartbeat**: updates last-seen time; compares client `generation_id` to current — mismatch means **rebalance needed**.
- **Stale eviction**: heartbeats older than TTL are removed; generation bumps and rebalance.
- **Assignment**: for each assigned partition, `start_offset` comes from `OffsetStore.Get(group, topic, partition)`.
- **Leave**: removes member; empty group is deleted; otherwise rebalance.

Background `Run(ctx, interval)` periodically evicts stale members across all groups.

## Typical consumer sequence

1. `JoinGroup` → receive `generation_id`.
2. Loop: `Heartbeat` until `rebalance_needed` is false (or re-join after handling rebalance).
3. `GetAssignment` → list of `(partition, start_offset)`.
4. For each partition: `Fetch` from committed/start offset; process; `CommitOffset` with the next offset to read.
5. On rebalance or process exit: `LeaveGroup` or rely on heartbeat timeout for cleanup.

## Error mapping (gRPC)

`internal/interface/grpc/mq_service.go` maps domain/infrastructure errors, for example:

- Unknown topic → `NotFound`
- Invalid partition → `OutOfRange`
- Offset regression → `InvalidArgument`
- Stale generation / coordinator preconditions → `FailedPrecondition` or `NotFound` as appropriate

## Configuration

Environment-driven settings are documented in `message-queue-requirements.md` and loaded in `pkg/config` (gRPC port, HTTP port, data directory, partition count, retention, heartbeat timeout, fetch batch default, log level).

## Related files (quick reference)

| Area | Path |
| --- | --- |
| Proto | `proto/mq/v1/mq.proto` |
| Main | `cmd/server/main.go` |
| gRPC handlers | `internal/interface/grpc/mq_service.go` |
| Publish / fetch / commit / coordinator use cases | `internal/application/*.go` |
| Partition routing | `internal/application/partition.go` |
| Memory + WAL store | `internal/infrastructure/store/partition_store.go`, `wal.go` |
| Offsets on disk | `internal/infrastructure/offset/offset_store.go` |
| Groups | `internal/infrastructure/coordinator/group_coordinator.go` |
| Metrics | `pkg/metrics/prometheus.go` |
