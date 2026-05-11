# Future enhancements

---

## 1. Disk-backed storage (replace in-memory + WAL)

Currently every message lives in a `[]Message` slice in memory, with the WAL used only for crash replay. Memory is the ceiling for retention, startup is slow at scale, and GC rewrites the entire WAL.

**Recommended:** Replace `MemoryPartitionStore` with an embedded LSM engine like **Pebble** (pure Go, used by CockroachDB). Key = `topic/partition/offset`, Fetch = range scan, retention = `DeleteRange`. Same `domain.PartitionStore` interface — no application or gRPC changes needed. Eliminates all custom WAL code; compaction and crash recovery are handled internally.

## 2. Dead-letter queue (DLQ)

A single poison message can block an entire partition forever. Messages that fail processing N times should be moved to a `<topic>.dlq` partition and the consumer offset advanced.

- Add `MQ_MAX_DELIVERY_ATTEMPTS` config (default 5).
- Expose `mq_dlq_messages_total` Prometheus counter.

## 3. Backpressure

When a partition hits `MQ_MAX_PARTITION_SIZE`, old messages are silently GC'd even if unconsumed. Instead, return `RESOURCE_EXHAUSTED` to the producer so it can back off, preventing silent data loss.

## 4. TLS and authentication

- gRPC server-side TLS (cert/key via mounted Secret).
- mTLS or token-based auth for multi-tenant deployments.

## 5. Cooperative sticky rebalance

The current range assignor reassigns all partitions on every membership change. A cooperative-sticky strategy would only move partitions that must change, reducing fetch gaps during rolling deploys.

## 6. Corner-case tests

| Area | Test case |
|------|-----------|
| WAL | Truncated record mid-write (crash simulation) — replay should skip partial tail |
| Coordinator | Concurrent Join + Leave from the same member (race condition) |
| Offset store | Disk-full during flush — should return error, not silently lose data |
| Retention GC | GC runs while Fetch is in progress on the same partition |
| gRPC | Payload exceeding 4 MB default gRPC limit |
