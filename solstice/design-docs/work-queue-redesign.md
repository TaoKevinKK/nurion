# Work Queue Redesign: From Kafka Partitions to Single-Queue Model

## Status

**Status**: PROPOSED
**Author**: AI Assistant
**Created**: 2026-01-31

---

## Problem Statement

The current Solstice architecture uses Tansu (Kafka-compatible) message queues with partition-based parallelism. This design has several pain points:

1. **Partition Management Complexity**: Manual partition assignment, rebalancing, and orphaned partition recovery require significant code and are error-prone.

2. **Worker-Partition Coupling**: Each partition can only be consumed by one worker in a consumer group, leading to:
   - Idle workers when `num_workers > num_partitions`
   - Load imbalance when partition data is skewed
   - Complex rebalancing logic on worker failure

3. **No True Round-Robin**: Kafka's design fundamentally prevents multiple consumers from round-robin consuming the same partition.

4. **Operational Overhead**: Choosing optimal partition count, handling partition reassignment, and debugging partition-related issues adds cognitive load.

---

## Proposed Solution

Replace the Kafka partition model with a **single-queue, multi-consumer work queue** that supports:

- **True round-robin consumption**: Any worker can claim any message
- **Cross-queue transactions**: Atomic ACK upstream + write downstream
- **Durable persistence**: S3-backed storage via SlateDB
- **Automatic timeout recovery**: Reclaim messages from dead workers
- **Simple API**: No partition concepts exposed to users

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Driver Process                           │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              WorkQueueServer (Rust + tonic)               │  │
│  │  ┌───────────────┐  ┌────────────┐  ┌─────────────────┐   │  │
│  │  │   SlateDB     │  │  tokio     │  │  gRPC Server    │   │  │
│  │  │ (S3 backed)   │  │  runtime   │  │  (tonic)        │   │  │
│  │  └───────────────┘  └────────────┘  └─────────────────┘   │  │
│  │                                                           │  │
│  │  In-Memory State:                                         │  │
│  │  ├── pending: HashMap<Queue, VecDeque<Message>>           │  │
│  │  ├── claimed: HashMap<Queue, HashMap<MsgId, ClaimInfo>>   │  │
│  │  └── leases:  HashMap<WorkerId, LeaseInfo>                │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              ▲                                  │
│                              │ PyO3                             │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Python Binding: workqueue_py.WorkQueueBroker             │  │
│  │    - start(db_path, host, port) -> address                │  │
│  │    - stop()                                               │  │
│  │    - get_stats() -> dict                                  │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                 ▲              ▲              ▲
                 │ gRPC         │ gRPC         │ gRPC
             Worker 0       Worker 1       Worker 2
            (Python)       (Python)       (Python)
```

---

## Design Details

### 1. Data Model

#### Key Schema (SlateDB)

```
Messages:
  msg:{queue}:{msg_id} → {payload: bytes, created_at: f64}

Pending Index (ordered by time):
  pending:{queue}:{timestamp}:{msg_id} → ""

Claimed Index:
  claimed:{queue}:{msg_id} → {worker_id, lease_id, claimed_at}
```

#### Message State Machine

```
                    ┌──────────────────┐
                    │                  │
                    ▼                  │ timeout / nack
    ┌─────────┐  claim   ┌─────────┐   │
    │ PENDING │─────────▶│ CLAIMED │───┘
    └─────────┘          └────┬────┘
                              │ ack (atomic with downstream write)
                              ▼
                         ┌─────────┐
                         │  DONE   │ (message deleted)
                         └─────────┘
```

### 2. gRPC API

```protobuf
service WorkQueue {
  // Consumer API
  rpc Claim(ClaimRequest) returns (ClaimResponse);
  rpc Ack(AckRequest) returns (AckResponse);
  rpc Nack(NackRequest) returns (NackResponse);
  rpc AckAndForward(AckAndForwardRequest) returns (AckAndForwardResponse);

  // Producer API
  rpc Push(PushRequest) returns (PushResponse);
  rpc PushBatch(PushBatchRequest) returns (PushBatchResponse);

  // Heartbeat (bidirectional streaming)
  rpc HeartbeatStream(stream HeartbeatPing) returns (stream HeartbeatPong);

  // Stats
  rpc GetStats(GetStatsRequest) returns (GetStatsResponse);
}
```

### 3. Key Operations

#### 3.1 Claim (Worker pulls messages)

```
Worker                          Server
   │                               │
   │──── Claim(queue, batch=10) ──▶│
   │                               │ 1. Lock queue
   │                               │ 2. Pop N messages from pending
   │                               │ 3. Add to claimed map
   │                               │ 4. Batch write to SlateDB:
   │                               │    - Delete pending:* keys
   │                               │    - Put claimed:* keys
   │                               │ 5. Unlock queue
   │◀─── [msg1, msg2, ...] ────────│
```

#### 3.2 AckAndForward (Cross-Queue Transaction)

This is the critical operation for exactly-once semantics between stages:

```
Worker                          Server
   │                               │
   │── AckAndForward(             │
   │     upstream_queue,          │
   │     upstream_msg_ids,        │
   │     downstream_queue,        │
   │     downstream_payloads      │
   │   ) ─────────────────────────▶│
   │                               │ ATOMIC batch write:
   │                               │ 1. Delete msg:{upstream}:*
   │                               │ 2. Delete claimed:{upstream}:*
   │                               │ 3. Put msg:{downstream}:*
   │                               │ 4. Put pending:{downstream}:*
   │                               │
   │                               │ Then update in-memory:
   │                               │ 5. Remove from claimed map
   │                               │ 6. Add to pending queue
   │◀── [new_msg_ids] ─────────────│
```

**Why this is atomic**: SlateDB's `batch_write` guarantees all-or-nothing semantics. If the driver crashes mid-operation, on recovery:
- If batch succeeded: downstream messages exist, upstream deleted
- If batch failed: upstream messages still exist, will be reclaimed and reprocessed

#### 3.3 Heartbeat (Lease Renewal)

```
Worker                          Server
   │                               │
   │══ HeartbeatStream ═══════════▶│ (bidirectional stream)
   │                               │
   │── Ping(worker_id, lease_id) ─▶│ Update lease timestamp
   │◀── Pong(ok=true) ─────────────│
   │                               │
   │── Ping(...) ─────────────────▶│
   │◀── Pong(...) ─────────────────│
   │         ...                   │
   │                               │
   │ (connection drops)            │
   │                               │ Lease expires after timeout
   │                               │ → Recovery loop reclaims messages
```

#### 3.4 Timeout Recovery

Background task runs every N seconds:

```python
async def recover_timeout_messages():
    now = time.now()

    for queue, claimed_map in claimed.items():
        for msg_id, claim_info in claimed_map.items():
            # Skip if not timed out
            if now - claim_info.claimed_at < CLAIM_TIMEOUT:
                continue

            # Check if worker is still alive
            lease = worker_leases.get(claim_info.worker_id)
            is_alive = (
                lease and
                lease.lease_id == claim_info.lease_id and
                now - lease.last_heartbeat < CLAIM_TIMEOUT
            )

            if not is_alive:
                # Reclaim message
                batch_write([
                    Delete(f"claimed:{queue}:{msg_id}"),
                    Put(f"pending:{queue}:{now}:{msg_id}", ""),
                ])

                # Update in-memory
                claimed_map.remove(msg_id)
                pending[queue].push_back(msg)
```

### 4. Startup Recovery

On driver restart, recover state from SlateDB:

```python
async def recover_from_db():
    # 1. Recover pending messages
    for key, _ in db.scan_prefix("pending:"):
        queue, msg_id = parse_key(key)
        msg_data = db.get(f"msg:{queue}:{msg_id}")
        pending[queue].push_back(Message(msg_id, msg_data))

    # 2. Reclaim all claimed messages (workers are gone after restart)
    for key, _ in db.scan_prefix("claimed:"):
        queue, msg_id = parse_key(key)
        msg_data = db.get(f"msg:{queue}:{msg_id}")

        # Move from claimed to pending
        now = time.now()
        batch_write([
            Delete(key),
            Put(f"pending:{queue}:{now}:{msg_id}", ""),
        ])

        pending[queue].push_back(Message(msg_id, msg_data))
```

---

## Implementation: Rust + PyO3

### Why Rust?

| Aspect | Python (asyncio) | Rust (tokio + tonic) |
|--------|------------------|---------------------|
| SlateDB calls | PyO3 FFI overhead | Native, zero overhead |
| gRPC performance | ~50k QPS | ~200k+ QPS |
| Concurrency | GIL limited | True multi-threading |
| Memory | GC overhead | Zero GC |
| Existing code | - | tansu-py already uses PyO3 |

### Project Structure

```
lib/workqueue-rs/
├── Cargo.toml
├── build.rs                    # protobuf compilation
├── proto/
│   └── workqueue.proto
├── src/
│   ├── lib.rs                  # PyO3 entry point
│   ├── server.rs               # gRPC server
│   ├── service.rs              # WorkQueueService implementation
│   ├── storage.rs              # SlateDB wrapper
│   ├── types.rs                # Data structures
│   └── recovery.rs             # Timeout recovery logic
└── python/
    └── workqueue_py/
        └── __init__.py
```

### Key Rust Dependencies

```toml
[dependencies]
pyo3 = { version = "0.20", features = ["extension-module"] }
tokio = { version = "1", features = ["full"] }
tonic = "0.10"
prost = "0.12"
slatedb = "0.1"
dashmap = "5"              # Concurrent HashMap
parking_lot = "0.12"       # Fast locks
```

### Python API

```python
from workqueue_py import WorkQueueBroker

# Driver side
broker = WorkQueueBroker()
address = broker.start(
    db_path="s3://bucket/workqueue",  # or local path
    host="0.0.0.0",
    port=0,                           # auto-assign
    claim_timeout_secs=60,
)
print(f"WorkQueue started at {address}")

# Pass address to workers via Ray runtime...

# On shutdown
broker.stop()
```

---

## Potential Issues and Mitigations

### 1. Message Ordering

**Issue**: With multiple workers claiming messages concurrently, processing order is not guaranteed.

**Mitigation**:
- For most use cases (stateless transforms), ordering doesn't matter
- If ordering is needed, use a single worker or add ordering key support in future

### 2. Large Payload OOM

**Issue**: If payloads are huge, in-memory pending queue may cause OOM.

**Mitigation**:
- Current design: payloads stored separately in `SplitPayloadStore` (Ray Object Store)
- Queue messages only contain metadata (~100 bytes each)
- Future: add queue depth limits with backpressure

### 3. SlateDB Scan Performance on Recovery

**Issue**: `scan_prefix("pending:")` may be slow with millions of messages.

**Mitigation**:
- Scan is O(n) but only happens once at startup
- SlateDB uses LSM-tree, prefix scans are efficient
- For extreme cases, add pagination or parallel scan

### 4. Driver Single Point of Failure

**Issue**: If driver crashes, entire queue is unavailable.

**Mitigation**:
- SlateDB persists all state to S3, so data is not lost
- On driver restart, full state is recovered from SlateDB
- Future: support standby driver for HA (out of scope for now)

### 5. Unbounded Queue Growth (Backpressure)

**Issue**: If downstream is slow, pending queue grows unboundedly.

**Mitigation**:
- Add max queue depth config
- When queue is full, `Push` blocks or returns error
- Upstream workers will naturally slow down (backpressure)

```rust
async fn push(&self, queue: &str, payload: &[u8]) -> Result<String, Status> {
    let pending = self.pending.get(queue);
    if pending.len() >= self.max_queue_depth {
        return Err(Status::resource_exhausted("Queue full"));
    }
    // ... normal push
}
```

### 6. Memory-Storage Consistency

**Issue**: If we update memory before SlateDB write completes, crash can cause inconsistency.

**Mitigation**:
- Always write SlateDB first, then update memory
- On recovery, SlateDB is source of truth
- Code pattern:

```rust
// CORRECT: Storage first, then memory
await db.batch_write(ops);      // 1. Persist
pending.push_back(msg);          // 2. Update memory

// WRONG: Memory first
pending.push_back(msg);          // Memory updated
await db.batch_write(ops);       // Crash here = inconsistent
```

### 7. Claim Contention

**Issue**: Multiple workers calling `Claim` simultaneously on same queue.

**Mitigation**:
- Use `parking_lot::Mutex` per queue (not global lock)
- Lock scope is minimal (just pop from VecDeque)
- In practice, claim is fast enough that contention is rare

```rust
async fn claim(&self, queue: &str, batch_size: usize) -> Vec<Message> {
    let pending = self.pending.get(queue);
    let mut guard = pending.lock();  // Per-queue lock

    let messages: Vec<_> = (0..batch_size)
        .filter_map(|_| guard.pop_front())
        .collect();

    drop(guard);  // Release lock before I/O

    // Now do SlateDB write without holding lock
    await self.db.batch_write(...);

    messages
}
```

### 8. Heartbeat Stream Disconnection

**Issue**: If worker crashes, how quickly do we detect it?

**Mitigation**:
- gRPC stream detects TCP connection loss quickly
- Additionally, recovery loop runs every N seconds
- Effective detection time = min(TCP timeout, recovery interval)
- Recommended: `claim_timeout = 60s`, `recovery_interval = 10s`

### 9. Duplicate Processing on Recovery

**Issue**: After driver restart, reclaimed messages may be processed again.

**Mitigation**:
- This is inherent to at-least-once delivery
- Downstream stages should be idempotent
- Use `split_id` for deduplication (existing design)
- `AckAndForward` guarantees: either both succeed or neither

### 10. Multi-Job Isolation

**Issue**: Should multiple jobs share one WorkQueue instance?

**Recommendation**:
- Each job gets its own WorkQueueBroker instance
- Different SlateDB paths for isolation
- Simpler resource management and debugging

```python
# Job 1
broker1 = WorkQueueBroker()
broker1.start(db_path="s3://bucket/job1/queue")

# Job 2
broker2 = WorkQueueBroker()
broker2.start(db_path="s3://bucket/job2/queue")
```

---

## Migration Plan

### Phase 1: Implement WorkQueue (Rust)

1. Create `lib/workqueue-rs/` project
2. Implement gRPC service with tonic
3. Integrate SlateDB for persistence
4. Add PyO3 bindings
5. Unit tests for all operations

### Phase 2: Python Client

1. Create `WorkQueueClient` class using `grpcio`
2. Implement heartbeat streaming
3. Add connection retry logic
4. Integration tests with Rust server

### Phase 3: Integrate with Solstice

1. Update `StageMaster` to use `WorkQueueBroker`
2. Update `StageWorker` to use `WorkQueueClient`
3. Remove partition-related code from managers
4. Update recovery logic

### Phase 4: Remove Old Code

1. Remove `PartitionManager`
2. Simplify `RecoveryManager` (no partition tracking)
3. Remove Tansu broker management code
4. Update tests

---

## Comparison with Current Design

| Aspect | Current (Tansu/Kafka) | New (WorkQueue) |
|--------|----------------------|-----------------|
| Parallelism unit | Partition | Message |
| Consumer model | 1 partition : 1 consumer | N consumers : 1 queue |
| Load balancing | Manual partition assignment | Automatic (claim-based) |
| Cross-stage transaction | None (offset commit only) | Atomic ack + forward |
| Worker failure recovery | Partition reassignment | Message timeout + reclaim |
| Code complexity | High (PartitionManager, etc.) | Low (single queue model) |
| Persistence | Tansu storage backends | SlateDB (S3) |

---

## Open Questions

1. **Queue depth limits**: What should be the default max queue depth? How to handle backpressure signal to upstream?

2. **Batch size tuning**: What's the optimal batch size for `Claim`? Should it be adaptive?

3. **Payload storage**: Keep using Ray Object Store for payloads, or move to SlateDB?

4. **Metrics**: What metrics should the WorkQueue expose? (queue depth, claim rate, ack latency, etc.)

---

## P2: Payload Rebuild on Data Loss

> **Priority**: P2 (Future Work)
>
> This section addresses recovery when payload data is lost from Ray Object Store.

### Problem

Payloads are stored in Ray Object Store (in-memory), which can lose data due to:
- Node crash/restart
- Object reference lost → GC
- Memory pressure → eviction
- Object Store failure

When payload is lost, the split message in queue becomes a "dangling reference".

### Design Principle

**Workers do NOT rebuild payloads themselves.** Reasons:
- Worker may lack required resources (GPU, memory)
- Operator may have state that's hard to recreate
- Rebuild would block normal processing

Instead: **Detect → Report → Replay from Source**

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Payload Missing Flow                           │
│                                                                     │
│  Worker detects payload missing                                     │
│       │                                                             │
│       │ nack(reason=PAYLOAD_MISSING)                                │
│       ▼                                                             │
│  WorkQueue Server                                                   │
│       │                                                             │
│       │ Route to rebuild_queue                                      │
│       ▼                                                             │
│  Rebuild Coordinator (Driver)                                       │
│       │                                                             │
│       │ 1. Batch rebuild requests by source                         │
│       │ 2. Call source_stage.replay(locator)                        │
│       ▼                                                             │
│  Source Stage                                                       │
│       │                                                             │
│       │ Re-read from external source (Iceberg, S3, etc.)            │
│       ▼                                                             │
│  Data flows through pipeline again                                  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Model Enhancement

```python
@dataclass
class Split:
    split_id: str
    stage_id: str
    payload_key: str

    # Source lineage - trace back to original data source
    source_lineage: SourceLineage

    # Rebuild tracking
    rebuild_count: int = 0
    max_rebuild: int = 3

@dataclass
class SourceLineage:
    """Records where this split originally came from, for replay"""
    source_id: str                    # Source stage ID
    source_type: str                  # "iceberg", "s3", "kafka", etc.
    source_path: str                  # table name / path / topic
    locator: Dict[str, Any]           # e.g., {"partition": 0, "offset": 1000}
```

### API Enhancement

```protobuf
message NackRequest {
  string queue = 1;
  repeated string msg_ids = 2;
  NackReason reason = 3;
}

enum NackReason {
  NACK_REASON_UNSPECIFIED = 0;
  NACK_REASON_PROCESSING_FAILED = 1;  // Normal retry
  NACK_REASON_PAYLOAD_MISSING = 2;    // Needs rebuild from source
  NACK_REASON_SKIP = 3;               // Skip this message
}
```

### Worker Logic

```python
class StageWorker:
    async def process_message(self, msg: Message):
        split = Split.from_bytes(msg.payload)

        # Check rebuild limit
        if split.rebuild_count >= split.max_rebuild:
            logger.error(f"Max rebuild exceeded for {split.split_id}")
            await self.queue_client.nack(msg.queue, [msg.msg_id], reason=SKIP)
            return

        # Try to get payload
        payload = await self.payload_store.get(split.payload_key)

        if payload is None:
            # Report missing, don't rebuild here
            logger.warning(f"Payload missing for {split.split_id}, requesting rebuild")
            await self.queue_client.nack(msg.queue, [msg.msg_id], reason=PAYLOAD_MISSING)
            return

        # Normal processing...
```

### Rebuild Coordinator

```python
class RebuildCoordinator:
    """Runs in driver, coordinates replay from source"""

    async def run(self):
        while True:
            requests = await self.queue.claim("rebuild_queue", batch_size=100)
            if not requests:
                await asyncio.sleep(1)
                continue

            # Group by source for batch replay
            by_source = defaultdict(list)
            for req in requests:
                rebuild_req = RebuildRequest.from_bytes(req.payload)
                source_id = rebuild_req.original_split.source_lineage.source_id
                by_source[source_id].append(rebuild_req)

            # Trigger replay
            for source_id, reqs in by_source.items():
                await self._trigger_rebuild(source_id, reqs)

            await self.queue.ack("rebuild_queue", [r.msg_id for r in requests])

    async def _trigger_rebuild(self, source_id: str, requests: List[RebuildRequest]):
        source_stage = self.sources[source_id]
        locators = [r.original_split.source_lineage.locator for r in requests]

        try:
            # Merge adjacent locators for efficiency
            merged = self._merge_locators(locators)
            for locator in merged:
                await source_stage.replay(locator)
        except SourceDataNotFound:
            logger.error(f"Source data unavailable, cannot rebuild")
            # Alert or skip
```

### Edge Cases

| Case | Handling |
|------|----------|
| Source data also gone | Alert user, skip message |
| Rebuild loop (keeps failing) | `rebuild_count` limit, then skip |
| Stage doesn't support rebuild | Configurable policy: skip or fail |

### Configuration

```python
class StageConfig:
    rebuild_policy: RebuildPolicy = RebuildPolicy.FROM_SOURCE

class RebuildPolicy(Enum):
    FROM_SOURCE = "from_source"   # Replay from source (default)
    SKIP = "skip"                 # Skip, don't rebuild
    FAIL = "fail"                 # Fail the job
```

### Benefits

- **Correct resources**: Rebuild goes through normal pipeline, uses stage-configured resources
- **Correct state**: Uses normal workers with properly initialized operators
- **Simple**: No operator serialization, no complex rebuild logic
- **Reliable**: Source data (Iceberg/S3) is typically durable
- **Batch optimized**: Multiple rebuild requests can be merged

---

## References

- [SlateDB Documentation](https://github.com/slatedb/slatedb)
- [tonic gRPC](https://github.com/hyperium/tonic)
- [PyO3 User Guide](https://pyo3.rs/)
- Existing design: `tansu-pyo3-binding.md`
- Existing design: `exactly-once-semantics.md`

---

*Last Updated: 2026-01-31*
