# Work Queue Redesign: From Kafka Partitions to Single-Queue Model

## Status

**Status**: ✅ IMPLEMENTED
**Author**: AI Assistant
**Created**: 2026-01-31
**Updated**: 2026-02-01

### Implementation Status

| Component | Status | Notes |
|-----------|--------|-------|
| Rust Server (`lib/anvil-rs/`) | ✅ Done | gRPC + SlateDB + PyO3 |
| GC-based Ack | ✅ Done | Messages retained until GC, safer recovery |
| State API | ✅ Done | `state_get`, `state_put`, atomic ack+state |
| Python Client | ✅ Done | `anvil_py.client.AnvilClient` |
| Solstice Integration | ✅ Done | `engine/queue/anvil.py` |
| Unified Worker Exit | ✅ Done | No EOF messages, uses `notify_upstream_finished` + queue drained |

---

## Problem Statement

The current Nurion Runtime architecture uses Tansu (Kafka-compatible) message queues with partition-based parallelism. This design has several pain points:

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
│  │              AnvilServer (Rust + tonic)               │  │
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
│  │  Python Binding: anvil_py.AnvilBroker             │  │
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
  pending:{queue}:{timestamp_nanos}:{msg_id} → ""

Claimed Index:
  claimed:{queue}:{msg_id} → {worker_id, lease_id, claimed_at}

Acked Index (for GC):
  acked:{queue}:{timestamp_nanos}:{msg_id} → ""

State Entries:
  state:{namespace}:{key} → {value: bytes}
```

#### Message State Machine

```
                    ┌──────────────────┐
                    │                  │
                    ▼                  │ timeout / nack
    ┌─────────┐  claim   ┌─────────┐   │
    │ PENDING │─────────▶│ CLAIMED │───┘
    └─────────┘          └────┬────┘
                              │ ack (atomic with downstream write + state update)
                              ▼
                         ┌─────────┐
                         │  ACKED  │ (message retained for safety)
                         └────┬────┘
                              │ GC (after retention period)
                              ▼
                         ┌─────────┐
                         │ DELETED │ (message body removed)
                         └─────────┘
```

**GC-based Ack Design**: Messages are not deleted immediately on ack. Instead:
1. `ack()` moves message from CLAIMED to ACKED index
2. Message body is retained for the retention period (default: 1 hour)
3. Background GC task periodically deletes old ACKED messages
4. Benefits: Safer recovery, debugging support, auditability

### 2. gRPC API

```protobuf
service Anvil {
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

#### 3.4 Unified Worker Exit Mechanism

Workers exit when:
1. `_upstream_finished` flag is set (via `notify_upstream_finished()` remote call)
2. Queue is drained: `pending_count == 0 AND claimed_count == 0`

```python
# StageWorker._run_claim_loop()
while self._running:
    records = self.upstream_queue.claim(queue, batch_size, timeout_ms=1000)

    if not records:
        # Check exit condition
        if self._upstream_finished and self._is_queue_drained():
            break  # Exit gracefully
        continue

    # Process messages...

def _is_queue_drained(self) -> bool:
    stats = self.upstream_queue.get_stats(queue)
    return stats["pending_count"] == 0 and stats["claimed_count"] == 0
```

**Benefits over EOF messages**:
- Unified exit logic for all workers (no special EOF handling)
- No EOF message that only one worker can claim
- Reliable completion detection via queue stats
- Simpler code, fewer edge cases

**Trigger flow**:
```
Source Stage:
  1. SourceMaster produces all splits
  2. SourceMaster calls worker_manager.notify_upstream_finished()
  3. Workers detect _upstream_finished + queue drained → exit

Processing Stages:
  1. Upstream stage completes
  2. RayRunner calls downstream_master.notify_upstream_finished()
  3. Workers detect _upstream_finished + queue drained → exit
```

#### 3.5 Timeout Recovery

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

### 4. Integrated State Management

#### 4.1 Problem: State Store Partitioning with Work-Stealing

The original design used partition-scoped state stores (one SlateDB per partition). This worked well when worker:partition was 1:1, but causes issues with Anvil's work-stealing model:

```
问题: SlateDB 只支持单写者

旧模型 (1:1):
  Worker_0 ──独占写──> SlateDB_partition_0  ✓
  Worker_1 ──独占写──> SlateDB_partition_1  ✓

Anvil work-stealing (N:M):
  Worker_0 ─┬─ 可能写 ──> SlateDB_partition_0
            └─ 可能写 ──> SlateDB_partition_1  ❌ 多写者冲突！
```

If workers can process any message, and state is partitioned by key hash, we'd need:
- Many small SlateDB instances (one per partition)
- Complex locking across processes
- Or give up work-stealing benefits

#### 4.2 Solution: State Integrated into Anvil Server

Since Anvil Server already manages SlateDB as a single-writer process, extend it to also manage operator state:

```
┌─────────────────────────────────────────────────────────┐
│           Anvil Server (单进程, 单写者)              │
│  ┌─────────────┐    ┌─────────────┐                    │
│  │ Message DB  │    │  State DB   │  ← 同一进程管理     │
│  │  (SlateDB)  │    │  (SlateDB)  │    无多写者问题     │
│  └─────────────┘    └─────────────┘                    │
└─────────────────────────────────────────────────────────┘
         ↑                    ↑
         │    gRPC            │
    ┌────┴────┬───────────────┴────┐
    │         │                    │
 Worker_0  Worker_1            Worker_2
 (任意消息) (任意消息)          (任意消息)
```

**Key insight**: Workers don't write state directly. They request state operations from the server, which serializes all writes through a single SlateDB instance.

#### 4.3 Extended gRPC API

```protobuf
service Anvil {
  // Existing consumer API
  rpc Claim(ClaimRequest) returns (ClaimResponse);
  rpc Ack(AckRequest) returns (AckResponse);
  rpc Nack(NackRequest) returns (NackResponse);
  rpc AckAndForward(AckAndForwardRequest) returns (AckAndForwardResponse);

  // Existing producer API
  rpc Push(PushRequest) returns (PushResponse);
  rpc PushBatch(PushBatchRequest) returns (PushBatchResponse);

  // NEW: State operations (standalone)
  rpc StateGet(StateGetRequest) returns (StateGetResponse);
  rpc StatePut(StatePutRequest) returns (StatePutResponse);

  // Existing heartbeat & stats
  rpc HeartbeatStream(stream HeartbeatPing) returns (stream HeartbeatPong);
  rpc GetStats(GetStatsRequest) returns (GetStatsResponse);
}

// Extended Claim with state read
message ClaimRequest {
  string queue = 1;
  int32 batch_size = 2;
  // NEW: optionally fetch state for these keys
  repeated string state_keys = 3;
  string state_namespace = 4;  // e.g., "{job_id}/{stage_id}"
}

message ClaimResponse {
  repeated ClaimedMessage messages = 1;
  // NEW: requested state values
  map<string, bytes> states = 2;
}

// Extended Ack with atomic state update
message AckRequest {
  string queue = 1;
  repeated string msg_ids = 2;
  // NEW: atomic state updates
  string state_namespace = 3;
  map<string, bytes> state_puts = 4;    // key -> value to set
  repeated string state_deletes = 5;     // keys to delete
}

// Standalone state operations (for non-message scenarios)
message StateGetRequest {
  string namespace = 1;
  repeated string keys = 2;
}

message StateGetResponse {
  map<string, bytes> values = 1;
}

message StatePutRequest {
  string namespace = 1;
  map<string, bytes> puts = 2;
  repeated string deletes = 3;
}

message StatePutResponse {}
```

#### 4.4 Key Schema for State

```
State entries:
  state:{namespace}:{key} → {value: bytes}

Example:
  state:job_123/stage_dedupe:sha256_abc123 → "1"
  state:job_123/stage_cc:label:doc_001 → "cluster_42"
```

#### 4.5 Atomic Ack + State Update

The critical operation is atomically acknowledging messages AND updating state:

```
Worker                          Server
   │                               │
   │── Ack(                        │
   │     queue="input",            │
   │     msg_ids=["m1", "m2"],     │
   │     state_namespace="j1/s1",  │
   │     state_puts={              │
   │       "key_hash_1": "1",      │
   │       "key_hash_2": "1"       │
   │     }                         │
   │   ) ─────────────────────────▶│
   │                               │ ATOMIC batch write to SlateDB:
   │                               │ 1. Delete claimed:input:m1
   │                               │ 2. Delete claimed:input:m2
   │                               │ 3. Delete msg:input:m1
   │                               │ 4. Delete msg:input:m2
   │                               │ 5. Put state:j1/s1:key_hash_1 → "1"
   │                               │ 6. Put state:j1/s1:key_hash_2 → "1"
   │◀── AckResponse ───────────────│
```

**Why atomic**: If worker crashes between ack and state update:
- Without atomicity: Message acked but state not updated → duplicate not detected on retry
- With atomicity: Either both succeed or both fail → consistent

#### 4.6 Example: Dedup with Integrated State

```python
class HashDedupeOperator(Operator):
    async def process_batch(self):
        # 1. Claim messages AND fetch state for dedup keys
        key_hashes = []  # Will be populated after seeing messages

        # First claim without state (we don't know keys yet)
        messages = await self.client.claim(queue=self.input_queue, batch_size=100)

        # Compute key hashes
        key_hashes = [self._compute_key_hash(m.payload) for m in messages]

        # 2. Batch fetch state for all keys
        states = await self.client.state_get(
            namespace=f"{self.job_id}/{self.stage_id}",
            keys=key_hashes
        )

        # 3. Filter duplicates (keys already in state)
        new_messages = []
        new_keys = {}
        for msg, key_hash in zip(messages, key_hashes):
            if key_hash not in states:
                new_messages.append(msg)
                new_keys[key_hash] = b"1"

        # 4. Process non-duplicate messages
        outputs = [self._process(m) for m in new_messages]

        # 5. Atomic: Ack all messages + update state + forward outputs
        await self.client.ack_and_forward(
            upstream_queue=self.input_queue,
            upstream_msg_ids=[m.msg_id for m in messages],
            downstream_queue=self.output_queue,
            downstream_payloads=outputs,
            state_namespace=f"{self.job_id}/{self.stage_id}",
            state_puts=new_keys
        )
```

#### 4.7 Shuffle Without Queue Partitions

With integrated state, shuffle operations don't need queue partitions:

```
旧模型 (需要 queue partition):
  数据按 key hash 路由到 queue partition
  Worker 固定消费特定 partition
  同 key 数据必然被同一 worker 处理

新模型 (无 queue partition):
  所有数据进入同一 queue
  任意 worker claim 任意消息
  State 按 key hash 组织 (在 server 端)
  同 key 的 state 操作由 server 串行化
```

**Benefits**:
- No partition skew (work-stealing自动负载均衡)
- Simpler API (no partition concepts)
- Single queue depth to monitor

**Trade-off**:
- All state operations go through server (potential bottleneck for very high-frequency state access)
- Mitigation: Batch state operations, use claim_with_state pattern

#### 4.8 Handling Partition Skew (Salted Aggregation)

For aggregations where same key must be combined, use two-phase approach:

```
Phase 1 (Partial Aggregate - 分散热点):
  key -> hash(key + salt) 分散到不同 worker
  每个 worker 做局部聚合
  State key: "{key}:{salt}" -> partial_result

Phase 2 (Final Aggregate - 合并结果):
  合并同一 key 的所有 partial results
  State key: "{key}" -> final_result
```

```python
@dataclass
class SaltedGroupByConfig(OperatorConfig):
    group_keys: List[str]
    agg_func: str  # "sum", "count", "min", "max" (必须可结合)
    salt_factor: int = 10  # 热点 key 分散成 10 份
```

### 5. Startup Recovery

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

### 6. Garbage Collection (GC)

Background GC task cleans up acked messages after the retention period:

```rust
// recovery.rs - GcTask
async fn run_gc(storage: &AnvilStorage, retention_ns: u64) {
    let now = now_nanos();
    let cutoff = now - retention_ns;

    // Scan all acked messages
    for (queue, timestamp_ns, msg_id) in storage.scan_all_acked().await {
        if timestamp_ns < cutoff {
            // Delete acked index + message body
            storage.delete(&acked_key(&queue, timestamp_ns, &msg_id)).await;
            storage.delete(&msg_key(&queue, &msg_id)).await;
        }
    }
}
```

**Configuration** (`AnvilConfig`):
```rust
pub struct AnvilConfig {
    // ... other fields ...
    pub acked_retention_secs: f64,  // Default: 3600.0 (1 hour)
    pub gc_interval_secs: f64,       // Default: 60.0 (1 minute)
}
```

**Python API**:
```python
broker = AnvilBrokerManager(
    db_path="file:///tmp/anvil",
    acked_retention_secs=3600.0,  # Keep acked messages for 1 hour
    gc_interval_secs=60.0,        # Run GC every minute
)
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
lib/anvil-rs/
├── Cargo.toml
├── build.rs                    # protobuf compilation
├── proto/
│   └── anvil.proto
├── src/
│   ├── lib.rs                  # PyO3 entry point
│   ├── server.rs               # gRPC server
│   ├── service.rs              # AnvilService implementation
│   ├── storage.rs              # SlateDB wrapper (messages)
│   ├── state.rs                # State store (integrated state management)
│   ├── types.rs                # Data structures
│   └── recovery.rs             # Timeout recovery logic
└── python/
    └── anvil_py/
        ├── __init__.py
        └── client.py           # AnvilClient with state support
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
# === Broker (Driver side) ===
from anvil_py import AnvilBroker, BrokerConfig

config = BrokerConfig(
    db_path="s3://bucket/anvil",  # or file:///path
    host="0.0.0.0",
    port=0,                           # auto-assign
    claim_timeout_secs=60.0,
    recovery_interval_secs=10.0,
    acked_retention_secs=3600.0,      # GC: 1 hour retention
    gc_interval_secs=60.0,            # GC: run every minute
)

broker = AnvilBroker(config)
broker.start()
print(f"Anvil started at port {broker.get_port()}")
broker.stop()

# === Client (Worker side) ===
from anvil_py.client import AnvilClient

client = AnvilClient("localhost:50051", worker_id="worker-1")
client.start()

# Producer
msg_id = client.push("queue", b"payload")

# Consumer
messages = client.claim("queue", batch_size=10, timeout_ms=1000)
client.ack("queue", [m.msg_id for m in messages])

# Atomic ack + forward + state update
client.ack_and_forward(
    upstream_queue="input",
    upstream_msg_ids=["m1", "m2"],
    downstream_queue="output",
    downstream_payloads=[b"out1", b"out2"],
    state_namespace="job1/stage1",
    state_puts={"key1": b"value1"},
)

# State operations
values = client.state_get("job1/stage1", ["key1", "key2"])
client.state_put("job1/stage1", puts={"key3": b"value3"}, deletes=["key1"])

client.stop()
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

### 10. State Operation Bottleneck

**Issue**: All state operations go through the single Anvil Server, which could become a bottleneck for high-frequency state access.

**Mitigation**:
- Batch state operations (read multiple keys in one RPC)
- Use `claim_with_state` pattern to combine message fetch + state read
- Use `ack_with_state` pattern to combine ack + state write
- For very high throughput needs, consider sharding by state namespace across multiple servers

```rust
// Server-side optimization: batch state operations
async fn state_get(&self, req: StateGetRequest) -> StateGetResponse {
    // Use SlateDB multi-get for efficiency
    let values = self.db.multi_get(
        req.keys.iter().map(|k| format!("state:{}:{}", req.namespace, k))
    ).await;

    StateGetResponse { values }
}
```

**Capacity estimates**:
- SlateDB read: ~100k ops/sec (mostly cache hits)
- SlateDB write: ~50k ops/sec (batched)
- gRPC overhead: minimal with connection pooling
- Expected bottleneck: ~50k state updates/sec per server

For most data processing workloads (batch processing, ETL), this is sufficient. For streaming with very high key cardinality, may need sharding.

### 11. State Namespace Isolation

**Issue**: Different jobs/stages should not see each other's state.

**Mitigation**:
- Mandatory `namespace` parameter in all state operations
- Namespace format: `{job_id}/{stage_id}`
- Server validates namespace format
- Optional: ACL for cross-job state access (future)

```rust
fn validate_namespace(namespace: &str) -> Result<(), Status> {
    let parts: Vec<&str> = namespace.split('/').collect();
    if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
        return Err(Status::invalid_argument("Invalid namespace format"));
    }
    Ok(())
}
```

### 12. Multi-Job Isolation

**Issue**: Should multiple jobs share one Anvil instance?

**Recommendation**:
- Each job gets its own AnvilBroker instance
- Different SlateDB paths for isolation
- Simpler resource management and debugging

```python
# Job 1
broker1 = AnvilBroker()
broker1.start(db_path="s3://bucket/job1/queue")

# Job 2
broker2 = AnvilBroker()
broker2.start(db_path="s3://bucket/job2/queue")
```

---

## Migration Plan

### Phase 1: Implement Anvil (Rust)

1. Create `lib/anvil-rs/` project
2. Implement gRPC service with tonic
3. Integrate SlateDB for persistence
4. Add PyO3 bindings
5. Unit tests for all operations

### Phase 2: Python Client

1. Create `AnvilClient` class using `grpcio`
2. Implement heartbeat streaming
3. Add connection retry logic
4. Integration tests with Rust server

### Phase 3: Integrate with Solstice

1. Update `StageMaster` to use `AnvilBroker`
2. Update `StageWorker` to use `AnvilClient`
3. Remove partition-related code from managers
4. Update recovery logic

### Phase 4: Remove Old Code

1. Remove `PartitionManager`
2. Simplify `RecoveryManager` (no partition tracking)
3. Remove Tansu broker management code
4. Update tests

---

## Comparison with Current Design

| Aspect | Current (Tansu/Kafka) | New (Anvil) |
|--------|----------------------|-----------------|
| Parallelism unit | Partition | Message |
| Consumer model | 1 partition : 1 consumer | N consumers : 1 queue |
| Load balancing | Manual partition assignment | Automatic (claim-based) |
| Cross-stage transaction | None (offset commit only) | Atomic ack + forward |
| Worker failure recovery | Partition reassignment | Message timeout + reclaim |
| Code complexity | High (PartitionManager, etc.) | Low (single queue model) |
| Persistence | Tansu storage backends | SlateDB (S3) |
| State management | Separate SlateDB per partition | Integrated in Anvil Server |
| State write model | Worker writes directly | Server-mediated (single writer) |
| Shuffle support | Queue partitions | State-based (any worker, any key) |
| Partition skew | Manual rebalancing | Work-stealing (automatic) |

---

## Open Questions

1. **Queue depth limits**: What should be the default max queue depth? How to handle backpressure signal to upstream?

2. **Batch size tuning**: What's the optimal batch size for `Claim`? Should it be adaptive?

3. **Payload storage**: Keep using Ray Object Store for payloads, or move to SlateDB?

4. **Metrics**: What metrics should the Anvil expose? (queue depth, claim rate, ack latency, etc.)

5. **State TTL**: Should state entries have automatic expiration? Useful for:
   - Dedup state cleanup after job completion
   - Preventing unbounded state growth
   - Options: job-scoped cleanup, TTL per namespace, manual cleanup API

6. **State size limits**: What's the max value size for state entries? Large values (>1MB) may impact performance.
   - Option A: Reject large values at API level
   - Option B: Store large values in separate storage (S3) with reference in state

7. **State migration**: When job restarts with different parallelism, how to handle existing state?
   - Current design: State is key-based, not worker-based, so no migration needed
   - Question: Should we support state snapshot/restore for debugging?

8. **Concurrent state updates**: What happens if two workers update the same state key simultaneously?
   - Current design: Server serializes all writes, last-write-wins
   - Question: Do we need CAS (compare-and-swap) operations for some use cases?

9. **State read consistency**: Should state reads be strongly consistent or eventually consistent?
   - Current design: Strong consistency (single SlateDB writer)
   - Trade-off: Latency vs consistency

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
│  Anvil Server                                                   │
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
- Existing design: `deprecated/tansu-pyo3-binding.md`
- Existing design: `deprecated/exactly-once-semantics.md`

---

## Changelog

- **2026-02-01 (continued)**: CC operator refactoring for 10B+ scale
  - Removed local SlateDB state store from operators (shuffle.py, connected_components.py)
  - Edges now flow through payload (Arrow tables) - scales to 10B+ records
  - Labels tracked via @master_callable aggregation
  - Future: labels stored via Anvil state API (state_get/state_put)
  - Removed: state_store, _ensure_partition_acquired(), SlateDB imports from operators
  - Updated: CCIterateOperator, CCIterateMaster, related tests
  - See "Connected Components Operator Redesign" section below
- **2026-02-01**: Implementation complete
  - Added GC-based ack mechanism (safer than immediate delete)
  - Added unified worker exit (replaced EOF messages)
  - State API fully implemented
  - Solstice integration complete
- **2026-01-31**: Initial design proposed

---

## Connected Components Operator Redesign (2026-02-01)

### Problem Statement

The original CC operator design used local SlateDB partition state stores for labels and edges:
- `label:{doc_id}` → current label
- `edges:{doc_id}` → comma-separated neighbor doc_ids
- `__doc_ids__` → set of all doc_ids per partition

This design had critical issues:

1. **Doesn't scale to 10B+ records**: SlateDB per partition means N partitions × M records = very large state
2. **Partition conflicts with Anvil work-stealing**: When workers can process any message, partition-based state leads to multi-writer conflicts (SlateDB is single-writer)
3. **Complexity**: `_ensure_partition_acquired()` calls throughout the codebase

### New Design: Payload-Based Iteration

**Key insight**: Edges are the bulk of the data (10B-100B for dedup). Labels are small (one per doc, 1-10B).

**Solution**:
- **Edges → Payload**: Flow through Arrow tables, scales to any size
- **Labels → Tracked via iteration**: No external state needed for basic convergence
- **Future: Labels → Anvil State API**: For advanced use cases

### Architecture

```
Old Design (SlateDB partitions):
  CCIterateOperator
    │
    ├── state_store (SlateDB per partition)
    │     ├── label:{doc_id} → label
    │     └── edges:{doc_id} → neighbors
    │
    └── _ensure_partition_acquired()

New Design (Payload-based):
  CCIterateOperator
    │
    ├── process_data(table) → table with edges column
    │     Input:  (doc_id, neighbor_label, current_label?, edges?)
    │     Output: (doc_id, label, edges, changed)
    │
    ├── @master_callable get_iteration_changes() → int
    │
    └── @master_callable recompute_labels(edges_data) → int
```

### Data Flow

**Iteration 1**:
```
Input:  Candidate pairs (doc_id_1, doc_id_2)
        ↓
CCInitOperator: Generate bidirectional messages
        ↓
Output: (doc_id, neighbor_label)  ← neighbor's label (initially = doc_id)
        ↓
CCIterateOperator: Compute labels, collect edges
        ↓
Output: (doc_id, label, edges, changed)  ← edges in payload!
```

**Iteration N (N > 1)**:
```
Input:  Output from iteration N-1 (doc_id, label, edges)
        ↓
CCIterateOperator.recompute_labels(): New labels from edges
        ↓
Output: Updated labels, convergence check via @master_callable
```

### Implementation Changes

**CCIterateConfig**:
```python
@dataclass
class CCIterateConfig(ShuffleOperatorConfig):
    doc_id_column: str = "doc_id"
    neighbor_label_column: str = "neighbor_label"
    current_label_column: str = "current_label"
    edges_column: str = "edges"  # NEW: edges in payload
    max_iterations: int = 100
    convergence_threshold: int = 0
    # REMOVED: state_store_path
```

**CCIterateOperator**:
```python
@operator(CCIterateConfig)
class CCIterateOperator(ShuffleOperator):
    def __init__(self, config, runtime):
        super().__init__(config, runtime)
        self._iteration_changes: int = 0  # In-memory counter
        # REMOVED: state_store, _acquired_partitions

    def process_data(self, table) -> pa.Table:
        # Input: (doc_id, neighbor_label, current_label?, edges?)
        # Output: (doc_id, label, edges, changed)
        #
        # Edges are merged and output for next iteration
        ...
        return pa.table({
            "doc_id": ...,
            "label": ...,
            "edges": ...,  # Comma-separated, flows to next iteration
            "changed": ...,
        })

    @master_callable
    def get_iteration_changes(self) -> int:
        return self._iteration_changes

    @master_callable
    def recompute_labels(self, edges_data: List[Dict]) -> int:
        # For iteration 2+, compute new labels from edges
        ...
```

**CCIterateMaster**:
```python
class CCIterateMaster(StageMaster):
    async def run(self):
        # Iteration 1: normal queue processing
        await super().run()
        total_changes = await self._aggregate_worker_changes()

        # Iteration 2+: coordinate recomputation
        while not converged and iteration < max:
            await self._reset_worker_iterations()
            total_changes = await self._recompute_worker_iterations()
            # Check convergence

    async def _aggregate_worker_changes(self) -> int:
        # Sum get_iteration_changes() from all workers
        ...

    # REMOVED: _read_state_changes() (no more SlateDB reading)
```

### Benefits

1. **Scales to 10B+ records**: Edges flow through Arrow tables, no single-node state limit
2. **Works with Anvil work-stealing**: No partition ownership, any worker processes any message
3. **Simpler code**: No `_ensure_partition_acquired()`, no SlateDB lifecycle management
4. **Future-proof**: Can add Anvil state API for labels when needed

### Trade-offs

1. **Iteration 2+ coordination**: Master needs to coordinate data flow back to workers
   - Mitigation: Output queue becomes input for next iteration (queue loopback)

2. **State persistence**: Labels not persisted between iterations (in-memory only)
   - Mitigation: For checkpointing, can output labels to queue or external storage

3. **Large gRPC payloads**: Edges data in payload may be large
   - Mitigation: Already using payload store (Ray Object Store) for large data

### Future: Anvil State API for Labels

For use cases requiring label persistence:
```python
# Worker gets label from state
label = await self.queue_client.state_get(f"job1/cc", f"label:{doc_id}")

# Atomic: ack message + update label
await self.queue_client.ack_with_state(
    queue="cc_input",
    msg_ids=[msg.msg_id],
    state_namespace="job1/cc",
    state_puts={f"label:{doc_id}": new_label.encode()},
)
```

This would require:
1. Anvil state API integration in operators
2. State cleanup after job completion
3. State size limits (labels are small, ~100 bytes per doc)

---

*Last Updated: 2026-02-01*
