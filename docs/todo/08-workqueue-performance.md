# WorkQueue Performance TODO

Track performance improvements for the WorkQueue broker and client.

> **Last Updated**: 2026-03-31
> **Scope**: `lib/workqueue-rs/`, `engine/_internal/queue/`

---

## Completed

- [x] **Atomic counter refactor** (2026-03-31)
  - Replaced SerializableSnapshot transactions with in-memory `AtomicU64` + `WriteBatch`
  - Split `meta:{queue}` into 6 independent counter keys (each with one writer class)
  - CAS loop for claim, `fetch_add` for push/ack/nack — zero transaction conflicts
  - Removed `claim_lock` from service layer (CAS replaces it)
  - **Impact**: 500 workers no longer stall (was completely stuck before)
  - **Tested**: 3 Rust DST stress tests (500 concurrent claims, push+claim+ack, group claim) + 5 Python multiprocess E2E tests

---

## TODO

### P0 — Rust gRPC Client (PyO3)

- [ ] Replace Python grpcio client with Rust tonic client exposed via PyO3

**Current architecture** (Python gRPC, ~49 msgs/s single-thread):
```
Python Worker → Python grpcio (pb2 ser/deser) → TCP → Rust tonic server
                   ↑ ~60-70% of latency here
```

**Target architecture** (Rust gRPC, estimated ~500-1000 msgs/s):
```
Python Worker → PyO3 call (GIL released) → Rust tonic client (prost) → TCP → Rust tonic server
```

**Why**: Python protobuf serialization/deserialization dominates RPC latency (~8ms out of ~20ms per RPC). Rust prost does the same in ~0.1ms. Additionally, GIL is released for the entire Rust call duration, enabling true parallelism in threaded Python.

**Implementation**:
1. Add `WorkQueueRustClient` in `lib/workqueue-rs/src/client.rs` using tonic
2. PyO3 bindings: `claim()`, `ack()`, `push()`, `claim_from_group()`, `ack_and_scatter()`
3. Async internally (tokio), sync Python API (block on tokio runtime)
4. Connection pooling and reconnection in Rust
5. Replace `workqueue_py.client.WorkQueueClient` (Python) with new Rust client
6. Keep Python gRPC stubs for debugging/admin tools

**Bonus**: combine `claim + ack` into a single PyO3 call (`claim_process_ack`) to halve RPC round trips.

**Estimated impact**: 10-20x throughput per connection

### P1 — Embedded Mode (No gRPC for Same-Node Workers)

- [ ] Allow workers on the same node as the broker to call storage directly

**Current**: All workers go through gRPC, even if co-located with the broker.

**Target**: Workers detect same-node broker and use direct Rust storage API via PyO3 (no network, no protobuf serialization).

```
Same-node worker → PyO3 → Rust storage.claim_messages() direct call
                   ↑ ~0.02ms per operation (vs ~20ms via gRPC)
```

**Implementation**:
1. `WorkQueueStorage` exposed via PyO3 as `WorkQueueLocalClient`
2. Same API as remote client, but calls storage directly
3. `WorkQueueQueueClient` auto-detects: if broker is local, use embedded mode
4. Requires shared `Arc<WorkQueueStorage>` between broker and client (same process)

**Blocker**: Workers run in separate Ray actor processes, not the same process as the broker. Would need either:
- (a) Multi-process shared storage (mmap-backed SlateDB) — complex
- (b) Run broker as a thread in each worker process — defeats single-broker design
- (c) Unix domain socket instead of TCP (lower overhead, same architecture) — simpler

**Practical first step**: Unix domain socket transport for same-node connections (~5x faster than TCP on localhost).

**Estimated impact**: 50-100x for same-node, but architecture-dependent

### P1 — Claim+Ack Combined RPC

- [ ] Add `ClaimProcessAck` RPC that combines claim and ack into one round trip

**Current**: Worker does 2 RPCs per message batch: `claim()` → process → `ack()`.

**Target**: For the common case (claim, process immediately, ack), combine into one RPC that returns claimed messages and accepts ack for previous batch in the same call.

```protobuf
rpc ClaimAndAckPrevious(ClaimAndAckRequest) returns (ClaimAndAckResponse);

message ClaimAndAckRequest {
  // Ack previous batch
  string ack_queue = 1;
  repeated string ack_msg_ids = 2;
  repeated string ack_claim_tokens = 3;
  // Claim next batch
  string claim_queue = 4;
  int32 batch_size = 5;
}
```

**Impact**: Halves RPC count, ~2x throughput improvement.

### P1 — Protocol v2: Unified API + Compact Wire Format

- [ ] Redesign gRPC protocol to eliminate redundancy and simplify API surface

**Current**: 18 RPC methods with significant duplication:
```
Queue API:  Claim / Ack / AckAndForward / MarkQueueFinished / IsQueueFinished / GetStats
Group API:  ClaimFromGroup / AckAndScatter / MarkGroupFinished / IsGroupFinished / GetGroupStats
Admin:      CreateQueue / DeleteQueue / CreateQueueGroup
Other:      Push / PushBatch / Nack / StateGet / StatePut / HeartbeatStream
```

**Target**: 6 RPC methods — QueueGroup is the only abstraction (`num_partitions=1` = plain queue):
```protobuf
service WorkQueue {
  rpc Claim(ClaimRequest) returns (ClaimResponse);         // unified claim + claim_from_group
  rpc Complete(CompleteRequest) returns (CompleteResponse); // unified ack + forward + scatter
  rpc Push(PushRequest) returns (PushResponse);            // push + push_batch
  rpc Control(ControlRequest) returns (ControlResponse);   // create/delete/finish/stats
  rpc HeartbeatStream(stream Ping) returns (stream Pong);
  rpc StateOp(StateRequest) returns (StateResponse);       // get + put
}
```

**Key design**:

1. **Unified `Claim`** — caller passes `queue_or_group` + optional `preferred_partitions`. Broker resolves whether it's a single queue or group internally. Non-shuffle stages (`num_partitions=1`) and shuffle stages use the same RPC.

2. **Unified `Complete`** — replaces `Ack`, `AckAndForward`, `AckAndScatter` with one RPC:
   ```protobuf
   message CompleteRequest {
     string upstream_queue = 1;
     repeated string msg_ids = 2;
     repeated string claim_tokens = 3;
     string worker_id = 4;
     string lease_id = 5;
     oneof downstream {
       ForwardPayload forward = 6;   // single-queue push (sink commit)
       ScatterPayload scatter = 7;   // multi-partition push (data flow)
     }
     StateUpdate state = 8;          // optional atomic state update
   }
   ```

3. **Compact wire format**:
   - `claim_token`: `u64` (8 bytes) instead of UUID string (36 bytes)
   - `Message` in claim response: drop `queue` (caller knows) and `created_at` (unused by workers)
   - Connection-level `worker_id`/`lease_id` (set once on heartbeat, not per-RPC)

4. **Remove dead fields**: `NackReason` (ignored), `max_depth` (unimplemented), `message_ttl_secs` (unimplemented), `delay_ms` (unimplemented), `force` delete (unused)

**Wire size reduction**: ~40% per RPC (compact tokens + drop redundant fields)

**Implementation**: Ship as v2 alongside Rust client (P0). Old Python client stays on v1 for backward compat. v1 proto deprecated but kept.

### P2 — Streaming Claim (Server Push)

- [ ] Replace poll-based claim with server-side streaming

**Current**: Workers poll `claim()` every 100-200ms. Empty polls waste bandwidth and add latency.

**Target**: Bidirectional streaming — worker opens a stream, server pushes messages as they arrive.

**Impact**: Lower latency (no polling delay), lower RPC overhead, better resource utilization.

---

## Protocol Redundancy Analysis

Current protocol issues documented for v2 design reference:

| Redundancy | Impact | Fix in v2 |
|---|---|---|
| `AckAndForward` = special case of `AckAndScatter` | 2 impls of same logic | `Complete` with `oneof downstream` |
| `Push` = `PushBatch` with size=1 | 2 RPC methods | Single `Push` with `repeated bytes` |
| `Claim` vs `ClaimFromGroup` | Caller must know queue type | Unified `Claim` — broker resolves |
| `worker_id` + `lease_id` on every RPC | ~100 bytes/RPC wasted | Connection-level identity |
| `claim_tokens` as UUID strings | 36 bytes × N per ack | `u64` claim_id (8 bytes) |
| `Message.queue` in claim response | Caller already knows | Drop from response |
| `Message.created_at` in claim response | Workers never use it | Drop from response |
| `NackReason` enum | Server ignores it | Remove (all nacks are retriable) |
| `max_depth`, `ttl`, `delay_ms`, `force` | Not implemented | Remove until implemented |

---

## Performance Reference

Measured on macOS M-series (2026-03-31):

| Layer | Throughput | Notes |
|---|---|---|
| Rust storage (DST, no gRPC) | >50,000 msgs/s | 500 concurrent tokio tasks |
| Python gRPC, single thread, batch=10 | 49 msgs/s | Bottleneck: Python protobuf |
| Python gRPC, 500 processes, batch=5 | ~46 msgs/s aggregate | Process spawn overhead dominates |
| Python gRPC, 200 processes (pytest) | ~46 msgs/s aggregate | Same — gRPC RTT is the limit |

**Key insight**: Rust server can handle >50K msgs/s. Python client caps at ~49 msgs/s per connection due to protobuf overhead. With 500 independent workers (Ray actors), aggregate is ~24K msgs/s — sufficient for current workloads but leaves headroom on the table.
