# Anvil Performance TODO

Track performance improvements for the Anvil broker and client.

> **Last Updated**: 2026-03-31
> **Scope**: `lib/anvil-rs/`, `engine/_internal/queue/`

---

## Completed

- [x] **Atomic counter refactor** (2026-03-31)
  - Replaced SerializableSnapshot transactions with in-memory `AtomicU64` + `WriteBatch`
  - Split `meta:{queue}` into 6 independent counter keys (each with one writer class)
  - CAS loop for claim, `fetch_add` for push/ack/nack — zero transaction conflicts
  - Removed `claim_lock` from service layer (CAS replaces it)
  - **Impact**: 500 workers no longer stall (was completely stuck before)
  - **Tested**: 3 Rust DST stress tests (500 concurrent claims, push+claim+ack, group claim) + 5 Python multiprocess E2E tests

- [x] **Rust gRPC Client (PyO3)** (2026-03-31)
  - `AnvilRustClient` in `src/client.rs` — tonic client exposed via PyO3
  - All 27 RPCs, background heartbeat (tokio), GIL released during all calls
  - Deleted Python grpcio client (`client.py`, `anvil_pb2.py`, `anvil_pb2_grpc.py`)
  - `AnvilQueueClient` uses Rust client directly (no fallback)
  - **Impact**: 10-20x throughput per connection (prost ~0.1ms vs Python protobuf ~8ms)

- [x] **Protocol v2: Unified Hot-Path RPCs** (2026-03-31)
  - Unified `Claim` RPC (replaces `Claim` + `ClaimFromGroup`) via `oneof source { queue, group }`
  - Unified `Complete` RPC (replaces `Ack` + `Nack` + `AckAndForward` + `AckAndScatter`) via `oneof action { ack, nack, forward, scatter }`
  - Unified `Push` RPC (replaces `Push` + `PushBatch`) with `repeated bytes payloads`
  - Slim `ClaimMessage`: dropped `queue` (caller knows) and `created_at` (unused by workers), `claim_token` inline
  - Removed dead fields: `NackReason`, `max_depth`, `message_ttl_secs`, `delay_ms`, `force`
  - **Wire reduction**: ~30-40% per hot-path RPC

- [x] **Claim+Ack Combined RPC** (2026-03-31)
  - `ClaimAndComplete` RPC: complete previous batch + claim next batch in one round trip
  - Server executes complete then claim atomically
  - **Impact**: Halves RPC count for steady-state workers

---

## TODO

### P1 — Embedded Mode (No gRPC for Same-Node Workers)

- [ ] Allow workers on the same node as the broker to call storage directly

**Current**: All workers go through gRPC, even if co-located with the broker.

**Target**: Workers detect same-node broker and use direct Rust storage API via PyO3 (no network, no protobuf serialization).

```
Same-node worker → PyO3 → Rust storage.claim_messages() direct call
                   ↑ ~0.02ms per operation (vs ~20ms via gRPC)
```

**Implementation**:
1. `AnvilStorage` exposed via PyO3 as `AnvilLocalClient`
2. Same API as remote client, but calls storage directly
3. `AnvilQueueClient` auto-detects: if broker is local, use embedded mode
4. Requires shared `Arc<AnvilStorage>` between broker and client (same process)

**Blocker**: Workers run in separate Ray actor processes, not the same process as the broker. Would need either:
- (a) Multi-process shared storage (mmap-backed SlateDB) — complex
- (b) Run broker as a thread in each worker process — defeats single-broker design
- (c) Unix domain socket instead of TCP (lower overhead, same architecture) — simpler

**Practical first step**: Unix domain socket transport for same-node connections (~5x faster than TCP on localhost).

**Estimated impact**: 50-100x for same-node, but architecture-dependent

### P2 — Streaming Claim (Server Push)

- [ ] Replace poll-based claim with server-side streaming

**Current**: Workers poll `claim()` every 100-200ms. Empty polls waste bandwidth and add latency.

**Target**: Bidirectional streaming — worker opens a stream, server pushes messages as they arrive.

**Impact**: Lower latency (no polling delay), lower RPC overhead, better resource utilization.

---

## Protocol Redundancy Analysis (v2 status)

| Redundancy | Status |
|---|---|
| `AckAndForward` = special case of `AckAndScatter` | **Fixed** — `Complete` with `oneof action` |
| `Push` = `PushBatch` with size=1 | **Fixed** — single `Push` with `repeated bytes` |
| `Claim` vs `ClaimFromGroup` | **Fixed** — unified `Claim` with `oneof source` |
| `Message.queue` in claim response | **Fixed** — dropped from `ClaimMessage` |
| `Message.created_at` in claim response | **Fixed** — dropped from `ClaimMessage` |
| `NackReason` enum | **Fixed** — removed |
| `max_depth`, `ttl`, `delay_ms`, `force` | **Fixed** — removed |
| `worker_id` + `lease_id` on every RPC | TODO — connection-level identity |
| `claim_tokens` as UUID strings | TODO — `u64` claim_id (needs storage change) |

---

## Performance Reference

### Before (Python grpcio client, 2026-03-31)

| Layer | Throughput | Notes |
|---|---|---|
| Rust storage (DST, no gRPC) | >50,000 msgs/s | 500 concurrent tokio tasks |
| Python gRPC, single thread, batch=10 | 49 msgs/s | Bottleneck: Python protobuf |
| Python gRPC, 500 processes, batch=5 | ~46 msgs/s aggregate | Process spawn overhead dominates |

### After (Rust tonic client, Protocol v2, 2026-03-31)

1000-client benchmark (`cargo test --release bench_1000_clients_stress`):

| Operation | Clients | Total Msgs | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|---|---|
| Push (batch=10, 256B payload) | 1000 | 100K | **52,307 msgs/s** | 115ms | 335ms | 340ms |
| Claim+Ack (batch=10) | 1000 | 100K | **18,373 msgs/s** | 448ms | 708ms | 913ms |
| Mixed (500 prod + 500 cons) | 1000 | 50K | **14,886 msgs/s** | 232ms | 616ms | 695ms |

**Benchmark**: `cd lib/anvil-rs && cargo test --release bench_full_suite -- --nocapture --ignored`

**Key insight**: Push throughput (52K msgs/s) is now at par with raw storage (50K), meaning gRPC overhead is negligible. Claim+Ack at 18K msgs/s with 1000 concurrent connections — a ~375x improvement over the old Python client (49 msgs/s single-thread).
