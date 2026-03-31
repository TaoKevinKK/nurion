---
globs:
  - lib/anvil-rs/**
  - engine/_internal/queue/**
---

# Anvil

> Rust-backed distributed queue. **All hot-path changes must maintain O(1) complexity.**

---

## Complexity Rules

| Operation | Complexity | Notes |
|---|---|---|
| `push` | O(1) | Increment push_seq counter, write pending key |
| `claim` | O(1) | Read pending at claim_seq, move to claimed |
| `ack` | O(1) | Delete claimed key, increment acked counter |
| `nack` | O(1) | Move back to pending |
| `state_get` / `state_put` | O(1) | Direct key lookup |
| `get_queue_stats` | O(1) | Read counters from QueueMeta |
| `recover_expired_claims` | O(n) | Background, every 10s — scan claimed keys |
| `gc_acked_messages` | O(n) | Background, every 60s — scan acked keys |
| `delete_queue` | O(n) | Admin only |

**Never introduce scans in O(1) operations. Use counters in QueueMeta instead.**

---

## Key Schema (RocksDB)

```
meta:{queue}                 → QueueMeta { claim_seq, push_seq, pending_count, claimed_count, acked_count }
pending:{queue}:{seq}        → msg_id
msg:{queue}:{msg_id}         → QueueMessage JSON
claimed:{queue}:{msg_id}     → ClaimInfo { worker_id, claimed_at, timeout_secs }
acked:{queue}:{ts}:{msg_id}  → ""  (prefix-deleted by GC)
state:{namespace}:{key}      → bytes
```

---

## Atomicity

- Use `WriteBatch` for all multi-key operations
- `ack_and_forward`: atomically ack upstream + push downstream — **never split this into two operations**
- `state_put + ack`: atomic — state updates are committed with ack, no partial writes

---

## Python Frontend

```python
# engine/_internal/queue/anvil.py
client = AnvilQueueClient(endpoint)
msg = await client.claim(queue_name, timeout_secs=30)
await client.ack_and_forward(msg.msg_id, output_queue, output_payload)
await client.nack(msg.msg_id)  # re-enqueue for retry

# State operations
await client.state_put(namespace="job_123", key="cursor", value=b"offset_100")
value = await client.state_get(namespace="job_123", key="cursor")
```

---

## Testing

```python
# In-memory queue for unit tests (no disk, no server process)
queue = Anvil(db_path="memory://")

# Or via fixture parameter:
def test_foo(anvil_db_path="memory://"):
    ...
```

---

## Rust Source Layout

```
lib/anvil-rs/src/
  storage.rs    → all persistent ops: push, claim, ack, nack, state, queue meta, GC, QueueGroup
  service.rs    → gRPC service implementation (AnvilService)
  server.rs     → broker inner (start/stop server)
  state.rs      → in-memory coordination (per-queue claim locks, lease tracking)
  types.rs      → data structures (QueueMessage, QueueMeta, QueueGroupMeta, etc.)
  recovery.rs   → background tasks: RecoveryTask (expire claims) + GcTask (delete acked)
  lib.rs        → PyO3 module entry + broker lifecycle
proto/
  anvil.proto → gRPC service + message definitions
python/         → Python package (PyO3 bindings)
```

See `lib/anvil-rs/AGENTS.md` for full constraints and future work (push_with_dedup).
