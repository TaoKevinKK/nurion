# NVMe WRITE_REPLICATED — Cross-Node Payload Replication

_Design document — April 2026_

---

## Status

**Status**: PROPOSED
**Author**: Enwei Jiao
**Created**: 2026-04-01
**Branch**: feat/bounded-queue-flow-control (will move to dedicated branch)

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Design Overview](#2-design-overview)
3. [Component Changes](#3-component-changes)
4. [Write Path](#4-write-path)
5. [Read Path](#5-read-path)
6. [Replica Target Selection](#6-replica-target-selection)
7. [Failure Analysis](#7-failure-analysis)
8. [Configuration](#8-configuration)
9. [Implementation Plan](#9-implementation-plan)
10. [Performance Impact](#10-performance-impact)
11. [Alternatives Considered](#11-alternatives-considered)

---

## 1. Problem Statement

`NvmeSplitPayloadStore` was designed with "NVMe is cache, S3 is truth" as a core
principle. Without S3, a node death means permanent data loss — downstream workers
nack, but upstream messages are already acked, so there is no recovery path.

This is the common case in **neocloud environments** (Lambda, CoreWeave, Fluidstack):
- GPU machines have strong CPUs, large memory, and fast NVMe SSDs
- No S3 or object storage in the same region
- Cross-region S3 adds 50-200ms latency — unacceptable for hot-path data

### What happens today (WRITE_BACK, no S3)

```
Stage A worker on Node 1:
  process_split() → store(payload) on Node 1 NVMe → ack_and_scatter()
                                                      ↓
Stage B worker on Node 2:
  claim() → get_with_hint(key, {flight: "grpc://node1:18815"})
                                    ↓
                          Flight do_get from Node 1 → OK ✓

If Node 1 dies:
  Stage B: get_with_hint → Flight fails → S3 fallback → no S3 → None
  → nack → re-enqueue to Stage A output queue
  → Stage A worker claims it → needs Stage A's INPUT payload
  → If that payload's node is also gone → cascade failure, data lost ❌
```

### Goal

Provide payload durability without S3 by replicating to a peer node's NVMe,
using the existing Arrow Flight infrastructure.

---

## 2. Design Overview

### One sentence

Write every payload to local NVMe **and** one remote peer's NVMe via Arrow Flight
`do_put`, so either copy can serve reads if the other node dies.

### What changes

| Component | Change | Lines (est.) |
|---|---|---|
| `WritePolicy` enum | Add `WRITE_REPLICATED` value | 3 |
| `_flight_server_proc.py` | Add `do_put()` method | ~30 |
| `FlightPayloadServer` | Add `do_put()` method (in-process server) | ~25 |
| `NvmeSplitPayloadStore` | Add `_store_replicated()`, peer discovery, replica tracking | ~60 |
| `NvmeSplitPayloadStore.get_with_hint` | Try replica endpoints before giving up | ~10 |
| `NvmeSplitPayloadStore.get_location` | Include `replicas` in payload_loc | 3 |
| **Total** | | **~130** |

### What does NOT change

- `SplitPayloadStore` ABC — no new abstract methods
- `StageWorker` — already uses `store()` / `get_with_hint()` / `get_location()`
- Anvil queue operations — no changes
- `ack_and_scatter` semantics — unchanged
- WRITE_BACK and WRITE_THROUGH paths — untouched

---

## 3. Component Changes

### 3.1 WritePolicy Enum

```python
# nvme_payload_store.py

class WritePolicy(str, Enum):
    WRITE_THROUGH = "write_through"
    WRITE_BACK = "write_back"
    WRITE_REPLICATED = "replicated"    # NEW
```

No S3 required. No S3 URI validation for this policy.

### 3.2 Flight Server — Add `do_put()`

The Flight server subprocess (`_flight_server_proc.py`) and in-process server
(`FlightPayloadServer`) both need a `do_put()` method to receive replicated
payloads.

```python
# _flight_server_proc.py — add to _FlightServer class

def do_put(
    self, context: flight.ServerCallContext, descriptor: flight.FlightDescriptor,
    reader: flight.MetadataRecordBatchReader, writer: flight.FlightMetadataWriter,
):
    """Receive a replicated payload and write to local NVMe."""
    key = descriptor.path[0].decode()
    safe = _sanitize_key(key)
    prefix = safe[:2] if len(safe) >= 2 else "00"

    # Read all batches into a table
    table = reader.read_all()

    # Write to first available job dir (scan root for job dirs)
    for entry in os.scandir(self._root_dir):
        if not entry.is_dir() or entry.name.startswith("."):
            continue
        out_dir = os.path.join(entry.path, prefix)
        os.makedirs(out_dir, exist_ok=True)
        final_path = os.path.join(out_dir, f"{safe}.arrow")
        tmp_path = final_path + f".tmp.replica.{os.getpid()}"
        with open(tmp_path, "wb") as f:
            w = ipc.new_file(f, table.schema)
            w.write_table(table)
            w.close()
        os.rename(tmp_path, final_path)
        return

    raise flight.FlightInternalError("No job directory found for replica write")
```

**Key decisions:**
- Writes to the **first** job dir found under root (replicas don't need multi-disk
  balancing — they're insurance copies, not primary storage)
- Uses the same hash-prefix bucketing as `NvmeDisk` for consistency
- Atomic write (tmp + rename) for crash safety
- No quota enforcement on replicas — they share the peer's disk space. If the
  peer is running low, the `do_put` will fail with ENOSPC, and the store
  gracefully degrades (see §7)

### 3.3 NvmeSplitPayloadStore — Replication Logic

```python
# nvme_payload_store.py — new __init__ parameters and _store_replicated method

class NvmeSplitPayloadStore(SplitPayloadStore):

    def __init__(
        self,
        root_dirs: List[str],
        job_id: str,
        write_policy: WritePolicy = WritePolicy.WRITE_BACK,
        s3_uri: Optional[str] = None,
        s3_options: Optional[Dict[str, Any]] = None,
        flight_port: int = 0,
        quota_bytes: Optional[int] = None,
        node_ip: Optional[str] = None,
        replica_count: int = 1,          # NEW: number of remote replicas
    ):
        # ... existing init ...
        self._replica_count = replica_count
        self._peer_endpoints: Optional[List[str]] = None  # lazily discovered
        self._peer_idx = 0  # round-robin counter
```

#### Peer Discovery

```python
def _discover_peers(self) -> List[str]:
    """Discover Flight server endpoints on other nodes in the Ray cluster.

    Uses ray.nodes() to find alive nodes, then constructs Flight endpoints
    using the well-known FLIGHT_SERVER_PORT.

    Returns a list of grpc:// endpoints excluding this node.
    """
    import ray

    my_ip = self._resolve_node_ip()
    peers = []
    for node in ray.nodes():
        if not node.get("Alive"):
            continue
        node_ip = node.get("NodeManagerAddress", "")
        if node_ip and node_ip != my_ip:
            peers.append(f"grpc://{node_ip}:{FLIGHT_SERVER_PORT}")
    return peers
```

#### Store (Replicated)

```python
def _store_replicated(self, key: str, payload: SplitPayload) -> str:
    """Write to local NVMe + replicate to peer node via Flight do_put."""
    assert self._disk_pool is not None

    # 1. Local write (sync, fast)
    self._disk_pool.write(key, payload)

    # 2. Replicate to peer (best-effort async)
    #    Failure does NOT block the write — local copy is the primary.
    if self._replica_count > 0:
        try:
            self._replicate_to_peer(key, payload)
        except Exception as e:
            logger.warning(f"Replica write failed for {key}: {e}")
            # Degrade gracefully: local-only, same as WRITE_BACK
            self._metrics.setdefault("replica_failures", 0)
            self._metrics["replica_failures"] += 1

    self._metrics["stored"] += 1
    return key

def _replicate_to_peer(self, key: str, payload: SplitPayload) -> None:
    """Send payload to a peer node's Flight server via do_put."""
    if self._peer_endpoints is None:
        self._peer_endpoints = self._discover_peers()
    if not self._peer_endpoints:
        return  # Single-node cluster, nothing to replicate

    # Round-robin peer selection
    peer = self._peer_endpoints[self._peer_idx % len(self._peer_endpoints)]
    self._peer_idx += 1

    client = self._get_or_create_client(peer)
    descriptor = flight.FlightDescriptor.for_path(key.encode())
    writer, _ = client.do_put(descriptor, payload.data.schema)
    writer.write_table(payload.data)
    writer.close()
```

#### Updated store() dispatch

```python
def store(self, key: str, payload: SplitPayload) -> str:
    self._ensure_initialized()
    assert self._disk_pool is not None

    if self._write_policy == WritePolicy.WRITE_THROUGH:
        return self._store_write_through(key, payload)
    elif self._write_policy == WritePolicy.WRITE_REPLICATED:
        return self._store_replicated(key, payload)
    else:
        return self._store_write_back(key, payload)
```

### 3.4 Location Metadata — Add Replicas

```python
def get_location(self, key: str) -> Optional[Dict[str, Any]]:
    self._ensure_initialized()
    loc: Dict[str, Any] = {"flight": self._flight_endpoint}
    if self._s3_root:
        safe = _sanitize_key(key)
        loc["s3"] = f"{self._s3_root}/{safe}.arrow"
    if (
        self._write_policy == WritePolicy.WRITE_REPLICATED
        and self._peer_endpoints
    ):
        # Record which peer got the replica (round-robin, so it's the
        # most recently selected peer). This lets readers go straight
        # to the right node.
        last_peer = self._peer_endpoints[
            (self._peer_idx - 1) % len(self._peer_endpoints)
        ]
        loc["replicas"] = [last_peer]
    return loc
```

Result:
```python
{
    "flight": "grpc://10.0.1.1:18815",       # primary
    "replicas": ["grpc://10.0.1.2:18815"],    # replica(s)
    "s3": None                                 # no S3 in neocloud
}
```

### 3.5 Read Path — Try Replicas

Extend `get_with_hint()` to try replica endpoints between primary Flight and S3:

```python
def get_with_hint(
    self, key: str, location_hint: Optional[Dict[str, Any]] = None
) -> Optional[SplitPayload]:
    self._ensure_initialized()
    assert self._disk_pool is not None

    # Tier 1: Local NVMe (mmap, ~0.1ms)
    result = self._disk_pool.read(key)
    if result is not None:
        self._metrics["local_hits"] += 1
        return result

    if not location_hint:
        if self._s3_fs:
            result = self._read_s3(key)
            if result is not None:
                self._metrics["s3_hits"] += 1
                return result
        return None

    # Tier 2: Primary remote NVMe via Flight
    endpoint = location_hint.get("flight")
    if endpoint and endpoint != self._flight_endpoint:
        try:
            table = self._flight_get(endpoint, key)
            if table is not None:
                self._metrics["remote_hits"] += 1
                return SplitPayload.from_arrow(table, split_id=key)
        except Exception as e:
            logger.debug(f"Flight get (primary) failed for {key}: {e}")

    # Tier 2.5: Replica NVMe via Flight  ← NEW
    for replica_ep in location_hint.get("replicas", []):
        if replica_ep == self._flight_endpoint:
            # Replica is on this node — already checked in Tier 1
            continue
        try:
            table = self._flight_get(replica_ep, key)
            if table is not None:
                self._metrics.setdefault("replica_hits", 0)
                self._metrics["replica_hits"] += 1
                return SplitPayload.from_arrow(table, split_id=key)
        except Exception as e:
            logger.debug(f"Flight get (replica) failed for {key}: {e}")

    # Tier 3: S3 (if configured)
    s3_path = location_hint.get("s3")
    if s3_path:
        result = self._read_s3_path(s3_path, key)
        if result is not None:
            self._metrics["s3_hits"] += 1
            return result

    return None
```

---

## 4. Write Path

```
_store_replicated(key, payload):
  ┌─────────────────────────────┐
  │ 1. local NVMe write (sync)  │ ← ~0.5ms (1MB), ~3ms (10MB)
  │    disk_pool.write(key, p)   │
  └──────────────┬──────────────┘
                 │
  ┌──────────────▼──────────────┐
  │ 2. Flight do_put to peer    │ ← ~1-3ms (1MB), ~5-10ms (10MB) on 100Gbps
  │    (sync, best-effort)      │
  │    failure → log + continue │
  └──────────────┬──────────────┘
                 │
  ┌──────────────▼──────────────┐
  │ 3. return key               │
  └─────────────────────────────┘
```

### Why sync replication, not async?

Async (fire-and-forget) replication would have a vulnerability window: if the
node dies between local write and async replica completion, the replica is
incomplete. Since the whole point of WRITE_REPLICATED is durability without S3,
the replica write must complete before `store()` returns.

### Why best-effort (catch exception)?

If the peer is temporarily overloaded or unreachable, blocking the entire pipeline
is worse than temporarily degrading to single-copy. The worker continues with
local-only storage (effectively WRITE_BACK), and subsequent writes retry
replication to the next peer (round-robin).

---

## 5. Read Path

```
get_with_hint(key, location_hint):

  Tier 1: Local NVMe (mmap)          ← ~0.1ms
    │ miss
    ▼
  Tier 2: Primary Flight endpoint    ← ~0.3-10ms
    │ fail (node dead)
    ▼
  Tier 2.5: Replica Flight endpoint  ← ~0.3-10ms  (NEW)
    │ fail (both nodes dead)
    ▼
  Tier 3: S3 (if configured)         ← ~50ms
    │ fail or not configured
    ▼
  return None → StageWorker nacks
```

**Common case (no failures):** Tier 1 local hit. Zero overhead from replication.

**Single node failure:** Tier 2 fails → Tier 2.5 succeeds from replica. No
recompute needed.

**Two nodes fail simultaneously:** Both tiers fail → nack → upstream recompute.
This is acceptable: probability is very low, and the existing nack mechanism
handles it.

---

## 6. Replica Target Selection

### Strategy: Round-Robin Across Alive Peers

```python
# peer_endpoints = ["grpc://10.0.1.2:18815", "grpc://10.0.1.3:18815", ...]
# Each store() call picks the next peer in round-robin order.

Payload 1 → replica on Node 2
Payload 2 → replica on Node 3
Payload 3 → replica on Node 2
...
```

### Why round-robin, not "replica on the node most likely to consume"?

1. We don't know at write time which node will consume the payload
2. Round-robin distributes replica load evenly across the cluster
3. No additional metadata or coordination needed
4. If a specific peer is down, the next `store()` naturally rotates to another

### Why not rack-aware or failure-domain-aware?

Neocloud machines are typically in one failure domain (one rack, one AZ). Rack
awareness would add complexity for no benefit. If the environment has distinct
failure domains, this can be extended later by grouping `ray.nodes()` by
`Resources` tags.

### Peer List Refresh

The peer list is discovered once at `_ensure_initialized()` time and does NOT
auto-refresh during the job. Rationale:
- Nodes rarely join/leave during a pipeline run
- Stale peers fail fast (Flight connection refused → caught, logged, next peer)
- Refreshing on every write would add `ray.nodes()` overhead

A manual refresh can be triggered via `_peer_endpoints = None` if needed.

---

## 7. Failure Analysis

| Failure | Behavior | Data Safe? |
|---|---|---|
| **Peer unreachable during write** | `_replicate_to_peer` raises → caught → local-only | ✅ local copy exists |
| **Peer NVMe full** | `do_put` fails ENOSPC → caught → local-only | ✅ local copy exists |
| **Primary node dies after write** | Reader tries primary (fail) → replica (success) | ✅ replica serves |
| **Replica node dies** | Reader tries primary (success) → never reaches replica | ✅ primary serves |
| **Both nodes die** | Reader gets None → nack → upstream recompute | ⚠️ recompute needed |
| **Worker crash (no node death)** | NVMe still on disk → same node worker reads locally | ✅ local + replica both exist |
| **Peer list stale (node left)** | Flight connect fails → caught → try next peer | ✅ degrades to local |
| **Single-node cluster** | `_peer_endpoints` is empty → no replica → same as WRITE_BACK | ✅ expected |

### Both-Nodes-Die Probability

For a cluster of N nodes, probability that 2 specific nodes (primary + replica)
both fail within the same recompute window:

```
P(both fail) = P(node fail)² ≈ (0.01)² = 0.0001  (1 in 10,000)

With round-robin spreading replicas across N-1 peers:
  Any given payload's replica is on 1 specific peer
  P(that specific pair fails) = P(primary) × P(replica) ≈ 0.0001

For 1M payloads across 32 nodes:
  Expected payloads affected by double-failure ≈ 1M × 0.0001 = 100
  These 100 payloads trigger nack + upstream recompute
  Upstream likely on a different node pair → recompute succeeds
```

Acceptable for all practical scenarios.

---

## 8. Configuration

### URI Format

```
# No S3, replicated
nvme:///mnt/nvme0/nurion,/mnt/nvme1/nurion?write_policy=replicated&replica_count=1

# With S3 + replicated (belt and suspenders)
nvme:///mnt/nvme0/nurion?write_policy=replicated&s3_fallback=s3://bucket/pfx
```

### Job-Level Config

```python
@dataclass
class JobConfig:
    payload_store_uri: str = "nvme:///mnt/nvme0/nurion?write_policy=replicated"
```

### Per-Stage Override

```python
@dataclass
class ExpensiveLLMConfig(OperatorConfig):
    payload_write_policy: str = "replicated"   # 30s/split, can't lose
    replica_count: int = 2                      # extra paranoid: 2 replicas

@dataclass
class CheapFilterConfig(OperatorConfig):
    payload_write_policy: str = "write_back"   # 5ms/split, recompute OK
```

### Defaults

| Parameter | Default | Notes |
|---|---|---|
| `write_policy` | `write_back` | Existing default, unchanged |
| `replica_count` | `1` | One remote copy (total 2 copies including local) |

---

## 9. Implementation Plan

### Phase 1: Core (MVP)

**Files to modify:**

1. **`_flight_server_proc.py`** — Add `do_put()` to `_FlightServer` class
   - Receive Arrow table via Flight protocol
   - Write to first job dir with atomic tmp+rename
   - ~30 lines

2. **`nvme_payload_store.py`** — Three changes:
   - a. Add `WRITE_REPLICATED` to `WritePolicy` enum (3 lines)
   - b. Add `_store_replicated()`, `_replicate_to_peer()`, `_discover_peers()` (~60 lines)
   - c. Extend `get_with_hint()` to try `replicas` list (~10 lines)
   - d. Extend `get_location()` to include replica endpoint (~5 lines)
   - e. Add `replica_count` to `__init__`, `__getstate__`, `__setstate__` (~10 lines)

3. **`FlightPayloadServer`** (in-process server) — Add `do_put()` for unit tests
   - Same logic as subprocess version
   - ~25 lines

**Estimated total: ~130 lines across 2 files.**

### Phase 2: Polish (Optional, Post-MVP)

- Async replication via ThreadPoolExecutor (overlap compute + replica write)
- Peer health tracking (skip known-dead peers without waiting for connect timeout)
- Metrics dashboard integration (replica_hits, replica_failures)
- `replica_count=2` support (write to 2 peers)

### Testing

```python
# Unit test: FlightPayloadServer do_put
def test_flight_do_put():
    server = FlightPayloadServer.get_or_start(job_dirs=[tmpdir], port=0)
    # Flight client writes to server
    client = flight.connect(f"grpc://localhost:{server.port}")
    desc = flight.FlightDescriptor.for_path(b"test_key")
    writer, _ = client.do_put(desc, table.schema)
    writer.write_table(table)
    writer.close()
    # Verify file exists on disk
    assert (Path(tmpdir) / "te" / "test_key.arrow").exists()

# Unit test: store + get_with_hint with replica
def test_write_replicated_read_from_replica():
    store1 = NvmeSplitPayloadStore(root_dirs=[node1_dir], ...)
    store2 = NvmeSplitPayloadStore(root_dirs=[node2_dir], ...)
    # store1 writes with replication to store2's Flight server
    store1.store("key1", payload)
    loc = store1.get_location("key1")
    # Simulate node1 dead: store2 reads from replica
    result = store2.get_with_hint("key1", loc)
    assert result is not None

# Distributed test (with Ray cluster)
@pytest.mark.distributed
def test_write_replicated_node_failure():
    # Deploy on 2+ nodes, write with replication
    # Kill primary node, verify reads succeed from replica
```

---

## 10. Performance Impact

### Write Latency

| Payload Size | WRITE_BACK | WRITE_REPLICATED | Overhead |
|---|---|---|---|
| 1 MB | 0.5ms | 1.5ms (+1ms Flight) | +1ms |
| 10 MB | 3ms | 8ms (+5ms Flight) | +5ms |
| 100 MB | 20ms | 30ms (+10ms Flight) | +10ms |

Flight overhead on 100Gbps RDMA network. On 25Gbps, multiply Flight portion by ~4x.

### When overhead matters

For stages with compute time >> write time (LLM inference 1-60s), replication
overhead is negligible (< 1%). For cheap stages (filter 5ms), replication
doubles the write time. Use per-stage policy to avoid this:

```python
# Expensive: replicate (overhead invisible)
ExpensiveLLMConfig(payload_write_policy="replicated")

# Cheap: write_back (accept recompute risk)
CheapFilterConfig(payload_write_policy="write_back")
```

### Read Latency (No Change for Common Case)

Read always tries local NVMe first. Replication adds zero overhead to the
read path unless the primary node is down (in which case the alternative is
nack + recompute, which is far more expensive).

### Space Overhead

Each replicated payload exists on 2 nodes (local + 1 peer). With round-robin,
replica space is evenly distributed. For a 32-node cluster:

```
Each node stores:
  - Its own payloads (primary)
  - ~1/31 of every other node's payloads (replicas)
  ≈ 2x local NVMe usage vs WRITE_BACK

NVMe capacity on GPU machines: typically 3.84TB - 7.68TB
Pipeline data footprint: typically 100GB - 1TB
Replica overhead: well within capacity
```

---

## 11. Alternatives Considered

### A. MinIO on NVMe

Deploy MinIO across cluster NVMe for S3-compatible object storage.

| Pro | Con |
|---|---|
| Zero code change | External dependency (MinIO cluster) |
| Erasure coding (stronger than 2-copy) | Operational overhead |
| Existing S3 code path works | Not all neoclouds allow extra services |

**Verdict:** Best for teams that can run MinIO. WRITE_REPLICATED is for
environments where MinIO is not an option.

### B. Lineage-Based Cascade Recompute

Track full message lineage, replay acked messages when payload is lost.

| Pro | Con |
|---|---|
| No extra storage | ~700+ lines, touches Rust + Python + Proto |
| Works for any topology | Dedup needed for fan-out/shuffle |
| | Stateful operators need idempotency |
| | Recompute is expensive for LLM stages |

**Verdict:** Over-engineered for the problem. Replication is simpler and
cheaper than recompute for expensive stages. See conversation analysis for
detailed breakdown.

### C. Async Replication (Fire-and-Forget)

Same as WRITE_REPLICATED but don't wait for Flight do_put to complete.

| Pro | Con |
|---|---|
| Near-zero write overhead | Vulnerability window (node dies before replica lands) |
| | Defeats the purpose of replication for durability |

**Verdict:** Rejected. If durability matters enough to replicate, it matters
enough to wait for confirmation. The sync overhead (1-10ms) is acceptable.

### D. Raft-Based Replicated Log

Full consensus protocol for payload writes.

**Verdict:** Massive over-engineering. Payloads are write-once, read-few,
immutable — no need for consensus. Simple synchronous copy is sufficient.
