# NVMe-Tiered PayloadStore with Arrow Flight Data Plane

_Design document — March 2026_

---

## Status

**Status**: PROPOSED
**Author**: AI Assistant
**Created**: 2026-03-23

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Design Principles](#2-design-principles)
3. [Architecture Overview](#3-architecture-overview)
4. [Write Policies](#4-write-policies)
5. [NvmeSplitPayloadStore](#5-nvmesplitpayloadstore)
6. [Arrow Flight Data Plane](#6-arrow-flight-data-plane)
7. [Multi-Disk Support](#7-multi-disk-support)
8. [NVMe Space Management](#8-nvme-space-management)
9. [Failure Analysis](#9-failure-analysis)
10. [Integration Points](#10-integration-points)
11. [Implementation Plan](#11-implementation-plan)
12. [Performance Evaluation](#12-performance-evaluation)
13. [Alternatives Considered](#13-alternatives-considered)

---

## 1. Problem Statement

The current `RaySplitPayloadStore` stores all inter-stage payloads in Ray's
distributed object store (shared memory). This breaks down for multimodal data
processing:

| Issue | Impact |
|---|---|
| **Memory pressure** | Large payloads (images, video, embeddings) consume cluster RAM. Object store overflow triggers uncontrolled spilling. |
| **GC instability** | Reference-counted object lifecycle causes latency spikes under high throughput. |
| **Serialization overhead** | Python pickle: ~3 memory copies per cross-node transfer for 100MB Arrow tables. |
| **No tiered storage** | All-or-nothing: everything in memory (Ray) or everything remote (Fsspec on S3). |

`FsspecSplitPayloadStore` on S3 provides durability but is too slow for hot-path
inter-stage data (~50ms PUT, ~20ms GET first byte).

**Gap:** No storage tier between "everything in RAM" and "everything remote."
GPU cluster nodes have NVMe SSDs (3-7 GB/s sequential, 500K+ IOPS) that sit idle.

---

## 2. Design Principles

Lessons from Alluxio, JuiceFS, Haystack, and other distributed storage systems:

### Principle 1: NVMe is Cache, S3 is Storage

```
S3 = source of truth (durable, shared, unlimited)
NVMe = read/write acceleration layer (fast, local, ephemeral)
```

NVMe is always disposable. If every payload on every NVMe in the cluster vanished
simultaneously, the job would continue (slower, via S3 fallback). This invariant
eliminates the need for disk identity tracking, registry reconstruction, and
micro-lineage for fault tolerance.

### Principle 2: Write Policy is a Per-Stage Decision

Different operators have different compute-to-IO ratios:

| Operator type | Compute time | S3 PUT (50ms) impact | Best policy |
|---|---|---|---|
| Filter / resize | 5-20ms | 100-250% overhead | WRITE_BACK |
| Feature extraction | 50-200ms | 25-50% overhead | WRITE_THROUGH |
| LLM inference | 1-60s | <1% overhead | WRITE_THROUGH |
| Video transcoding | 1-30s | <1% overhead | WRITE_THROUGH |

WRITE_THROUGH (S3 write first, then NVMe cache) has zero vulnerability window.
WRITE_BACK (NVMe first, S3 async) has a small window but is faster for cheap
stages. The user chooses per-stage based on recompute cost.

### Principle 3: Location Follows the Message, Not a Registry

Inspired by Facebook Haystack: the client gets the storage location as part of the
request, not via a separate directory lookup.

`DataQueueMessage.metadata` carries `payload_loc` = `{flight_endpoint, s3_key}`.
The consumer knows exactly where to read — no registry RPC, no cache, no SPOF.

```
Consumer receives message (metadata includes location)
  → read from payload_loc.flight_endpoint     ← direct, zero lookup
  → fallback to payload_loc.s3_key            ← always works
```

This works because payloads are immutable: once stored, the location never changes.
The message IS the registry entry.

### Principle 4: Self-Healing Through Immutability

Payloads are write-once, read-few, delete. Combined with embedded locations:

- **Node dies?** Flight endpoint in message fails → S3 fallback. No monitoring needed.
- **Node rejoins with new IP?** Old messages use old endpoint (fails → S3). New
  messages carry new endpoint (works). Gradual, automatic transition.
- **NVMe full?** Degrade to S3-direct. No complex GC needed for correctness.

No component needs to "know" the cluster topology or track node liveness.

---

## 3. Architecture Overview

### Component Count: 3 (not 11)

```
┌─────────────────────────────────────────────────────────────┐
│                  NvmeSplitPayloadStore                       │
│                                                             │
│  ┌──────────────┐  ┌────────────────┐  ┌─────────────────┐ │
│  │  NvmeDiskPool │  │  WritePolicy   │  │  S3 Tier        │ │
│  │  (multi-disk) │  │  (per-stage)   │  │  (fsspec)       │ │
│  └──────────────┘  └────────────────┘  └─────────────────┘ │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  FlightPayloadServer (per-process daemon thread)     │   │
│  │  Serves local NVMe files via Arrow Flight protocol   │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

No Ray actors. No registry. No node agents. No monitoring threads.

### Data Flow

```
Write (WRITE_THROUGH):
  operator output
    → S3 write (sync, ~50ms)          ← data is durable
    → NVMe cache write (sync, ~0.1ms) ← for fast re-reads
    → return key + {flight_endpoint, s3_key} as location

Write (WRITE_BACK):
  operator output
    → NVMe write (sync, ~0.1ms)       ← data available immediately
    → S3 write (async background)      ← data becomes durable later
    → return key + {flight_endpoint, s3_key} as location

Read (all policies):
  consumer receives message with payload_loc
    1. local NVMe (mmap)               ← ~0.1ms, same-node
    2. remote NVMe (Flight, from loc)  ← ~0.2-9ms, cross-node
    3. S3 (from loc)                   ← ~50ms, always works
```

---

## 4. Write Policies

### 4.1 Policy Definitions

Two policies, following standard cache terminology:

```python
class WritePolicy(Enum):
    WRITE_THROUGH = "write_through"
    """S3 first (pipelined), then NVMe cache.
    Latency: ~50ms (hidden when compute > S3 time). Durability: immediate.
    Use when: recompute is expensive (LLM, video encoding, external API)."""

    WRITE_BACK = "write_back"
    """NVMe first (sync), S3 async in background (if configured).
    Latency: ~0.5-3ms. Durability: eventual (S3 configured) or none (no S3).
    Use when: recompute is cheap, or S3 is not configured.
    Default policy."""
```

Write policy answers **"who gets written first"**. Whether S3 exists at all
is determined by the URI configuration (`s3_fallback` parameter), not the policy:

```
WRITE_BACK + s3_fallback=s3://...   → NVMe sync + S3 async
WRITE_BACK + no s3_fallback          → NVMe only
WRITE_THROUGH + s3_fallback=s3://... → S3 pipelined + NVMe cache
WRITE_THROUGH + no s3_fallback       → error at init (needs S3)
```

Two orthogonal axes: **write order** (policy) × **S3 presence** (config).

### 4.2 Configuration

Write policy is set via `OperatorConfig` or job-level config:

```python
# Per-stage (operator config)
@dataclass
class MyExpensiveOpConfig(OperatorConfig):
    payload_write_policy: str = "write_through"  # Don't lose my 10-min LLM results

@dataclass
class MyResizeConfig(OperatorConfig):
    payload_write_policy: str = "write_back"  # Cheap, recompute is fine

# Job-level default (applies to all stages without explicit policy)
@dataclass
class JobConfig:
    payload_store_uri: str = "ray://"
    payload_write_policy: str = "write_back"  # Default: fast, S3 async if configured
```

### 4.3 WRITE_THROUGH Pipeline Optimization

For stages where `compute_time > S3_write_time`, S3 latency can be fully hidden:

```
Pipelined WRITE_THROUGH:
  Batch N:   [compute 200ms] → [start S3 async] ──────→ [wait S3 ✓] → [ack]
  Batch N+1:                   [compute 200ms] → [start S3 async] → ...
                                ↑ overlap ↑

  Effective per-batch: max(200ms, 50ms) = 200ms — S3 latency hidden.
```

**Important limitation:** Pipeline overlap only works within a single batch's
compute-vs-S3 timing. Since `store()` and `flush_pending_writes()` are called
within the same `_process_and_ack()` invocation, there is no cross-batch overlap.

```
When compute_time < S3_write_time (e.g., resize 5ms, S3 80ms):

  Batch N: [compute 5ms] → [store NVMe 3ms] → [flush waits 72ms] → [ack]
  Total: ~80ms per batch (dominated by S3 write, not compute)

  This is 8-16x slower than WRITE_BACK for cheap stages.
```

**Recommendation:** Use WRITE_BACK for stages where `compute_time < S3_write_time`
(typically < 50ms). WRITE_THROUGH is most beneficial when recompute cost is high,
which usually correlates with long compute time.

**Future optimization (not in MVP):** True cross-batch pipelining would require
restructuring the StageWorker claim loop to start S3 uploads for batch N while
computing batch N+1. This is a significant change and deferred to a later phase.

Implementation: `store()` returns immediately after starting the S3 upload.
Before `ack_and_scatter()`, wait for all pending S3 futures:

```python
def store(self, key, payload) -> str:
    if self._write_policy == WritePolicy.WRITE_THROUGH:
        future = self._s3_executor.submit(self._write_s3, key, payload)
        self._pending_s3_futures[key] = future
        self._write_nvme_cache(key, payload)  # best-effort cache populate
        return key

def flush_pending_writes(self):
    """Called by StageWorker before ack_and_scatter(). Blocks until all
    S3 writes for this batch are confirmed.

    Auto-degrades: if S3 fails consecutively, switches to WRITE_BACK
    to prevent repeated worker crashes.
    """
    errors = []
    for key, future in self._pending_s3_futures.items():
        try:
            future.result(timeout=120)
        except Exception as e:
            errors.append((key, e))
    self._pending_s3_futures.clear()

    if errors:
        self._consecutive_s3_failures += len(errors)
        if self._consecutive_s3_failures >= self.S3_FAILURE_THRESHOLD:
            logger.warning(
                f"S3 failed {self._consecutive_s3_failures} times, "
                f"auto-degrading to WRITE_BACK"
            )
            self._write_policy = WritePolicy.WRITE_BACK
        raise IOError(f"S3 write failed for {len(errors)} payloads: {errors[0][1]}")
    else:
        self._consecutive_s3_failures = 0
```

---

## 5. NvmeSplitPayloadStore

### 5.1 Class Design

```python
class NvmeSplitPayloadStore(SplitPayloadStore):
    """NVMe + S3 two-tier storage with Arrow Flight cross-node reads.

    Design principles:
    - NVMe is cache, S3 is storage (no vulnerability window in WRITE_THROUGH)
    - Location info embedded in queue messages (no registry, no SPOF)
    - Write policy configurable per-stage (WRITE_THROUGH / WRITE_BACK)
    - Multiple NVMe disks per node with quota enforcement

    URI: nvme:///mnt/nvme0/nurion,/mnt/nvme1/nurion?s3_fallback=s3://bucket/prefix
    """

    # Per-process Flight server singleton (shared across workers on same node)
    _flight_server_lock: ClassVar[threading.Lock] = threading.Lock()
    _flight_servers: ClassVar[Dict[int, FlightPayloadServer]] = {}  # port → server

    def __init__(
        self,
        root_dirs: List[str],
        job_id: str,
        write_policy: WritePolicy = WritePolicy.WRITE_BACK,
        s3_uri: Optional[str] = None,
        s3_options: Optional[Dict[str, Any]] = None,
        flight_port: int = 5555,
        quota_bytes: Optional[int] = None,
    ):
        self._job_id = job_id
        self._write_policy = write_policy
        self._node_ip = ray.util.get_node_ip_address()
        self._flight_port = flight_port
        self._flight_endpoint = f"grpc://{self._node_ip}:{flight_port}"

        # Multi-disk pool
        self._disk_pool = NvmeDiskPool(root_dirs, job_id, quota_bytes)

        # S3 tier (required for WRITE_THROUGH, optional for WRITE_BACK)
        self._s3_uri = s3_uri
        if write_policy == WritePolicy.WRITE_THROUGH and not s3_uri:
            raise ValueError("WRITE_THROUGH requires s3_fallback in URI")
        if s3_uri:
            import fsspec.core
            full_path = f"{s3_uri.rstrip('/')}/{job_id}"
            self._s3_fs, self._s3_root = fsspec.core.url_to_fs(
                full_path, **(s3_options or {})
            )
            self._s3_fs.mkdirs(self._s3_root, exist_ok=True)

        # S3 write executor (for async and pipelined writes)
        self._s3_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="s3")
        self._pending_s3_futures: Dict[str, Future] = {}

        # Flight client pool (endpoint → client, reused)
        self._flight_clients: Dict[str, flight.FlightClient] = {}

        # Start Flight server (per-process singleton)
        self._ensure_flight_server()

        # Metrics
        self._metrics = {"stored": 0, "local_hits": 0, "remote_hits": 0, "s3_hits": 0}
```

### 5.2 Store (Write Path)

```python
def store(self, key: str, payload: SplitPayload) -> str:
    if self._write_policy == WritePolicy.WRITE_THROUGH:
        return self._store_write_through(key, payload)
    else:  # WRITE_BACK
        return self._store_write_back(key, payload)

def _store_write_through(self, key, payload):
    """S3 first (pipelined), then NVMe cache."""
    # Start S3 upload (will be awaited in flush_pending_writes)
    future = self._s3_executor.submit(self._write_s3, key, payload)
    self._pending_s3_futures[key] = future
    # NVMe cache (best-effort — failure here just means no local cache)
    try:
        self._disk_pool.write(key, payload)
    except OSError:
        pass  # NVMe full or error — S3 has the data, we're fine
    self._metrics["stored"] += 1
    return key

def _store_write_back(self, key, payload):
    """NVMe first. If S3 is configured, upload async in background."""
    try:
        self._disk_pool.write(key, payload)
    except OSError:
        if self._s3_uri:
            # NVMe full — fall back to sync S3 write
            self._write_s3(key, payload)
            self._metrics["stored"] += 1
            return key
        raise  # No S3 fallback, propagate error
    # Async S3 upload (if configured, fire-and-forget)
    if self._s3_uri:
        self._s3_executor.submit(self._write_s3, key, payload)
    self._metrics["stored"] += 1
    return key

def flush_pending_writes(self):
    """Block until all pipelined S3 writes complete.
    Called by StageWorker before ack_and_scatter()."""
    errors = []
    for key, future in self._pending_s3_futures.items():
        try:
            future.result(timeout=120)
        except Exception as e:
            errors.append((key, e))
    self._pending_s3_futures.clear()
    if errors:
        raise IOError(f"S3 write failed for {len(errors)} payloads: {errors[0][1]}")
```

### 5.3 Get (Read Path)

```python
def get(self, key: str) -> Optional[SplitPayload]:
    """Standard get (no location hint). Checks local NVMe only."""
    return self._read_local(key)

def get_with_hint(self, key: str, location_hint: Optional[dict] = None) -> Optional[SplitPayload]:
    """Get with location hint from message metadata. Three-tier fallback."""
    # Tier 1: Local NVMe — mmap, zero-copy
    local = self._read_local(key)
    if local is not None:
        self._metrics["local_hits"] += 1
        return local

    if not location_hint:
        return None

    # Tier 2: Remote NVMe via Arrow Flight
    endpoint = location_hint.get("flight")
    if endpoint and endpoint != self._flight_endpoint:
        try:
            table = self._flight_get(endpoint, key)
            if table is not None:
                self._metrics["remote_hits"] += 1
                return SplitPayload.from_arrow(table, split_id=key)
        except Exception:
            pass  # Endpoint unreachable, fall through to S3

    # Tier 3: S3 (always available for WRITE_THROUGH / WRITE_BACK)
    s3_key = location_hint.get("s3")
    if s3_key:
        result = self._read_s3(s3_key, key)
        if result is not None:
            self._metrics["s3_hits"] += 1
            return result

    return None  # Payload truly lost → nack + recompute
```

### 5.4 Location Metadata

```python
def get_location(self, key: str) -> Optional[dict]:
    """Return location info to embed in queue message metadata.

    This replaces the centralized registry. The message IS the registry entry.
    """
    loc = {"flight": self._flight_endpoint}
    if self._s3_uri:
        safe_key = _sanitize_key(key)
        loc["s3"] = f"{self._s3_root}/{safe_key}.arrow"
    return loc
```

### 5.5 Local I/O

```python
def _read_local(self, key: str) -> Optional[SplitPayload]:
    """Read from local NVMe via mmap. Returns None if not on any local disk."""
    result = self._disk_pool.read(key)
    return result

def _write_s3(self, key: str, payload: SplitPayload):
    """Write Arrow IPC to S3. Used by all policies."""
    safe_key = _sanitize_key(key)
    s3_path = f"{self._s3_root}/{safe_key}.arrow"
    with self._s3_fs.open(s3_path, "wb") as f:
        writer = ipc.new_file(f, payload.data.schema)
        writer.write_table(payload.data)
        writer.close()

def _read_s3(self, s3_path: str, key: str) -> Optional[SplitPayload]:
    """Read Arrow IPC from S3."""
    try:
        with self._s3_fs.open(s3_path, "rb") as f:
            reader = ipc.open_file(f)
            table = reader.read_all()
        return SplitPayload.from_arrow(table, split_id=key)
    except FileNotFoundError:
        return None
```

---

## 6. Arrow Flight Data Plane

Arrow Flight is purpose-built for high-performance Arrow data transfer over gRPC.

### 6.1 Why Arrow Flight

Arrow Flight transfers Arrow IPC data over gRPC with near-zero protocol overhead.
For 100MB payloads on 100Gbps networks:

```
                    Arrow Flight     Ray remote call
Serialization       0 (IPC on disk)  ~50ms (pickle)
Scheduling          0 (direct gRPC)  ~1ms (Ray scheduler)
Transfer            ~9ms             ~9ms (object store)
Deserialization     ~0.2ms           ~50ms (unpickle)
Total               ~9.4ms           ~110ms
```

### 6.2 Flight Server (Per-Process Singleton)

```python
class FlightPayloadServer(flight.FlightServerBase):
    """Serves Arrow IPC files from local NVMe disks.

    Runs as a daemon thread within the worker process. No Ray actor needed.
    Shared across all StageWorkers in the same process via class-level singleton.
    """

    def __init__(self, job_dirs: List[str], port: int = 5555,
                 max_concurrent_reads: int = 8):
        location = flight.Location.for_grpc_tcp("0.0.0.0", port)
        super().__init__(location)
        self._job_dirs = job_dirs
        self._semaphore = threading.Semaphore(max_concurrent_reads)

    def do_get(self, context, ticket):
        key = ticket.ticket.decode()
        safe_key = _sanitize_key(key)

        acquired = self._semaphore.acquire(timeout=30)
        if not acquired:
            raise flight.FlightUnavailableError("Server overloaded, retry later")
        try:
            for job_dir in self._job_dirs:
                path = f"{job_dir}/{safe_key}.arrow"
                if os.path.exists(path):
                    source = pa.memory_map(path, "r")
                    reader = ipc.open_file(source)
                    table = reader.read_all()
                    return flight.RecordBatchStream(table)
            raise flight.FlightUnavailableError(f"Not found: {key}")
        finally:
            self._semaphore.release()
```

### 6.3 Flight Client

```python
def _flight_get(self, endpoint: str, key: str) -> Optional[pa.Table]:
    """Fetch from remote node. Short timeout — fail fast, fall back to S3."""
    client = self._get_or_create_client(endpoint)
    try:
        reader = client.do_get(flight.Ticket(key.encode()))
        return reader.read_all()
    except Exception:
        # Any failure → close bad connection, return None for S3 fallback
        self._flight_clients.pop(endpoint, None)
        return None

def _get_or_create_client(self, endpoint: str) -> flight.FlightClient:
    if endpoint not in self._flight_clients:
        # Short connect timeout — don't block on dead nodes
        self._flight_clients[endpoint] = flight.connect(
            endpoint,
            generic_options=[("grpc.keepalive_timeout_ms", "2000")],
        )
    return self._flight_clients[endpoint]
```

---

## 7. Multi-Disk Support

### 7.1 URI Format

```
# Single disk, Nurion root dir at /mnt/nvme0/nurion
nvme:///mnt/nvme0/nurion?s3_fallback=s3://bucket/shuffle

# Multiple disks (comma-separated)
nvme:///mnt/nvme0/nurion,/mnt/nvme1/nurion?s3_fallback=s3://bucket/shuffle

# With per-disk quota
nvme:///mnt/nvme0/nurion,/mnt/nvme1/nurion?s3_fallback=s3://...&quota_gb=500
```

Paths point to Nurion's own subdirectory, not the disk root.
Other workloads (vLLM, checkpoints) coexist safely.

### 7.2 Directory Layout

```
/mnt/nvme0/                         ← NVMe disk 0 mount
  ├── nurion/                       ← Nurion root (user-specified in URI)
  │   ├── job-123/                  ← Per-job payload files
  │   │   ├── key1.arrow
  │   │   └── key2.arrow
  │   └── job-456/
  ├── vllm-cache/                   ← Other workloads (untouched)
  └── checkpoints/                  ← Other workloads (untouched)

/mnt/nvme1/
  └── nurion/
      └── job-123/
          ├── key3.arrow
          └── key4.arrow
```

### 7.3 NvmeDiskPool

```python
class NvmeDiskPool:
    """Manages multiple NVMe disks. Writes to the disk with most free space."""

    def __init__(self, root_dirs: List[str], job_id: str,
                 quota_bytes: Optional[int] = None):
        self._disks = [NvmeDisk(d, job_id, quota_bytes) for d in root_dirs]
        self._key_to_disk: Dict[str, int] = {}

    def write(self, key: str, payload: SplitPayload) -> str:
        """Atomic write (tmp + rename) to disk with most space."""
        disk = max(self._disks, key=lambda d: d.available_bytes())
        if disk.available_bytes() <= 0:
            raise OSError(errno.ENOSPC, "All NVMe disks full or over quota")
        path = disk.write(key, payload)
        self._key_to_disk[key] = self._disks.index(disk)
        return path

    def read(self, key: str) -> Optional[SplitPayload]:
        """Check known disk first, then scan all disks."""
        idx = self._key_to_disk.get(key)
        if idx is not None:
            return self._disks[idx].read(key)
        for disk in self._disks:
            result = disk.read(key)
            if result is not None:
                return result
        return None

    @property
    def all_job_dirs(self) -> List[str]:
        return [d.job_dir for d in self._disks]


class NvmeDisk:
    """Single NVMe disk."""

    def __init__(self, root_dir: str, job_id: str,
                 quota_bytes: Optional[int] = None):
        self._root_dir = root_dir
        self._job_dir = f"{root_dir}/{job_id}"
        self._quota_bytes = quota_bytes
        os.makedirs(self._job_dir, exist_ok=True)

    @property
    def job_dir(self) -> str:
        return self._job_dir

    def available_bytes(self) -> int:
        _, _, fs_free = shutil.disk_usage(self._root_dir)
        if self._quota_bytes is None:
            return fs_free
        used = sum(f.stat().st_size for f in Path(self._job_dir).rglob("*.arrow"))
        return min(fs_free, max(0, self._quota_bytes - used))

    def write(self, key: str, payload: SplitPayload) -> str:
        safe_key = _sanitize_key(key)
        tmp_path = f"{self._job_dir}/{safe_key}.arrow.tmp.{os.getpid()}"
        final_path = f"{self._job_dir}/{safe_key}.arrow"
        with open(tmp_path, "wb") as f:
            writer = ipc.new_file(f, payload.data.schema)
            writer.write_table(payload.data)
            writer.close()
        os.rename(tmp_path, final_path)  # Atomic on POSIX
        return final_path

    def read(self, key: str) -> Optional[SplitPayload]:
        safe_key = _sanitize_key(key)
        path = f"{self._job_dir}/{safe_key}.arrow"
        if not os.path.exists(path):
            return None
        source = pa.memory_map(path, "r")
        reader = ipc.open_file(source)
        table = reader.read_all()
        return SplitPayload.from_arrow(table, split_id=key)

    def delete(self, key: str) -> bool:
        safe_key = _sanitize_key(key)
        path = f"{self._job_dir}/{safe_key}.arrow"
        try:
            os.unlink(path)
            return True
        except FileNotFoundError:
            return False
```

---

## 8. NVMe Space Management

### 8.1 Three-Watermark System

```python
class NvmeSpaceManager:
    """Monitors space usage relative to quota and filesystem capacity.
    Only manages Nurion's own directory, never touches other workloads' files."""

    HIGH = 0.80       # Start proactive cleanup
    CRITICAL = 0.90   # Aggressive cleanup
    FATAL = 0.95      # Degrade to S3-direct

    def __init__(self, root_dir, job_dir, quota_bytes):
        self._root_dir = root_dir
        self._job_dir = job_dir
        self._quota_bytes = quota_bytes

    def check(self) -> str:
        """Returns current mode: 'normal' | 'gc' | 's3_direct'."""
        ratio = self._usage_ratio()
        if ratio >= self.FATAL:
            return "s3_direct"
        if ratio >= self.HIGH:
            return "gc"
        return "normal"

    def _usage_ratio(self) -> float:
        _, total, fs_free = shutil.disk_usage(self._root_dir)
        fs_ratio = 1 - fs_free / total
        if self._quota_bytes:
            used = sum(f.stat().st_size for f in Path(self._job_dir).rglob("*"))
            quota_ratio = used / self._quota_bytes
            return max(fs_ratio, quota_ratio)
        return fs_ratio
```

### 8.2 Payload Cleanup

Payloads are deleted after downstream consumption (existing pattern in StageWorker):

```python
# stage_worker.py: after ack_and_scatter
for key in batch.consumed_payload_keys:
    self.payload_store.delete(key)  # Best-effort, failure is OK
```

Job cleanup deletes entire job directory:

```python
def clear(self) -> int:
    count = 0
    for disk in self._disk_pool._disks:
        job_dir = disk.job_dir
        if os.path.exists(job_dir):
            files = list(Path(job_dir).glob("*.arrow"))
            count += len(files)
            shutil.rmtree(job_dir, ignore_errors=True)
    # Also clean S3
    if self._s3_uri:
        try:
            self._s3_fs.rm(self._s3_root, recursive=True)
        except Exception:
            pass
    return count
```

---

## 9. Failure Analysis

### 9.1 Core Invariant

**For WRITE_THROUGH: S3 always has the data before ack.** No recovery mechanism needed.

**For WRITE_BACK: S3 may not have the data.** Existing WorkQueue nack + upstream
recompute handles this (same as any operator crash — the message is re-enqueued).

### 9.2 Failure Scenarios

| Failure | WRITE_THROUGH | WRITE_BACK (with S3) | WRITE_BACK (no S3) |
|---|---|---|---|
| **Worker crash** | nack → retry → S3 has data ✓ | nack → retry → NVMe or S3 | nack → retry → NVMe if on same node |
| **Node dies** | S3 has data ✓ | S3 may have data, else recompute | Data lost → recompute |
| **Node IP changes** | Flight fails → S3 ✓ | Flight fails → S3 or recompute | Flight fails → data inaccessible |
| **NVMe full** | NVMe skip, S3 OK ✓ | Degrade to sync S3 write | store() fails → nack |
| **OOM** | Restart, S3 has data ✓ | Restart, S3 or recompute | Restart, recompute |
| **S3 unavailable** | flush blocks → timeout → auto-degrade to WRITE_BACK | NVMe has data, uploads retry | N/A |
| **Flight overload** | Semaphore rejects → S3 ✓ | Same | Data unavailable remotely |

### 9.3 Why No Registry / Disk Identity / Micro-Lineage Needed

**Registry:** Location is in the message. No central lookup needed.

**Disk identity:** When a node rejoins with a new IP but old NVMe data, the data
is still on disk. New workers on that node will produce new messages with the new
Flight endpoint. Old messages carry the old endpoint — Flight fails, S3 fallback.
No identity tracking needed because the system self-heals through message flow.

**Micro-lineage:** WRITE_THROUGH ensures S3 always has the data before ack.
WRITE_BACK accepts the small risk of recompute via standard nack semantics.
No special recompute mechanism needed beyond what WorkQueue already provides.

### 9.4 Flight Port Conflict (Multi-Job)

When multiple jobs run on the same node, they may both try to start a Flight
server on the same port. The current singleton pattern (`_flight_servers` ClassVar)
means the second job reuses the first job's server, but that server only knows
about the first job's directories.

**Solution:** Use per-job port allocation or dynamic job_dir registration.

```python
# Option A: Per-job port (simple, recommended for MVP)
flight_port = 5555 + hash(job_id) % 1000  # Deterministic per-job

# Option B: Dynamic job_dir registration (no port conflict)
class FlightPayloadServer:
    def add_job_dirs(self, dirs: List[str]):
        """Called when a new job starts on this node."""
        with self._lock:
            self._job_dirs.extend(dirs)

    def remove_job_dirs(self, dirs: List[str]):
        """Called when a job completes."""
        with self._lock:
            self._job_dirs = [d for d in self._job_dirs if d not in dirs]
```

Option B is cleaner but requires a shared Flight server process.
For MVP, use Option A.

### 9.5 S3 Persistent Failure + WRITE_THROUGH

If S3 is unreachable for an extended period, WRITE_THROUGH workers will
repeatedly fail in `flush_pending_writes()`, crash, and be restarted by
RecoveryManager — creating a crash loop.

**Solution:** Auto-degrade to WRITE_BACK after N consecutive failures.
See `flush_pending_writes()` in §4.3. This trades durability for availability:
the pipeline continues with NVMe-only storage until S3 recovers.

### 9.6 Temporary File Cleanup

Worker crashes leave `.tmp.{pid}` files on NVMe. These leak disk space.

**Solution:** On store initialization, clean up stale tmp files:

```python
def _cleanup_stale_tmp_files(self):
    """Remove .tmp files from crashed workers."""
    for disk in self._disk_pool._disks:
        for tmp in Path(disk.job_dir).glob("*.arrow.tmp.*"):
            try:
                tmp.unlink()
            except OSError:
                pass
```

### 9.7 Worst Case: WRITE_BACK + Node Death Before S3 Upload

```
Timeline:
  1. store() writes NVMe          ✓
  2. S3 upload started (async)    in progress...
  3. ack_and_scatter() succeeds   ✓ (message delivered downstream)
  4. Node dies                    ✗ S3 upload incomplete

Consumer:
  1. Local NVMe: not this node
  2. Flight: node dead, connection refused
  3. S3: file not found (upload didn't finish)
  → get_with_hint returns None
  → StageWorker nacks all messages in this batch
  → Messages re-enqueued to upstream queue
  → Upstream re-processes → new payload on different node → succeeds
```

This is the same recovery path as any worker crash. No special mechanism needed.
The cost is reprocessing one batch of upstream work.

---

## 10. Integration Points

### 10.1 SplitPayloadStore ABC Extension

```python
class SplitPayloadStore(ABC):
    @abstractmethod
    def store(self, key: str, payload: SplitPayload) -> str: ...

    @abstractmethod
    def get(self, key: str) -> Optional[SplitPayload]: ...

    @abstractmethod
    def delete(self, key: str) -> bool: ...

    @abstractmethod
    def clear(self) -> int: ...

    # New methods (default no-ops for backward compatibility)

    def get_with_hint(self, key: str, location_hint: Optional[dict] = None) -> Optional[SplitPayload]:
        """Get with optional location hint. Override for location-aware stores."""
        return self.get(key)

    def get_location(self, key: str) -> Optional[dict]:
        """Return location metadata to embed in queue message."""
        return None

    def flush_pending_writes(self):
        """Block until all async writes complete. Called before ack_and_scatter."""
        pass
```

### 10.2 StageWorker Changes

```python
# stage_worker.py: read with hint
message = queue_message_from_bytes(record.value)
hint = message.metadata.get("payload_loc")
payload = self.payload_store.get_with_hint(message.payload_key, hint)

# stage_worker.py: write with location
self.payload_store.store(out_id, out_payload)
loc = self.payload_store.get_location(out_id)
out_msg = DataQueueMessage(
    message_id=out_id,
    split_id=out_id,
    payload_key=out_id,
    metadata={
        "source_stage": self.stage_id,
        **({"payload_loc": loc} if loc else {}),
    },
)

# stage_worker.py: flush before ack (WRITE_THROUGH pipeline support)
self.payload_store.flush_pending_writes()
self.queue_client.ack_and_scatter(...)
```

### 10.3 Factory

```python
def _create_payload_store(self) -> SplitPayloadStore:
    uri = self.job.config.payload_store_uri
    if uri.startswith("ray://"):
        store = RaySplitPayloadStore(name=f"payload_store_{self.job.job_id}")
        store.wait_ready()
        return store
    elif uri.startswith("nvme://"):
        root_dirs, params = parse_nvme_uri(uri)
        policy = WritePolicy(
            params.get("write_policy", self.job.config.payload_write_policy)
        )
        quota = int(params["quota_gb"]) * (1024**3) if "quota_gb" in params else None
        return NvmeSplitPayloadStore(
            root_dirs=root_dirs,
            job_id=self.job.job_id,
            write_policy=policy,
            s3_uri=params.get("s3_fallback"),
            s3_options=self.job.config.payload_store_options,
            flight_port=int(params.get("flight_port", "5555")),
            quota_bytes=quota,
        )
    else:
        return FsspecSplitPayloadStore(...)
```

### 10.4 Files to Create/Modify

| File | Action | Description |
|---|---|---|
| `_internal/core/split_payload_store.py` | Modify | Add `get_with_hint`, `get_location`, `flush_pending_writes` to ABC. Add `NvmeSplitPayloadStore`. |
| `_internal/core/nvme_flight.py` | Create | `FlightPayloadServer`, `NvmeDiskPool`, `NvmeDisk`, `NvmeSpaceManager` |
| `_internal/core/stage_worker.py` | Modify | Use `get_with_hint`, embed `payload_loc`, call `flush_pending_writes` |
| `_internal/core/job.py` | Modify | Add `payload_write_policy` to `JobConfig` |
| `_internal/runtime/ray_runner.py` | Modify | Factory extension for `nvme://` URI |
| `nurion/__init__.py` | Modify | Export `NvmeSplitPayloadStore`, `WritePolicy` |
| `tests/core/test_nvme_payload_store.py` | Create | Unit tests (local filesystem as NVMe stand-in) |

---

## 11. Implementation Plan

### Phase 0: Local NVMe Store + S3 Write-Through (MVP)

| Task | Description |
|---|---|
| `NvmeDisk`, `NvmeDiskPool` | Multi-disk Arrow IPC read/write with quota |
| `NvmeSplitPayloadStore` | WRITE_THROUGH policy, local read, S3 write |
| `flush_pending_writes` | Pipelined S3 writes with ack-time barrier |
| `WritePolicy` enum | WRITE_THROUGH / WRITE_BACK |
| ABC extension | `get_with_hint`, `get_location`, `flush_pending_writes` |
| Factory | `nvme://` URI parsing, multi-disk support |
| Unit tests | All three policies, disk full degradation |
| Benchmark | vs Ray store, vs Fsspec store, various payload sizes |

**Deliverable:** Working `nvme://` store with S3 durability. Single-node only
(no Flight yet). Already useful for non-shuffle pipelines.

### Phase 1: Arrow Flight + Message-Embedded Location

| Task | Description |
|---|---|
| `FlightPayloadServer` | Per-process daemon thread, concurrency limiter |
| Flight client in store | Connection pool, short timeout |
| `get_with_hint` | Three-tier read path (local → Flight → S3) |
| `get_location` | Embed flight endpoint + s3 key in location |
| StageWorker changes | `payload_loc` in metadata, `get_with_hint` in read |
| Integration tests | Multi-node pipeline with cross-node Flight reads |

**Deliverable:** Full cross-node read support. No registry, no actors.

### Phase 2: Locality-Aware Claim

| Task | Description |
|---|---|
| `origin_node` in metadata | Record which node stored each payload |
| Node-aware partition assignment | WorkerManager considers node placement |
| Monitoring | Local hit rate, Flight read rate, S3 fallback rate |

**Deliverable:** 70-90% local NVMe hit rate for non-shuffle pipelines.

### Phase 3: Advanced

| Task | Priority |
|---|---|
| Small-file packing for S3 | P2 |
| Large-file chunked Flight streaming | P2 |
| Broker-side locality hint (Rust) | P2 |
| Cross-job NVMe cache reuse | P3 |
| RDMA data plane (Rust) | P3 |

---

## 12. Performance Evaluation

### 12.1 Write Latency by Policy

Store latency for a single payload (NVMe sequential write, no fsync):

| Payload | NVMe write | S3 PUT | WRITE_THROUGH (flush) | WRITE_BACK |
|---------|-----------|--------|----------------------|------------|
| 1MB | ~0.5ms | ~50ms | 50ms | 0.5ms |
| 10MB | ~3ms | ~80ms | 80ms | 3ms |
| 100MB | ~20ms | ~350ms | 350ms | 20ms |

WRITE_THROUGH flush time = max(0, S3_time - time_since_store). Hidden when
`compute_time > S3_time`; dominates when `compute_time < S3_time`.

### 13.2 Read Latency by Tier

| Payload | Local NVMe (mmap) | Remote Flight (100Gbps) | S3 GET |
|---------|------------------|------------------------|--------|
| 1MB | ~0.2ms | ~0.3ms | ~25ms |
| 10MB | ~1.2ms | ~1.2ms | ~50ms |
| 100MB | ~12ms | ~9.5ms | ~200ms |

Remote Flight is faster than local mmap for large payloads because Flight
streams over the network while mmap may page-fault sequentially.

### 13.3 Throughput per Worker

Single worker, 10MB payloads:

| Compute time | WRITE_THROUGH | WRITE_BACK | Ray Object Store |
|-------------|---------------|---------------|------------------|
| 5ms (resize) | ~120 MB/s | ~500 MB/s | ~300 MB/s |
| 50ms (embed) | ~175 MB/s | ~175 MB/s | ~150 MB/s |
| 200ms (LLM) | ~48 MB/s | ~48 MB/s | ~48 MB/s |

For compute-bound stages (>50ms), all policies perform similarly.
For IO-bound stages (<50ms), WRITE_BACK is 4x faster than WRITE_THROUGH.

### 13.4 Comparison with Ray Object Store

| Dimension | Ray Object Store | NVMe (WRITE_BACK) | NVMe (WRITE_THROUGH) |
|-----------|-----------------|--------------|---------------------|
| Store 10MB | ~10ms | ~3ms | ~3ms + flush |
| Get 10MB (local) | ~2ms (zero-copy) | ~1.2ms (mmap) | same |
| Get 10MB (remote) | ~15ms (plasma) | ~1.2ms (Flight) | same |
| Memory usage | payload × refs | ~0 (mmap/OS) | same |
| Capacity | cluster RAM | NVMe (TBs) | same |
| Durability | none | NVMe only | S3 confirmed |
| GC pressure | high | none | none |

### 13.5 Scaling Bottlenecks

| Bottleneck | Threshold | Mitigation |
|---|---|---|
| NVMe write bandwidth | ~5 GB/s per disk | Multi-disk striping (NvmeDiskPool) |
| S3 upload bandwidth | ~500 MB/s per node | Configurable thread count; region-local S3 |
| S3 PUT rate limit | ~3500 PUT/s per prefix | Use per-job prefix (already done) |
| Flight server concurrency | Semaphore(8) default | Configurable; bounded by NVMe read BW |
| Arrow IPC serialization | ~1 CPU core per GB/s | Unavoidable; but better than pickle (~0.3 core/GB) |

---

## 13. Alternatives Considered

### 13.1 Centralized Registry Design

An alternative approach uses a `NvmeBlobRegistry` (Ray Named Actor) for
key-to-location mapping. This would require disk identity tracking, node death
monitoring, cache invalidation, and micro-lineage for the S3 upload vulnerability
window.

**Rejected because:** A centralized registry introduces SPOF, cache coherence
complexity, and ~11 interacting subsystems. Embedding location in messages
achieves the same result with 3 components and zero coordination.

### 13.2 Alluxio / JuiceFS as External Service

**Rejected because:** Additional operational complexity (deployment, monitoring,
upgrades). Nurion's requirements are simpler — immutable payloads with known
lifecycle — and can be met with a library-level solution.

### 13.3 Ray Object Store with Controlled Spilling

**Rejected because:** Spilling is uncontrolled (unpredictable latency spikes),
pickle serialization overhead persists, no S3 durability tier, no locality awareness.

### 13.4 Pure S3 (Enhanced FsspecSplitPayloadStore)

The simplest possible approach: just use S3 for everything.

**Not rejected outright** — this is what WRITE_THROUGH mode effectively does for
the write path. The NVMe tier adds value as a read cache (local reads avoid S3
GET latency) and as a write buffer (WRITE_BACK for cheap stages).

### 13.5 Consistent Hashing for Deterministic Placement

Use `hash(key) % N` to determine which node should store a payload, enabling
registry-free reads.

**Not chosen because:** Conflicts with write-local. The writer may not be on the
"correct" node. Would require cross-node writes on the store path, adding latency.
Message-embedded location achieves the same registry-free property without
constraining write placement.
