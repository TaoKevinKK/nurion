# WebUI API v2

## Motivation

The current WebUI API (18 endpoints) has three core problems:

1. **Inaccurate data**: Worker info is guessed from ack event scans (status/start_time/end_time all inferred)
2. **Poor performance**: Nearly all endpoints call `list_events(limit=100000)` doing O(N) full scans then filtering in memory
3. **Redundant/dead code**: `partition_offsets` returns empty, stages list duplicates job detail, lineage overview does a full scan with no value

Goal: Streamline to 12 endpoints (9 pipeline + 3 serve), ensure data accuracy, and provide a reliable data source for the diagnostic agent.

## Design Principles

1. **Write on occurrence, read on demand**: Worker lifecycle is written when it happens, not reconstructed by scanning at read time
2. **O(1) first**: Throughput uses QueueStats counters (O(1)), no event scanning to compute rates
3. **Single responsibility**: Each endpoint does one thing — no scan-filter-aggregate in a single endpoint
4. **Unified pipeline + serve**: Both subsystems write monitoring data to Anvil state

## Storage Schema Changes

### New Keys

Add worker metadata in the existing `job:{job_id}` namespace:

```
worker:{stage_id}:{worker_id} → {
    worker_id, stage_id, status, timestamp,
    start_time?, end_time?
}
```

Add a `serve` namespace (independent of pipeline jobs):

```
serve (namespace)
├── model:{model_id}            → { model_id, status, deploy_time, config }
├── worker:{model_id}:{wid}    → { worker_id, model_id, status, timestamp,
│                                    backend, tp_size, endpoint, node_id }
└── event:{model_id}:{ts_ns}:{wid} → { event_type, timestamp, ... }
```

### New schema.py Functions

```python
# Pipeline worker metadata
def worker_key(stage_id: str, worker_id: str) -> str:
    return f"worker:{stage_id}:{worker_id}"

# Serve
def serve_namespace() -> str:
    return "serve"

def serve_model_key(model_id: str) -> str:
    return f"model:{model_id}"

def serve_worker_key(model_id: str, worker_id: str) -> str:
    return f"worker:{model_id}:{worker_id}"

def serve_event_key(model_id: str, ts_ns: int, worker_id: str) -> str:
    return f"event:{model_id}:{ts_ns}:{worker_id}"
```

## Write Path Changes

### 1. StageMaster Writes Worker Metadata

**File**: `engine/_internal/core/stage_master.py`

Add `_write_worker_state(worker_id, status, **extra)`, reusing the `_write_stage_state()` pattern
(uses existing `self._queue_client.state_put()`).

Trigger points:

| Event | Status | Location |
|-------|--------|----------|
| Spawn success | `RUNNING` | After `spawn_worker()` returns in `start()` / `scale_up()` |
| Worker completed | `COMPLETED` | In `run()` when `wait_for_completion()` returns completed |
| Worker failed | `FAILED` | In `run()` when `wait_for_completion()` returns failed |
| Scale down | `STOPPED` | In `scale_down()` after `stop_worker()` returns |

### 2. StageWorker Writes Nack Events

**File**: `engine/_internal/core/stage_worker.py`

The existing payload_missing nack (line 255) is missing `state_puts`. Fix:

```python
ts_ns = time.time_ns()
nack_puts = {}
for mid in msg_ids:
    nack_puts[event_key(self.stage_id, ts_ns, mid)] = encode_json({
        "event_type": "nack",
        "timestamp": time.time(),
        "worker_id": self.worker_id,
        "stage_id": self.stage_id,
        "reason": "payload_missing",
    })
    ts_ns += 1

self.queue_client.nack(..., state_puts=nack_puts)
```

The `nack()` method already supports the `state_puts` parameter (anvil.py:308) — no changes needed.

### 3. ModelPool Writes InferenceWorker Lifecycle

**File**: `engine/_internal/serve/pool.py`

ModelPool accepts an optional `state_writer: AnvilQueueClient` (injected by ModelServiceManager).

Trigger points:

| Event | Status | Location |
|-------|--------|----------|
| Spawn complete | `LOADING` | Before `_spawn_worker()` returns |
| Worker ready | `READY` | First heartbeat with is_ready=True |
| Worker stopped | `STOPPED` | Inside `_stop_worker()` |
| Worker crash | `FAILED` | Actor death detection |

### 4. ModelServiceManager Writes Model Metadata

**File**: `engine/_internal/serve/manager.py`

- `deploy_model()` → writes `model:{model_id}` with status=DEPLOYED
- `undeploy_model()` → writes `model:{model_id}` with status=UNDEPLOYED
- `__init__` accepts optional `broker_endpoint`, creates AnvilQueueClient passed to Pool

## API Changes

### Deleted (9 Endpoints)

| Endpoint | Reason |
|----------|--------|
| `GET /api/jobs/{id}/stages` | Redundant — job detail already contains stages |
| `GET /api/.../stages/{id}/metrics` | O(N) full scan |
| `GET /api/.../stages/{id}/workers` | Redundant — `/workers?stage_id=` replaces it |
| `GET /api/.../stages/{id}/offsets` | Dead code, returns `{}` |
| `GET /api/.../stages/{id}/throughput` | Merged into stage detail's queue_stats |
| `GET /api/.../workers/{id}` | Merged into `/workers?worker_id=` |
| `GET /api/.../workers/{id}/metrics` | O(N) full scan |
| `GET /api/.../lineage/overview` | O(N) full scan |
| `GET /api/.../lineage/stages/{id}/splits` | O(N) full scan |
| `GET /api/.../exceptions` | Merged into `/events?event_type=nack,timeout` |

Deleted file: `engine/_internal/webui/api/exceptions.py`

### Added (4 Endpoints)

| Endpoint | Description |
|----------|-------------|
| `GET /api/jobs/{id}/events` | Unified event query, supports `?stage_id=&worker_id=&event_type=&limit=` |
| `GET /api/serve/models` | List deployed models |
| `GET /api/serve/workers` | InferenceWorker lifecycle status, supports `?model_id=` |
| `GET /api/serve/events` | Serve event history, supports `?model_id=&limit=` |

New files: `engine/_internal/webui/api/events.py`, `engine/_internal/webui/api/serve.py`

### Retained and Enhanced (3 Endpoints)

| Endpoint | Changes |
|----------|---------|
| `GET /api/jobs/{id}` | Stages gain `queue_stats` field (QueueStats O(1)) |
| `GET /api/.../stages/{id}` | Add `queue_stats`, remove throughput/offsets/metrics |
| `GET /api/.../workers` | Read from worker metadata (not event scanning), support `?stage_id=&worker_id=` |

### Final API (12 Endpoints)

| # | Endpoint | Complexity |
|---|----------|------------|
| 1 | `GET /api/jobs` | O(jobs) |
| 2 | `GET /api/jobs/{id}` | O(stages) |
| 3 | `GET /api/jobs/{id}/stages/{sid}` | O(stages) |
| 4 | `GET /api/jobs/{id}/workers` | O(workers) |
| 5 | `GET /api/jobs/{id}/workers/{wid}/logs` | Realtime |
| 6 | `GET /api/jobs/{id}/workers/{wid}/stacktrace` | Realtime |
| 7 | `GET /api/jobs/{id}/events` | O(limit) |
| 8 | `GET /api/jobs/{id}/lineage/splits/{sid}` | O(1) |
| 9 | `GET /api/jobs/{id}/lineage/splits/{sid}/trace` | O(depth) |
| 10 | `GET /api/serve/models` | O(models) |
| 11 | `GET /api/serve/workers` | O(serve_workers) |
| 12 | `GET /api/serve/events` | O(limit) |

## manager.py Changes

### Rewrite list_workers

Read from worker metadata instead of scanning events:

```python
def list_workers(self, job_id, stage_id=None, worker_id=None, limit=100, offset=0):
    namespace = job_namespace(job_id)
    prefix = f"worker:{stage_id}:" if stage_id else "worker:"
    entries = self._storage.state_scan_prefix(namespace, prefix=prefix, limit=0)
    workers = [decode_json(e["value"]) for e in entries]
    # filter + sort + paginate
    ...
```

### Delete Deprecated Methods

- `get_metrics_samples()`, `get_metrics_history()`, `rate()`, `get_throughput()`
- `get_worker_history()`, `list_worker_events()`, `list_exceptions()`
- `list_splits_by_stage()`, `get_lineage_overview()`, `get_partition_offsets()`

### Enhance get_job_archive

Add `queue_stats` to each stage (QueueStats O(1) counters):

```python
stage["queue_stats"] = {
    "pending_count": stats.pending_count,
    "claimed_count": stats.claimed_count,
    "total_pushed": stats.total_pushed,
    "total_acked": stats.total_acked,
}
```

The frontend can poll `total_acked` deltas to compute throughput — no event scanning needed.

## File Manifest

| File | Action |
|------|--------|
| `_internal/webui/state/schema.py` | Add worker_key + serve_*_key |
| `_internal/core/stage_master.py` | Add _write_worker_state, 5 call sites |
| `_internal/core/stage_worker.py` | Add state_puts to nack |
| `_internal/serve/pool.py` | Add _write_worker_state, optional state_writer |
| `_internal/serve/manager.py` | Optional broker_endpoint, model metadata writes |
| `_internal/webui/state/manager.py` | Rewrite list_workers, delete 10 methods, enhance get_job_archive |
| `_internal/webui/api/events.py` | New file |
| `_internal/webui/api/serve.py` | New file |
| `_internal/webui/api/stages.py` | Simplify to 1 endpoint |
| `_internal/webui/api/workers.py` | Simplify to 3 endpoints |
| `_internal/webui/api/lineage.py` | Simplify to 2 endpoints |
| `_internal/webui/api/exceptions.py` | Delete |
| `_internal/webui/app.py` | Update router registration + HTML page routes |
