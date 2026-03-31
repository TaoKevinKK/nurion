# Nurion Architecture Reference

> Comprehensive system design. Read before proposing architectural changes.
> **Update this file when execution model or component relationships change.**

---

## Execution Pipeline (High Level)

```
User Code
  └── Job(stages=[Stage(config=MyOperatorConfig(), ...)])
        │
        ▼
  RayJobRunner.run()                          # runtime/ray_runner.py
    ├── AnvilBrokerManager.start()        # starts anvil-rs broker
    ├── SplitPayloadStore.create()            # Ray object store or fsspec
    ├── For each Stage:
    │     └── StageMaster (Ray actor)         # core/stage_master.py
    │           ├── SourceManager             # plan_splits() → push to queue
    │           ├── WorkerManager             # spawn N StageWorkers
    │           ├── RecoveryManager           # monitor + respawn failed workers
    │           └── SinkManager              # background commit loop
    │                 └── StageWorker × N    # core/stage_worker.py
    │                       └── Operator     # user operator instance
    ├── SimpleAutoscaler                      # runtime/autoscaler.py
    └── JobBackpressureController             # runtime/backpressure.py
```

---

## Data Flow (per Split)

```
Source (plan_splits)
  │  Push Split metadata → upstream queue
  ▼
Anvil (anvil-rs, Rust)
  │  claim() → StageWorker (atomic, competing consumers)
  ▼
StageWorker
  1. claim_from_group(merge_upstream=N) → [QueueMessage × N]
  2. fetch SplitPayload from SplitPayloadStore (Arrow tables)
  3. merge payloads (Arrow concat) if N > 1
  4. operator.process_split(split, payload) → PayloadResult
  5. store output SplitPayload → SplitPayloadStore
  6. ack_and_scatter (atomic: ack upstream + push to downstream QueueGroup)
  ▼
Next Stage's QueueGroup ...
  ▼
Sink (write to storage)
  └── SinkManager: batched commit (e.g., LanceDB fragment → commit)
```

---

## Queue Model (Anvil)

All inter-stage data flows through QueueGroup (1 partition for non-shuffle, N for shuffle).
Source planner queues and sink commit queues remain as single queues (internal coordination).
Workers compete via `claim_from_group()` (broker-directed partition selection).

```
Key Schema (RocksDB via anvil-rs):
  meta:{queue}               → QueueMeta { claim_seq, push_seq, pending_count, claimed_count }
  pending:{queue}:{seq}      → msg_id
  msg:{queue}:{msg_id}       → QueueMessage JSON
  claimed:{queue}:{msg_id}   → ClaimInfo { worker_id, claimed_at, timeout_secs }
  acked:{queue}:{ts}:{msg_id} → ""  (GC'd by background task)
  state:{namespace}:{key}    → bytes  (operator persistent state)
```

**O(1) hot paths**: `push`, `claim`, `ack`, `nack`, `state_get`, `state_put`, `get_queue_stats`
**O(n) background**: `recover_expired_claims` (10s), `gc_acked_messages` (60s), `delete_queue`

---

## Operator Contract

```python
@dataclass
class MyConfig(OperatorConfig):
    param: str
    batch_size: int = 32
    # Optional overrides:
    def get_merge_upstream(self) -> int: return 4   # merge 4 upstream msgs
    def create_source(self) -> SourceStrategy: ...  # for source operators
    def create_sink_committer(self) -> SinkCommitter: ...  # for sink operators

@operator(MyConfig)   # binds Config ↔ Operator bidirectionally
class MyOperator(Operator):
    def __init__(self, config: MyConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        # runtime: job_id, stage_id, worker_id, broker_endpoint
        # DO NOT add set_*() methods or mutable state here

    def process_split(self, split: Split, payload: Optional[SplitPayload] = None) -> PayloadResult:
        # Return options:
        #   None                    → drop (filter)
        #   SplitPayload            → 1:1 map
        #   Iterator[SplitPayload]  → 1:N explode
        #   async versions of above also work
        return payload

    @master_callable   # allows StageMaster to call via worker.invoke_operator()
    def get_stats(self) -> dict: ...
```

**State access** (via Anvil, atomic with ack):
```python
# In process_split, use broker_endpoint from runtime:
# state_get(namespace, key) / state_put(namespace, key, value)
# These are atomic with ack_and_scatter — no partial updates
```

---

## Serve Module Actor Model

```
ModelServiceManager (Ray actor, control plane)        serve/manager.py
  ├── GPUAllocator (plain object, bin-packing)         serve/allocator.py
  └── ModelPool × M (plain object, per model)          serve/pool.py
        └── InferenceWorker × N (Ray actors, vLLM)     serve/worker.py

ModelRegistry (Ray Named Actor, service discovery)     serve/registry.py
  └── worker_url list per model_id

ModelClient (plain object, client-side LB)             serve/client.py
  └── HTTP round-robin to InferenceWorker URLs

Two manager modes:
  attached  → actor dies with job (default, create_manager())
  detached  → actor survives job exit (lifetime="detached"), reconnect via connect()
```

---

## StageMaster Lifecycle

```
start()
  ├── SourceManager.start()   → plan_splits() → push to queue (or DirectProducer)
  ├── SinkManager.start()     → launch commit background loop
  └── WorkerManager.start()   → spawn initial workers

run()
  └── monitor loop:
        ├── RecoveryManager.check() → respawn failed workers
        ├── Autoscaler.tick()       → scale up/down based on queue depth
        ├── BackpressureController  → pause/resume upstream source
        └── check completion: all splits acked + no claimed messages

stop()
  ├── SinkManager.finalize()  → flush pending commits
  ├── WorkerManager.stop()    → drain + stop workers
  └── SourceManager.stop()
```

---

## StageMaster ↔ StageWorker Communication

- **Normal**: Workers pull via `claim()` from Anvil (no push from master)
- **Master → Worker**: `worker.invoke_operator("method_name", *args)` for `@master_callable` methods
- **Worker failure**: RecoveryManager detects via Ray actor death; re-enqueues claimed messages via Anvil `nack`/recovery

---

## SplitPayload (Data Format)

```python
@dataclass
class SplitPayload:
    split_id: str
    data: pa.Table          # Arrow table (columnar)
    metadata: Dict[str, Any]

# Large payloads stored in SplitPayloadStore:
#   RaySplitPayloadStore   → Ray object store (in-cluster)
#   FsspecSplitPayloadStore → S3/GCS (cross-cluster / persistence)
```

---

## Control Plane (FastAPI)

```
control/control/
  app.py                    → create_app() factory, mounts all routers
  api/routes/
    health.py               → GET /health
    iceberg_catalog.py      → Iceberg REST catalog proxy
    lance_namespace.py      → LanceDB namespace CRUD
    k8s.py                  → Kubernetes cluster management
  services/
    iceberg_catalog_service.py
    lance_table_service.py
    k8s_cluster_service.py, k8s_connection.py
    rayjob_service.py, rayjob_sync_service.py
    localqueue_service.py
  models/                   → SQLAlchemy ORM
  schemas/                  → Pydantic request/response
  db/session.py             → AsyncSession factory
```

---

## Anvil Rust Architecture

```
lib/anvil-rs/
  src/
    lib.rs          → PyO3 module entry + broker lifecycle
    storage.rs      → all persistent ops: push, claim, ack, nack, state, queue meta, GC, QueueGroup
    service.rs      → gRPC service implementation (AnvilService)
    server.rs       → broker inner (start/stop server)
    state.rs        → in-memory coordination (per-queue claim locks, lease tracking)
    types.rs        → data structures (QueueMessage, QueueMeta, QueueGroupMeta, etc.)
    recovery.rs     → background tasks: RecoveryTask (expire claims) + GcTask (delete acked)
  proto/
    anvil.proto → gRPC service + message definitions
  python/           → Python bindings (PyO3)
```

---

## Key Invariants

1. **Queue operations are O(1)** — counters in QueueMeta, no scans in hot path
2. **Operators are stateless** — all persistent state via Anvil `state_get`/`state_put`
3. **Exactly-once semantics** — `ack_and_scatter` is atomic (ack upstream + push to downstream QueueGroup in one WriteBatch)
4. **Unified QueueGroup** — all inter-stage data flows through QueueGroup (1 partition for non-shuffle, N for shuffle); workers compete via `claim_from_group()`
5. **Operator config is immutable** — frozen after `__init__`; no `set_*()` methods
6. **Core never imports operators** — `_internal/core/` must not reference specific operator types from `_internal/operators/`. Behavior differences are expressed through `OperatorConfig` hooks (`get_output_partition_count()`, `create_source()`, etc.)
