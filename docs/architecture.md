# Nurion Engine Architecture

This document describes the internal architecture of Nurion Engine -- a Ray-based, high-throughput data processing framework with streaming-style execution.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Core Abstractions](#2-core-abstractions)
3. [Runtime Architecture](#3-runtime-architecture)
4. [Data Flow](#4-data-flow)
5. [Anvil: The Message Backbone](#5-anvil-the-message-backbone)
6. [Component Managers](#6-component-managers)
7. [Autoscaling](#7-autoscaling)
8. [Fault Tolerance & Recovery](#8-fault-tolerance--recovery)
9. [LLM Inference Architecture](#9-llm-inference-architecture)
10. [Multi-Model Serve Layer](#10-multi-model-serve-layer)
11. [WebUI & Observability](#11-webui--observability)

---

## 1. System Overview

Nurion Engine processes data through **DAG pipelines** where each node is a **Stage** that runs one or more **Workers**. Workers pull data from upstream queues, process it through **Operators**, and push results to downstream queues.

```
                         ┌─────────────────────────────────────────┐
                         │            RayJobRunner                 │
                         │         (Job Orchestrator)              │
                         │                                         │
                         │  ┌───────────┐  ┌───────────┐           │
                         │  │  Stage     │  │  Stage     │          │
                         │  │  Master 0  │  │  Master 1  │  ...     │
                         │  │  (Source)  │  │  (Xform)  │          │
                         │  └─────┬─────┘  └─────┬─────┘          │
                         │        │              │                 │
                         │   ┌────┴────┐    ┌────┴────┐            │
                         │   │Worker(s)│    │Worker(s)│            │
                         │   └────┬────┘    └────┬────┘            │
                         │        │              │                 │
                         │        ▼              ▼                 │
                         │   [Output Q]     [Output Q]             │
                         │                                         │
                         │   ┌──────────────────────────────────┐  │
                         │   │    Anvil Broker (Rust/gRPC)  │  │
                         │   └──────────────────────────────────┘  │
                         │                                         │
                         │   ┌──────────────────────────────────┐  │
                         │   │     SplitPayloadStore             │  │
                         │   │  (Ray Object Store / S3 / fsspec) │  │
                         │   └──────────────────────────────────┘  │
                         └─────────────────────────────────────────┘
```

**Key properties:**

- **Pull-based**: Workers pull messages from upstream queues (not push)
- **Queue-driven**: All inter-stage communication goes through Anvil
- **Competing consumers**: Multiple workers compete for messages on the same queue (no partitions)
- **Natural backpressure**: Queue lag signals upstream to slow down
- **Elastic**: Workers can be added/removed at runtime without rebalancing

---

## 2. Core Abstractions

### 2.1 Job

A `Job` is a DAG of Stages. It holds:
- A `JobConfig` (queue backend URI, payload store URI, autoscale config, WebUI config)
- A set of `Stage` objects connected by directed edges

```python
job = Job(job_id="my_pipeline", config=JobConfig(anvil_db_path="memory://"))
job.add_stage(source_stage)
job.add_stage(transform_stage, upstream_stages=["source"])
job.add_stage(sink_stage, upstream_stages=["transform"])
```

### 2.2 Stage

A `Stage` wraps an `OperatorConfig` with runtime settings:

| Property | Description |
|----------|-------------|
| `stage_id` | Unique identifier |
| `operator_config` | Immutable config that creates the Operator |
| `parallelism` | `int` (fixed) or `(min, max)` tuple (auto-scale) |
| `worker_resources` | Ray resources per worker (`num_cpus`, `num_gpus`, `memory`) |
| `batch_size` | Messages claimed per batch |
| `backpressure_threshold_lag` | Queue lag threshold to activate backpressure |

### 2.3 Operator & OperatorConfig

The processing logic lives in `Operator.process_split()`. Configuration is separated into:

- **OperatorConfig** (user-defined, immutable): Model parameters, file paths, thresholds
- **OperatorRuntime** (system-assigned, immutable): `job_id`, `stage_id`, `worker_id`, `broker_endpoint`

The `@operator` decorator binds a config class to its operator class:

```python
@dataclass
class MyConfig(OperatorConfig):
    threshold: float = 0.5

@operator(MyConfig)
class MyOperator(Operator):
    def process_split(self, split, payload):
        ...
```

**`process_split()` return types:**

| Return Type | Behavior |
|-------------|----------|
| `None` | Drop the split (filter) |
| `SplitPayload` | Single output (map, 1:1) |
| `Iterator[SplitPayload]` | Multiple outputs (explode, 1:N) |
| `async def` returning the above | Async variants |
| `AsyncIterator[SplitPayload]` | Async generator |
| `RawOutputBytes` | Raw bytes forwarded to output queue (sink commit metadata) |

### 2.4 Split & SplitPayload

- **Split**: Metadata (split_id, stage_id, data_range, parent lineage)
- **SplitPayload**: Actual data, backed by a `pyarrow.Table` for zero-copy operations

Data is stored in a `SplitPayloadStore` (Ray Object Store by default, or S3/fsspec), and only a reference key is passed through the queue.

---

## 3. Runtime Architecture

### 3.1 RayJobRunner

The top-level orchestrator. It:

1. Initializes Ray and creates a shared **Anvil broker** (Rust process)
2. Creates a **SplitPayloadStore** for cross-stage data sharing
3. Creates **StageMaster** instances in topological order (sources first)
4. Starts all masters, monitors progress, handles failures
5. Coordinates **autoscaling** via `SimpleAutoscaler`
6. Manages **WebUI** integration (embedded server, state writer)
7. Propagates upstream completion signals downstream

### 3.2 StageMaster

Each stage has a `StageMaster` that orchestrates its workers. It is **not** a Ray actor -- it runs as a coroutine in the RayJobRunner's event loop.

**Responsibilities:**

- Creates and manages the stage's output queue
- Delegates to component managers (see [Section 6](#6-component-managers))
- Handles source production (for source stages)
- Manages sink commit loop (for sink stages)
- Provides scaling interface for the autoscaler

### 3.3 StageWorker

`StageWorker` is a **Ray Actor** that executes the processing loop:

```
┌──────────────────────────────────────────────────────┐
│                    StageWorker                        │
│                                                      │
│   1. claim(batch_size) ← upstream queue              │
│   2. retrieve payloads ← SplitPayloadStore           │
│   3. merge payloads (if merge_upstream > 1)          │
│   4. operator.process_split(split, payload)          │
│   5. store output payload → SplitPayloadStore        │
│   6. ack_and_forward() → ack upstream + push output  │
│   7. repeat until queue drained + safe_to_exit       │
│                                                      │
└──────────────────────────────────────────────────────┘
```

**Key behaviors:**

- **Stateless**: All state lives in Anvil server or payload store
- **Atomic ack-and-forward**: Upstream messages are acked and output is pushed in a single atomic operation
- **Graceful exit**: Workers exit when notified that upstream is finished AND the queue is drained
- **Supports sync/async**: `process_split()` can be sync, async, or a generator

---

## 4. Data Flow

### 4.1 Message Flow Through the Pipeline

```
Source Stage                    Transform Stage                  Sink Stage
┌──────────┐                   ┌──────────┐                    ┌──────────┐
│ Source    │  push(msg)        │ Worker   │  push(msg)         │ Worker   │
│ Operator │ ──────────►       │          │ ──────────►        │          │
│          │            Queue A │          │             Queue B │          │
└──────────┘                   └──────────┘                    └──────────┘
                                    │                               │
                                claim(N)                        claim(N)
                                    │                               │
                              process_split()                 process_split()
                                    │                               │
                              ack_and_forward()               ack_and_forward()
```

### 4.2 Payload vs. Queue Message

Queue messages are lightweight -- they carry only a **payload key** (reference), not the data itself:

```
QueueMessage {
    message_id: "msg_001"
    split_id: "job:stage:001"
    payload_key: "ray_object_ref_abc123"   ← points to SplitPayloadStore
    message_type: DATA | EOF
}
```

The actual `SplitPayload` (PyArrow Table) is stored in `SplitPayloadStore`:
- **`ray://`** (default): Ray Object Store -- zero-copy, in-memory
- **`s3://...`** or **`file://...`**: fsspec-backed storage for large payloads

### 4.3 Completion Signaling

There are no EOF messages in the queue. Instead:

1. Source finishes producing → calls `mark_queue_finished()` on its output queue
2. StageMaster detects all upstream queues finished + stage's input queue is drained
3. Notifies workers via `safe_to_exit` flag
4. Workers exit gracefully after processing remaining messages
5. StageMaster marks its own output queue as finished
6. Signal cascades downstream through the DAG

---

## 5. Anvil: The Message Backbone

Anvil is an embedded Rust-based message broker that provides the queue backbone for all inter-stage communication.

### 5.1 Architecture

```
┌──────────────────────────────────────────────┐
│              Anvil Broker                 │
│          (Rust process, gRPC API)             │
│                                               │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐      │
│  │ Queue A  │  │ Queue B  │  │ Queue C  │     │
│  │(source→  │  │(xform→  │  │(xform→  │     │
│  │ xform)   │  │  sink)  │  │ commit) │     │
│  └─────────┘  └─────────┘  └─────────┘      │
│                                               │
│  Storage: SlateDB (S3-backed) or memory://    │
│  State Store: key-value per queue             │
└──────────────────────────────────────────────┘
```

### 5.2 Message Lifecycle

```
PENDING ──claim()──► CLAIMED ──ack()──► ACKED ──GC──► DELETED
                         │
                     nack() or
                     timeout
                         │
                         ▼
                      PENDING (returned to queue tail)
```

1. **push()**: Producer adds a message → status: PENDING
2. **claim()**: Worker atomically grabs N messages with a lease timeout → status: CLAIMED
3. **ack()**: Worker confirms processing → status: ACKED (retained until GC)
4. **nack()**: Worker rejects message → returns to PENDING at queue tail
5. **ack_and_forward()**: Atomically acks upstream + pushes to downstream queue

### 5.3 Key Design Choices

| Choice | Rationale |
|--------|-----------|
| **Single queue, no partitions** | Enables work-stealing load balancing; no partition-worker coupling |
| **Competing consumers** | Workers compete via `claim()` -- natural load distribution |
| **Claim-based leasing** | Messages auto-return to PENDING if worker dies (timeout-based) |
| **GC-based ack retention** | Acked messages retained for debugging; cleaned up by GC pass |
| **Integrated state store** | Operators access state via Anvil server (single-writer, no conflicts) |
| **Embedded broker** | No external dependencies; `memory://` for tests, `file://` for persistence |

### 5.4 Exactly-Once Semantics

Nurion achieves effectively-once processing through:

- **Source deduplication**: `push_with_dedup` using deterministic business keys prevents duplicate source data
- **Idempotent sinks**: Sink operators designed to handle replayed messages
- **Atomic ack_and_forward**: Cross-stage atomicity prevents partial processing

---

## 6. Component Managers

StageMaster delegates to specialized managers:

```
StageMaster
├── WorkerManager      – Worker lifecycle (spawn, stop, readiness)
├── RecoveryManager    – Failure tracking and worker recovery
├── SourceManager      – Source split production (SplitPlanner / DirectProducer)
└── SinkManager        – Sink commit loop (SinkCommitter)
```

### 6.1 WorkerManager

Manages the lifecycle of `StageWorker` Ray actors:

- **Spawn**: Creates actors with resource requirements (`num_cpus`, `num_gpus`, `memory`)
- **Readiness check**: Verifies workers are healthy after spawn (handles resource constraints)
- **Graceful stop**: Signals workers to exit, waits for completion
- **Completion detection**: Uses `ray.wait()` for event-driven monitoring
- **Safe-to-exit**: Notifies workers when upstream is finished and queue is drained

### 6.2 RecoveryManager

Handles worker failures with a sliding-window approach:

- Tracks failure timestamps within a configurable time window
- Calculates failure rate relative to worker count
- Applies exponential backoff between recovery attempts
- **Gives up** when failure rate exceeds `max_failures_per_worker` threshold
- Spawns replacement workers via WorkerManager

### 6.3 SourceManager

Manages source data production:

- **SplitPlanner sources**: Creates a planner queue, produces splits asynchronously, workers consume splits
- **DirectProducer sources**: Executes the producer directly (no workers needed)
- **Backpressure**: Pauses split production when downstream queues are full
- **Completion**: Marks planner queue as finished when production completes

### 6.4 SinkManager

Manages batched commit coordination for sink stages:

- Creates a **commit queue** (workers push commit metadata here instead of to stage output)
- Runs background **commit loop** (`SinkCommitter.run_commit_loop()`)
- Handles finalize when all data is committed
- Example: Lance sink workers write fragments, SinkManager batch-commits them

---

## 7. Autoscaling

Nurion uses threshold-based autoscaling optimized for batch/offline workloads.

### 7.1 Pipeline Autoscaling (SimpleAutoscaler)

The `SimpleAutoscaler` runs in `RayJobRunner` and monitors all stages:

```
┌────────────────────────────────────────────┐
│            SimpleAutoscaler                │
│                                            │
│  For each stage:                           │
│    1. Read queue stats (pending, claimed)  │
│    2. If pending > threshold → scale UP    │
│    3. If idle > timeout → scale DOWN       │
│    4. Cooldown between decisions           │
│    5. Check Ray available resources        │
│                                            │
│  Interval: 15-30 seconds                   │
│  Cooldown: 30-60 seconds                   │
└────────────────────────────────────────────┘
```

**Scale-up rule**: `pending_count > scale_up_threshold` (configurable per stage)

**Scale-down rule**: `pending_count == 0 AND claimed_count == 0` for `scale_down_idle_seconds`

### 7.2 Serve Layer Autoscaling

The serve layer (`ModelPool`) has its own autoscaler for inference workers:

- Scale up when `pending_requests > threshold * ready_workers`
- Scale down after `idle_seconds` with no pending requests
- Configurable via `AutoscaleConfig`
- Can be frozen/unfrozen for manual control

---

## 8. Fault Tolerance & Recovery

### 8.1 Worker Failure Recovery

```
Worker crashes → ray.wait() detects exit
    │
    ▼
RecoveryManager.record_failures()
    │
    ├── Failure rate OK? → spawn replacement worker
    │                       (claimed messages auto-return after timeout)
    │
    └── Failure rate too high? → fail the stage → fail the job
```

- **Claimed messages**: Auto-return to PENDING after `claim_timeout_secs` (default: 60s)
- **No data loss**: Unacked messages are automatically retried by new or existing workers
- **Sliding window**: Only recent failures count (old failures age out)
- **Backoff**: Exponential delay between recovery attempts (0.5s → 5s cap)

### 8.2 Source Replay Protection

Sources may replay data after recovery. Protection mechanisms:

- `push_with_dedup`: Uses deterministic business keys to prevent duplicate pushes
- Idempotent sink operators: Designed to handle replayed data gracefully

### 8.3 Poison Message Handling

- **nack()** returns messages to the **tail** of the queue (not head)
- Prevents a single bad message from blocking all workers
- After multiple failures, the message eventually ages out or is manually handled

### 8.4 GPU Process Cleanup

For LLM inference workers with GPU subprocesses:

- `PR_SET_PDEATHSIG(SIGKILL)`: Kernel-level guarantee that vLLM subprocess dies when parent actor dies
- `os.setsid()`: Process group isolation for clean killpg() shutdown
- `__del__` fallback: Emergency force-kill of subprocess

---

## 9. LLM Inference Architecture

Nurion supports three modes for LLM inference:

### 9.1 Embedded Mode

The vLLM/SGLang engine runs **inside** the Nurion worker process:

```
StageWorker (Ray Actor, 4 GPUs)
├── EmbeddedLLMOperator
│   └── vLLM LLM engine (in-process)
│       └── 4 GPU workers (multiprocessing, NOT Ray)
└── Claims from upstream queue, processes batch, pushes to downstream
```

- **Zero HTTP overhead**: Direct Python calls to engine
- **Lazy initialization**: Engine loaded on first `process_split()`
- **vLLM uses mp internally**: `distributed_executor_backend="mp"` avoids nesting Ray within Ray
- **Best for**: Single-model batch processing with dedicated GPUs

### 9.2 External Mode with nurion.serve

Separate inference servers managed by `ModelServiceManager`:

```
ModelServiceManager
├── ModelRegistry (Ray actor + aiohttp HTTP server)
│   └── /register, /heartbeat, /endpoints_status, /health
└── ModelPool (per model)
    ├── InferenceWorker 0 (subprocess: vLLM server :8001)
    ├── InferenceWorker 1 (subprocess: vLLM server :8002)
    └── Autoscaler (background task)

Pipeline Workers (ExternalLLMOperator)
└── ModelClient → HTTP GET registry → endpoints
└── httpx POST /v1/chat/completions → round-robin to vLLM servers
```

- **Independent scaling**: Inference servers and pipeline workers scale separately
- **Multi-model**: Deploy multiple models with different TP sizes and resource requirements
- **Service discovery**: ModelClient caches endpoints with TTL, round-robin load balancing
- **Best for**: Multi-model scenarios, shared inference services

### 9.3 External Mode (Direct)

Call any existing OpenAI-compatible endpoint:

```python
ExternalLLMOperatorConfig(
    base_url="http://my-vllm-server:8000",
    model="Qwen/Qwen2.5-72B-Instruct",
    messages_field="messages",
)
```

- **No management overhead**: Use pre-existing services
- **Best for**: Connecting to managed inference endpoints

---

## 10. Multi-Model Serve Layer

### 10.1 Component Overview

```
ModelServiceManager (Python, driver process)
│
├── ModelRegistry (Ray async actor)
│   ├── Embedded aiohttp HTTP server (data plane)
│   │   └── /register, /unregister, /heartbeat
│   │   └── /endpoints, /endpoints_status, /models, /status, /health
│   └── Ray methods (control plane)
│       └── get_http_url(), list_models(), start(), stop()
│
├── ModelPool "model_A" (Ray actor)
│   ├── InferenceWorker 0 (Ray actor + vLLM subprocess)
│   ├── InferenceWorker 1 (Ray actor + vLLM subprocess)
│   └── Autoscaler (asyncio background task)
│
└── ModelPool "model_B" (Ray actor)
    ├── InferenceWorker 0 (Ray actor + SGLang subprocess)
    └── Autoscaler
```

### 10.2 Actor Lifecycle Modes

**Attached (default):**
- All actors are reference-counted
- Auto-destroyed when the job exits (or when `ModelServiceManager` is GC'd)
- `PR_SET_PDEATHSIG` on subprocesses guarantees GPU cleanup

**Detached:**
- Actors created with `lifetime="detached"` survive job exit
- Must be explicitly destroyed via `manager.shutdown()`
- Reconnect from a new job via `ModelServiceManager.connect()`

### 10.3 Actor Reference Chain

```
manager._registry ────────────► ModelRegistry actor
manager._pools["model_A"] ────► ModelPool actor
    pool._registry ────────────► ModelRegistry actor (keeps alive)
    pool._workers["w1"] ──────► InferenceWorker actor
        worker._registry ─────► ModelRegistry actor (keeps alive)
```

Every layer holds a reference to the registry, ensuring it stays alive as long as any component needs it.

### 10.4 InferenceWorker Lifecycle

```
__init__() → start server subprocess
    │
start() → launch background tasks
    │
    ├── _monitor_server() → stream subprocess stdout to Ray logs
    └── _wait_for_ready() → poll /health until 200
        │
        ├── Register with registry via HTTP POST /register
        └── Start heartbeat loop (POST /heartbeat every 2s)
            │
shutdown() → unregister → SIGTERM → SIGKILL (timeout) → cleanup
```

### 10.5 Service Discovery Flow

```
ExternalLLMOperator
    │
    └── ModelClient.get_endpoints("model_A")
        │
        ├── Cache hit (TTL < 30s)? → return cached endpoints
        │
        └── Cache miss → HTTP GET registry/endpoints_status?model_id=model_A
            │
            └── Returns: [{"endpoint": "http://10.0.1.5:8001", "pending": 3, ...}]
                │
                └── Filter ready endpoints → return URLs
```

Requests are distributed via `EndpointSelectPolicy` (round-robin with random offset per worker, ensuring distributed workers don't all start from index 0).

---

## 11. WebUI & Observability

Nurion includes a web-based debugging interface for monitoring jobs.

### 11.1 Architecture

**Dual-mode design:**
- **Embedded mode**: WebUI runs alongside the job via Ray Serve (port 8000)
- **History server**: Standalone service for viewing completed jobs

**Storage:**
- **Prometheus**: Real-time metrics (1s granularity)
- **SlateDB**: Historical archives (30s snapshots + events)

### 11.2 Features

- Live stage progress, ETA, and throughput metrics
- Partition-level offset tracking and skew detection
- Worker resource usage (CPU/Memory/GPU)
- Split lineage and data flow visualization
- Exception tracking with root cause hints

### 11.3 Tech Stack

| Component | Technology |
|-----------|-----------|
| Backend | FastAPI + Ray Serve |
| Frontend | HTMX + Alpine.js + Jinja2 |
| Styling | Pico CSS |
| Real-time | Server-Sent Events (SSE) |
| Charts | Chart.js |

---

## Appendix

### A. File Structure

```
engine/
├── _internal/
│   ├── core/
│   │   ├── job.py              # Job, JobConfig, WebUIConfig
│   │   ├── stage.py            # Stage, StageRuntime
│   │   ├── operator.py         # Operator, OperatorConfig, OperatorRuntime, @operator
│   │   ├── source_operator.py  # SourceOperator (read + offset tracking)
│   │   ├── models.py           # Split, SplitPayload, QueueMessage, FailurePolicy
│   │   ├── stage_master.py     # StageMaster (per-stage orchestrator)
│   │   ├── stage_worker.py     # StageWorker (Ray Actor, processing loop)
│   │   └── managers/
│   │       ├── worker_manager.py    # Worker lifecycle
│   │       ├── recovery_manager.py  # Failure tracking & recovery
│   │       ├── source_manager.py    # Source split production
│   │       └── sink_manager.py      # Sink commit coordination
│   │
│   ├── runtime/
│   │   ├── ray_runner.py       # RayJobRunner (top-level orchestrator)
│   │   ├── autoscaler.py       # SimpleAutoscaler
│   │   └── backpressure.py     # JobBackpressureController
│   │
│   ├── queue/
│   │   ├── backend.py          # Queue abstraction (QueueBackend protocol)
│   │   └── anvil.py        # Anvil Rust broker integration
│   │
│   ├── operators/
│   │   ├── sources/            # Lance, Iceberg, Spark, File sources
│   │   ├── sinks/              # Lance, File, Print sinks
│   │   ├── map.py              # Map, FlatMap, MapBatches
│   │   ├── filter.py           # Filter
│   │   ├── llm/
│   │   │   ├── embedded.py     # EmbeddedLLMOperator (in-process vLLM/SGLang)
│   │   │   ├── operator.py     # ExternalLLMOperator (HTTP-based)
│   │   │   └── utils.py        # Shared utilities
│   │   └── http/               # HTTP operator base (rate limiter, circuit breaker)
│   │
│   ├── serve/
│   │   ├── config.py           # ModelConfig, AutoscaleConfig, WorkerState
│   │   ├── manager.py          # ModelServiceManager (control plane)
│   │   ├── pool.py             # ModelPool (worker lifecycle + autoscaling)
│   │   ├── registry.py         # ModelRegistry (aiohttp service discovery)
│   │   ├── worker.py           # InferenceWorker (vLLM/SGLang subprocess)
│   │   └── client.py           # ModelClient (async endpoint discovery)
│   │
│   └── webui/
│       ├── app.py              # FastAPI + Ray Serve integration
│       ├── storage/            # SlateDB + Prometheus storage
│       ├── collectors/         # Metric collectors
│       ├── api/                # REST API routes
│       └── templates/          # Jinja2 + HTMX templates
│
├── nurion/
│   └── __init__.py             # Public API re-exports
│
├── workflows/                  # Example workflows
├── design/                     # Architecture decision records (in docs/design/)
└── tests/                      # Unit + integration tests
```

### B. Design Principles

1. **Pull-based, queue-driven**: Workers pull work, enabling natural load balancing and backpressure
2. **Config/runtime separation**: `OperatorConfig` (user, immutable) vs `OperatorRuntime` (system, immutable)
3. **Stateless workers**: All state in Anvil server or payload store -- workers are disposable
4. **Single queue, no partitions**: Eliminates partition-worker coupling, enables work-stealing
5. **Atomic operations**: `ack_and_forward` ensures cross-stage consistency
6. **Explicit references**: ActorHandles passed explicitly, no magic `ray.get_actor` lookups
7. **Minimal instance state**: Prefer local variables and config-derived values over `self._` attributes
8. **Let exceptions propagate**: Low-level code should not swallow exceptions

### C. Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Queue backend | Anvil (Rust + SlateDB) | Embedded, no external dependencies, high throughput |
| Queue model | Single queue, competing consumers | No partition rebalancing; work-stealing load balancing |
| Payload transport | Reference keys through queue | Queue stays lightweight; data in Object Store or S3 |
| Exactly-once | Source dedup + idempotent sinks | Simpler than cross-stage dedup; works with work-stealing |
| Worker model | Ray Actors | Natural isolation, resource management, fault detection |
| Autoscaling | Threshold-based with cooldown | Simple, sufficient for batch workloads |
| Serve layer | Custom (not Ray Serve) | Imperative API, no YAML, client-side load balancing |
| Subprocess cleanup | PR_SET_PDEATHSIG | Kernel-level guarantee, handles SIGKILL/OOM |
