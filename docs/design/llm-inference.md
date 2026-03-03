# LLM Inference Design

---

## Implementation Status (Updated 2026-02-06)

| Component | Status | Notes |
|-----------|--------|-------|
| **EmbeddedLLMOperator** | ✅ Complete | `operators/llm/embedded.py` - vLLM/SGLang offline batch |
| **ExternalLLMOperator** | ✅ Complete | `operators/llm/operator.py` - External HTTP API calls |
| **_internal.serve** | ✅ Complete | Multi-model inference service layer |
| **ModelServiceManager** | ✅ Complete | Control plane for deploy/scale/shutdown |
| **ModelRegistry** | ✅ Complete | aiohttp service discovery (embedded in Ray actor) |
| **ModelClient** | ✅ Complete | Async endpoint discovery + caching |
| **InferenceWorker** | ✅ Complete | vLLM/SGLang subprocess management |
| **ModelPool** | ✅ Complete | Worker lifecycle + built-in autoscaling |
| **Async process_split** | ✅ Complete | StageWorker supports sync/async/iterator returns |

---

## Overview

Nurion Runtime provides three modes for LLM inference:

| Mode | Class | Use Case | Throughput |
|------|-------|----------|------------|
| **Embedded** | `EmbeddedLLMOperator` | Batch processing with dedicated GPUs | **Highest** |
| **External (_internal.serve)** | `ExternalLLMOperator` + `ModelServiceManager` | Multi-model serving with dynamic scaling | High |
| **External (direct)** | `ExternalLLMOperator` + `base_url` | Existing external services | Medium |

---

## Architecture

### Mode 1: Embedded Engine (Recommended for single-model batch)

Loads vLLM/SGLang engine directly inside Nurion runtime workers. Zero HTTP overhead.

```
Source ──> Transform ──> LLM Stage (embedded vLLM) ──> Sink
                              │
                    ┌─────────┴──────────┐
                    │  Worker 0 (4 GPU)  │
                    │  ┌──────────────┐  │
                    │  │  vLLM Engine │  │
                    │  │  (in-process)│  │
                    │  └──────────────┘  │
                    └────────────────────┘
```

### Mode 2: External with nurion.serve (Multi-model + dynamic scaling)

For scenarios requiring multiple models, autoscaling, and service discovery.

```
┌──────────────────────────────────────────────────────────────────────┐
│  ModelServiceManager (driver)                                        │
│  ├── ModelRegistry (Ray actor + aiohttp server)                     │
│  │    └── HTTP API: /register, /heartbeat, /endpoints_status        │
│  └── ModelPool (Ray actor, per model)                                │
│       ├── InferenceWorker 0 (subprocess: vLLM server :8001)         │
│       ├── InferenceWorker 1 (subprocess: vLLM server :8002)         │
│       └── Autoscaler (background task)                               │
├──────────────────────────────────────────────────────────────────────┤
│  Solstice Workflow (STEP 2)                                          │
│  ┌──────────────────────────────────────────┐                        │
│  │  StageWorker 0..N (ExternalLLMOperator)  │                        │
│  │    └── ModelClient (async, cached)       │                        │
│  │         └── HTTP GET registry → endpoints│                        │
│  │    └── httpx POST /v1/chat/completions   │──── round-robin ──►   │
│  └──────────────────────────────────────────┘    vLLM servers       │
└──────────────────────────────────────────────────────────────────────┘
```

**Data flow**:
1. `ModelServiceManager` creates `ModelRegistry` + `ModelPool` actors
2. `ModelPool` spawns `InferenceWorker` actors, each running vLLM as subprocess
3. Workers register with registry via HTTP heartbeat
4. `ExternalLLMOperator` uses `ModelClient` to discover endpoints from registry
5. Requests are distributed via round-robin with random offset

**Key design choices**:
- **Registry = aiohttp in Ray async actor**: High-throughput HTTP for data plane, Ray actor for lifecycle
- **Non-detached actors**: Auto-cleanup on job exit, no GPU leak
- **ActorHandle passed explicitly**: `manager → pool → worker` chain, no `ray.get_actor` lookups
- **PR_SET_PDEATHSIG**: Kernel-level guarantee that vLLM subprocess dies when actor dies
- **Async process_split**: Native `asyncio.gather` for concurrent HTTP requests, no ThreadPoolExecutor hacks

---

## _internal.serve Components

### ModelServiceManager

Control plane entry point. Creates registry, deploys models, manages lifecycle.

```python
from _internal.serve import ModelServiceManager, ModelConfig

manager = ModelServiceManager()

await manager.deploy_model(ModelConfig(
    model_id="Qwen/Qwen2.5-72B-Instruct",
    model_source="Qwen/Qwen2.5-72B-Instruct",
    tensor_parallel_size=4,
    min_workers=2,
    max_workers=8,
))

# Shutdown (or let job exit — non-detached actors auto-cleanup)
await manager.shutdown()
```

### ModelRegistry

aiohttp HTTP server embedded in a Ray async actor. Workers POST heartbeats,
clients GET endpoint lists.

HTTP routes:
- `POST /register` — worker registers endpoint
- `POST /heartbeat` — worker reports status (pending, running)
- `GET /endpoints_status?model_id=...` — get endpoints with load info
- `GET /health` — health check

### ModelPool

Manages `InferenceWorker` actors for a single model. Built-in autoscaler.

- `scale_to(n)` — scale to N workers
- `start_autoscaler(config)` — background loop: scale up on high pending, scale down on idle
- `get_status()` — fetches status from registry (single HTTP call)
- `shutdown()` — stops autoscaler, gracefully shuts down all workers

### InferenceWorker

Ray actor that runs vLLM/SGLang as a subprocess.

- Starts vLLM via `subprocess.Popen` with `PR_SET_PDEATHSIG(SIGKILL)`
- Polls `/health` until ready, then registers with registry
- Background heartbeat loop reports `/metrics` (pending/running) to registry
- Streams vLLM subprocess stdout to Ray actor logs

### ModelClient

Async endpoint discovery. Used by `ExternalLLMOperator`.

```python
client = ModelClient(registry=registry_handle)
endpoints = await client.get_endpoints("Qwen/Qwen2.5-72B-Instruct")
# → ["http://10.1.48.251:8001", "http://10.1.48.251:8002"]
```

### EndpointSelectPolicy

Round-robin with random offset for distributed load balancing.

```python
selector = EndpointSelectPolicy(endpoints)
for request in batch:
    endpoint = selector.next()  # round-robin from random start
```

---

## ExternalLLMOperator

Async operator that calls vLLM/SGLang HTTP API. Inherits from `Operator` directly
(not `HttpOperator` — rate limiting and circuit breaker are unnecessary for own servers).

```python
@dataclass
class ExternalLLMOperatorConfig(OperatorConfig):
    # Endpoint mode
    base_url: str = ""                              # Direct mode
    use_model_client: bool = False                  # ModelClient mode
    registry: Optional[ray.ActorHandle] = None      # Required when use_model_client=True

    # Model name (used for both endpoint discovery and API body)
    model: str = ""

    # HTTP
    timeout: float = 120.0
    max_retries: int = 3

    # Generation parameters
    max_tokens: int = 512
    temperature: float = 0.7
    top_p: float = 0.95

    # Vision mode
    prompt: str = ""
    image_field: str = ""
    detail: Literal["auto", "low", "high"] = "auto"

    # Output
    output_field: str = "response"
    batch_size: int = 32  # Concurrent requests per split
```

**Async process_split**: Uses `asyncio.gather` for concurrent batch requests.
StageWorker natively supports `async def process_split` (also supports sync,
iterator, and async iterator returns via `PayloadResult` type).

---

## Actor Lifecycle & GPU Cleanup

All actors are **non-detached** (reference-counted):
- When `ModelServiceManager` is GC'd or job exits, all actors die
- `PR_SET_PDEATHSIG(SIGKILL)` on vLLM subprocess ensures GPU memory release
- No `lifetime="detached"`, no orphan processes

**ActorHandle reference chain** (keeps actors alive):
```
manager._registry ──► ModelRegistry actor
manager._pools[id] ──► ModelPool actor
    pool._registry ──► ModelRegistry actor (extra ref)
    pool._workers[id] ──► InferenceWorker actor
        worker._registry ──► ModelRegistry actor (extra ref)
```

---

## File Structure

```
engine/
├── operators/
│   ├── http/
│   │   ├── operator.py          # HttpOperator base (rate limiter, circuit breaker)
│   │   ├── rate_limiter.py      # For external API rate limiting
│   │   └── circuit_breaker.py
│   └── llm/
│       ├── embedded.py          # EmbeddedLLMOperator (recommended for batch)
│       ├── operator.py          # ExternalLLMOperator + EndpointSelectPolicy
│       └── utils.py             # Shared: message building, image encoding
├── serve/
│   ├── __init__.py              # Public API exports
│   ├── config.py                # ModelConfig, AutoscaleConfig, WorkerState
│   ├── manager.py               # ModelServiceManager (control plane)
│   ├── pool.py                  # ModelPool (worker lifecycle + autoscaling)
│   ├── registry.py              # ModelRegistry (aiohttp service discovery)
│   ├── worker.py                # InferenceWorker (vLLM subprocess)
│   └── client.py                # ModelClient (async endpoint discovery)
└── core/
    ├── operator.py              # PayloadResult type (sync/async/iterator)
    └── stage_worker.py          # _collect_outputs (handles all return types)
```

---

## Usage Examples

### Example 1: Embedded VLM Captioning

```python
from _internal.operators.llm import EmbeddedLLMOperatorConfig

job.add_stage(Stage(
    stage_id="caption",
    operator_config=EmbeddedLLMOperatorConfig(
        backend="vllm",
        model="Qwen/Qwen3-VL-32B-Instruct",
        tensor_parallel_size=4,
        prompt="Describe this image in detail.",
        image_field="image_bytes",
        kv_cache_dtype="fp8_e4m3",
        vllm_enable_chunked_prefill=True,
    ),
    parallelism=4,
    worker_resources={"num_gpus": 4},
))
```

### Example 2: External with _internal.serve

```python
from _internal.serve import ModelServiceManager, ModelConfig
from _internal.operators.llm import ExternalLLMOperatorConfig

# STEP 1: Deploy model
manager = ModelServiceManager()
await manager.deploy_model(ModelConfig(
    model_id="Qwen/Qwen3-VL-32B-Instruct",
    model_source="Qwen/Qwen3-VL-32B-Instruct",
    tensor_parallel_size=4,
    min_workers=2, max_workers=4,
))

# STEP 2: Use in workflow
job.add_stage(Stage(
    stage_id="caption",
    operator_config=ExternalLLMOperatorConfig(
        use_model_client=True,
        registry=manager.registry,
        model="Qwen/Qwen3-VL-32B-Instruct",
        prompt="Describe this image.",
        image_field="image",
        batch_size=16,
    ),
    parallelism=8,
))
```

### Example 3: Direct external service

```python
job.add_stage(Stage(
    stage_id="inference",
    operator_config=ExternalLLMOperatorConfig(
        base_url="http://vllm-service:8000",
        model="Qwen/Qwen2.5-72B-Instruct",
        messages_field="messages",
    ),
    parallelism=8,
))
```

---

## Design Decisions

### 1. Why embedded mode over HTTP?

Zero overhead: direct Python call, zero-copy via Ray Object Store, natural backpressure.
HTTP mode has 1-5ms latency per request, serialization cost, and connection management.

### 2. Why _internal.serve instead of Ray Serve?

- **Imperative control**: `deploy_model()` returns when ready, `scale_to()` returns when done
- **Compatible with Ray Data**: No abstraction mismatch
- **Simpler**: No deployment graph, no YAML, no K8s-style declarations
- **No single-point Router**: Client-side load balancing via round-robin

### 3. Why not use a separate namespace for serve actors?

All actors (registry, pool, worker, StageWorker) run in the same Ray Job namespace.
Using a separate namespace caused cross-namespace lookup failures.

### 4. Why non-detached actors?

- Auto-cleanup on job exit (even SIGKILL/OOM)
- No GPU memory leaks
- No orphan processes
- Trade-off: actors die with the job (no persistent serving across jobs)

### 5. Why pass ActorHandle explicitly instead of ray.get_actor?

- **Reference counting**: Non-detached actors die when all handles are GC'd. Explicit passing ensures each layer holds a reference.
- **No hidden dependencies**: No magic name lookups, clear ownership chain.
- **Serializable**: ActorHandle can be passed through OperatorConfig dataclass.

### 6. Why async process_split?

- Native `asyncio.gather` for concurrent HTTP requests within a batch
- No `ThreadPoolExecutor` hacks to bridge sync/async
- StageWorker detects coroutine returns and awaits automatically

---

## Future Work

1. **Weighted round-robin** — Select policy based on pending count from registry
2. **LoRA Adapter Support** — Dynamic adapter loading per model
3. **Speculative Decoding** — Draft model acceleration
4. **Cross-worker Prefix Caching** — Share common prefixes across workers
5. **Multi-job persistent serving** — Detached mode for shared inference services

---

*Last updated: 2026-02-06*
