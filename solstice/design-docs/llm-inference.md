# LLM Inference Design

## Overview

Solstice supports large-scale LLM batch inference with two modes:

1. **Managed Mode** - Solstice automatically manages SGLang Router and GPU Workers
2. **External Service Mode** - Connect to externally deployed vLLM/SGLang/OpenAI services

Both modes use HTTP calls to OpenAI-compatible APIs and share the same fault tolerance mechanisms.

## Architecture

### Mode 1: Solstice-Managed Inference Service

```
┌─────────────────────────────────────────────────────────────────┐
│                      LLMStageMaster                             │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │              SGLang Router Actor                        │   │
│   │              - Load balancing (round_robin/cache_aware) │   │
│   │              - Health checking                          │   │
│   │              - Dynamic worker registration              │   │
│   └────────────────────────┬────────────────────────────────┘   │
│                            │ HTTP                               │
│     ┌──────────────────────┼──────────────────────┐            │
│     ▼                      ▼                      ▼            │
│ ┌────────────┐       ┌────────────┐       ┌────────────┐       │
│ │ GPU Worker │       │ GPU Worker │       │ GPU Worker │       │
│ │   Actor 1  │       │   Actor 2  │       │   Actor N  │       │
│ │  (SGLang)  │       │  (SGLang)  │       │  (SGLang)  │       │
│ └────────────┘       └────────────┘       └────────────┘       │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │              CPU StageWorkers (LLMOperator)             │   │
│   │              - Pull data from queue                     │   │
│   │              - Call Router HTTP API                     │   │
│   │              - Rate limiting + Circuit breaker          │   │
│   └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Mode 2: External Inference Service

```
┌─────────────────────────────────────────────────────────────────┐
│                        Solstice Job                             │
│                                                                 │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐           │
│  │  Worker 1   │   │  Worker 2   │   │  Worker N   │           │
│  │ LLMOperator │   │ LLMOperator │   │ LLMOperator │           │
│  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘           │
│         └────────────────┬┴────────────────┘                   │
│                          │ HTTP                                │
└──────────────────────────┼──────────────────────────────────────┘
                           ▼
              ┌────────────────────────────────┐
              │   External Service             │
              │   vLLM / SGLang / OpenAI / ... │
              └────────────────────────────────┘
```

---

## Core Components

### 1. LLMOperatorConfig

Unified LLM inference configuration supporting text and multimodal inputs:

```python
@dataclass
class LLMOperatorConfig(HttpOperatorConfig):
    # Model configuration
    model: str = ""
    max_tokens: int = 512
    temperature: float = 0.7
    top_p: float = 0.95

    # Output field
    output_field: str = "response"
    batch_size: int = 32

    # --- Text-only mode ---
    messages_field: str = ""  # Column containing chat messages

    # --- Vision mode ---
    prompt: str = ""          # Fixed prompt (shared by all rows)
    prompt_field: str = ""    # Per-row prompt column (overrides prompt)
    image_field: str = ""     # Single image column (base64/bytes)
    image_url_field: str = "" # Image URL column
    images_field: str = ""    # Multi-image column (list)
    detail: Literal["auto", "low", "high"] = "auto"

    # --- Managed mode configuration ---
    managed: bool = False
    router_config: RouterConfig = field(default_factory=RouterConfig)
    worker_config: WorkerConfig = field(default_factory=WorkerConfig)
    num_workers: Optional[int] = None  # None = auto-detect from cluster GPUs
    gpus_per_worker: int = 1
```

### 2. SGLang Router Actor

Manages the SGLang Router process, providing load balancing and service discovery:

```python
@dataclass
class RouterConfig:
    host: str = "0.0.0.0"
    port: int = 0  # 0 = auto-assign
    policy: Literal["round_robin", "random", "cache_aware"] = "cache_aware"
    health_check_interval: float = 10.0
    health_check_timeout: float = 5.0

@ray.remote(num_cpus=1)
class SGLangRouterActor:
    async def start(self) -> str:
        """Start router, return endpoint URL"""

    async def register_worker(self, worker_id: str, worker_url: str) -> bool:
        """Register a GPU worker"""

    async def unregister_worker(self, worker_id: str) -> bool:
        """Unregister a GPU worker"""

    async def stop(self):
        """Stop router"""
```

### 3. SGLang Worker Actor

Manages individual SGLang Server processes:

```python
@dataclass
class WorkerConfig:
    model_path: str = ""
    tensor_parallel_size: int = 1
    gpu_memory_utilization: float = 0.9
    max_model_len: int = 0  # 0 = auto
    host: str = "0.0.0.0"
    port: int = 0
    additional_args: list[str] = field(default_factory=list)
    startup_timeout: float = 600.0

@ray.remote
class SGLangWorkerActor:
    async def start(self) -> str:
        """Start SGLang Server, register with router, return endpoint"""

    async def stop(self):
        """Stop server, unregister from router"""
```

### 4. LLMStageMaster

StageMaster that manages inference infrastructure:

```python
class LLMStageMaster(StageMaster):
    async def start(self):
        if self._operator_config.managed:
            await self._start_inference_infrastructure()
        await super().start()

    def _infer_num_workers(self, gpus_per_worker: int) -> int:
        """Auto-detect worker count from cluster resources"""
        cluster_resources = ray.cluster_resources()
        total_gpus = int(cluster_resources.get("GPU", 0))
        return total_gpus // gpus_per_worker
```

---

## Fault Tolerance

### 1. Node Blacklist

Prevents scheduling to nodes with hardware issues:

```python
@dataclass
class NodeBlacklistConfig:
    enabled: bool = True
    quarantine_ttl_seconds: float = 300.0  # 5 min
    failures_to_blacklist: int = 2
    max_blacklisted_nodes: int = 10

class NodeBlacklist:
    def record_failure(self, node_id: str, worker_id: str, reason: str) -> bool:
        """Record failure, returns True if node was blacklisted"""

    def is_blacklisted(self, node_id: str) -> bool:
        """Check if node is blacklisted"""
```

### 2. Per-Split Timeout

Detects stuck workers:

```python
@dataclass
class TimeoutConfig:
    enabled: bool = True
    split_timeout_seconds: float = 600.0  # 10 min
    grace_period_seconds: float = 30.0

class TimeoutMonitor:
    def record_split_start(self, worker_id: str, split_id: str):
        """Record processing start"""

    def record_heartbeat(self, worker_id: str):
        """Heartbeat update"""

    def check_timeouts(self) -> list[str]:
        """Return timed out worker IDs"""
```

### 3. Rate Limiting

Pre-allocation + local token bucket to avoid per-request remote calls:

```python
@ray.remote(num_cpus=0)
class GlobalRateLimiter:
    """Global token distributor"""
    def request_tokens(self, count: int) -> int:
        """Batch token request"""

    def return_tokens(self, count: int):
        """Return unused tokens"""

class LocalRateLimiter:
    """Local token bucket, periodically refills from global"""
    def acquire(self) -> bool:
        """Fast local acquire, no remote call"""

    def release(self):
        """Release token"""
```

### 4. Circuit Breaker

Fast failure when service is unavailable:

```python
@dataclass
class CircuitBreakerConfig:
    enabled: bool = True
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_requests: int = 3

class CircuitBreaker:
    # States: CLOSED -> OPEN -> HALF_OPEN -> CLOSED
    def can_proceed(self) -> bool
    def record_success(self)
    def record_failure(self)
```

---

## Usage Examples

### Mode 1: Solstice-Managed Inference Service

```python
from solstice.core.job import Job
from solstice.core.stage import Stage
from solstice.operators.llm import LLMOperatorConfig, WorkerConfig

job = Job(job_id="llm_batch")

# Text inference
job.add_stage(Stage(
    stage_id="inference",
    operator_config=LLMOperatorConfig(
        managed=True,
        worker_config=WorkerConfig(
            model_path="Qwen/Qwen2.5-72B-Instruct",
            tensor_parallel_size=8,
        ),
        num_workers=None,  # Auto-detect
        gpus_per_worker=8,
        messages_field="messages",
        max_tokens=512,
    ),
    parallelism=(4, 16),
))

# Vision inference (fixed prompt)
job.add_stage(Stage(
    stage_id="vlm_inference",
    operator_config=LLMOperatorConfig(
        managed=True,
        worker_config=WorkerConfig(
            model_path="Qwen/Qwen2.5-VL-72B-Instruct",
            tensor_parallel_size=8,
        ),
        prompt="Describe this image in detail.",
        image_field="image_base64",
    ),
    parallelism=(4, 16),
))
```

### Mode 2: External Inference Service

```python
job.add_stage(Stage(
    stage_id="inference",
    operator_config=LLMOperatorConfig(
        base_url="http://vllm-service:8000",
        model="Qwen/Qwen2.5-72B-Instruct",
        messages_field="messages",

        # Rate limiting + circuit breaker
        max_concurrent_requests=50,
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=10,
            recovery_timeout=60.0,
        ),
    ),
    parallelism=(8, 32),
))
```

---

## File Structure

```
solstice/operators/
├── http/
│   ├── __init__.py
│   ├── operator.py          # HttpOperator base class
│   ├── rate_limiter.py      # GlobalRateLimiter + LocalRateLimiter
│   └── circuit_breaker.py   # CircuitBreaker
└── llm/
    ├── __init__.py
    ├── config.py             # RouterConfig, WorkerConfig
    ├── operator.py           # LLMOperator, LLMOperatorConfig
    ├── router_actor.py       # SGLangRouterActor
    ├── worker_actor.py       # SGLangWorkerActor
    └── stage_master.py       # LLMStageMaster

solstice/core/
├── fault_tolerance.py        # NodeBlacklist, TimeoutMonitor
└── stage_config.py           # StageConfig (@final)
```

---

## Design Decisions

### 1. Why use SGLang Router instead of building our own?

- SGLang Router already implements cache-aware scheduling, health checking, and dynamic registration
- Avoids reinventing the wheel, focuses on Solstice's core value
- Active SGLang community with continuous performance optimizations

### 2. Why use pre-allocation mode for rate limiting?

- Per-request remote calls create massive Ray task overhead, becoming a bottleneck
- Pre-allocation + local token bucket makes most operations local
- Only periodic refills require remote calls

### 3. Why merge VLM into LLMOperatorConfig?

- Both use OpenAI Chat Completions API underneath
- Only difference is content structure (text-only vs image+text)
- Reduces class count, simplifies user experience

### 4. Why default num_workers to None?

- Users shouldn't need to know how many GPUs the cluster has
- Auto-detect from `ray.cluster_resources()`
- Maximizes cluster resource utilization

---

## Future Work

1. **PD Separation** - Prefill-Decode disaggregation optimization, requires SGLang support
2. **Dynamic Scaling** - Dynamically adjust GPU worker count based on queue backlog
3. **Multi-Model Support** - Multiple models in the same Job
4. **Embedding Mode** - Efficient batch processing for embedding models

---

*Last updated: 2026-01-15*
