# LLM Inference Design

---

## Implementation Status (Updated 2026-01-20)

| Component | Status | Notes |
|-----------|--------|-------|
| **EmbeddedLLMOperator** | ✅ Complete | `operators/llm/embedded.py` - vLLM/SGLang offline batch |
| **EmbeddedLLMOperatorConfig** | ✅ Complete | Supports text, VLM, KV Cache optimization |
| **ExternalLLMOperator** | ✅ Complete | `operators/llm/operator.py` - External API calls |
| **HttpOperator base** | ✅ Complete | `operators/http/operator.py` |
| **Rate Limiter** | ✅ Complete | GlobalRateLimiter + LocalRateLimiter |
| **Circuit Breaker** | ✅ Complete | `operators/http/circuit_breaker.py` |

---

## Overview

Solstice provides two modes for LLM batch inference:

| Mode | Class | Use Case | Throughput |
|------|-------|----------|------------|
| **Embedded** | `EmbeddedLLMOperator` | Batch processing with dedicated GPUs | **Highest** |
| **External** | `ExternalLLMOperator` | External services, shared infrastructure | Medium |

**Recommendation**: Use Embedded mode for batch processing workloads.

---

## Architecture

### Mode 1: Embedded Engine (Recommended)

The embedded mode loads vLLM or SGLang engine directly inside Solstice workers,
eliminating all HTTP overhead and enabling zero-copy data transfer.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Solstice Job                                        │
│                                                                              │
│   Source Stage ──────> Transform Stage ──────> LLM Stage ──────> Sink Stage │
│                                                    │                         │
│                                                    ▼                         │
│   ┌────────────────────────────────────────────────────────────────────┐    │
│   │                      LLM Stage Workers                              │    │
│   │                                                                     │    │
│   │  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐              │    │
│   │  │  Worker 0   │   │  Worker 1   │   │  Worker N   │              │    │
│   │  │ ┌─────────┐ │   │ ┌─────────┐ │   │ ┌─────────┐ │              │    │
│   │  │ │ vLLM/   │ │   │ │ vLLM/   │ │   │ │ vLLM/   │ │              │    │
│   │  │ │ SGLang  │ │   │ │ SGLang  │ │   │ │ SGLang  │ │              │    │
│   │  │ │ Engine  │ │   │ │ Engine  │ │   │ │ Engine  │ │              │    │
│   │  │ └─────────┘ │   │ └─────────┘ │   │ └─────────┘ │              │    │
│   │  │   (GPU)     │   │   (GPU)     │   │   (GPU)     │              │    │
│   │  └─────────────┘   └─────────────┘   └─────────────┘              │    │
│   │         ▲                 ▲                 ▲                      │    │
│   │         │ Pull            │ Pull            │ Pull                 │    │
│   │         └─────────────────┴─────────────────┘                      │    │
│   │                           │                                        │    │
│   │                    Upstream Queue                                  │    │
│   └────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│   Data Flow:                                                                 │
│   - Workers pull Arrow Tables from upstream queue                           │
│   - Payloads transferred via Ray Object Store (zero-copy)                  │
│   - Engine processes batch directly, no serialization                       │
│   - Natural backpressure via pull-based model                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key Advantages**:
- ✅ Zero HTTP overhead
- ✅ Zero-copy data transfer (Ray Object Store)
- ✅ Natural backpressure (pull-based)
- ✅ Continuous batching handled by vLLM/SGLang
- ✅ Offset-based fault recovery

### Mode 2: HTTP External Service

For scenarios where inference services are shared or externally managed.

```
┌─────────────────────────────────────────────────────────────────┐
│                        Solstice Job                             │
│                                                                 │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐           │
│  │  Worker 1   │   │  Worker 2   │   │  Worker N   │           │
│  │ External-   │   │ External-   │   │ External-   │           │
│  │ LLMOperator │   │ LLMOperator │   │ LLMOperator │           │
│  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘           │
│         └────────────────┬┴────────────────┘                   │
│                          │ HTTP + Rate Limiter + Circuit Breaker│
└──────────────────────────┼──────────────────────────────────────┘
                           ▼
              ┌────────────────────────────────┐
              │   External Service             │
              │   vLLM / SGLang / OpenAI / ... │
              └────────────────────────────────┘
```

---

## Core Components

### 1. EmbeddedLLMOperatorConfig (Recommended)

Configuration for embedded vLLM/SGLang inference:

```python
@dataclass
class EmbeddedLLMOperatorConfig(OperatorConfig):
    # Backend selection
    backend: Literal["vllm", "sglang"] = "vllm"

    # Model configuration
    model: str = ""                           # Required: model name/path
    tensor_parallel_size: int = 1             # GPUs per model instance
    max_model_len: int = 8192                 # Context length
    gpu_memory_utilization: float = 0.9       # vLLM memory fraction
    quantization: Optional[str] = None        # "awq", "gptq", etc.
    trust_remote_code: bool = True

    # --- KV Cache optimization (both backends) ---
    kv_cache_dtype: Optional[str] = None      # "auto", "fp8_e4m3", "fp8_e5m2", "fp16"
    enable_chunked_prefill: bool = False      # vLLM: chunked prefill for long prompts

    # --- vLLM-specific KV Cache offloading ---
    kv_offloading_size_gb: Optional[float] = None  # GB to offload to CPU
    kv_offloading_backend: Optional[str] = None    # "native", "lmcache"

    # --- SGLang-specific memory optimization ---
    mem_fraction_static: Optional[float] = None  # Static memory fraction
    attention_backend: Optional[str] = None   # "fa3", "flashinfer", etc.

    # Generation parameters
    temperature: float = 0.7
    top_p: float = 0.95
    max_tokens: int = 1024
    stop: list[str] = field(default_factory=list)

    # Input fields
    prompt: str = ""              # Fixed prompt for all rows
    prompt_field: str = ""        # Column with per-row prompts
    messages_field: str = ""      # Column with chat messages (text-only)
    image_field: str = ""         # Column with single image bytes
    images_field: str = ""        # Column with list of images

    # Output field
    output_field: str = "response"
```

> **Note**: PD disaggregation (prefill-decode separation) is NOT supported in offline
> batch mode. It's an online serving optimization for Ray Serve. For batch processing,
> use KV cache quantization (`kv_cache_dtype`) and chunked prefill instead.

### 2. EmbeddedLLMOperator

Operator that embeds inference engine directly:

```python
class EmbeddedLLMOperator(Operator):
    def setup(self) -> None:
        """Load model into GPU memory."""
        if self._config.backend == "vllm":
            from vllm import LLM, SamplingParams
            self._engine = LLM(
                model=self._config.model,
                tensor_parallel_size=self._config.tensor_parallel_size,
                ...
            )
        else:
            import sglang as sgl
            self._engine = sgl.Engine(model_path=self._config.model, ...)

    def process_split(self, split: Split, payload: SplitPayload):
        """Process batch through embedded engine."""
        table = payload.to_table()
        prompts = self._extract_prompts(table)
        
        # Direct engine call - no HTTP!
        outputs = self._engine.generate(prompts, self._sampling_params)
        
        return self._build_output(table, outputs)
```

### 3. ExternalLLMOperatorConfig (External Services)

For calling external LLM APIs (vLLM, SGLang, OpenAI, etc.):

```python
@dataclass
class ExternalLLMOperatorConfig(HttpOperatorConfig):
    model: str = ""
    max_tokens: int = 512
    temperature: float = 0.7
    
    # Text-only mode
    messages_field: str = ""
    
    # Vision mode
    prompt: str = ""
    prompt_field: str = ""
    image_field: str = ""
    
    # Rate limiting + circuit breaker (inherited from HttpOperatorConfig)
    max_concurrent_requests: int = 100
    requests_per_second: float = 0
    circuit_breaker: CircuitBreakerConfig = ...
```

---

## Performance Comparison

Based on official documentation and community benchmarks:

| Metric | Embedded Mode | HTTP Mode | Improvement |
|--------|---------------|-----------|-------------|
| **Latency overhead** | ~0 (direct call) | 1-5ms (HTTP) | **>>10x** |
| **Data transfer** | Zero-copy (Plasma) | Serialization | **>>2x** |
| **GPU utilization** | 90%+ (continuous batch) | 70-85% (request gaps) | **+20%** |
| **Backpressure** | Natural (pull-based) | Manual config | **Simpler** |
| **Fault recovery** | Offset-based | Retry/rerun | **Faster** |

**Sources**:
- vLLM Offline Inference: https://docs.vllm.ai/en/latest/serving/offline_inference.html
- SGLang Offline Engine: https://docs.sglang.io/basic_usage/offline_engine_api.html
- Daft vs Ray Data benchmark: https://docs.daft.ai/en/stable/benchmarks/

---

## Usage Examples

### Example 1: Text Batch Inference (Embedded)

```python
from solstice.core.job import Job
from solstice.core.stage import Stage
from solstice.operators.llm import EmbeddedLLMOperatorConfig

job = Job(job_id="text_batch")

job.add_stage(Stage(
    stage_id="inference",
    operator_config=EmbeddedLLMOperatorConfig(
        backend="vllm",
        model="Qwen/Qwen2.5-72B-Instruct",
        tensor_parallel_size=8,
        messages_field="messages",
        max_tokens=1024,
    ),
    parallelism=1,  # 1 worker = 1 engine instance
    resources={"num_gpus": 8},
))
```

### Example 2: VLM Image Captioning (Embedded)

```python
job.add_stage(Stage(
    stage_id="caption",
    operator_config=EmbeddedLLMOperatorConfig(
        backend="vllm",
        model="Qwen/Qwen2.5-VL-72B-Instruct",
        tensor_parallel_size=4,
        prompt="Describe this image in detail.",
        image_field="image_bytes",
        max_tokens=2048,
    ),
    parallelism=2,  # 2 engines, each with 4 GPUs
    resources={"num_gpus": 4},
))
```

### Example 3: KV Cache Optimization (vLLM)

```python
job.add_stage(Stage(
    stage_id="caption",
    operator_config=EmbeddedLLMOperatorConfig(
        backend="vllm",
        model="Qwen/Qwen2.5-VL-72B-Instruct",
        tensor_parallel_size=4,
        prompt="Describe this image in detail.",
        image_field="image_bytes",
        # KV cache optimization
        kv_cache_dtype="fp8_e4m3",        # 50% memory reduction
        enable_chunked_prefill=True,       # Handle long prompts
        kv_offloading_size_gb=16.0,        # Offload to CPU RAM
    ),
    parallelism=2,
    resources={"num_gpus": 4},
))
```

### Example 4: SGLang Memory Optimization

```python
job.add_stage(Stage(
    stage_id="caption",
    operator_config=EmbeddedLLMOperatorConfig(
        backend="sglang",
        model="Qwen/Qwen2.5-VL-72B-Instruct",
        tensor_parallel_size=4,
        prompt="Describe this image.",
        image_field="image_bytes",
        kv_cache_dtype="fp8_e5m2",          # Quantized KV cache
        mem_fraction_static=0.85,           # Reserve 85% for static memory
        attention_backend="fa3",            # Flash attention 3
    ),
    parallelism=2,
    resources={"num_gpus": 4},
))
```

### Example 5: External Service

```python
from solstice.operators.llm import ExternalLLMOperatorConfig

job.add_stage(Stage(
    stage_id="inference",
    operator_config=ExternalLLMOperatorConfig(
        base_url="http://vllm-service:8000",
        model="Qwen/Qwen2.5-72B-Instruct",
        messages_field="messages",
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
    ├── embedded.py           # EmbeddedLLMOperator (recommended for batch)
    └── operator.py           # ExternalLLMOperator (for external services)
```

---

## Design Decisions

### 1. Why embedded mode over HTTP?

**Problem**: HTTP-based inference has inherent overhead:
- Serialization/deserialization
- Network latency
- Connection management
- Base64 encoding for images

**Solution**: Embedded engine eliminates all these:
- Direct Python function call
- Zero-copy via Ray Object Store
- No network hop

**Evidence**:
- Ray Serve + vLLM has 2-3x higher latency than standalone vLLM
  (Source: [Ray Community Discussion](https://discuss.ray.io/t/ray-serve-llm-apis-has-2-3x-higher-latency/22356))

### 2. Why support both vLLM and SGLang?

Both engines have their strengths:

| Feature | vLLM | SGLang |
|---------|------|--------|
| Maturity | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| Community | Larger | Growing |
| Quantization | AWQ, GPTQ, FP8 | AWQ, GPTQ |
| Hidden states | ❌ | ✅ |
| Async modes | ✅ | ✅ |
| Multimodal | ✅ | ✅ |

**Recommendation**: Start with vLLM for stability, use SGLang for advanced features.

### 3. Why keep HTTP mode?

HTTP mode is still valuable for:
- Shared inference services across teams
- External API providers (OpenAI, Anthropic)
- When GPU resources are managed separately
- A/B testing different models

### 4. Why not use a managed Router + Workers architecture?

A Router + Workers architecture was considered but rejected:
- Extra process management (Router, Workers)
- HTTP overhead between components
- Single point of contention (Router)

Embedded mode is simpler and faster:
- One engine per worker
- Direct invocation
- Natural load balancing via Solstice's pull model

### 5. Why support both vLLM and SGLang?

Different backends have different strengths for batch processing:

| Feature | vLLM | SGLang |
|---------|------|--------|
| KV Cache Quantization | FP8 | FP8, FP4 |
| CPU Offloading | Native, LMCache | - |
| Attention Backends | FlashAttention | FA3, FlashInfer |
| Memory Management | PagedAttention | Custom |
| Stability | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| Community | Larger | Growing |

**Recommendation**: Use vLLM for production stability.

---

## Advanced Features

### KV Cache Optimization

KV Cache stores key/value tensors from attention layers, consuming significant GPU memory.
Optimization strategies for batch processing:

| Strategy | Config | Effect | Backend |
|----------|--------|--------|---------|
| **Quantization** | `kv_cache_dtype="fp8_e4m3"` | ~50% memory reduction | Both |
| **Chunked Prefill** | `enable_chunked_prefill=True` | Handle long prompts | vLLM |
| **CPU Offload** | `kv_offloading_size_gb=16.0` | Extend effective batch | vLLM |

### Why No PD Disaggregation in Offline Mode?

PD (Prefill-Decode) disaggregation is an **online serving optimization**, not a batch
processing optimization:

1. **Problem it solves**: In online serving, long prefill can block decode responses,
   increasing latency (TTFT - Time To First Token)

2. **Why not for batch**: In batch mode, all requests are processed together.
   There's no latency concern - we optimize for throughput.

3. **vLLM/SGLang offline API**: The `LLM.generate()` and `sgl.Engine.generate()` APIs
   already handle continuous batching internally. They interleave prefill and decode
   phases automatically for optimal GPU utilization.

4. **Overhead**: PD separation requires KV cache network transfer between processes
   (via nixl/mooncake). For batch, this overhead often exceeds the benefit.

**Recommendation**: For batch processing, use:
- `kv_cache_dtype="fp8_e4m3"` to reduce KV cache memory by ~50%
- `enable_chunked_prefill=True` to handle long prompts efficiently
- More workers (data parallelism) for higher throughput

---

## Future Work

1. **LoRA Adapter Support** - Dynamic adapter loading for fine-tuned models
2. **Speculative Decoding** - Speed up generation with draft models
3. **Cross-worker Prefix Caching** - Share common prefixes across workers
4. **Guided Generation** - JSON schema, regex constraints

---

*Last updated: 2026-01-20*
