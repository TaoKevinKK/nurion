# Serve Module TODO

Track implementation status of the model inference serving system.

> **Last Updated**: 2026-03-24
> **Design Docs**: `../design/llm-inference.md`, `../design/gpu-scheduling-and-routing.md`
> **Scope**: `engine/_internal/serve/`, `engine/_internal/operators/llm/`

---

## Completed

### Core Serving Infrastructure (2026-02)

- [x] **ModelServiceManager** — Ray actor control plane (`serve/manager.py`)
- [x] **ModelPool** — Per-model worker tracking and scale up/down (`serve/pool.py`)
- [x] **GPUAllocator** — GPU allocation with anti-fragmentation (`serve/allocator.py`)
- [x] **InferenceWorker** — Ray actor running vLLM/SGLang server (`serve/worker.py`)
- [x] **ModelRegistry** — Named actor for service discovery (`serve/registry.py`)
- [x] **ModelClient** — HTTP client with round-robin LB (`serve/client.py`)
- [x] **Attached/Detached manager modes** — `create_manager(detached=True)`

### LLM Operators (2026-02)

- [x] **EmbeddedLLMOperator** — In-process vLLM/SGLang engine (`operators/llm/embedded.py`)
- [x] **ExternalLLMOperator** — HTTP client to OpenAI-compatible API (`operators/llm/operator.py`)
- [x] **ChatCompletionsClient** — Retry, context-length detection (`operators/llm/client.py`)
- [x] **RoutedChatCompletionsClient** — Multi-model endpoint selection skeleton (`operators/llm/client.py`)

### Union-Find Service (2026-02)

- [x] **UFShard, UFClient, UnionFindServiceManager** — Distributed dedup service
- [x] **Checkpoint/restore via PayloadStore**

---

## TODO

### High Priority — Differentiation Features

- [ ] **Declarative serve ↔ pipeline integration**
  - `RayJobRunner` auto-deploys models declared in Stage configs before execution
  - `ExternalLLMOperatorConfig` auto-discovers registry by model_id (no manual handle injection)
  - Job-scoped lifecycle: deploy on start, cleanup on completion
  - See `01-roadmap.md` §2.1

- [x] **Public API export for LLM operators** ✅ (`d5902c9`, 2026-03)
  - `nurion/__init__.py` exports all 4 LLM operator classes

### Medium Priority — GPU Efficiency

- [x] **Token-length-based model routing** ✅ (`d5902c9`, 2026-03)
  - `RoutedChatCompletionsClient` with `estimate_tokens()`, `pick_model()`, context-length fallback
  - `ModelRoutingConfig` dataclass implemented

- [x] **GPU compaction (defragmentation)** ✅ (`d5902c9` + `1ee25cf`, 2026-03)
  - `GPUAllocator.plan_compaction()` implemented
  - Manager calls compaction on deployment failure

- [x] **Fractional GPU support** ✅ (`d5902c9`, 2026-03)
  - `ModelConfig.get_worker_resources()` auto-infers `num_gpus=0.5` when TP=1 and `gpu_memory_utilization < 0.5`

- [ ] **Two-phase ordered deployment (`deploy_models()`)**
  - Design: `../design/gpu-scheduling-and-routing.md` §deploy_models
  - Large models deploy sequentially first (avoid fragmentation), then small models in parallel

### Low Priority — Observability and Quality

- [x] **Worker node reporting** ✅ (`d5902c9`, 2026-03)
  - `InferenceWorker.get_node_id()` implemented via `ray.get_runtime_context().get_node_id()`
  - Allocator tracks GPU allocation by `node_id`

- [ ] **ModelPool FAILED status on InferenceWorker crash**
  - Design: `../design/webui-api-v2.md` — worker crash → FAILED status write
  - Current: pool writes LOADING, READY, STOPPED but not FAILED on actor death
  - Requires actor death callback integration

- [ ] **Per-row group-by-model batching**
  - Design: `../design/gpu-scheduling-and-routing.md` §6.5
  - Current: per-request routing via `RoutedChatCompletionsClient` (simpler, works)
  - Design proposes: group split rows by estimated token count → batch by model → parallel process
  - Potential 10-20% throughput improvement for mixed-length workloads

- [ ] **INDEX.md consistency**
  - LLM operator class names in `_internal/INDEX.md` are outdated
  - `LlmOperatorConfig` → `ExternalLLMOperatorConfig`, `EmbeddedInference` → `EmbeddedLLMOperator`

---

## Design Changes from `gpu-scheduling-and-routing.md`

The design doc was written before the serve module was implemented. Current status:

| Design Item | Status | Notes |
|-------------|--------|-------|
| GPUAllocator bin-packing | ✅ Implemented | Best-fit with random tiebreaker |
| Manager as Ray actor | ✅ Implemented | `ModelServiceManager` is `@ray.remote` |
| Pool as plain object | ✅ Implemented | `ModelPool` is not a Ray actor |
| Fractional GPU | ✅ Implemented | Auto `num_gpus=0.5` when TP=1 + low utilization |
| deploy_models() ordering | ❌ Not implemented | |
| Compaction | ✅ Implemented | `plan_compaction()` + auto-trigger on deploy failure |
| Node reporting | ✅ Implemented | `get_node_id()` + per-node allocation tracking |
| ModelRoutingConfig | ✅ Implemented | `RoutedChatCompletionsClient` + `ModelRoutingConfig` |
| Per-row token routing | ✅ Implemented | `estimate_tokens()` + `pick_model()` + context-length fallback |
