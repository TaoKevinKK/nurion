# Serve Module TODO

Track implementation status of the model inference serving system.

> **Last Updated**: 2026-03-02
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
  - See `roadmap.md` §2.1

- [ ] **Public API export for LLM operators**
  - `EmbeddedLLMOperatorConfig`, `ExternalLLMOperatorConfig` not in `nurion/__init__.py`
  - Users forced to import from `_internal` (unstable path)
  - Quick fix: add imports + `__all__` entries

### Medium Priority — GPU Efficiency

- [ ] **Token-length-based model routing**
  - Design: `../design/gpu-scheduling-and-routing.md` §3
  - Estimate token count per row → group by target model → batch route
  - `RoutedChatCompletionsClient` skeleton exists, needs implementation
  - Unlocks 30-50% cost reduction for mixed-length inference workloads

- [ ] **GPU compaction (defragmentation)**
  - Design: `../design/gpu-scheduling-and-routing.md` §Compaction
  - Manager-coordinated: freeze → evict → respawn → unfreeze
  - Prevents GPU fragmentation in multi-model long-running deployments

- [ ] **Fractional GPU support**
  - Design: `../design/gpu-scheduling-and-routing.md` §Fractional
  - Auto `num_gpus=0.5` when TP=1 and `gpu_memory_utilization < 0.5`
  - Doubles GPU utilization for small models

- [ ] **Two-phase ordered deployment (`deploy_models()`)**
  - Design: `../design/gpu-scheduling-and-routing.md` §deploy_models
  - Large models deploy sequentially first (avoid fragmentation), then small models in parallel

### Low Priority — Observability

- [ ] **Worker node reporting**
  - `get_node_id()` via `ray.get_runtime_context()` for per-node GPU tracking

- [ ] **INDEX.md consistency**
  - LLM operator class names in `_internal/INDEX.md` are outdated
  - `LlmOperatorConfig` → `ExternalLLMOperatorConfig`, `EmbeddedInference` → `EmbeddedLLMOperator`

---

## Design Changes from `gpu-scheduling-and-routing.md`

The design doc was written before the serve module was implemented. Current status:

| Design Item | Status | Notes |
|-------------|--------|-------|
| GPUAllocator bin-packing | ✅ Implemented | Basic allocation exists; best-fit optimization pending |
| Manager as Ray actor | ✅ Implemented | `ModelServiceManager` is `@ray.remote` |
| Pool as plain object | ✅ Implemented | `ModelPool` is not a Ray actor |
| Fractional GPU | ❌ Not implemented | |
| deploy_models() ordering | ❌ Not implemented | |
| Compaction | ❌ Not implemented | |
| Node reporting | ❌ Not implemented | |
| ModelRoutingConfig | ❌ Not implemented | |
| Per-row token routing | ❌ Not implemented | |
