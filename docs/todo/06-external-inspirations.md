# External Project Inspirations

Design patterns from external projects evaluated for Nurion's **offline batch inference** use case.

> **Last Updated**: 2026-03-26
> **Key Finding**: Most LLM serving frameworks (Dynamo, Llumnix) are online-focused. Their core innovations (KV-aware routing, live migration, SLA-driven scaling) solve online problems. Value for Nurion is limited to design patterns, not features.

---

## Sources

| Project | Version | Focus | Repo |
|---------|---------|-------|------|
| NVIDIA Dynamo | 1.0 (2026-03-16) | Datacenter-scale inference orchestration (Rust + Go) | [ai-dynamo/dynamo](https://github.com/ai-dynamo/dynamo) |
| Llumnix | v1 (2026-02) | LLM scheduling + live KV migration (Go + Python, OSDI 2024) | [llumnix-project/llumnix](https://github.com/llumnix-project/llumnix) |
| Llumnix-Ray | v0 (2024-12) | Ray-based Llumnix prototype | [llumnix-project/llumnix-ray](https://github.com/llumnix-project/llumnix-ray) |
| Cosmos-Xenna | v0.2.1 (2026-03-12) | NVIDIA distributed AI inference pipeline (Ray) | See `05-xenna-inspirations.md` |
| Ray Data LLM | Ray 2.44+ (2025) | Ray's batch LLM inference pipeline | [Ray Data LLM Docs](https://docs.ray.io/en/latest/data/working-with-llms.html) |
| Daft + vLLM | 2025 | DataFrame-native batch inference with prefix bucketing | [Daft Blog](https://www.daft.ai/blog/cutting-llm-batch-inference-time-in-half-dynamic-prefix-bucketing-at-scale) |
| Data-Juicer | v1.5.1 (2026-03-17) | Alibaba LLM data processing (200+ OPs, SIGMOD 2024) | [GitHub](https://github.com/modelscope/data-juicer) |

---

## Context: Why Most Online Serving Features Don't Apply

Nurion's LLM workflows (image captioning, multi-OCR fusion) are offline batch jobs:
- All requests share the same system prompt → prefix caching works automatically on every worker after first request
- Throughput (tokens/s/GPU) matters, not latency (TTFT/ITL)
- Data is bounded and known upfront → no need for adaptive routing
- Requests are short-lived and replaceable → retry is cheaper than live migration

---

## Applicable Patterns

### P0 — AsyncLLMEngine for EmbeddedLLMOperator (2x throughput)

- [ ] Switch `EmbeddedLLMOperator` from synchronous `LLM.generate()` to `AsyncLLMEngine`
- **Ray Data LLM approach**: 7-stage disaggregated pipeline with dual-layer async execution — batch-level concurrency (Ray Data) + token-level continuous batching (vLLM AsyncEngine). Achieves **2x throughput** vs synchronous `LLM` class.
- **Nurion problem**: `EmbeddedLLMOperator` calls `LLM.generate(batch)` synchronously. If a batch has 10 requests where 9 generate 100 tokens and 1 generates 8000 tokens, the GPU sits mostly idle while the 9 short requests wait for the 1 long request. No new work can enter until the entire batch completes.
- **Fix**: Use `AsyncLLMEngine` so requests complete independently. As each finishes, its KV cache is freed and new requests can be injected immediately. GPU stays fully utilized via vLLM's continuous batching.
- **Scope**: `_internal/operators/llm/embedded.py` — change engine initialization and `process_split` to use async generate API
- **Estimated impact**: 2x throughput for workloads with variable output lengths (Multi-OCR: 100~8000 tokens)
- **Reference**: [Ray Data LLM 2x Throughput Blog](https://www.anyscale.com/blog/ray-data-llm-2x-throughput-vs-vllm)

### P0 — GPU-Memory-Aware Load Balancing (Serve Module)

- [ ] Replace round-robin `ModelClient` routing with GPU cache utilization-aware routing
- **Llumnix approach**: Poll each instance's `gpu_cache_usage_perc` via metrics, route to instance with lowest GPU memory pressure.
- **Nurion problem**: `ModelClient` uses round-robin. When output lengths vary significantly (Multi-OCR: 100~8000 tokens), some workers' GPU KV cache fills up while others are idle.
- **Implementation path**: Periodically scrape vLLM's `/metrics` endpoint for `gpu_cache_usage_perc` and `num_requests_waiting`. Store per-worker metrics in `ModelRegistry`. `ModelClient` routes to worker with lowest cache usage.
- **Scope**: `serve/registry.py` (add metrics field), `serve/client.py` (routing logic), `serve/pool.py` (metrics scraping loop)

### P0 — AIConfigurator: Micro-Benchmark-Driven Auto-Tuning

- [ ] Build operator performance model for automatic batch_size / worker_count tuning
- **Dynamo approach**: Decompose inference into atomic operations (GEMM, Attention, Communication), measure independently, recombine to predict throughput across thousands of config candidates in seconds without GPU
- **Nurion problem**: Users must manually guess `merge_upstream`, `batch_size`, and worker count. Wrong choices cause 2-5x throughput loss (too small = GPU underutilized, too large = OOM)
- **Implementation path**: For each operator type, run micro-benchmarks at varying batch sizes on target GPU → build throughput curve → auto-recommend optimal `merge_upstream` and parallelism
- **Scope**: New `runtime/auto_tune.py` module + CLI command `nurion tune <operator_config>`
- **Reference**: [NVIDIA blog: Removing the Guesswork from Disaggregated Serving](https://developer.nvidia.com/blog/removing-the-guesswork-from-disaggregated-serving/)

### P1 — KVBM Block Lifecycle for NVMe PayloadStore

- [ ] Adopt RAII-style block lifecycle and per-path async transfer queues for NVMe PayloadStore
- **Dynamo approach**:
  - Block states: `Reset → Partial → Complete → Registered → (drop) → Reset`
  - TransferManager: independent async queue per transfer path (GPU→CPU, CPU→NVMe, NVMe→S3)
  - Write filtering: only offload blocks with frequency ≥ 2 to NVMe (extends SSD lifespan)
  - Dedup by sequence hash: avoid storing duplicate blocks
- **Nurion applicability**: `NvmeSplitPayloadStore` currently uses simple put/get. Adopting:
  - Explicit lifecycle states would improve debugging and leak detection
  - Per-path queues would allow concurrent NVMe reads + S3 fallback writes
  - Frequency-based write filtering would reduce NVMe wear in high-churn workloads
- **Reference**: `docs/design-docs/kvbm-design.md` in Dynamo repo

### P1 — Planner Correction Factor for Autoscaler

- [ ] Add actual/predicted throughput correction to `SimpleAutoscaler`
- **Dynamo approach**: `correction = actual_throughput / predicted_throughput`, applied to next scaling decision. Prevents systematic over/under-scaling when predictions drift.
- **Nurion problem**: `SimpleAutoscaler` uses raw queue depth thresholds. If split processing time varies (e.g., some images have much more text than others), queue depth alone can't distinguish "workers are slow" from "input burst"
- **Implementation path**: Track actual splits_processed_per_second per stage, compare with expected rate, adjust `scale_up_lag_threshold` dynamically
- **Scope**: Small addition to `runtime/autoscaler.py`

### P2 — ModelExpress Weight Streaming for Worker Startup

- [ ] Explore GPU-to-GPU weight transfer for faster InferenceWorker scale-up
- **Dynamo approach**: First worker loads model from disk, subsequent workers stream weights from first worker via NVLink/RDMA. Cold start: ~60s → ~10s.
- **Nurion applicability**: `InferenceWorker` scale-up currently each worker independently loads model. For 70B+ models this takes 30-60s per worker.
- **Prerequisite**: Requires NIXL or similar GPU transfer library; evaluate if vLLM/SGLang have native support
- **Priority**: Low — startup time is amortized over long batch jobs

### P2 — NIXL Unified Storage Abstraction

- [ ] Unify PayloadStore backends behind a single transport-agnostic API
- **Dynamo approach**: NIXL provides one API for GPU memory / CPU / NVMe / S3. Backend auto-selected based on source/target types. Three-phase async transfer (create → post → poll).
- **Nurion applicability**: Currently three separate store classes (`RaySplitPayloadStore`, `NvmeSplitPayloadStore`, `FsspecSplitPayloadStore`). A unified interface with automatic backend selection and async transfer would simplify the codebase.
- **Priority**: Low — current separate implementations work; unification is a refactoring exercise

---

## Applicable Patterns from Llumnix

- GPU-Memory-Aware Load Balancing → consolidated into P0 above
- [x] Instance Staleness Detection ✅ Already implemented — `ModelPool._check_worker_health()` (PR #70, 2026-03-25). Llumnix has `stalenessFilter` + configurable failure domains; Nurion's lightweight RPC ping is sufficient for offline.

---

## Applicable Patterns from Ray Data LLM / Industry

### P1 — Job-Level Checkpoint/Resume

- [ ] Enable batch job resume from last checkpoint after crash or spot instance preemption
- **Ray Data LLM approach**: Pipeline resumes from last successful block stored in local or cloud storage. Critical for spot instance cost savings.
- **Nurion status**: Anvil ack provides split-level durability (acked splits survive restart). But no job-level "resume from where we left off" — a restarted job re-processes all splits.
- **Implementation path**: On job restart, scan Anvil for already-acked splits and skip them in source planner. Thin wrapper over existing ack state.
- **Scope**: `runtime/ray_runner.py` (restart logic), `core/managers/source_manager.py` (skip acked splits)
- **Estimated impact**: Enables spot instances (3-5x cheaper), tolerates transient failures in multi-hour jobs

---

## Applicable Patterns from Data-Juicer

> DJ's execution model (dataset.map() chain) is inferior to Nurion's multi-stage pipeline.
> DJ's code quality is poor — do not port code directly. Borrow design ideas only.
> Nurion advantages: exactly-once semantics, Arrow zero-copy, Rust Anvil, pull-based backpressure.

### P1 — Sample-Level Tracer

- [ ] Track per-split row-level changes (kept/filtered/modified) at each stage for pipeline debugging
- **DJ approach**: `RayTracer` actor records which samples were modified/filtered by each OP. Useful for debugging but implemented as Python dict diffing (slow, imprecise).
- **Nurion advantage**: Arrow tables have typed schemas — column-level and row-level diffs can be computed efficiently via zero-copy.
- **Design**:
  - **Stats tier (zero cost)**: Every split records `SplitTrace{input_rows, output_rows, columns_added, columns_removed}` — always on
  - **Row tier (sampled)**: At `lineage_sample_rate > 0`, sample N rows and record which were filtered/modified. Uses existing `WebUIConfig.lineage_sample_rate` infrastructure
  - **Value tier (debug only)**: Record before/after values for sampled rows. Only in explicit debug mode
- **Storage**: Write to WebUI state (`_write_worker_state` mechanism) or Anvil state namespace `lineage:{job_id}`
- **Scope**: `core/stage_worker.py` (hook around `process_split`), `core/models.py` (`SplitTrace` dataclass)

### P1 — Data Profiler CLI

- [ ] Standalone tool for dataset statistics and before/after comparison
- **DJ approach**: `Analyzer` module with overall/column-wise/correlation/diversity analysis. Generates stats tables and distribution plots. Implemented as Python dict iteration (slow on large datasets).
- **Nurion advantage**: Datasets are Arrow/Lance — profiling uses columnar compute (PyArrow `pc.*` functions), orders of magnitude faster than row-by-row iteration.
- **Design**:
  - Core: `profile.py` module, accepts `pa.Table` → returns `ProfileResult` dataclass
  - Per-column auto-detection:
    - Numeric: count, null%, min/max, mean/std, p50/p95/p99, histogram
    - String/text: length distribution, empty%, avg tokens, language breakdown
    - Binary (image): count, size distribution
    - Categorical: cardinality, top-K values, frequency table
  - CLI: `nurion profile <path> [--columns col1,col2] [--output report.json]`
  - Diff: `nurion profile diff before.json after.json` → shows row count change, column stat deltas
- **Scope**: New `_internal/tools/profile.py` + CLI entry point
- **Estimated effort**: Small — mostly PyArrow compute wrappers + JSON/HTML output

---

## Not Applicable to Nurion

Documented to prevent re-evaluation.

| Feature | Source | Why Not Applicable |
|---|---|---|
| **KV-aware routing** (radix tree, cost function) | Dynamo | Offline: same system prompt → all workers auto-cache prefix after first request |
| **Live KV cache migration** | Llumnix | Offline: requests are short-lived; retry is simpler and cheaper than migration |
| **Prefill/Decode disaggregation** | Dynamo, Llumnix | Offline: no TTFT optimization needed; extra hop reduces throughput |
| **SLA-driven scaling** (TTFT/ITL targets) | Dynamo, Llumnix | Offline: no per-request SLA; queue-depth threshold is sufficient |
| **Adaptive PD role switching** | Llumnix | Offline: steady load, no need to dynamically reassign instance roles |
| **Rescheduler / continuous rebalancing** | Llumnix | Offline: route new requests well instead of migrating existing ones |
| **Predictor-enhanced scheduling** | Llumnix | Offline: steady load makes staleness correction unnecessary |
| **Softmax worker selection** | Dynamo | Round-robin (or memory-aware) + vLLM continuous batching is effective |
| **Three-plane separation** | Dynamo | Architectural reference, but Ray + Anvil covers offline needs |
| **Priority routing / agent hints** | Dynamo | Offline splits are homogeneous; no priority differentiation needed |
| **Agentic inference** | Dynamo | Online interactive scenario only |
| **CRIU checkpoint/restore** | Dynamo | Too invasive for current deployment model |
| **Blade-KVT** (GPU direct transfer) | Llumnix | Only useful for live migration, which offline doesn't need |
| **Batch API** (`/v1/batches`) | Llumnix v1 | Nurion has its own pipeline orchestration; no need for standalone batch API |
| **Prefix bucketing** (sort inputs by prefix) | Daft | Nurion workflows use identical system prompts → no prefix diversity to bucket |
| **LMCache** (distributed KV cache) | LMCache | Cross-worker KV sharing mainly benefits online; each offline worker auto-caches shared prefix locally |
| **Speculative decoding** | vLLM/SGLang | Latency optimization; at batch sizes 32+, throughput benefit is minimal |
| **CPU/GPU stage disaggregation** (7-stage) | Ray Data LLM | Nurion's multi-stage pipeline already supports separate CPU/GPU stages; users can compose them |
| **dataset.map() execution model** | Data-Juicer | Nurion's stage pipeline is more flexible: independent scaling, exactly-once, pull-based backpressure |
| **OP Fusion** (merge consecutive filters) | Data-Juicer | Nurion's `get_merge_upstream()` is a better design — explicit, user-controlled batching vs implicit fusion |
| **200+ operator ecosystem** | Data-Juicer | Quantity over quality; Nurion focuses on core operators + user-defined via `@operator` decorator |
| **YAML recipe system** | Data-Juicer | Nice-to-have UX improvement but not a priority; Nurion's Python Job/Stage API is more powerful |
| **OP-level runtime_env isolation** | Data-Juicer | Over-engineering for Nurion's use case; single venv with uv is sufficient |
| **Embodied AI operators** (3D pose, hand mesh) | Data-Juicer | Niche scenario, not on Nurion roadmap |
