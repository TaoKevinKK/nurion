# Cosmos-Xenna Inspirations

Valuable design patterns extracted from [nvidia-cosmos/cosmos-xenna](https://github.com/nvidia-cosmos/cosmos-xenna), ranked by applicability to Nurion.

> **Last Updated**: 2026-03-24
> **Source Version**: Xenna v0.2.1 (2026-03-12)
> **Background**: Xenna is NVIDIA's distributed AI inference pipeline framework, built on Ray, focused on multi-stage GPU inference orchestration.

---

## P0 — Directly Applicable, High ROI

### Fragmentation-Aware GPU Scheduling (Serve Module)

- [ ] Introduce fragmentation-aware GPU allocation to optimize utilization under multi-model concurrent deployment
- **Current state**: `GPUAllocator` has bin-packing but ignores node topology and fragmentation
- **Xenna approach**: Rust LP solver (`good_lp` + `microlp`), models allocation as optimization problem, minimizes cross-node fragmentation
- **Nurion applicability**: `serve/allocator.py` — concurrent deployment of mixed TP sizes (e.g., 3x TP=4 + 2x TP=2) can cause fragmentation that LP can globally optimize
- **Implementation path**: New scheduling module in anvil-rs crate, or standalone Rust crate + PyO3
- **Related**: `docs/todo/02-serve.md`

### Two-Level Initialization: Node-level + Worker-level (Serve Module)

- [ ] Support `setup_on_node` + `setup` two-level initialization for InferenceWorker
- **Current state**: Each InferenceWorker independently downloads/loads model; N workers on same node repeat downloads
- **Xenna approach**:
  ```
  setup_on_node()  → called once per node, downloads model to local disk (shared)
  setup()          → called once per worker, loads from local disk to GPU
  ```
- **Nurion applicability**: `serve/worker.py` — deploying 70B+ models with 2-4 workers per node wastes significant time and bandwidth
- **Implementation path**: Add node-level setup phase in `ModelPool.scale_up()`, coordinate via Ray Named Actor per node
- **Related**: `docs/todo/02-serve.md`

---

## P1 — Valuable, Requires Adaptation

### Throughput-Oriented Autoscaling

- [ ] Upgrade autoscaler from queue-depth-based to throughput-measurement + LP solver
- **Current state**: `SimpleAutoscaler` decides based on queue depth (pending_count / claimed_count)
- **Xenna approach**: Sliding window measurement of `batches_per_second_per_worker` per stage, LP solver for globally optimal worker allocation, supports `over_provision_factor`
- **Advantage**: Queue depth is a lagging indicator (queue already backed up); throughput is a leading indicator
- **Nurion applicability**: `runtime/autoscaler.py` — in multi-stage pipelines, queue depth reacts slowly when bottleneck stage shifts
- **Implementation path**: Anvil already has `get_queue_stats()`; add per-worker throughput sampling, Rust-side LP solver
- **Note**: Nurion's exactly-once semantics and backpressure must be factored into scaling decisions

### Worker Health Management Parameters

- [ ] Add production-grade fault tolerance knobs for StageWorker
- **Current state**: `RecoveryManager` detects worker death and restarts, but lacks fine-grained control
- **Xenna parameters**:
  | Parameter | Purpose |
  |-----------|---------|
  | `worker_max_lifetime_m` | Periodic worker restart to prevent memory leaks (especially common with GPU processes) |
  | `max_setup_failure_percentage` | Tolerate N% setup failures (distributed FS flakiness) |
  | `reset_workers_on_failure` | Fully rebuild worker on GPU state corruption (rather than simple retry) |
  | `ignore_failures` | Skip failed tasks and continue (acceptable data loss in some processing scenarios) |
- **Nurion applicability**: `core/managers/recovery_manager.py`, add corresponding fields to `OperatorConfig`
- **Related**: `docs/todo/04-runtime-prod-hardening.md`

### GPU Orphan Process Detection

- [ ] Add NodeResourceMonitor that periodically scans for GPU processes not managed by Ray actors
- **Current state**: GPU processes may linger after worker crash, consuming VRAM until node restart
- **Xenna approach**: `NodeResourceMonitor._scan_gpu_orphans()` scans GPU processes via pynvml, compares against Ray actor PID list, cleans up orphans
- **Nurion applicability**: Long-running Serve module (detached mode) and multi-job shared cluster scenarios
- **Implementation path**: Background task in Serve `ModelServiceManager`

---

## P2 — Long-Term Reference, Not Urgent

### P2P Artifact Distribution (BitTorrent-style)

- [ ] P2P model weight distribution for large-scale cluster deployments
- **Current state**: Each node independently downloads models from HuggingFace/S3
- **Xenna approach**: Rust HTTP P2P server, rarest-first chunk scheduling, significant impact at 100+ nodes
- **Nurion applicability**: Simultaneous large model deployment at >10 nodes causes bandwidth bottleneck
- **Low priority reason**: Current user scale is small; two-level init (P0) already solves intra-node duplicate downloads

### Self-Managed GPU Allocation (Bypass Ray Scheduling)

- [ ] Investigate `RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=1` + pynvml self-managed GPU
- **Xenna approach**: Disables Ray's CUDA_VISIBLE_DEVICES management, uses pynvml for GPU UUID discovery, implements fragmentation-aware allocation
- **Trade-off**: More flexible GPU control (fractional GPU, SPMD groups) but loses Ray native scheduling convenience
- **Nurion applicability**: Consider if fragmentation-aware scheduling (P0) cannot be fully achieved within Ray framework
- **Risk**: May conflict with Ray version upgrades; `EXPERIMENTAL` flag has no long-term stability guarantee

### SPMD Distributed Inference Support

- [ ] Support torchrun-style multi-GPU/multi-node tensor parallelism worker groups
- **Xenna approach**: `Resources(gpus=8, is_spmd=True)` auto-sets RANK/WORLD_SIZE/MASTER_ADDR env vars, creates WorkerGroup
- **Nurion current state**: Serve module handles TP via vLLM/SGLang built-in support, no need for self-managed SPMD
- **Applicable when**: Custom training loops (Phase 3 RL) would make SPMD a necessity

### Request Deduplication (NVMe PayloadStore)

- [ ] Implement request deduplication in `NvmeSplitPayloadStore.get_with_hint()`
- **Current state**: Multiple workers concurrently reading same S3 payload each issue independent S3 GETs
- **Xenna/foyer approach**: Concurrent fetches for same key automatically coalesced into single remote request
- **Implementation path**: Python asyncio.Lock per key or `asyncio.Event` dedup; no need to introduce foyer-rs
- **Applicable when**: Shuffle/fan-out scenarios where multiple workers read same upstream payload

---

## Designs Not Applicable to Nurion

Documented to avoid re-evaluation in the future.

| Xenna Design | Reason Not Applicable |
|---|---|
| Stateful Stage model (setup loads model into self) | Nurion's stateless operator is a core design principle ensuring exactly-once and fault tolerance; Serve module handles model lifecycle separately |
| Ray object store for inter-stage communication | Nurion's Anvil provides persistence + exactly-once — a core differentiator |
| attrs instead of dataclass | Marginal benefit, high migration cost; Nurion uses dataclass throughout |
| Tests co-located with source files | Nurion has a well-established `tests/` structure with marker system |
