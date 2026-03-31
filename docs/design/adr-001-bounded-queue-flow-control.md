# ADR-001: Bounded Queue Flow Control for Multi-Stage Pipelines

**Status:** Accepted
**Date:** 2026-03-31
**Deciders:** Nurion core team

## Context

Nurion is a Ray-based distributed data pipeline engine that processes Arrow
payloads through multi-stage workflows (Source → CPU Transform → GPU Inference
→ Sink). Payloads flow between stages via an Anvil queue broker (Rust-backed,
O(1) hot paths).

**Production failures observed (March 2026):**

| Failure | Root Cause | Impact |
|---------|-----------|--------|
| Worker OOM | Unbounded payloads in flight | Node killed by OOM killer |
| NVMe disk full | Source pushes faster than sink drains | Write failures cascade |
| Network saturation | Too many concurrent Flight reads | All workers stall |
| 500-worker stall | Queue contention under high concurrency | Zero throughput |
| GPU starvation | Upstream can't feed GPU fast enough | GPU idle, wasting $$$ |

**Core problem:** Inter-stage queues are unbounded. Source pushes all splits
immediately; queue grows without limit; memory, disk, and network are consumed
until a node crashes. Current backpressure only checks queue depth — a necessary
but insufficient signal.

**Primary optimization target:** GPU utilization. GPUs are 10-100x more
expensive per hour than CPU. A system that keeps GPUs busy while not crashing
nodes is the goal.

## Decision

**Adopt memory-budget-based bounded queues** as the unified flow control
mechanism. Each inter-stage queue has a `max_pending` derived from a node memory
budget. Backpressure propagates backward naturally from the bottleneck stage
(typically GPU). No multi-dimensional resource monitoring needed.

Full design: [`bounded-queue-flow-control.md`](bounded-queue-flow-control.md)

## Options Considered

### Option A: Multi-Dimensional Resource Monitoring

Monitor memory, disk, network, object store, and S3 bandwidth per node.
Throttle source when any dimension exceeds its threshold.

| Dimension | Assessment |
|-----------|------------|
| Complexity | **High** — 5 monitors, 5 thresholds, interaction logic |
| Cost | Low (psutil + statvfs are cheap) |
| Scalability | Medium — per-node monitoring is O(1) but aggregation adds RPC |
| Team familiarity | Low — no prior art in codebase |
| Correctness | **Fragile** — thresholds are workload-dependent |

**Pros:**
- Fine-grained visibility into resource usage
- Can react to each resource independently

**Cons:**
- 5 thresholds to tune per workload (memory: 85%? 90%? depends on operator)
- Reactive — by the time you measure OOM risk, it may be too late
- Interaction effects (which threshold wins when multiple are high?)
- None of the 5 dimensions directly relate to GPU utilization
- Prior art: abandoned proposal in `multi-resource-backpressure.md`

### Option B: Bounded Queues with Memory Budget (Chosen)

Allocate a fraction of node memory (default 40%) as pipeline buffer budget.
Divide across stages proportionally. Convert to message counts via observed
payload sizes. Enforce bounds in the Anvil broker.

| Dimension | Assessment |
|-----------|------------|
| Complexity | **Low** — one mechanism, one config parameter |
| Cost | Near-zero (counter check in push/ack_and_scatter hot path) |
| Scalability | High — each queue enforces its own bound, no coordination |
| Team familiarity | High — Flink, DALI, Ray Data all use this pattern |
| Correctness | **Proactive** — prevents overload, doesn't react to it |

**Pros:**
- One parameter (`buffer_memory_fraction`) replaces five monitoring dimensions
- Proactive: memory usage is bounded by design, not by reaction
- GPU-centric: `min_prefetch` ensures GPU always has data queued
- Battle-tested: Flink (credit-based), NVIDIA DALI (prefetch buffer), Ray Data (`max_buffered_batches`)
- Natural backpressure propagation: bottleneck stage paces the entire pipeline
- Adaptive: payload size EMA refines bounds at runtime
- No per-stage configuration: byte budget + payload observation handles mixed workloads

**Cons:**
- Less visibility into which specific resource is under pressure
- Initial bound estimate is rough (refined after ~50 messages)
- Does not prevent pathological cases (e.g., single 50GB payload, operator memory leak)
  — mitigated by NodeHealthGuard circuit breaker

### Option C: Static Rate Limiting (Spark Streaming Style)

Measure processing throughput, set source rate to match.

| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium |
| Cost | Low |
| Scalability | Medium |
| Team familiarity | Medium |
| Correctness | Medium — convergence is slow |

**Pros:**
- Simple concept
- Works well for steady-state

**Cons:**
- Slow to converge (needs to observe throughput first)
- Doesn't handle payload size variance
- Doesn't handle multi-stage bottleneck shifts
- Not used by any modern streaming engine for internal flow control

## Trade-off Analysis

The core trade-off is **visibility vs simplicity**:

- **Option A** gives fine-grained resource visibility but requires tuning 5
  thresholds per workload and is reactive (too late when OOM is imminent).
- **Option B** gives less visibility but is proactive (memory usage is bounded
  by construction) and requires zero tuning for most workloads.

For GPU-centric workloads where the goal is "keep GPUs busy, don't crash nodes,"
Option B is strictly better: it directly addresses GPU starvation (via
`min_prefetch`) and resource exhaustion (via memory budget) in one mechanism.

Option A's visibility gap is addressed by a thin **NodeHealthGuard** circuit
breaker (defense-in-depth) — if bounded queues are sized correctly, it never
fires.

**Why not both?** Adding monitoring on top of bounded queues adds complexity
without benefit. If the bound is correct, monitoring tells you "everything is
fine." If the bound is wrong, monitoring might catch it too late. Better to size
the bound correctly.

## Consequences

### What becomes easier
- **Zero-config flow control**: default `buffer_memory_fraction=0.4` works for most pipelines
- **Predictable memory usage**: total buffered bytes ≤ `node_memory × fraction`
- **GPU utilization**: `min_prefetch` guarantees GPU always has data queued
- **Multi-stage pipelines**: backpressure propagates automatically, no per-stage tuning
- **Debugging**: "why is my pipeline slow?" → check `source_blocked_ratio` and `worker_idle_ratio`

### What becomes harder
- **Diagnosing which resource is the bottleneck**: bounded queues hide the specific resource (memory vs disk vs network). Mitigated by NodeHealthGuard logging.
- **Workloads with extreme payload size variance** within a single stage: EMA adapts but may oscillate. May need damping factor tuning in `AdaptiveQueueBound`.
- **Very fast GPU operators** (<100ms/batch): may need `min_prefetch=4` to avoid starvation. Default 2 might underperform.

### What we'll need to revisit
- **Autoscaler signals**: Current `scale_up_lag_threshold` / `scale_down_lag_threshold` must be replaced with `source_blocked_ratio` / `worker_idle_ratio` (Phase 2)
- **Source push model**: Current `plan_splits() → push all` must become incremental (Phase 1)
- **Anvil broker**: Must support `max_pending` per QueueGroup and return `QueueFull` (Phase 1, Rust change)
- **`buffer_memory_fraction` default**: 0.4 is a guess. Validate in production with diverse workloads. May need adjustment or auto-detection.

## Action Items

1. [ ] **Phase 1 (P0)**: Bounded queue in Anvil broker — `max_pending` enforcement, `QueueFull` error, `PipelineFlowConfig`, `compute_stage_bounds()`, `AdaptiveQueueBound`, SourceManager incremental push
2. [ ] **Phase 2 (P1)**: Autoscaler signal update — `source_blocked_ratio`, `worker_idle_ratio`, replace lag-based signals
3. [ ] **Phase 3 (P1)**: Safety net — `NodeHealthGuard`, `NvmeNodeService` (Flight + health guard)
4. [ ] **Phase 4 (P2)**: Continuous throttle — `throttle_ratio` in `BackpressureSignal` for smoother control
5. [ ] Deprecate `multi-resource-backpressure.md` (**done**)
6. [ ] Validate `buffer_memory_fraction=0.4` default across production workloads
7. [ ] Add `source_blocked_ratio` / `worker_idle_ratio` to WebUI dashboard
