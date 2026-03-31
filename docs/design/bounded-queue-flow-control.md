# Bounded Queue Flow Control

> Unified design for backpressure, autoscaling, and resource safety.
> Supersedes: `multi-resource-backpressure.md` (wrong direction — monitoring N
> resource dimensions is complex and fragile; bounding the queue solves all of
> them implicitly).
> Builds on: `dynamic-worker-scaling.md` (AIMD autoscaler — keep, extend signals).
> **Update this doc when flow control strategy changes.**

---

## 1. Problem Statement

Production failures observed (March 2026):

| Failure | Root Cause | Queue Depth Signal |
|---------|-----------|-------------------|
| OOM on workers | Unbounded payloads in flight | Queue may be LOW |
| NVMe disk full | Source pushes faster than sink drains | Queue may be LOW |
| Network saturation | Too many concurrent Flight reads | Queue HIGH |
| 500 workers stall | Queue contention (fixed: atomic counters) | Queue ZERO |
| GPU idle | Upstream can't feed GPU fast enough | Queue EMPTY |

**Current backpressure only checks queue depth.** A system can have empty queues
but be about to OOM, or full queues but idle GPUs. Queue depth alone is
insufficient — but monitoring 5 resource dimensions is the wrong fix.

---

## 2. Key Insight: Bound the Queue, Not the Resources

### What battle-tested systems do

| System | Model | Backpressure |
|--------|-------|-------------|
| **Flink** | Credit-based flow control | Downstream sends credits; upstream blocks without credits |
| **NVIDIA DALI** | Pull-based prefetch | Fixed-depth prefetch buffer; GPU pulls; full buffer = upstream stops |
| **Ray Data** | Bounded buffer | `max_buffered_batches`; producer blocks when buffer full |
| **Spark Streaming** | Rate limiter | Measures throughput, adjusts pull rate |
| **DeepSpeed** | Static schedule | Pipeline micro-batches statically scheduled |

**Common pattern: bounded buffers, not resource monitoring.**

With a bounded queue, all resource problems become self-limiting:

| Resource | Why bounded queue solves it |
|----------|---------------------------|
| Memory | At most `W × P` payloads in flight → predictable RSS |
| NVMe disk | At most `W × P` files on disk → cannot fill disk |
| Network | At most `W × P` concurrent Flight reads → self-limited |
| S3 bandwidth | Sink buffer bounded → write rate bounded |
| GPU utilization | `P` prefetch batches → GPU always has data ready |

**One parameter (`prefetch_factor`) replaces five monitoring dimensions.**

---

## 3. Design

### 3.1 Bounded Inter-Stage Queues

```
Source ──push──▶ [QueueGroup: max_pending = W×P] ──claim──▶ Stage 1
                  │                                          │
                  │ source blocks when                       │ ack_and_scatter
                  │ pending >= max_pending                   │
                  ▼                                          ▼
          (natural backpressure)              [QueueGroup: max_pending = W×P] ──▶ Stage 2
                                                                                    │
                                                                                    ▼
                                                                              [Sink buffer]

W = downstream stage's current worker count
P = prefetch_factor (default: 3)
```

**How it works:**
1. Each inter-stage QueueGroup has `max_pending = downstream_workers × prefetch_factor`
2. `push()` / `ack_and_scatter()` blocks (or returns backpressure signal) when
   `pending_count >= max_pending`
3. As downstream workers `ack()`, pending decreases → upstream unblocks
4. GPU stages are always the bottleneck → everything upstream adapts to GPU rate

**Sizing rationale:**
- `prefetch_factor = 3` means each GPU worker always has 2-3 batches queued ahead
- If GPU takes 10s/batch and upstream takes 2s/batch, GPU never starves
- If GPU takes 1s/batch, upstream naturally rate-matches (bounded by queue)

### 3.2 Source Rate Control

Current source pushes all splits upfront (`plan_splits()` → push all). This
defeats bounded queues because all data enters the queue immediately.

**Change: source pushes incrementally, respecting queue bound.**

```python
# SourceManager (core/managers/source_manager.py)
class SourceManager:
    async def _produce_loop(self):
        for split in self._source.plan_splits():
            # Block until queue has room (natural backpressure)
            while self._queue_stats.pending_count >= self._max_pending:
                await asyncio.sleep(0.5)
                if self._should_stop:
                    return
            self._push_split(split)
```

This is equivalent to Flink's credit system: the queue bound IS the credit pool.

### 3.3 Worker Output Backpressure

Workers currently do unlimited `ack_and_scatter()`. With bounded downstream
queues, `ack_and_scatter` naturally blocks when downstream is full.

**Implementation: Anvil broker enforces the bound.**

```rust
// storage.rs — push_messages / ack_and_scatter
fn push_messages(&self, queue: &str, msgs: &[Message]) -> Result<()> {
    let counters = self.load_or_init_counters(queue).await?;
    let pending = counters.pending_count();

    if let Some(max) = self.get_queue_max_pending(queue) {
        if pending >= max {
            return Err(StorageError::QueueFull);
        }
    }
    // ... normal push
}
```

Python side: `StageWorker` catches `QueueFull`, waits, retries — no data loss.

---

## 4. Autoscaler Signal Changes

### 4.1 Problem with Queue Depth as Scaling Signal

With bounded queues, the source pushes until the queue is full, then blocks.
**Queue depth is always near `max_pending`** — it's no longer a useful scaling signal.

### 4.2 New Signals

| Signal | Meaning | Action |
|--------|---------|--------|
| `source_blocked_ratio` | % time source spends waiting for queue room | High → downstream bottleneck → scale UP downstream |
| `worker_idle_ratio` | % time workers spend waiting for data | High → downstream over-provisioned → scale DOWN |
| `processing_latency_p99` | Tail latency per batch | Rising → workers overloaded or data skew |

```python
# autoscaler.py
def _tick(self):
    # Replace queue_depth signal with supply/demand balance
    if self._source_blocked_ratio > 0.5:
        # Source is blocked >50% — downstream can't keep up
        self._scale_up(additive=self._compute_step())
    elif self._worker_idle_ratio > 0.5:
        # Workers idle >50% — too many workers
        self._scale_down(factor=0.5)
    # else: system in balance, hold
```

### 4.3 Compatibility with Existing AIMD

The existing AIMD structure (additive increase / multiplicative decrease,
asymmetric cooldowns) is correct. Only the input signal changes:

| Component | Keep | Change |
|-----------|------|--------|
| AIMD cooldowns (up=15s, down=60s) | ✅ | — |
| Eager fill at startup | ✅ | — |
| `_get_spawnable_count()` resource check | ✅ | — |
| `max_scale_step = 32` | ✅ | — |
| `scale_up_lag_threshold = 500` | ❌ | Replace with `source_blocked_ratio` |
| `scale_down_lag_threshold = 100` | ❌ | Replace with `worker_idle_ratio` |

### 4.4 Convergence (Analogous to TCP Slow Start)

```
Phase 1: Eager Fill (0-30s)
  Workers: min_parallelism → max_parallelism (fill fast)
  Source: pushes until queue bound hit
  Queue: fills to max_pending within seconds

Phase 2: Probe Bottleneck (30s-2min)
  source_blocked_ratio rises → downstream is bottleneck
  OR worker_idle_ratio rises → upstream is bottleneck
  Autoscaler adjusts worker count toward balance

Phase 3: Steady State (2min+)
  source_blocked_ratio ≈ 0.3 (source occasionally waits — normal)
  worker_idle_ratio ≈ 0.1 (workers occasionally wait — normal)
  GPU utilization: high (always has prefetched data)

Phase 4: Adaptive (continuous)
  Data characteristics change (e.g., larger records) →
  source_blocked_ratio shifts → autoscaler re-adjusts
```

---

## 5. Safety Net (Not Congestion Control)

Bounded queues handle normal operation. A thin safety net handles edge cases
that the bound can't prevent (e.g., a single payload that's 50GB, or a memory
leak in user operator code).

```python
class NodeHealthGuard:
    """Circuit breaker. Not a throttle — triggers only on imminent failure.

    If bounded queues are sized correctly, this never fires.
    Exists for defense-in-depth only.
    """

    MEMORY_FLOOR_MB = 1024    # 1 GB
    DISK_FLOOR_PCT = 0.05     # 5%

    def check(self) -> Optional[str]:
        """Returns failure reason, or None if healthy."""
        import psutil

        mem = psutil.virtual_memory()
        if mem.available < self.MEMORY_FLOOR_MB * 1048576:
            return f"OOM imminent: {mem.available // 1048576}MB available"

        if self._nvme_path:
            disk = os.statvfs(self._nvme_path)
            if disk.f_bavail / disk.f_blocks < self.DISK_FLOOR_PCT:
                return f"Disk full: {disk.f_bavail / disk.f_blocks:.1%} remaining"

        return None
```

**StageMaster integration:**

```python
# stage_master.py monitor loop
health = self._health_guard.check()
if health:
    logger.critical(f"Node health critical: {health}")
    self._pause_source()
    # Also: nack all claimed messages so other nodes can process them
```

---

## 6. NvmeNodeService (Minimal)

The `_FlightServerActor` evolves into `NvmeNodeService` with exactly two jobs:

1. **Flight server subprocess** (existing — gRPC isolation from Ray)
2. **NodeHealthGuard** (new — disk + memory circuit breaker)

**No metrics reporting, no bandwidth monitoring, no resource tracking.**
Bounded queues make those unnecessary.

```python
class NvmeNodeService:
    """Per-node: Flight server + health guard. Job-level actor (not detached).

    Why job-level: code updates deploy automatically with new jobs.
    Flight subprocess persists via port binding (OS-level singleton).
    """

    def __init__(self, root_dirs, job_id):
        self._flight_port = self._ensure_flight_subprocess(root_dirs)
        self._health_guard = NodeHealthGuard(root_dirs[0])

    def get_flight_port(self) -> int:
        return self._flight_port

    def check_health(self) -> Optional[str]:
        return self._health_guard.check()

    def _ensure_flight_subprocess(self, root_dirs) -> int:
        """Start Flight subprocess if not already running (port binding = singleton)."""
        # Try connecting to existing subprocess on well-known port
        # If port in use → reuse. If free → start new subprocess.
        ...
```

---

## 7. Configuration

```python
@dataclass
class StageConfig:
    # Existing
    min_parallelism: int = 1
    max_parallelism: int = 4
    batch_size: int = 100

    # New: flow control
    prefetch_factor: int = 3  # queue bound = downstream_workers × prefetch_factor
    # Most users never touch this. Default 3 works for:
    #   - GPU stages (3 batches ahead = GPU never starves)
    #   - CPU stages (3 batches ahead = good overlap)
    #   - Source stages: N/A (source is producer, not consumer)
```

**No resource budgets. No bandwidth limits. No thresholds to tune.**
The only knob is `prefetch_factor`, and the default works for most workloads.

---

## 8. Implementation Plan

### Phase 1: Bounded Queue (P0)

**Goal: eliminate OOM / disk-full / network-saturation failures.**

| Task | File | Effort |
|------|------|--------|
| Add `max_pending` to QueueGroup metadata | `lib/anvil-rs/src/storage.rs` | S |
| `push_messages` returns `QueueFull` when at bound | `lib/anvil-rs/src/storage.rs` | S |
| `ack_and_scatter` respects downstream bound | `lib/anvil-rs/src/storage.rs` | M |
| SourceManager push loop respects bound | `core/managers/source_manager.py` | S |
| StageWorker retries on `QueueFull` | `core/stage_worker.py` | S |
| `prefetch_factor` in StageConfig | `core/job.py` | S |
| Calculate `max_pending` at stage start | `core/stage_master.py` | S |

### Phase 2: Autoscaler Signal Update (P1)

**Goal: autoscaler converges faster with bounded queues.**

| Task | File | Effort |
|------|------|--------|
| Track `source_blocked_ratio` in SourceManager | `core/managers/source_manager.py` | S |
| Track `worker_idle_ratio` in WorkerManager | `core/managers/worker_manager.py` | S |
| Replace lag-based signals in SimpleAutoscaler | `runtime/autoscaler.py` | M |
| Expose blocked/idle ratios in WebUI | `webui/state/writer.py` | S |

### Phase 3: Safety Net (P1)

**Goal: defense-in-depth for edge cases.**

| Task | File | Effort |
|------|------|--------|
| `NodeHealthGuard` (memory + disk check) | `core/nvme_payload_store.py` | S |
| StageMaster checks health each loop | `core/stage_master.py` | S |
| NvmeNodeService (Flight + health guard) | `core/nvme_payload_store.py` | M |

### Phase 4: Continuous Throttle (P2, optional)

**Goal: smoother backpressure than binary pause/resume.**

| Task | File | Effort |
|------|------|--------|
| SourceManager supports `throttle_ratio` (sleep between pushes) | `core/managers/source_manager.py` | S |
| BackpressureSignal carries ratio (not just pause/resume) | `runtime/backpressure.py` | S |

---

## 9. What This Deprecates

| Document | Status | Reason |
|----------|--------|--------|
| `multi-resource-backpressure.md` | **Deprecated** | Wrong direction: monitoring N resource dimensions is complex and fragile. Bounded queues solve all resource problems implicitly. |
| `deprecated/partition-backpressure-improvements.md` | **Already deprecated** | Referenced old Tansu/Kafka model. |

| Code concept | Status | Reason |
|--------------|--------|--------|
| `JobBackpressureController` (queue-depth only) | **Replace** | Queue depth is meaningless with bounded queues. Replace with `source_blocked_ratio`. |
| `BackpressureSignal.PAUSE / RESUME` | **Extend** | Keep as circuit breaker, add `throttle_ratio` for proportional control. |
| Multi-resource monitors (MemoryMonitor, DiskMonitor, etc.) | **Don't build** | Bounded queues make them unnecessary. NodeHealthGuard is sufficient. |

## 10. What This Keeps

| Component | Why keep |
|-----------|---------|
| AIMD cooldowns (up=15s, down=60s) | Proven in production. Asymmetric = correct (GPUs expensive, fill fast). |
| Eager fill at startup | Critical for GPU utilization. New job should saturate GPUs within 30s. |
| `_get_spawnable_count()` resource check | Still needed: don't schedule workers that can't get CPU/GPU. |
| `RecoveryManager` with exponential backoff | Orthogonal to flow control. Keep as-is. |
| `max_scale_step = 32` | Allows filling a full GPU node in one step. |
| FlightServerProcess subprocess isolation | Required: Arrow Flight gRPC conflicts with Ray gRPC. |
| NVMe write policies (WRITE_THROUGH / WRITE_BACK) | Orthogonal to flow control. Keep as-is. |
| S3 auto-degrade on failure | Orthogonal. Keep as-is. |

---

## Appendix A: Why Not Multi-Dimensional Congestion Control

The `multi-resource-backpressure.md` proposal suggested monitoring memory, disk,
network, object store, and S3 — five dimensions with independent thresholds.

Problems with that approach:
1. **Threshold tuning**: What's the right memory threshold? 85%? 90%? Depends on
   workload, payload size, and operator memory usage. One size doesn't fit all.
2. **Latency**: By the time you measure OOM risk, it may be too late.
3. **Interaction effects**: High memory + high disk + moderate network — which
   threshold wins? Priority logic adds complexity.
4. **Irrelevance to GPU**: None of the 5 dimensions directly relate to GPU
   utilization, which is the primary optimization target.

Bounded queues solve all of these:
- No thresholds to tune (just `prefetch_factor`)
- Proactive (prevents overload, doesn't react to it)
- No interaction effects (one bound controls everything)
- GPU-centric (prefetch_factor sized for GPU consumption rate)

## Appendix B: Prefetch Factor Sizing Guide

| Workload | GPU time/batch | Upstream time/batch | Recommended P | Why |
|----------|---------------|--------------------|----|-----|
| LLM inference | 5-30s | 0.5-2s | 2 | GPU is slow, small prefetch enough |
| Video encoding | 2-10s | 0.5-1s | 3 | Default, good overlap |
| Image transform | 0.1-1s | 0.1-0.5s | 4 | GPU fast, need deeper buffer |
| CPU-only pipeline | N/A | varies | 3 | Default works |
