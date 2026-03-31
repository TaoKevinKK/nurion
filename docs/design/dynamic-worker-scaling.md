# Dynamic Worker Scaling Design

> NOTE: The current implementation uses the embedded Anvil backend. See
> `work-queue-redesign.md`.

_Design document for Nurion Engine auto-scaling feature_
_Created: December 2025_

---

## Implementation Status (Updated 2026-03-24)

| Component | Status | Notes |
|-----------|--------|-------|
| **SimpleAutoscaler** | ✅ Complete | `runtime/autoscaler.py` |
| **AutoscaleConfig** | ✅ Complete | Dataclass with threshold settings |
| **Queue Lag Metrics** | ✅ Complete | Anvil pending/claimed via job-level stats client |
| **Worker Scale Up/Down** | ✅ Complete | Via `WorkerManager` |
| **Cooldown Period** | ✅ Complete | Prevents thrashing |
| **Manual Override API** | ❌ Deprioritized | Low value for batch workloads; removed from TODO |
| **Resource-Aware Scaling** | ✅ Complete | `_get_spawnable_count()` queries `ray.available_resources()` for quantitative resource check |
| **Eager Fill** | ✅ Complete | `eager_fill()` scales to available capacity immediately after startup |
| **AIMD Cooldowns** | ✅ Complete | `cooldown_up_s=15`, `cooldown_down_s=60` — fast scale-up, slow scale-down |
| **Bottleneck Prioritization** | ❌ Not Implemented | Future work |

**Current Implementation:**
- Threshold-based scaling using Anvil pending/claimed
- Resource-aware step sizing via `_get_spawnable_count()` (quantitative, not boolean)
- Eager fill on startup to immediately use available cluster capacity
- AIMD cooldowns: aggressive scale-up (15s), conservative scale-down (60s)
- Scale down only when pending is low and claimed == 0
- Configurable check interval (default 10s)
- Backpressure is evaluated by a job-level controller using Anvil stats

---

## 1. Overview

This document describes the design for dynamic worker scaling in Nurion Engine, a batch/offline data processing framework. The design prioritizes simplicity over complexity, recognizing that offline processing has different requirements than real-time streaming.

### 1.1 Goals

1. **Balanced throughput**: Prevent stages from becoming bottlenecks or starving
2. **Resource efficiency**: Scale workers up/down based on actual load
3. **Fault tolerance**: Handle worker failures gracefully
4. **Manual intervention**: Allow operators to override automatic decisions
5. **Simplicity**: Minimize code complexity and external dependencies

### 1.2 Non-Goals

- Sub-second scaling decisions (offline processing tolerates delays)
- Complex distributed consensus (single coordinator is sufficient)
- Persistent scaling state (can be reconstructed on restart)
- Predictive scaling (reactive is sufficient for batch workloads)

## 2. Context: Offline vs Real-Time

Nurion Engine is an **offline/batch processing** framework, not a real-time streaming system. This distinction is crucial for design decisions:

| Dimension | Real-Time Streaming | Offline Batch (Nurion Engine) |
|-----------|--------------------|-----------------------|
| Data source | Unbounded, continuous | **Bounded, controllable rate** |
| Latency requirement | Milliseconds~seconds | **Minutes~hours acceptable** |
| Fault tolerance | Must recover precisely | **Can re-run stages** |
| Backpressure | Critical, upstream uncontrollable | **Source rate controllable** |
| Scaling decisions | Must be instant | **10-30 second delay acceptable** |

**Key insight**: Since the source rate is controllable and latency requirements are relaxed, we can use a much simpler architecture than real-time systems like Flink or Kafka Streams.

## 3. Architecture

### 3.1 Component Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          RayJobRunner                                    │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                      SimpleAutoscaler                               │ │
│  │                                                                     │ │
│  │  • In-memory state only (no persistence needed)                     │ │
│  │  • 15-30 second decision interval                                   │ │
│  │  • Simple threshold-based rules                                     │ │
│  │  • Manual override via configuration                                │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                   │
│  │ StageMaster  │──│ StageMaster  │──│ StageMaster  │                   │
│  │   (Source)   │  │  (Process)   │  │    (Sink)    │                   │
│  │              │  │              │  │              │                   │
│  │ Workers[1-3] │  │ Workers[1-8] │  │ Workers[1-2] │                   │
│  └──────────────┘  └──────────────┘  └──────────────┘                   │
│         │                 │                 │                            │
│         └─────────────────┴─────────────────┘                            │
│                           │                                              │
│                 Anvil (SlateDB-backed)                               │
│                 • Data flow between stages                               │
│                 • Pending/claimed counters                               │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Design Principles

1. **Single coordinator**: The `SimpleAutoscaler` runs within `RayJobRunner`, not as a separate distributed component. This eliminates distributed consensus complexity.

2. **In-memory state**: Scaling decisions and worker counts are kept in memory. On restart, state is reconstructed from actual `StageMaster` status.

3. **Slow-paced decisions**: Scaling decisions are made every 15-30 seconds, not continuously. This is sufficient for batch workloads and reduces system overhead.

4. **Direct control path**: `StageMaster` is in-process for scaling actions; metrics are fetched from Anvil.

## 4. Detailed Design

### 4.1 Configuration

```python
@dataclass
class AutoscaleConfig:
    """Autoscaling configuration."""
    
    enabled: bool = True
    check_interval_s: float = 15.0  # Decision interval
    
    # Scaling thresholds
    scale_up_lag_threshold: int = 1000    # Scale up if queue lag > threshold
    scale_down_lag_threshold: int = 100   # Scale down if lag < threshold
    
    # Damping
    cooldown_s: float = 60.0              # Cooldown after scaling
    max_scale_step: int = 2               # Max workers to add/remove per decision
    
    # Manual overrides
    fixed_workers: Optional[Dict[str, int]] = None  # {"stage_id": count}
    frozen_stages: Set[str] = field(default_factory=set)  # Stages to skip
```

### 4.2 Metrics Collection

Metrics are collected via a job-level Anvil stats client; StageMaster is used
only for worker counts and control actions.

```python
@dataclass
class StageMetrics:
    stage_id: str
    worker_count: int
    min_workers: int
    max_workers: int
    input_queue_lag: int      # Anvil pending_count
    input_queue_claimed: int  # Anvil claimed_count (in-flight)
    output_queue_size: int    # Messages in output queue
    is_running: bool
    is_finished: bool
    is_source: bool
```

Queue stats are sourced from Anvil (`pending_count`, `claimed_count`, `total_pushed`, `total_acked`)
through a single job-level client. Worker/master/operator progress counters are not used
for autoscaling decisions.

### 4.3 Scaling Algorithm

The algorithm uses simple threshold-based rules:

```python
def compute_desired_workers(metrics: StageMetrics) -> int:
    """
    Compute desired worker count based on queue lag.
    
    Rules:
    1. Manual override has highest priority
    2. Scale up if input queue lag > threshold
    3. Scale down if pending is low and claimed == 0
    4. Otherwise maintain current count
    """
    config = metrics.config
    current = metrics.worker_count
    
    # Rule 1: Manual override
    if stage_id in fixed_workers:
        return fixed_workers[stage_id]
    
    # Rule 2: Scale up on high lag
    if metrics.input_queue_lag > scale_up_lag_threshold:
        return min(current + max_scale_step, config.max_workers)
    
    # Rule 3: Scale down only when mostly idle (low pending + no in-flight)
    if metrics.input_queue_lag < scale_down_lag_threshold and metrics.input_queue_claimed == 0:
        if current > config.min_workers:
            return max(current - 1, config.min_workers)
    
    # Rule 4: Maintain
    return current
```

### 4.4 Cooldown and Damping

To prevent thrashing (rapid scale up/down cycles):

1. **Cooldown period**: After scaling a stage, wait `cooldown_s` seconds before scaling it again
2. **Max step size**: Scale at most `max_scale_step` workers per decision
3. **Hysteresis**: Different thresholds for scale-up vs scale-down

### 4.5 Manual Intervention

Operators can intervene through the `RayJobRunner` API:

```python
# Set fixed worker count for a stage
runner.set_stage_workers("gpu_inference", 10)

# Freeze a stage (disable autoscaling)
runner.freeze_stage("gpu_inference")

# Unfreeze (re-enable autoscaling)  
runner.unfreeze_stage("gpu_inference")

# Pause all autoscaling
runner.pause_autoscaling()

# Resume autoscaling
runner.resume_autoscaling()

# Get current status
status = runner.get_autoscale_status()
```

## 5. Fault Tolerance

### 5.1 Worker Failure

When a worker fails (Ray actor dies):

1. `StageMaster` detects the failure via `ray.wait()` on worker tasks
2. Failed worker is removed from the worker pool
3. If `worker_count < min_workers`, a new worker is spawned immediately
4. Unprocessed messages are returned to pending and re-consumed by other workers

```python
# In StageMaster.run()
for worker_id, task in list(self._worker_tasks.items()):
    ready, _ = ray.wait([task], timeout=0.1)
    if ready:
        try:
            ray.get(ready[0])
        except Exception as e:
            logger.warning(f"Worker {worker_id} failed: {e}")
            self._workers.pop(worker_id, None)
            self._worker_tasks.pop(worker_id, None)
            
            # Auto-replenish if below minimum
            if len(self._workers) < self.config.min_workers:
                await self._spawn_worker()
```

### 5.2 StageMaster Failure

If a `StageMaster` fails, the entire stage is restarted by `RayJobRunner`. The stage resumes
from Anvil storage state; pending/claimed counts determine remaining work.

### 5.3 Coordinator Failure

If `RayJobRunner` (and thus `SimpleAutoscaler`) fails:

1. The job restarts from scratch
2. `SimpleAutoscaler` reconstructs state from current `StageMaster` status
3. No persistent state to recover - decisions are recomputed

**Why is this acceptable?**

- Batch jobs are expected to run for minutes/hours
- Re-running scaling decisions is cheap
- Critical queue state is persisted in Anvil storage

## 6. Resource Management

### 6.1 Resource Vectors

Different stages may have different resource requirements:

```python
@dataclass
class StageConfig:
    # Worker resource requirements
    num_cpus: float = 1.0
    num_gpus: float = 0.0
    memory_mb: int = 0
    
    # Scaling bounds
    min_workers: int = 1
    max_workers: int = 4
```

### 6.2 Global Resource Constraints

When cluster resources are limited, the autoscaler respects Ray's resource constraints:

```python
def can_spawn_worker(config: StageConfig) -> bool:
    """Check if resources are available for a new worker."""
    available = ray.available_resources()
    
    if config.num_cpus > available.get("CPU", 0):
        return False
    if config.num_gpus > 0 and config.num_gpus > available.get("GPU", 0):
        return False
    
    return True
```

### 6.3 Bottleneck Prioritization

When resources are scarce, prioritize stages that are bottlenecks:

```python
def prioritize_stages(metrics: Dict[str, StageMetrics]) -> List[str]:
    """
    Return stages sorted by scaling priority.
    
    Bottleneck indicators:
    - High input queue lag
    - High worker utilization
    - Many downstream stages affected
    """
    def priority(m: StageMetrics) -> float:
        lag_score = m.input_queue_lag / 1000  # Normalize
        downstream_factor = 1 + len(m.downstream_stages) * 0.2
        return lag_score * downstream_factor
    
    return sorted(metrics.keys(), key=lambda s: priority(metrics[s]), reverse=True)
```

## 7. Observability

### 7.1 Logging

All scaling decisions are logged:

```python
logger.info(f"Scaling {stage_id}: {current} -> {target} workers "
            f"(lag={lag}, reason={reason})")
```

### 7.2 Metrics Export

Metrics can be exported for monitoring dashboards:

```python
def get_autoscale_status() -> Dict[str, Any]:
    return {
        "enabled": self.config.enabled,
        "stages": {
            stage_id: {
                "current_workers": metrics.worker_count,
                "desired_workers": self._compute_desired(metrics),
                "input_queue_lag": metrics.input_queue_lag,
                "last_scale_time": self._last_scale_time.get(stage_id),
                "is_frozen": stage_id in self.config.frozen_stages,
            }
            for stage_id, metrics in self._current_metrics.items()
        }
    }
```

## 8. Implementation Plan

### Phase 1: Core Autoscaler (MVP)

1. Add `AutoscaleConfig` dataclass
2. Implement `SimpleAutoscaler` class (~100 lines)
3. Integrate into `RayJobRunner`
4. Add basic logging

**Estimated effort**: 1-2 days

### Phase 2: Manual Intervention API

1. Add `set_stage_workers()`, `freeze_stage()`, etc. to `RayJobRunner`
2. Add CLI commands (optional)

**Estimated effort**: 0.5-1 day

### Phase 3: Resource-Aware Scaling

1. Add resource availability checks
2. Implement bottleneck prioritization

**Estimated effort**: 1 day

### Phase 4: Observability

1. Add structured logging for scaling events
2. Add metrics export endpoint

**Estimated effort**: 0.5-1 day

## 9. Testing Strategy

### 9.1 Unit Tests

- `test_scaling_decision`: Verify threshold-based decisions
- `test_cooldown`: Verify cooldown period is respected
- `test_manual_override`: Verify manual settings take priority
- `test_resource_check`: Verify resource availability checks

### 9.2 Integration Tests

- `test_scale_up_on_lag`: Create backlog, verify workers increase
- `test_scale_down_on_idle`: Clear backlog, verify workers decrease
- `test_worker_failure_recovery`: Kill worker, verify replenishment
- `test_frozen_stage`: Freeze stage, verify no scaling

### 9.3 End-to-End Tests

- Run multi-stage pipeline with autoscaling enabled
- Verify all data processed correctly
- Verify scaling events in logs

## 10. Future Considerations

### When to Revisit This Design

The simple design should be revisited if Solstice evolves to support:

1. **Real-time streaming**: Sub-second latency requirements
2. **Long-running jobs**: 24/7 operation requiring better state persistence
3. **Multi-tenant clusters**: Complex resource isolation needs
4. **Large-scale clusters**: 100+ stages requiring more sophisticated scheduling

### Potential Enhancements

- **Predictive scaling**: Use historical data to anticipate load
- **Cost optimization**: Prefer spot instances when possible
- **SLA-aware scheduling**: Priority levels for different jobs

## 11. References

- [Checkpoint and Recovery Design](deprecated/checkpoint-and-recovery.md) (deprecated)
- [Architecture Overview](deprecated/architecture.md)
- [Anvil Redesign](work-queue-redesign.md)

---

## 12. Resource-Aware Scaling (March 2026)

### Motivation

On elastic K8s clusters (e.g., SageMaker HyperPod), GPU nodes appear and disappear due to scheduling, spot reclaim, or hardware faults. The original autoscaler had two problems:

1. **Slow ramp-up**: `max_scale_step=2` meant 48 autoscaler cycles to fill 96 GPUs (~48 minutes with 60s cooldown). Every idle GPU minute is wasted compute.
2. **Boolean resource check**: `_check_cluster_resources()` only answered "can I add one worker?" — not "how many can I add?" When a node with 8 GPUs returned, only 2 workers were added per tick.

### Changes

#### `_get_spawnable_count(stage, max_needed) -> int`

Replaced the boolean `_check_cluster_resources()` with a quantitative check:

```python
available = ray.available_resources()
if gpu_stage:
    count = min(available_gpus / per_gpu, available_cpus / per_cpu)
elif cpu_stage:
    count = available_cpus / per_cpu
return min(count, max_needed)
```

Used in `_execute_decisions()` to cap the actual scale-up step by what the cluster can support right now. When a node with 8 GPUs returns, this returns 8 — the autoscaler spawns 8 workers in one tick.

#### `eager_fill(masters)`

One-shot scale-up called by `RayJobRunner` right after all stages start:

```
Job Start → StageMaster.start() spawns min_workers → eager_fill() fills to available capacity
```

Bridges the gap between `min_workers` (conservative) and current cluster capacity without waiting for the first autoscaler tick + queue lag buildup. Skips source stages (backpressure handles their rate).

#### AIMD Cooldowns

Inspired by TCP congestion control and K8s HPA stabilization windows:

| Direction | Cooldown | Rationale |
|-----------|----------|-----------|
| Scale UP | 15s | GPUs are expensive; fill fast |
| Scale DOWN | 60s | Brief dips are normal; don't overreact |

Replaced the single `cooldown_s` field with `cooldown_up_s` and `cooldown_down_s`.

#### Updated Defaults

| Parameter | Old | New | Rationale |
|-----------|-----|-----|-----------|
| `check_interval_s` | 15.0 | 10.0 | Faster response to node changes |
| `scale_up_lag_threshold` | 1000 | 500 | Scale up sooner |
| `cooldown` | 60s (both) | 15s up / 60s down | AIMD asymmetry |
| `max_scale_step` | 2 | 32 | Allow filling a full node in one step |

### Stage Parallelism Strategy (for integrators)

When using Nurion from a host framework (e.g., LAX), stages should be configured as:

| Stage Type | Parallelism | Why |
|------------|-------------|-----|
| Source | Fixed int | Backpressure pauses idle workers; paused workers are cheap (~0.5 CPU). Autoscaler skips source. |
| Processing (GPU) | `(min, max)` tuple | `min` from `available_resources()` (safe start), `max` from `cluster_resources()` (target). `eager_fill` + autoscaler bridge the gap. |
| Processing (CPU) | `(min, max)` tuple | Same pattern as GPU but CPU-based. |
| Sink | `(min, max)` tuple | No downstream backpressure protection. Autoscaler scales based on output queue lag. |

### Interaction with RecoveryManager

No coordination needed. They operate independently:

1. Worker dies → `RecoveryManager` respawns (exponential backoff 0.5-5s)
2. If node gone → `spawn_worker(is_min_worker=False)` times out (30s)
3. Autoscaler tick → `_get_spawnable_count()` sees 0 available → skips scale-up
4. Node returns → `_get_spawnable_count()` detects GPUs → autoscaler scales up

### Industry References

- **TCP AIMD**: Scale up additively (proportional to available resources), scale down conservatively — prevents flapping
- **K8s HPA**: Stabilization window = our `cooldown_down_s`; custom metrics = our queue lag
- **Flink Reactive Mode**: Parallelism adjusts to cluster size — our `eager_fill` + autoscaler achieves the same

---

_Last updated: March 2026_

## Resolved: Arrow Flight + Ray gRPC Conflict (March 2026)

Arrow Flight's gRPC shares C++ global state with Ray's internal gRPC. When
both run in the same process, the Flight server daemon thread silently dies.

**Root cause**: gRPC-core uses process-global singletons for the completion
queue, timer manager, and DNS resolver. Arrow Flight and Ray each initialize
these globals independently. The second initialization corrupts the first,
and the Flight server's `serve()` loop exits without raising.

**Fix**: `FlightServerProcess` — runs the Flight server in a subprocess instead
of a daemon thread. Full process isolation means separate gRPC globals.

- Entry point: `_internal/core/_flight_server_proc.py`
- Lifecycle: `PR_SET_PDEATHSIG` auto-kills subprocess when parent dies
- Communication: stdout for port (readiness signal), stdin for `add_dirs`
- Same `FlightPayloadServer` in-process class remains available for tests
- `NvmeSplitPayloadStore._ensure_initialized()` now uses `FlightServerProcess`
