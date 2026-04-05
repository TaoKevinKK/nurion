# Pipeline Controller

> Unified control plane for scaling, flow control, and liveness detection.
> **Supersedes:** `dynamic-worker-scaling.md` (autoscaler merged into controller).
> **Implements:** `bounded-queue-flow-control.md` §4 (autoscaler signal changes)
> and §3.2 (source rate control) at the control-plane level.
> **Update this doc when control-plane architecture changes.**

---

## 1. Problem Statement

The current control plane has **six independent systems** making flow-control
decisions from separate views of the same queue state:

| System | Location | Reads | Decides |
|--------|----------|-------|---------|
| `SimpleAutoscaler` | `runtime/autoscaler.py` | input lag | scale up/down |
| `JobBackpressureController` | `runtime/backpressure.py` | output size | pause source |
| `StageMaster` no-progress timeout | `core/stage_master.py` | worker completions | stage stuck? |
| `Worker` idle timeout | `core/stage_worker.py` | last claim time | fail fast |
| `Worker` QueueFull retry | `core/stage_worker.py` | ack result | sleep + retry |
| `SourceManager` backpressure | `core/managers/source_manager.py` | backpressure_fn | pause production |

These systems **contradict each other** under backpressure:

```
Bounded queue full → workers block on ack_and_scatter (QueueFull retry)
  → no workers complete (they're alive but blocked)
  → StageMaster: "no progress for 600s → FAILED"     ← WRONG
  → Autoscaler: "input lag high → scale UP"           ← WRONG (more workers = more blocking)
  → BackpressureController: "output full → pause"     ← CORRECT (but nobody told the other two)
```

**Root cause:** No single component sees the full picture. Each queries queue
stats independently, makes a local decision, and conflicts arise.

---

## 2. Design: One Controller, One Decision Loop

### 2.1 Architecture

Replace three separate systems with one `PipelineController`:

```
                     ┌──────────────────────────────────┐
                     │       PipelineController          │
                     │                                    │
                     │  Replaces:                         │
                     │    SimpleAutoscaler                │
                     │    JobBackpressureController       │
                     │    StageMaster.no_progress_timeout │
                     │                                    │
                     │  One tick() reads all stats,       │
                     │  makes all decisions consistently  │
                     └──────────┬───────────────────────-─┘
                                │
                 ┌──────────────┼──────────────┐
                 ▼              ▼              ▼
           scale_up/down  pause/resume    fail_stage
           (via master)   (via master)   (via master)
```

**Key invariant:** All three decisions (scaling, flow control, liveness) are made
from the **same snapshot** of queue stats in the **same tick**. They cannot
contradict each other.

### 2.2 The Tick

```python
class PipelineController:
    """Unified scaling, flow control, and liveness detection.

    Single control loop that reads queue stats once per tick and makes
    all flow-control decisions from a consistent snapshot.
    """

    def tick(self, masters: Dict[str, StageMaster]) -> None:
        snapshot = self._collect_stats(masters)

        for stage_id, m in snapshot.items():
            master = masters[stage_id]
            saturated = self._is_output_saturated(m)

            # ── 1. Scaling ──
            self._evaluate_scaling(stage_id, m, master, saturated)

            # ── 2. Source flow control ──
            if m.is_source:
                should_pause = saturated or self._any_downstream_saturated(
                    stage_id, snapshot
                )
                master.set_source_paused(should_pause)

            # ── 3. Liveness ──
            self._evaluate_liveness(stage_id, m, master, saturated)
```

### 2.3 Scaling Decision

```python
def _evaluate_scaling(self, stage_id, m, master, saturated):
    """Scale based on supply/demand, constrained by output saturation."""

    if m.is_source or m.is_finished:
        return

    now = time.monotonic()

    # Scale UP: input has work AND output can absorb more
    if m.input_pending > self._config.scale_up_threshold:
        if saturated:
            return  # Adding workers would just block on QueueFull
        if now - self._last_scale_up.get(stage_id, 0) < self._config.cooldown_up_s:
            return  # AIMD cooldown
        step = min(self._config.max_scale_step, m.max_workers - m.worker_count)
        step = min(step, self._spawnable_count(master))
        if step > 0:
            master.scale_up(step)
            self._last_scale_up[stage_id] = now

    # Scale DOWN: input is drained AND no claimed work
    elif m.input_pending < self._config.scale_down_threshold:
        if m.worker_count <= m.min_workers:
            return
        if m.input_claimed > 0:
            return  # Workers still processing
        if now - self._last_scale_down.get(stage_id, 0) < self._config.cooldown_down_s:
            return
        master.scale_down(1)
        self._last_scale_down[stage_id] = now
```

**Why output saturation gates scale-up:** When output queue is full, adding
workers only adds more `ack_and_scatter` retries. The bottleneck is downstream
(it can't consume fast enough). The correct action is to wait — or scale UP
the downstream stage instead.

### 2.4 Flow Control Decision

```python
def _evaluate_flow_control(self, stage_id, m, master, snapshot, saturated):
    """Pause source when downstream can't absorb."""

    if not m.is_source:
        return

    should_pause = saturated
    if not should_pause:
        # Also check downstream stages (transitive backpressure)
        for ds_id in self._downstream_ids(stage_id):
            ds = snapshot.get(ds_id)
            if ds and self._is_output_saturated(ds):
                should_pause = True
                break

    master.set_source_paused(should_pause)
```

Source pauses when its own output OR any downstream output is saturated. This
propagates backpressure from the bottleneck (typically GPU) all the way to
the source without intermediate monitoring.

### 2.5 Liveness Decision

```python
def _evaluate_liveness(self, stage_id, m, master, saturated):
    """Detect truly stuck stages (not backpressure)."""

    if m.worker_count == 0 or m.input_pending == 0:
        return  # No workers or no work → not stuck

    if m.seconds_since_last_completion <= self._config.liveness_timeout_s:
        return  # Recent progress → healthy

    # No progress for a while. Is it real or just backpressure?
    if saturated:
        # Workers are blocked on output → system is healthy, just slow
        master.reset_progress_timer()
        return

    # Genuine stuck: has work, has workers, output not full, no progress
    master.fail(
        f"No progress for {m.seconds_since_last_completion:.0f}s "
        f"(input_pending={m.input_pending}, workers={m.worker_count}, "
        f"output not saturated)"
    )
```

**The key insight:** A stage with high input, no completions, and **full output**
is NOT stuck — it's backpressured. Only a stage with high input, no completions,
and **available output capacity** is genuinely stuck (deadlock, worker bug, etc.).

### 2.6 Output Saturation Check

```python
def _is_output_saturated(self, m: StageMetrics) -> bool:
    """Output queue is near capacity → workers are likely QueueFull-blocked."""
    if m.output_max_pending <= 0:
        return False  # Unbounded queue never saturates
    return m.output_pending >= m.output_max_pending * 0.8
```

The 0.8 threshold accounts for batch granularity — a queue at 85% capacity may
still accept small batches but will soon block larger ones. This matches the
existing threshold in `BackpressureController.should_pause()` (line 66), so
behavior is consistent during migration.

---

## 3. StageMetrics

```python
@dataclass
class StageMetrics:
    stage_id: str
    worker_count: int
    min_workers: int
    max_workers: int
    is_source: bool
    is_finished: bool

    # Queue state (from broker)
    input_pending: int         # upstream messages waiting to be claimed
    input_claimed: int         # upstream messages currently being processed
    output_pending: int        # downstream messages waiting to be consumed
    output_max_pending: int    # downstream queue capacity (0 = unbounded)

    # Progress (from master)
    seconds_since_last_completion: float
```

Compared to current `StageMetrics` in autoscaler.py:
- **Added:** `output_max_pending`, `seconds_since_last_completion`
- **Removed:** `output_queue_size` (renamed to `output_pending` for clarity)
- **Removed:** `input_queue_lag` (renamed to `input_pending`)

---

## 4. Impact on Existing Components

### 4.1 StageMaster — Simplified

**Remove:**
- `_last_progress_time` tracking and no-progress timeout check
- `_check_backpressure()` method
- `_backpressure_provider` field
- `set_backpressure_provider()` method

**Add:**
- `set_source_paused(paused: bool)` — called by controller
- `fail(reason: str)` — called by controller on liveness failure
- `reset_progress_timer()` — called by controller when backpressured
- `report_completion_age() -> float` — seconds since last worker completion

**Run loop becomes:**
```python
while self._state == _StageState.RUNNING:
    # Check background task health
    if self._source_manager:
        self._source_manager.raise_if_production_failed()
    if self._sink_manager:
        self._sink_manager.raise_if_commit_failed()    # P1 fix

    # All workers exited?
    if self._worker_manager.worker_count == 0:
        if self._has_unprocessed_messages():
            await self._worker_manager.spawn_worker()
        else:
            self._state = _StageState.FINISHED
            break

    # Wait for worker lifecycle events
    completed, failed = await self._worker_manager.wait_for_completion()
    self._worker_manager.cleanup_workers(completed + failed)

    # Handle failures → recovery
    if failed:
        ...  # existing RecoveryManager logic, unchanged
    if completed:
        self._last_completion_time = time.monotonic()
        self._recovery_manager.record_success()
```

No scaling decisions. No backpressure decisions. No liveness decisions.
StageMaster is purely a **worker lifecycle manager**.

### 4.2 SourceManager — Simplified Backpressure

**Remove:**
- `backpressure_fn` parameter (no longer receives a callback)

**Change:**
- Reads `master._source_paused` flag directly (set by controller)

```python
async def _produce_splits(self, queue_client, running_fn):
    for split in self._source.plan_splits(self._stage_id):
        if not running_fn():
            break

        # Wait if controller says to pause
        while self._is_paused():
            await asyncio.sleep(self._pause_sleep_s)
            if not running_fn():
                return

        await self._produce_split_with_retry(queue_client, split, idx)
```

`_is_paused()` reads from master's flag, set by controller each tick.

### 4.3 Worker — Unchanged

Workers keep:
- **QueueFull retry** in `ack_and_scatter` — correct level for handling output-full
- **Idle timeout** — safety net for unresponsive broker (different from liveness)
- **`drained`-based exit** — broker-driven completion signal

Workers do NOT need to know about the controller. Their contract is unchanged:
claim → process → ack → exit when drained.

### 4.4 RayJobRunner — Simplified

**Remove:**
- `_autoscaler` and `_autoscale_task`
- `_backpressure_controller`
- `_build_stage_queue_configs()` for separate backpressure config

**Add:**
- `_controller = PipelineController(config, queue_stats_client, dag_edges)`
- `_controller_task = asyncio.create_task(controller.run_loop(masters))`

---

## 5. Files Changed

| Action | File | Notes |
|--------|------|-------|
| **New** | `runtime/pipeline_controller.py` | PipelineController + StageMetrics |
| **Delete** | `runtime/autoscaler.py` | Merged into PipelineController |
| **Delete** | `runtime/backpressure.py` | Merged into PipelineController |
| **Modify** | `core/stage_master.py` | Remove timeout/backpressure, add controller interface |
| **Modify** | `core/managers/source_manager.py` | Remove backpressure_fn, read pause flag |
| **Modify** | `core/managers/sink_manager.py` | Add `raise_if_commit_failed()` (P1 fix) |
| **Modify** | `runtime/ray_runner.py` | Replace autoscaler + backpressure with controller |
| **Modify** | `config.py` | Remove autoscaler_* configs, add controller_* |

**Kept unchanged:**
- `runtime/queue_stats.py` — still used by controller
- `core/stage_worker.py` — contract unchanged
- `core/managers/worker_manager.py` — still called by master for scale_up/down
- `core/managers/recovery_manager.py` — orthogonal to flow control
- All Anvil/broker code — data plane unchanged

---

## 6. Configuration

```python
@dataclass
class PipelineControllerConfig:
    # Tick interval
    tick_interval_s: float = 5.0

    # Scaling (AIMD, carried from dynamic-worker-scaling.md)
    scale_up_threshold: int = 500        # input_pending > this → scale up
    scale_down_threshold: int = 100      # input_pending < this → scale down
    cooldown_up_s: float = 15.0          # aggressive scale-up
    cooldown_down_s: float = 60.0        # conservative scale-down
    max_scale_step: int = 32             # fill a full GPU node in one step

    # Output saturation (used for all three decisions)
    output_saturation_ratio: float = 0.8

    # Liveness
    liveness_timeout_s: float = 600.0    # seconds without progress before stuck

    # Eager fill
    eager_fill_enabled: bool = True      # one-shot scale-up at startup
```

**Compared to current config:**
- `autoscaler_*` fields → merged into `PipelineControllerConfig`
- `stage_no_progress_timeout_s` → `liveness_timeout_s` (moved to controller)
- `backpressure_threshold_*` → removed (output_saturation_ratio replaces both)

---

## 7. Future: Signal Evolution

This design uses `input_pending` thresholds for scaling decisions, which
matches the current code. Per `bounded-queue-flow-control.md` §4.2, the
scaling signals should evolve to:

| Current (Phase 1) | Future (Phase 2) |
|--------------------|-------------------|
| `input_pending > scale_up_threshold` | `source_blocked_ratio > 0.5` |
| `input_pending < scale_down_threshold` | `worker_idle_ratio > 0.5` |

Phase 2 requires instrumenting SourceManager and WorkerManager to track
blocked/idle time ratios. The PipelineController architecture supports this
by simply swapping the signal in `_evaluate_scaling()` — no structural changes
needed.

---

## 8. Relationship to Other Design Docs

| Document | Relationship |
|----------|-------------|
| `bounded-queue-flow-control.md` | **Foundation.** PipelineController is the control-plane implementation of bounded-queue flow control. The queue bounds (data plane) are unchanged. |
| `dynamic-worker-scaling.md` | **Superseded.** AIMD logic, cooldowns, eager fill, resource checks are all carried into PipelineController. The standalone autoscaler is retired. |
| `anvil-semantics.md` | **Unchanged.** Broker semantics (claim, ack, drained) are the data plane. PipelineController reads stats but doesn't change broker behavior. |
| `queue-group-and-skew-handling.md` | **Unchanged.** QueueGroup stats are the input to PipelineController's decisions. |

---

## 9. What This Deprecates

| Artifact | Status | Reason |
|----------|--------|--------|
| `dynamic-worker-scaling.md` | **Deprecated** | Autoscaler merged into PipelineController. AIMD logic, cooldowns, eager fill preserved but no longer a standalone component. |
| `SimpleAutoscaler` class | **Delete** | Replaced by PipelineController |
| `JobBackpressureController` class | **Delete** | Replaced by PipelineController |
| `StageMaster.no_progress_timeout` | **Remove** | Liveness detection moved to PipelineController |
| `StageAutoscaleConfig` dataclass | **Remove** | Replaced by `PipelineControllerConfig` |

---

## 10. Collateral Fixes

The PipelineController redesign naturally addresses several known bugs:

| Issue | How it's fixed |
|-------|---------------|
| **P0: no-progress vs backpressure** | Liveness check includes `not saturated` guard — backpressure never triggers false stuck detection |
| **P1: Sink commit loop error swallowed** | StageMaster run loop adds `sink_manager.raise_if_commit_failed()` check (consistent with source_manager pattern) |
| **P4: mark_finished failure silent** | Controller's liveness check catches the downstream hang (input pending, no progress, output not saturated → stuck) |

P2 (worker nack on error) and P3 (per-partition budget overflow) are
independent fixes unrelated to the control plane — addressed separately.
