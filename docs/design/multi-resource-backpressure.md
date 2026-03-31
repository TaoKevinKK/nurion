# Multi-Resource Backpressure Design

_Status: Proposal_
_Created: March 2026_

## Problem

Current backpressure only considers **queue depth** (pending message count). This is insufficient for production stability. Observed failure modes:

| Resource | Failure Mode | Queue Depth Signal |
|----------|-------------|-------------------|
| CPU memory | Workers load large batches (images, embeddings) → OOM killer → pod restart | Queue may be low (workers just claimed data) |
| GPU memory | Model + batch exceeds VRAM → CUDA OOM → worker crash | Queue may be low |
| NVMe disk | Payload store fills disk → write failure → data loss | Queue may be low (payloads written, messages acked) |
| Object store | Ray shared memory full → spilling → cascading OOM | Queue may be low |
| Network | S3 bandwidth saturated → slow reads → upstream starvation | Queue may be HIGH (splits produced but data not fetched) |

**Key insight**: A system can have empty queues but be about to OOM. Queue depth alone is a necessary but insufficient backpressure signal.

## Industry References

| System | Resource-aware backpressure |
|--------|---------------------------|
| **TCP** | cwnd (bandwidth) + rwnd (receiver buffer) — dual constraint |
| **Flink** | Memory-based: buffer pool exhaustion triggers backpressure, not queue depth |
| **Spark** | Splits execution memory vs storage memory; throttles when execution memory low |
| **RDMA networks** | Credit-based flow control — credits represent buffer space, not message count |

Common pattern: **backpressure = f(queue_depth, available_memory, available_disk, ...)**

## Proposed Design

### Multi-Signal Backpressure Controller

Extend `JobBackpressureController` to check multiple resource dimensions:

```python
class ResourceBackpressureController:
    """Multi-dimensional backpressure using queue depth + resource pressure."""

    def should_pause(self, stage_id: str) -> bool:
        # Original queue-based check
        if self._queue_pressure(stage_id):
            return True

        # Resource-based checks (any one triggers pause)
        if self._memory_pressure():
            return True
        if self._disk_pressure():
            return True
        if self._object_store_pressure():
            return True

        return False
```

### Resource Monitors

Each monitor is lightweight (O(1) check, no syscalls in hot path):

#### Memory Monitor
```python
class MemoryMonitor:
    """Checks RSS of current process against configurable threshold."""

    def __init__(self, threshold_fraction: float = 0.85):
        self._threshold = threshold_fraction
        self._total = psutil.virtual_memory().total  # Cached once

    def is_pressured(self) -> bool:
        # psutil.Process().memory_info().rss is fast (~1μs)
        rss = psutil.Process().memory_info().rss
        return rss / self._total > self._threshold
```

#### Disk Monitor (for NVMe payload store)
```python
class DiskMonitor:
    """Checks NVMe payload store disk usage."""

    def __init__(self, path: str, threshold_fraction: float = 0.90):
        self._path = path
        self._threshold = threshold_fraction

    def is_pressured(self) -> bool:
        usage = shutil.disk_usage(self._path)
        return usage.used / usage.total > self._threshold
```

#### Object Store Monitor
```python
class ObjectStoreMonitor:
    """Checks Ray object store usage via ray.cluster_resources()."""

    def __init__(self, threshold_fraction: float = 0.85):
        self._threshold = threshold_fraction

    def is_pressured(self) -> bool:
        # Only meaningful when using ray:// payload store
        try:
            used = ray.cluster_resources().get("object_store_memory", 0)
            avail = ray.available_resources().get("object_store_memory", 0)
            if used == 0:
                return False
            return (used - avail) / used > self._threshold
        except Exception:
            return False
```

### Integration with Autoscaler

The autoscaler should also consider resource pressure when deciding to scale UP:

```python
def _compute_decisions(self, metrics):
    for stage_id, m in metrics.items():
        if m.input_queue_lag > threshold:
            # Before scaling up, check if the cluster is resource-pressured
            if self._is_resource_pressured():
                # Don't add workers — existing ones are already straining resources
                continue
            step = min(spawnable, max_scale_step)
            ...
```

### Per-Worker Memory Guard

Workers should proactively check their own memory before processing:

```python
class StageWorker:
    async def _process_and_ack(self, records):
        # Check memory before processing
        if self._memory_monitor and self._memory_monitor.is_pressured():
            # Nack the records (return to queue for other workers)
            self._nack_records(records)
            # Sleep briefly to let GC run
            gc.collect()
            await asyncio.sleep(1.0)
            return
        ...
```

## Implementation Plan

### Phase 1: Immediate (split_size fix)
- Reduce default split_size to limit per-worker memory
- Already done in NurionEngine — `split_size = min(io_sig.read_control_row_based_batch_size or 256, 1024)`

### Phase 2: Memory monitor in backpressure
- Add `MemoryMonitor` to `JobBackpressureController`
- Source pauses when cluster memory is high
- Low risk, high impact — prevents the most common OOM

### Phase 3: Disk monitor
- Add `DiskMonitor` for NVMe payload store
- Pause source when disk usage > 90%
- Prevents disk-full failures

### Phase 4: Per-worker memory guard
- Workers nack records when RSS exceeds threshold
- Allows GC before retrying
- Prevents individual worker OOM

### Phase 5: Autoscaler resource awareness
- Don't scale up when cluster is memory/disk pressured
- Prevents adding workers that would worsen the resource crisis

## Non-Goals

- **GPU memory monitoring**: CUDA OOM is better handled by correct batch_size configuration per pipeline, not runtime backpressure. Monitoring nvidia-smi from Python is expensive.
- **Network monitoring**: S3 throttling is transient and self-correcting. Retry logic handles it better than backpressure.
- **Predictive resource modeling**: Too complex for batch workloads. Reactive is sufficient.

---

_Last updated: March 2026_
