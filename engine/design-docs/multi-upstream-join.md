# Multi-Upstream Join Design

## Summary

Support stages with multiple upstream stages joined by column key(s), enabling diamond-shaped workflows. Uses hash-partitioned Build-Probe Join — the same approach as OLAP databases (ClickHouse, StarRocks, Doris).

## Motivation

Current limitation: each stage consumes from a single upstream queue. Fan-out uses competing consumers (each downstream gets a subset). We need:

1. **Fan-out (Broadcast)**: source output delivered to ALL downstream stages
2. **Multi-upstream Join**: consume from multiple upstreams, join by key column(s)
3. **Parallel join workers**: not limited to a single worker

### Target Use Case

```
LanceSource ──┬──> CaptionModelA ──┐
              │                     ├──> JoinStage (N workers) ──> LanceSink
              └──> CaptionModelB ──┘
```

Both caption stages see ALL source records. The join stage matches results by `file_id` with N parallel workers and outputs the combined table.

## User API

```python
from nurion import Job, JobConfig, Stage, JoinConfig

job = Job(job_id='diamond', config=JobConfig(workqueue_db_path="memory://"))

job.add_stage(Stage(
    stage_id='source',
    operator_config=LanceTableSourceConfig(table_path='/data/input'),
))

job.add_stage(Stage(
    stage_id='caption_a',
    operator_config=CaptionAConfig(...),
    parallelism=4,
), upstream_stages=['source'])

job.add_stage(Stage(
    stage_id='caption_b',
    operator_config=CaptionBConfig(...),
    parallelism=4,
), upstream_stages=['source'])

job.add_stage(Stage(
    stage_id='join_sink',
    operator_config=LanceSinkConfig(output_path='/data/output'),
    parallelism=4,
    join_config=JoinConfig(
        join_keys=['file_id'],
        join_type='inner',
        num_partitions=8,
    ),
), upstream_stages=['caption_a', 'caption_b'])
```

## Design

Three independent mechanisms composed by the runner:

### 1. Fan-out (Broadcast)

When a stage has multiple downstreams in the DAG, create per-downstream output queues. Workers push every output record to ALL downstream queues.

**Queue naming**: `{job_id}_{stage_id}_to_{downstream_id}`

**Output behavior**:
- Single downstream: `ack_and_forward` to one queue (atomic, today's path)
- Multiple downstreams: `push_batch` to each queue, then `ack` upstream

**Atomicity trade-off**: broadcast cannot use single-queue `ack_and_forward`. Push all first, then ack. If crash between push and ack → upstream redelivers (at-least-once). Downstream dedup handles duplicates.

### 2. Hash Partition Routing (Shuffle)

When a downstream stage has a `JoinConfig`, upstream workers hash-partition their output into per-partition queues. This ensures records with the same join key end up on the same join worker.

**Queue naming**: `{job_id}_{stage_id}_p{partition_id}`

**Partition function** (deterministic, cross-process stable):
```python
import zlib

def compute_partition(key_values: tuple, num_partitions: int) -> int:
    key_bytes = "|".join(str(v) for v in key_values).encode("utf-8")
    return zlib.crc32(key_bytes) % num_partitions
```

**Worker behavior**: partition the output Arrow table by hash, push each sub-table to its partition queue, then ack upstream.

### 3. Per-Partition Build-Probe Hash Join

Each join worker is assigned a subset of partitions. Processes **one partition at a time** using the standard database Build-Probe Hash Join:

```
For each assigned partition P:

  Phase 1 — Build:
    Claim ALL records from build-side partition queue (e.g. caption_a_pP)
    Hold claims (don't ack), build hash table in memory

  Phase 2 — Probe:
    Claim records from probe-side partition queue (e.g. caption_b_pP)
    For each record:
      Probe hash table → output matches (hold claims)
    When probe-side queue is drained:
      INNER join: discard unmatched build-side records
      LEFT/RIGHT/FULL: output unmatched with NULLs

  Phase 3 — Ack:
    Ack ALL records from both sides for partition P
    Free hash table memory

  Move to next partition
```

**Why per-partition ack**: partitions are independent. Ack after each partition completes. On crash, only the current partition needs to be retried.

## Queue Topology Example

```
source_to_caption_a ──> Caption_A workers ──> caption_a_p0 .. caption_a_p7
source_to_caption_b ──> Caption_B workers ──> caption_b_p0 .. caption_b_p7

Join Worker 0 (partitions 0,1):
  Process p0: build from caption_a_p0, probe from caption_b_p0, ack, done
  Process p1: build from caption_a_p1, probe from caption_b_p1, ack, done

Join Worker 1 (partitions 2,3):
  Process p2: build from caption_a_p2, probe from caption_b_p2, ack, done
  Process p3: build from caption_a_p3, probe from caption_b_p3, ack, done
...
```

## Fault Tolerance

Same as OLAP databases: **retry on crash**, no incremental checkpoint.

- **Crash during build phase**: claims expire → records back to queue → new worker retries partition from scratch
- **Crash during probe phase**: same — both build and probe claims expire → retry partition
- **Already-acked partitions**: safe, not affected by crash
- **Duplicates in downstream**: possible on retry, handled by existing downstream dedup

**claim_timeout** only needs to cover one partition's processing time (not the entire pipeline). Typically seconds to minutes.

## Filter Handling

Build side is fully read before probe starts. If one upstream filters out records, those keys simply aren't in the hash table. No special logic needed:

```
Build side (caption_a): {1, 2, 3, 4, 5}  → Hash Table
Probe side (caption_b): {1, 3, 5}         (filtered out 2, 4)

Probe result: matched = {1, 3, 5}
Build side unmatched = {2, 4} → INNER: discard; RIGHT: output with NULLs
```

## Scalability

- **Join parallelism**: scales linearly with workers (more workers = more partitions processed in parallel)
- **Memory**: only one partition's build-side hash table in memory at a time
- **num_partitions >= max_parallelism**: set higher for load balancing headroom
- **Skew**: more partitions distributes hot keys better

## Data Model Changes

### JoinConfig

```python
@dataclass(frozen=True)
class JoinConfig:
    join_keys: list[str]         # Column(s) to join on
    join_type: str = "inner"     # inner, left, right, full
    num_partitions: int = 8      # Hash partitions for distributed join
```

### StageRuntime

```python
@dataclass(frozen=True)
class StageRuntime:
    broker_endpoint: Optional[QueueEndpoint] = None
    upstream_queue_names: list[str] = field(default_factory=list)  # Was: upstream_queue_name
    claim_timeout_secs: float = 60.0
```

### WorkerRuntime

```python
@dataclass(frozen=True)
class WorkerRuntime:
    worker_id: str
    job_id: str
    stage_id: str
    broker_endpoint: Optional[QueueEndpoint] = None
    upstream_queue_names: list[str] = field(default_factory=list)
    output_queue_names: list[str] = field(default_factory=list)
    output_partition_keys: Optional[list[str]] = None  # For hash-route output
    join_config: Optional[JoinConfig] = None
    upstream_partition_queues: Optional[dict[str, list[str]]] = None
    batch_size: int = 100
    claim_timeout_secs: float = 60.0
```

## Files Changed

| File | Change |
|------|--------|
| `engine/_internal/core/stage.py` | `JoinConfig`, `StageRuntime.upstream_queue_names` |
| `engine/_internal/core/stage_worker.py` | Fan-out output, partition routing, join loop |
| `engine/_internal/core/stage_master.py` | Fan-out/partition queue creation, multi-queue completion |
| `engine/_internal/runtime/ray_runner.py` | Topology wiring, partition assignment |
| `engine/_internal/core/managers/worker_manager.py` | Per-worker partition assignment |
| `engine/_internal/core/partition.py` (new) | `partition_table()`, `compute_partition()` |
| `engine/tests/test_multi_upstream_join.py` (new) | Diamond workflow test |
