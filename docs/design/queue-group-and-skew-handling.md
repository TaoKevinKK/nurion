# QueueGroup: Partitioned Queue Abstraction and Skew Handling

_Design document — March 2026_

---

## Status

**Status**: IMPLEMENTED (Phases 1-5 complete)
**Author**: AI Assistant
**Created**: 2026-03-06

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Design Goals](#2-design-goals)
3. [QueueGroup Concept](#3-queuegroup-concept)
4. [New WorkQueue RPCs](#4-new-workqueue-rpcs)
5. [Skew Analysis and Handling](#5-skew-analysis-and-handling)
6. [Python-Side Simplification](#6-python-side-simplification)
7. [Migration Plan](#7-migration-plan)
8. [Alternatives Considered](#8-alternatives-considered)
9. [Open Questions](#9-open-questions)

---

## 1. Problem Statement

### 1.1 Partition Logic Leaks into Python

After PR #59 implemented shuffle partition routing, `StageMaster` and `StageWorker`
absorbed significant partition orchestration logic that belongs in the queue layer:

| Python code | What it does | Lines |
|---|---|---|
| `StageMaster._create_queue_client` | Loop-create N partition queues | ~10 |
| `StageWorker._run_partition_claim_loop` | Round-robin claim from N queues | ~50 |
| `StageWorker._shuffle_output_and_ack` | Split table → push to N queues → ack | ~90 |
| `StageMaster._has_unprocessed_messages` | Loop-check N queues for pending/claimed | ~15 |
| `StageMaster._poll_queue_completion` | Loop-poll N queues for finished+drained | ~30 |
| `WorkerManager._assign_partition_queues` | Static modulus assignment | ~15 |

**Total: ~210 lines of partition-aware code scattered across core files.**

This creates two problems:
1. **Dual code paths**: `_process_and_ack` branches on `partition_queue_names` —
   every change to output logic must be applied in both branches.
2. **Rigidity**: Partition count, queue names, and worker assignment are all static
   (fixed at stage start). No runtime adaptation.

### 1.2 No Skew Handling

When data is unevenly distributed across partition keys, some partition queues
accumulate orders of magnitude more messages than others. The current design has
no detection or mitigation mechanism. Workers assigned to cold partitions sit idle
while hot-partition workers are overloaded.

### 1.3 At-Least-Once Shuffle Output

`_shuffle_output_and_ack` uses push-then-ack (non-atomic across multiple queues).
If the worker crashes between pushing to partition queues and acking upstream, the
upstream messages are reprocessed, producing duplicates in downstream partitions.
This is acceptable (Spark has the same semantics), but we can do better.

---

## 2. Design Goals

| Goal | Priority |
|---|---|
| Reduce partition logic in StageMaster/StageWorker to near zero | P0 |
| Provide atomic ack + multi-partition push (exactly-once shuffle output) | P0 |
| Enable skew detection without Python-side queue scanning | P1 |
| Enable work-stealing for operators that don't require key affinity | P1 |
| Keep WorkQueue a generic queue — no payload inspection | P0 (constraint) |
| Maintain O(1) hot-path complexity for existing operations | P0 (constraint) |
| Leave room for future range-partition and dynamic split | P2 |

---

## 3. QueueGroup Concept

A **QueueGroup** is a named set of partition queues managed as a unit by the
WorkQueue broker. The broker stores group metadata alongside the individual queues:

```
group_meta:{group_name} → QueueGroupMeta {
    name: string,
    num_partitions: u32,
    version: u32,               // bumped on structural changes
    partition_queues: [string],  // derived names: "{group_name}_p{i}"
    created_at: f64,
}
```

### 3.1 Why a First-Class Concept?

The key insight: **Python currently maintains the "N queues are a group" relationship
in StageMaster state**. By moving this into WorkQueue, every operation that touches
"all partitions" becomes a single RPC instead of a Python loop.

### 3.2 Naming Convention

```
Group name:   {job_id}_{stage_id}
Queue names:  {job_id}_{stage_id}_p0, {job_id}_{stage_id}_p1, ...
```

These are derived deterministically from the group name, so the broker can
reconstruct them from `group_meta` without storing a separate list.

---

## 4. New WorkQueue RPCs

### 4.1 CreateQueueGroup

```protobuf
message CreateQueueGroupRequest {
  string group_name = 1;
  int32 num_partitions = 2;
}

message CreateQueueGroupResponse {
  repeated string queue_names = 1;
  int32 version = 2;
}
```

**Rust implementation**: Single transaction creates `group_meta` + N `queue_meta`
entries. Idempotent (returns existing group if names match).

**Replaces**: `StageMaster` loop over `create_queue()`.

### 4.2 AckAndScatter

Atomic ack upstream + push to multiple downstream partition queues.

```protobuf
message AckAndScatterRequest {
  // Upstream ack
  string upstream_queue = 1;
  repeated string upstream_msg_ids = 2;
  repeated string upstream_claim_tokens = 3;

  // Downstream scatter
  string group_name = 4;
  repeated PartitionPayload partitions = 5;

  // Worker identity
  string worker_id = 6;
  string lease_id = 7;

  // Atomic state updates
  string state_namespace = 8;
  map<string, bytes> state_puts = 9;
  repeated string state_deletes = 10;
}

message PartitionPayload {
  int32 partition_id = 1;
  repeated bytes payloads = 2;
}

message AckAndScatterResponse {
  bool success = 1;
  repeated string new_msg_ids = 2;
}
```

**Rust implementation**: Extends `ack_internal` pattern — single SlateDB transaction
that acks upstream messages, pushes to N downstream queues, and updates state.
Each downstream queue gets its own `push_seq` bump within the same transaction.

**Complexity**: O(M) where M = total messages across all partitions. No scans.

**Semantic upgrade**: From at-least-once (push-then-ack) to **exactly-once**
(ack + scatter in one transaction).

**Replaces**: `StageWorker._shuffle_output_and_ack` (~90 lines).

### 4.3 ClaimFromGroup

Worker claims from assigned partitions within a group. Broker chooses which
partition to serve from.

```protobuf
message ClaimFromGroupRequest {
  string group_name = 1;
  string worker_id = 2;
  string lease_id = 3;
  int32 batch_size = 4;
  int32 timeout_ms = 5;
  repeated int32 assigned_partitions = 6;

  // Skew handling
  bool allow_steal = 7;
  int64 steal_pending_threshold = 8;
}

message ClaimFromGroupResponse {
  repeated Message messages = 1;
  repeated string claim_tokens = 2;
  string source_queue = 3;
  int32 source_partition = 4;
  bool has_more = 5;
}
```

**Rust claim strategy** (in order):

1. Scan assigned partitions by descending pending count (serve hot partitions first)
2. Claim from the partition with highest pending count
3. If all assigned partitions empty AND `allow_steal=true`:
   - Scan all group partitions
   - Claim from any partition with `pending > steal_pending_threshold`
4. If nothing available, return empty (same timeout semantics as `Claim`)

**Complexity**: O(P) where P = number of assigned partitions (typically 1-4).
This is acceptable because P is small and the scan is over in-memory metadata
(`group_meta` + cached `QueueMeta`), not storage.

**Replaces**: `StageWorker._run_partition_claim_loop` (~50 lines) and unifies
with `_run_single_queue_claim_loop`.

### 4.4 IsGroupFinished

```protobuf
message IsGroupFinishedRequest {
  string group_name = 1;
}

message IsGroupFinishedResponse {
  bool all_finished = 1;
  bool all_drained = 2;
  bool safe_to_exit = 3;
  repeated PartitionStatus partitions = 4;
}

message PartitionStatus {
  int32 partition_id = 1;
  int64 pending_count = 2;
  int64 claimed_count = 3;
  bool finished = 4;
}
```

**Replaces**: `StageMaster._poll_queue_completion` loop and
`_has_unprocessed_messages` loop.

### 4.5 GetGroupStats

```protobuf
message GetGroupStatsRequest {
  string group_name = 1;
}

message GetGroupStatsResponse {
  repeated PartitionStats partitions = 1;
  int64 total_pending = 2;
  int64 total_claimed = 3;
  int64 max_partition_pending = 4;
  int64 median_partition_pending = 5;
  float skew_ratio = 6;               // max / median
  repeated int32 hot_partitions = 7;   // partitions where pending > 5x median
  int32 version = 8;
}

message PartitionStats {
  int32 partition_id = 1;
  int64 pending_count = 2;
  int64 claimed_count = 3;
  int64 total_pushed = 4;
  int64 total_acked = 5;
}
```

**Skew detection in Rust**: O(P) computation over partition metadata.
The `hot_partitions` field lets StageMaster react without doing any queue math.

**Replaces**: Python-side loop over `get_stats()` per partition.

### 4.6 MarkGroupFinished

```protobuf
message MarkGroupFinishedRequest {
  string group_name = 1;
}
```

Atomically marks all partition queues in the group as finished.

**Replaces**: `StageMaster` loop over `mark_queue_finished()`.

---

## 5. Skew Analysis and Handling

### 5.1 Types of Skew

| Type | Cause | Example | Frequency |
|---|---|---|---|
| Hash collision | Different keys hash to same partition | `hash(A) % 8 == hash(B) % 8` | Rare |
| Key frequency | One key has disproportionate data | `user_X` has 10M rows, others 100 | **Common** |
| Temporal burst | Sudden data spike for certain keys | Event storm | Occasional |

### 5.2 Why Dynamic Partition Split Does NOT Solve Key Frequency Skew

A shard/partition split strategy (as used by Silo, DynamoDB, etc.) works for
**range-based routing** where splitting a range guarantees load redistribution:

```
Range partition: ["f", "k") → split → ["f", "h") + ["h", "k")
  → Different keys ALWAYS go to different child partitions ✓
```

For **hash-based routing**, split is ineffective against key frequency skew:

```
Hash partition: hash(user_X) % 8 == 5
  → After split: hash(user_X) % 9 == ???
  → ALL rows for user_X still land on ONE partition ✗
  → The hot key is not divisible by hashing
```

Split only helps hash collision skew (different keys colliding), which is already
solvable by increasing `num_partitions` at configuration time.

### 5.3 Solution: Layered Skew Handling

Different operators have different key-affinity requirements. The skew strategy
must respect this:

| Operator type | Key affinity | Skew strategy | Layer |
|---|---|---|---|
| Map, Filter | None | Work-stealing via `ClaimFromGroup` | WorkQueue |
| Dedup (UFService) | Weak (service handles cross-shard) | Work-stealing | WorkQueue |
| Repartition | None | Work-stealing | WorkQueue |
| GroupBy (SUM, COUNT, AVG) | Strong, but pre-aggregable | Salted two-phase aggregation | DAG/Pipeline |
| Join (equi-join) | Strong, not splittable | Broadcast small side / skew join | DAG/Pipeline |

### 5.4 Work-Stealing (Queue Layer)

For operators that don't require key affinity (or have weak affinity), idle
workers can "steal" messages from hot partition queues.

**Mechanism**: Built into `ClaimFromGroup` (§4.3).

```
Normal:  Worker_7 claims from assigned partitions [7] → empty
Steal:   Worker_7 claims from partition 3 (highest pending) → gets work
```

**Configuration**: Controlled by `OperatorConfig`:

```python
@dataclass
class ShuffleOperatorConfig(OperatorConfig):
    partition_keys: List[str]
    num_partitions: int = 8
    allow_work_stealing: bool = False   # default: strict affinity

@dataclass
class RepartitionConfig(ShuffleOperatorConfig):
    allow_work_stealing: bool = True    # no affinity needed

@dataclass
class GroupByConfig(ShuffleOperatorConfig):
    allow_work_stealing: bool = False   # must preserve key grouping
```

**Why this works**: WorkQueue is already a competing-consumer model. Work-stealing
is a natural extension — it relaxes the partition-worker binding when semantics
allow it. No structural queue changes needed.

**Limitations**: Does not help when key affinity is required (GroupBy, Join).

### 5.5 Salted Two-Phase Aggregation (DAG Layer, Future)

For GroupBy with key affinity, the classic solution is salted sub-partitioning:

```
Phase 1 (salted shuffle):
  hash(user_X, salt=0) → partition 5 → local_sum = 3M
  hash(user_X, salt=1) → partition 2 → local_sum = 3.5M
  hash(user_X, salt=2) → partition 7 → local_sum = 3.5M

Phase 2 (unsalted reduce):
  hash(user_X) → partition 5 → global_sum = 10M
```

This is Spark AQE's approach. It operates at the pipeline DAG layer — the framework
automatically inserts an extra reduce stage. **Not in scope for this design** but
the QueueGroup abstraction supports it naturally (Phase 2 uses a non-partitioned
queue or a separate QueueGroup with fewer partitions).

---

## 6. Python-Side Simplification

### 6.1 StageWorker: Unified Claim Loop

Before (two loops, ~90 lines combined):

```python
async def _run_claim_loop(self):
    if self._runtime.assigned_partition_queue_names:
        await self._run_partition_claim_loop()     # 50 lines
    else:
        await self._run_single_queue_claim_loop()  # 40 lines
```

After (single loop):

```python
async def _run_claim_loop(self):
    while self._running:
        if self._output_group:
            resp = self.queue_client.claim_from_group(
                group_name=self._output_group,
                assigned_partitions=self._assigned_partitions,
                allow_steal=self._allow_steal,
                batch_size=self._batch_size,
                timeout_ms=1000,
            )
            records, source_queue = resp.messages, resp.source_queue
        else:
            records = self.queue_client.claim(self._upstream_queue, ...)
            source_queue = self._upstream_queue

        if records:
            await self._process_and_ack(records, source_queue=source_queue)
        elif self._should_exit():
            break
        else:
            await asyncio.sleep(0.05)
```

### 6.2 StageWorker: Unified Output Path

Before (branching output, ~120 lines combined):

```python
if self._output.partition_queue_names and not isinstance(collected, RawOutputBytes):
    await self._shuffle_output_and_ack(...)     # 90 lines
else:
    output = await self._serialize_outputs(...)  # 30 lines
    self.queue_client.ack_and_forward(...)
```

After:

```python
if self._output_group and not isinstance(collected, RawOutputBytes):
    partition_map = split_table_by_column(result_table, partition_column)
    partition_payloads = [
        PartitionPayload(pid, self._serialize_partition(pid, tables))
        for pid, tables in partition_map.items()
    ]
    self.queue_client.ack_and_scatter(
        upstream_queue=source_queue,
        upstream_msg_ids=batch.msg_ids,
        group_name=self._output_group,
        partition_payloads=partition_payloads,
        state_puts=event_puts,
    )
else:
    output = await self._serialize_outputs(collected, split_id)
    self.queue_client.ack_and_forward(...)
```

Note: The branch still exists (scatter vs forward), but `_shuffle_output_and_ack`
with its 90-line loop-push-then-ack is replaced by a single atomic RPC call.

### 6.3 StageMaster: Elimination of Partition Loops

| Before | After |
|---|---|
| Loop `create_queue` × N | `create_queue_group(name, N)` |
| Loop `get_stats` × N | `get_group_stats(name)` |
| Loop `is_queue_finished` × N | `is_group_finished(name)` |
| Loop `mark_queue_finished` × N | `mark_group_finished(name)` |
| Compute initial workers for partition coverage | Unchanged (still in Python) |

### 6.4 OutputRouting Simplification

Before:

```python
@dataclass(frozen=True)
class OutputRouting:
    queue_name: Optional[str] = None
    partition_queue_names: Optional[tuple[str, ...]] = None
    partition_column: Optional[str] = None
```

After:

```python
@dataclass(frozen=True)
class OutputRouting:
    queue_name: Optional[str] = None
    group_name: Optional[str] = None         # replaces partition_queue_names
    partition_column: Optional[str] = None
    allow_work_stealing: bool = False
```

`partition_queue_names` (a tuple of N strings) is replaced by `group_name`
(a single string). The broker derives queue names internally.

---

## 7. Migration Plan

### Phase 1: Rust Infrastructure (no Python behavior change)

1. Add `QueueGroupMeta` to `storage.rs`
2. Implement `CreateQueueGroup` and `MarkGroupFinished` RPCs
3. Implement `IsGroupFinished` and `GetGroupStats` RPCs
4. Add proto definitions and Python client wrappers
5. **Test**: Unit tests in Rust (DST) + Python client tests

### Phase 2: AckAndScatter (highest value)

1. Implement `ack_and_scatter_internal` in `storage.rs`
   - Extends `ack_internal` pattern: single transaction, multiple downstream queues
2. Add `AckAndScatter` RPC in `service.rs`
3. Add Python client wrapper
4. Migrate `StageWorker._shuffle_output_and_ack` → `ack_and_scatter`
5. **Test**: Existing shuffle tests must pass with new path
6. **Semantic upgrade**: at-least-once → exactly-once for shuffle output

### Phase 3: ClaimFromGroup + Unified Claim Loop

1. Implement `ClaimFromGroup` in `storage.rs` / `service.rs`
   - Claim strategy: highest-pending-first among assigned partitions
   - Work-stealing: when assigned empty + `allow_steal`, scan group
2. Add Python client wrapper
3. Merge `_run_single_queue_claim_loop` and `_run_partition_claim_loop`
4. Add `allow_work_stealing` to `OperatorConfig`
5. **Test**: Shuffle + non-shuffle stages work through unified loop

### Phase 4: StageMaster Simplification

1. Replace loop-create with `create_queue_group`
2. Replace loop-poll with `is_group_finished`
3. Replace loop-stats with `get_group_stats`
4. Use `hot_partitions` from `GetGroupStats` for observability (WebUI)
5. **Test**: End-to-end shuffle workflow tests

### Phase 5: Cleanup

1. Delete `shuffle.py:split_by_partition()` (unused, replaced by `partition.py`)
2. Simplify `OutputRouting` to use `group_name`
3. Remove `partition_queue_names` from `WorkerRuntime` and `StageRuntime`
4. Update `architecture.md`, `workqueue.md`, TODO files

---

## 8. Alternatives Considered

### 8.1 Dynamic Partition Split (Silo-style)

**Approach**: Split hot partitions at runtime by creating new queues and
updating a `PartitionMap` (inspired by Silo's `ShardSplitter`).

**Why rejected**: Silo uses range-based routing where split always separates
different keys. Nurion uses hash-based routing where `hash(key)` is deterministic —
splitting a partition does not redistribute rows for the same hot key. Only
effective against hash collision skew, which is already solvable by configuring
more partitions upfront.

**Future consideration**: If range-partitioning is added (e.g., time-range
partitions for streaming sources), Silo's split state machine
(Requested → Pausing → Cloning → Complete) with point-of-no-return error
classification is a proven design to adopt.

### 8.2 OutputWriter Strategy Pattern (Python-only refactor)

**Approach**: Extract `SingleQueueWriter` and `PartitionedWriter` strategy
classes in Python to eliminate the `if partition_queue_names` branch.

**Why deferred**: Reduces branching but doesn't reduce the total partition logic
in Python. The N-queue loop in `PartitionedWriter.write` would still exist.
QueueGroup subsumes this by moving the loop into Rust. The strategy pattern
may still be useful for code organization but is not the primary solution.

### 8.3 ClaimSource Abstraction (Python-only refactor)

**Approach**: Extract `SingleQueueSource` and `RoundRobinPartitionSource`
to unify claim loops.

**Why subsumed**: `ClaimFromGroup` makes the Python-side claim source trivial —
one RPC call regardless of single queue or partition group. The abstraction
becomes a simple `if group_name` check rather than a full strategy hierarchy.

---

## 9. Open Questions

1. **Transaction size for AckAndScatter**: With 100 partitions and 1000 output
   rows, the transaction touches ~100 queue metas + 1000 message entries. Is this
   within SlateDB's transaction size budget? Need benchmarking.

2. **ClaimFromGroup lock contention**: Currently each queue has its own
   `claim_lock`. ClaimFromGroup needs to acquire locks for multiple queues
   (or use a group-level lock). Which approach minimizes contention?

3. **Work-stealing fairness**: If many workers steal from the same hot partition,
   the partition's assigned worker may starve. Should the broker prefer the
   assigned worker? (e.g., 80/20 split: 80% chance assigned worker wins claim)

4. **Backward compatibility**: Should non-group queues continue to work as-is?
   (Yes — QueueGroup is additive, existing single-queue operations unchanged.)

5. **Salted aggregation framework support**: When we implement salted two-phase
   GroupBy (§5.5), should it be an automatic framework optimization (like Spark AQE)
   or an explicit user configuration? Automatic is better UX but requires skew
   detection before the pipeline starts (or mid-pipeline re-planning).
