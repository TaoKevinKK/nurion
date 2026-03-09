# Lesson: Hash Partition Split Cannot Solve Key Frequency Skew

## Context

While designing dynamic partition support for Nurion's shuffle mechanism, we
studied Silo (gadget-inc/silo) — a Rust job queue built on SlateDB that uses
range-based shard splitting to redistribute load at runtime.

The initial proposal was to add a `SplitPartition` RPC to WorkQueue, modeled on
Silo's `ShardSplitter` state machine (Requested → Pausing → Cloning → Complete).

## What we got wrong

We assumed that shard/partition splitting is a universal solution to data skew.
It is not. **The effectiveness of splitting depends entirely on the routing
strategy.**

### Range-based routing (Silo): split works

```
Partition 3 owns key range ["f", "k")
Split → Partition 3a ["f", "h") + Partition 3b ["h", "k")
→ user_frank → 3a, user_henry → 3b
→ Load is divided regardless of individual key frequency ✓
```

Each key is a distinct point in the range. Splitting the range always separates
different keys into different partitions.

### Hash-based routing (Nurion): split does NOT work for hot keys

```
hash("user_X") % 8 = 5  →  ALL 10M rows go to partition 5
Split partition 5 into two...
hash("user_X") % 9 = ?  →  ALL 10M rows go to ONE partition (still)
```

Hashing is deterministic per key. No matter how many times you split, all rows
for the same key produce the same hash. The hot key cannot be divided by changing
the number of partitions.

Split only helps **hash collision skew** (different keys accidentally landing on
the same partition), which is rare and already solvable by configuring more
partitions upfront.

## Root cause of the mistake

Silo and Nurion use fundamentally different routing strategies:

| Property | Silo (range) | Nurion (hash) |
|---|---|---|
| Key → partition mapping | Lexicographic interval | `hash(key) % N` |
| Split guarantee | Different keys always separate | Same key stays together |
| Hot-key divisible? | Yes (sub-range) | **No** (hash is deterministic) |
| Primary use case | Multi-tenant isolation | Shuffle for aggregation/join |

We applied a range-partition solution to a hash-partition system without
recognizing this fundamental incompatibility.

## Correct approach: layered skew handling

Different skew types require different solutions at different layers:

| Skew type | Solution | Layer |
|---|---|---|
| Hash collision (rare) | More partitions at config time | Configuration |
| Key frequency, no affinity needed | Work-stealing in ClaimFromGroup | WorkQueue (Rust) |
| Key frequency, affinity required | Salted two-phase aggregation | Pipeline DAG |
| Temporal burst | Backpressure + autoscaler | Runtime (existing) |

Work-stealing (idle workers claim from hot partitions) is the natural queue-layer
solution for hash-partitioned systems. It doesn't change the partition structure —
it relaxes the worker-partition binding when the operator's semantics allow it.

For operators that **require** key affinity (GroupBy, Join), the only solution is
at the DAG layer: salt the key to scatter rows across partitions, then add a
reduce stage to combine partial results. This is Spark AQE's approach.

## Rules derived

1. **Match the skew strategy to the routing strategy.** Split works for range
   routing. Work-stealing works for hash routing. Don't cross-apply.

2. **Skew handling must be operator-aware.** Whether work-stealing is safe depends
   on whether the operator requires key affinity. This decision belongs in
   `OperatorConfig`, not in the queue layer.

3. **Don't assume analogies transfer across routing models.** Silo's design is
   excellent for its use case (multi-tenant job queue with range routing). The
   split state machine, point-of-no-return error classification, and traffic
   pausing are all sound. But the core mechanism only works because range routing
   guarantees that splitting separates keys.

## What IS worth borrowing from Silo

Even though partition split doesn't apply, several Silo design elements are
valuable for other Nurion features:

- **QueueGroup as a first-class concept**: Silo manages shards as a coordinated
  set (ShardMap). Nurion should do the same for partition queues (QueueGroup).
- **Split state machine for future range partitions**: If Nurion adds range-based
  partitioning (e.g., time-range for streaming sources), Silo's 4-phase state
  machine with error classification is the design to follow.
- **Submodule organization**: Silo splits `JobStoreShard` into 10+ single-purpose
  files (enqueue.rs, dequeue.rs, lease.rs, etc.). Nurion's `stage_worker.py`
  would benefit from similar decomposition.
- **Deterministic simulation testing (DST)**: Silo uses Turmoil for network fault
  injection. Nurion's existing DST framework (PR #60) can adopt similar patterns
  for testing partition operations under failure.
