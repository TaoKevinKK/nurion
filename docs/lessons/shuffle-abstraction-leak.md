# Lesson: Shuffle Abstraction Leak into Core

## What happened

`StageMaster` and `StageWorker` (in `_internal/core/`) directly imported
`ShuffleOperatorConfig`, `ShuffleOperator`, and `split_by_partition()` from
`_internal/operators/shuffle`. This created an upward dependency from the core
layer to the operator layer, violating the architectural invariant that core
must remain operator-agnostic.

### Symptoms

- `stage_master.py` used `isinstance(config, ShuffleOperatorConfig)` to decide
  whether to create partition queues.
- `stage_worker.py` imported `ShuffleOperator.PARTITION_COLUMN` and
  `split_by_partition()` to route output rows to partition queues.
- Adding any new operator that needed partitioned output would have required
  further edits to core files.

## Root cause

The shuffle feature was implemented as a quick prototype that hardcoded the
operator type check instead of extending the `OperatorConfig` protocol. The
core layer should never know about specific operators — it should only interact
with them through the `OperatorConfig` hook methods.

## Fix

1. Added two hooks to `OperatorConfig`:
   - `get_output_partition_count() -> int` (0 = no partitioning)
   - `get_partition_column() -> str` (column name for routing)

2. `ShuffleOperatorConfig` overrides both hooks.

3. Created `core/partition.py` with a generic `split_table_by_column()` that
   has no operator dependency.

4. Cleaned `stage_master.py` and `stage_worker.py` to use the hooks and the
   generic partition utility, removing all `from _internal.operators` imports.

## Rule (Key Invariant #6)

> **Core never imports operators** — `_internal/core/` must not reference
> specific operator types from `_internal/operators/`. Behavior differences are
> expressed through `OperatorConfig` hooks.

## Verification

```bash
# Must return zero matches:
grep -r "from _internal.operators" engine/_internal/core/
```
