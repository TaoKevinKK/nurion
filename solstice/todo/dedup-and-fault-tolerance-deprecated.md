# Deduplication & Fault Tolerance TODO (DEPRECATED)

> **DEPRECATED** - This document describes the old CC label propagation dedup design.
> The dedup system has been replaced by the Union-Find Service architecture.
> See `design-docs/minhash-dedup.md` and `todo/dedup.md` for the current design.
>
> _Deprecated: 2026-02-09_

Track implementation status of deduplication operators and fault tolerance features.

> **Last Updated**: 2026-01-21

---

## ✅ Completed

### Exactly-Once Semantics (NEW - 2026-01-21)

- [x] **SemanticGuarantee Enum** - `core/operator.py`
  - `AT_LEAST_ONCE` (default): No dedup overhead
  - `EXACTLY_ONCE`: Offset-based deduplication
- [x] **Offset-based Deduplication** - `core/operator.py`
  - `last_offset` tracking per partition
  - `is_duplicate(offset)`: Skip if `offset <= last_offset`
  - Atomic save of offset + business state to SlateDB
- [x] **Config Propagation Chain**
  - `JobConfig.semantic_guarantee` → `StageConfig` → `WorkerManager` → `StageWorker` → `Operator`
  - Fixed bug where config was never passed to workers
- [x] **Fault Injection Framework** - `testing/fault_injection.py`
  - `FaultInjector` class for testing failure scenarios
  - `check_fault()` hooks in critical paths
  - Count-based and probability-based failure triggers
- [x] **Integration Tests** - `tests/test_exactly_once_integration.py`
  - Config propagation tests
  - Fault injection tests
  - State recovery tests

See `design-docs/exactly-once-semantics.md` for detailed design.

### Shuffle Framework

- [x] **ShuffleOperator base class** - `operators/shuffle.py`
  - Computes `__target_partition` column
  - Split by partition utility function
  - ✅ Fixed: Removes existing `__target_partition` before adding (2025-01-12)
- [x] **RepartitionOperator** - Basic repartition by hash

### Deduplication Operators

- [x] **HashDedupeOperator** - Exact deduplication by key columns
  - Uses DuckDB for batch-level dedup
  - SlateDB for cross-batch state (partition-scoped)
  - Stateless design - no in-memory cache

### MinHash Operators

- [x] **MinHashComputeOperator** - Compute MinHash signatures
- [x] **CandidatePairOperator** - Generate candidate pairs from LSH bands
  - Stateless design

### Connected Components (Label Propagation)

- [x] **CCInitOperator** - Initialize labels from candidate pairs
- [x] **CCIterateOperator** - One iteration of label propagation
- [x] **CCMessageOperator** - Generate messages for next iteration
- [x] **DedupeByClusterOperator** - Keep one doc per cluster
- [x] **CCIterateMaster** - Self-contained iterative master
  - ⚠️ **Iteration NOT implemented** - currently runs single pass
  - See TODO below for full iteration implementation

### State Management

- [x] **PartitionStateStore protocol** - Synchronous interface
- [x] **SlateDBPartitionStateStore** - SlateDB-backed implementation
  - Per-partition isolation
  - Built-in fencing (single writer)
  - Synchronous API (no async wrappers)

### DuckDB Integration

- [x] **DuckDBEngine** - Vectorized operations
  - `hash_partition`, `aggregate`, `join`, `filter`, `dedupe`

---

## 🚧 In Progress

*None*

---

## ✅ Recently Completed (2026-01-13)

### CCIterateMaster Full Iteration - IMPLEMENTED

- [x] **StageWorker iteration methods**
  - `start_iteration(iteration, config)` - Prepare worker for new iteration
  - `output_final_labels()` - Output final results after convergence
  - `report_partition_changes(partition_id, count)` - Record changes for convergence
  - `complete_iteration()` - Report changes to master

- [x] **CCIterateOperator integration**
  - `set_change_reporter(reporter)` - Set callback for reporting changes
  - `start_iteration(iteration, config)` - Prepare for new iteration
  - Reports changes during `process_data()` via change reporter

- [x] **CCIterateMaster full iteration loop**
  - Implements convergence detection (changes < threshold or max iterations)
  - Notifies workers of new iterations
  - Waits for all partitions to report
  - Tracks iteration statistics

### Worker State Store Integration - IMPLEMENTED

- [x] **StageWorker state store support**
  - `set_state_store_config(path)` - Configure state store path
  - `_init_state_store()` - Initialize SlateDB for assigned partitions
  - `_update_state_store_partitions()` - Handle partition rebalance
  - `_close_state_store()` - Release all partitions on shutdown

- [x] **WorkerManager integration**
  - Extracts `state_store_path` from operator config
  - Passes state store path to workers after creation

---

## 📋 TODO

### High Priority

- [ ] **Full Pipeline Checkpoint Recovery**
  - Current status: Exactly-once within a run works via offset tracking
  - Cross-run recovery still needs work:
    - ❌ No checkpoint file saving during execution
    - ❌ Cross-stage offset coordination
  - Note: Operator-level state recovery via SlateDB now works
  
  **Options:**
  1. Implement checkpoint barriers (Flink-style)
  2. Rely on idempotent sinks + replay from source

### Medium Priority

- [ ] **Shuffle Integration in StageWorker**
  - Use `__target_partition` column to route payloads
  - Call `produce(partition=N)` on Tansu queue

- [ ] **Data Skew Detection**
  - Monitor partition sizes during shuffle
  - Alert on significant skew (> 10x difference)
  - Initial draft, refine with production experience

- [ ] **Payload GC**
  - Clean up orphaned payloads in Ray Object Store
  - Track references across stages
  - Initial draft, needs more design

### Low Priority

- [ ] **GroupBy Operator**
  - Build on shuffle framework
  - Support incremental aggregation

- [ ] **Join Operator**
  - Hash join with shuffle
  - Broadcast join for small tables
  - Co-partitioned join optimization

- [ ] **Vector Deduplication**
  - Similar to MinHash but with vector embeddings
  - FAISS or similar for ANN search

---

## 🔄 Design Changes

### 1. Stateless Operators ✅

**Original idea**: Operators maintain in-memory caches

**Current implementation**: 
- Operators are fully stateless
- All state in SlateDB (partition-scoped)
- Enables fault tolerance and elastic scaling

### 2. Synchronous State Store ✅

**Original idea**: Async interface for state store

**Current implementation**:
- Synchronous interface
- SlateDB is embedded, no need for async
- Simpler code in operators

### 3. Self-Contained Iterative Stages ✅

**Original idea**: RayJobRunner orchestrates iterations

**Current implementation**:
- `CCIterateMaster` handles iteration internally
- Each iterative stage is self-contained
- Supports multiple iterative groups in one pipeline

### 4. Single Checkpoint File ✅

**Original idea**: Keep checkpoint history

**Current implementation**:
- Only one `checkpoint.json` file
- Atomic overwrite on save
- Simpler, sufficient for recovery

---

## 📝 Notes

### Checkpoint Recovery Strategy (When Implemented)

```
1. Job starts
2. Load checkpoint from storage
3. For each stage:
   - Get partition offsets from checkpoint
   - Pass offsets to workers
4. Workers:
   - Acquire partition from SlateDB (fencing)
   - Seek queue consumer to offset
   - Resume processing
5. Periodic checkpoint:
   - Collect offsets from all workers
   - Save to checkpoint storage
```

### State vs Checkpoint Distinction

| Aspect | State (SlateDB) | Checkpoint (fsspec) |
|--------|-----------------|---------------------|
| Purpose | Runtime business data | Recovery metadata |
| Data | Seen keys, labels | Offsets, snapshot IDs |
| Access | Random read/write | Sequential write, rare read |
| Volume | High (millions of keys) | Low (KB-MB) |
| Location | Per-partition | Per-job |

---

## References

- Design Docs: `design-docs/`
  - `design-docs/exactly-once-semantics.md` - Exactly-once design
  - `design-docs/checkpoint-and-recovery.md` - Checkpoint design
- Operators: `solstice/operators/`
- State: `solstice/state/`
- Checkpoint: `solstice/checkpoint/`
- Testing: `solstice/testing/fault_injection.py` - Fault injection framework
- Tests: 
  - `tests/test_*_operator.py`
  - `tests/test_connected_components.py`
  - `tests/test_exactly_once_integration.py` - Exactly-once tests
