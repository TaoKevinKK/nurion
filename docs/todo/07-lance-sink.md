# Lance Sink TODO

Track improvements to the Lance sink operator and committer.

> **Last Updated**: 2026-03-27
> **Scope**: `engine/_internal/operators/sinks/lance.py`, `engine/_internal/operators/sinks/lance_commit.py`
> **Design Context**: Lance 3.0 introduced `mem_wal` (MemTable + WAL) for streaming writes

---

## Completed

- [x] **Fix Arrow→pylist→Arrow roundtrip in `_build_table`** ✅ (2026-03-27)
  - `_build_table` was converting `SplitPayload.data` (already `pa.Table`) to Python dicts via `to_pylist()` then back via `from_pylist()`. Now operates directly on Arrow table using `drop_columns()`.

---

## TODO

### P1 — Lance 3.0 WAL for Streaming Sink Writes

- [ ] Replace two-phase fragment-commit architecture with Lance WAL direct writes

**Current architecture** (two-phase, ~300 lines in `lance_commit.py`):
```
StageWorker × N
  → process_split → write_fragments(data, path)     ← writes fragment files, NO version
  → ack_and_forward(fragment_metadata → commit_queue) ← atomic with upstream ack

SinkManager × 1 (background loop)
  → claim commit_queue → accumulate fragments
  → threshold(30s / 10 frags / 100K rows) → LanceDataset.commit(Append, fragments)
  → ack commit_queue messages
```

**Target architecture** (WAL, eliminates commit queue + committer):
```
StageWorker × N
  → process_split → region_writer.put(arrow_batch)  ← write to WAL (durable immediately)
  → ack upstream                                      ← simple ack

Lance internally: MemTable → flush to fragments → compaction
Each worker owns a Region (epoch fencing prevents conflicts)
```

**Benefits**:
- Simplify Lance sink code by ~50% (eliminate commit queue, SinkManager commit loop, version tracking)
- Multi-writer without coordination — each worker writes its own Region
- Built-in batching (MemTable accumulates until size/row threshold)
- Built-in compaction (Lance merges small fragments automatically)
- Lower write latency (WAL durable on write, async flush)

**Trade-offs**:
- Loses exactly-once guarantee — if worker crashes between WAL write and upstream ack, data may be duplicated on replay. Acceptable for offline batch (idempotent or post-dedup).
- Requires pylance 3.0+ with `mem_wal` Python bindings

**Blocker**: pylance 3.0.1 does not expose `mem_wal` Python bindings yet. The Rust crate `lance-3.0.0/src/dataset/mem_wal/` has the full implementation (29K lines, mature). Track upstream pylance releases for Python API availability.

**Implementation plan**:
1. Wait for pylance to expose `mem_wal` Python API (`DatasetMemWalExt.initialize_mem_wal()`, `mem_wal_writer()`)
2. Add `LanceSinkConfig.use_wal: bool = False` option
3. New `LanceWalSink` operator: each worker gets a `RegionWriter`, writes directly
4. Region assignment: use `worker_id` as region spec to partition writes
5. Keep existing fragment-based mode as default until WAL is battle-tested

**Lance 3.0 mem_wal key specs**:
- MemTable: lock-free append-only, bloom filter for staleness detection
- Configurable limits: max_memtable_size (256MB), max_memtable_rows (100K), max_unflushed_bytes (1GB backpressure)
- WAL: Arrow IPC serialization, bit-reversed file naming for S3 distribution
- Multi-writer: Region partitioning + epoch-based fencing (claim_epoch), one writer per region
- Read path: Scanner merges base table + all Region MemTables, with generation tracking for dedup

### P2 — Fragment Compaction Integration

- [ ] Add optional post-job compaction step for fragment consolidation
- **Problem**: Current sink produces many small fragments (one per split batch). Over time, read performance degrades due to fragment proliferation.
- **Solution**: After job completion, call `dataset.optimize.compact_files()` to merge small fragments
- **Scope**: `lance_commit.py` — add `compact_after_finalize: bool = False` to `LanceSinkConfig`
- **Note**: Lance 3.0 WAL includes automatic compaction, making this less relevant if WAL is adopted

### P2 — Upgrade pylance to 3.0.x

- [ ] Evaluate and upgrade pylance dependency from 2.0.1 to 3.0.x
- **New in 3.0**: mem_wal, `commit_batch` (multi-transaction commit), performance improvements
- **Risk**: Breaking API changes between major versions; need compatibility testing
- **Scope**: `pyproject.toml` dependency bump + integration test validation
