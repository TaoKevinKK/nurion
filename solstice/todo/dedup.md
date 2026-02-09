# Deduplication TODO

Track implementation status of the Union-Find Service dedup architecture.

> **Last Updated**: 2026-02-09 (v2: checkpoint + shuffle routing)
> **Design Doc**: `design-docs/minhash-dedup.md`

---

## Completed

### Union-Find Service (2026-02-09)

- [x] **UnionFind data structure** - `utils/union_find.py`
  - Union by rank + path compression
  - Arrow Table serialization (checkpoint/restore via PayloadStore)
  - String key support, batch operations, merge
- [x] **UFShard actor** - `serve/union_find/shard.py`
  - Band hash index for cross-batch matching
  - Cross-shard edge tracking
  - Checkpoint/restore
- [x] **UFClient** - `serve/union_find/client.py`
  - Routes `batch_match_and_union()` by band_hash to correct shard
  - Routes `batch_find()` by doc_id to correct shard
- [x] **UnionFindServiceManager** - `serve/union_find/manager.py`
  - Deploy/shutdown lifecycle
  - Cross-shard resolution
  - Cluster export

### Operators (2026-02-09)

- [x] **MinHashEncoderOperator** - `operators/dedup/encoder.py`
  - xxhash64 (replaces SHA-256, ~50x faster)
  - numpy vectorized signature computation
  - No signature in output (only band_hash, ~25x less data)
- [x] **BucketUnionOperator** - `operators/dedup/bucket_union.py`
  - Sends (band_hash, doc_id) to UFService
  - Stateless (no local matching)
- [x] **DedupFilterOperator** - `operators/dedup/filter.py`
  - Cluster table lookup or UFClient lookup mode
  - Keeps only representative documents

### Workflow & Tests (2026-02-09)

- [x] **Workflow orchestrator** - `workflows/minhash_dedup.py`
  - Two-job pipeline: union job + filter job
  - `run_dedup_pipeline()` for full orchestration
- [x] **Unit tests** (57 tests) - `tests/test_union_find.py`, `tests/test_dedup_operators.py`
- [x] **Integration test** - `tests/test_minhash_dedup_workflow.py`
  - 10K document dataset with 80 ground-truth plagiary pairs
  - Validates recall >40% and precision 100%

### UFShard Checkpoint (2026-02-09)

- [x] **Checkpoint via PayloadStore** - `serve/union_find/shard.py`, `manager.py`
  - Shard receives PayloadStore handle at init, writes checkpoints directly
  - No data round-trip through manager (same pattern as StageWorker)
  - Auto-checkpoint every `checkpoint_interval` ops; auto-restore on startup
  - Three payloads per shard: `uf_ckpt:{cluster}:{shard}:{uf|band_index|cross_edges}`
  - Manager: `force_checkpoint()`, `clear_checkpoints` on shutdown
  - See `design-docs/minhash-dedup.md` "Checkpoint and Fault Tolerance"

---

## TODO

### High Priority

- [ ] **Shuffle partition routing**
  - `__target_partition` column produced by ShuffleOperator is not yet used for routing
  - Current dedup works without it (shard-side band_hash index handles cross-batch matching)
  - Proper shuffle routing needed for general shuffle operators (GroupBy, Join, etc.)
  - Design TBD: should be a first-class concept in the queue/runner layer, not in StageWorker

- [ ] **PayloadStore S3 backend**
  - Required for large-scale checkpoint persistence
  - Currently only Ray Object Store backend exists
  - Need: streaming read/write for large payloads, TTL/cleanup

### Medium Priority

- [ ] **Large bucket auto-splitting**
  - When a band_hash has >500K docs, secondary hash split
  - Prevents single-shard memory hotspot
  - Implement in `UFShard.batch_match_and_union()`

- [ ] **Embedding dedup**
  - Add `EmbeddingEncoderOperator` (model-based encoding)
  - Add ANN search stage (FAISS/ScaNN)
  - Reuse UFService `batch_union()` + DedupFilter

- [ ] **Benchmark: Solstice vs Datatrove**
  - Same dataset, same MinHash parameters
  - Compare: throughput, recall, precision, memory usage
  - Target: comparable quality, better scalability

### Low Priority

- [ ] **Text normalization**
  - Datatrove uses `simplify_text()` (lowercase, strip punctuation, normalize whitespace)
  - Current encoder uses basic `text.lower().split()`
  - Add configurable `TextNormConfig`

- [ ] **Word tokenizer**
  - Datatrove uses language-aware word tokenizers
  - Current encoder uses whitespace split
  - Add optional tokenizer support

- [ ] **Hash precision configuration**
  - Datatrove supports 32-bit and 64-bit hash precision
  - Current encoder uses 64-bit only
  - 32-bit may be sufficient and saves memory at 10B+ scale

---

## Deprecated

The following components were removed in the Union-Find Service redesign:

| Component | Old Location | Reason |
|-----------|-------------|--------|
| CandidatePairOperator | `operators/minhash/candidates.py` | O(n^2) pairwise comparison replaced by O(n) shard-side index |
| CCInitOperator | `operators/connected_components.py` | CC label propagation replaced by Union-Find |
| CCIterateOperator | `operators/connected_components.py` | Multi-round iteration replaced by one-pass Union-Find |
| CCMessageOperator | `operators/connected_components.py` | No message generation needed |
| DedupeByClusterOperator | `operators/connected_components.py` | Replaced by DedupFilterOperator |
| CCIterateMaster | `operators/cc_master.py` | No iterative master needed |
| Old workflow (v1) | `workflows/minhash_dedup.py` | 7-stage pipeline replaced by 3-stage |

See `todo/dedup-and-fault-tolerance-deprecated.md` for the old implementation status.
