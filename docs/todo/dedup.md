# Deduplication TODO

Track implementation status of the Union-Find Service dedup architecture.

> **Last Updated**: 2026-03-02
> **Design Doc**: `../design/minhash-dedup.md`

---

## Completed

### Union-Find Service (2026-02-09)

- [x] **UnionFind data structure** - `_internal/utils/union_find.py`
  - Union by rank + path compression
  - Arrow Table serialization (checkpoint/restore via PayloadStore)
  - String key support, batch operations, merge
- [x] **UFShard actor** - `_internal/serve/union_find/shard.py`
  - Band hash index for cross-batch matching
  - Cross-shard edge tracking
  - Checkpoint/restore
- [x] **UFClient** - `_internal/serve/union_find/client.py`
  - Routes `batch_match_and_union()` by band_hash to correct shard
  - Routes `batch_find()` by doc_id to correct shard
- [x] **UnionFindServiceManager** - `_internal/serve/union_find/manager.py`
  - Deploy/shutdown lifecycle
  - Cross-shard resolution
  - Cluster export

### Operators (2026-02-09)

- [x] **MinHashEncoderOperator** - `_internal/operators/dedup/encoder.py`
  - xxhash64 (replaces SHA-256, ~50x faster)
  - numpy vectorized signature computation
  - No signature in output (only band_hash, ~25x less data)
- [x] **BucketUnionOperator** - `_internal/operators/dedup/bucket_union.py`
  - Sends (band_hash, doc_id) to UFService
  - Stateless (no local matching)
- [x] **DedupFilterOperator** - `_internal/operators/dedup/filter.py`
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

- [x] **Checkpoint via PayloadStore** - `_internal/serve/union_find/shard.py`, `_internal/serve/union_find/manager.py`
  - Shard receives PayloadStore handle at init, writes checkpoints directly
  - No data round-trip through manager (same pattern as StageWorker)
  - Auto-checkpoint every `checkpoint_interval` ops; auto-restore on startup
  - Three payloads per shard: `uf_ckpt:{cluster}:{shard}:{uf|band_index|cross_edges}`
  - Manager: `force_checkpoint()`, `clear_checkpoints` on shutdown
  - See `../design/minhash-dedup.md` "Checkpoint and Fault Tolerance"

---

## TODO

### High Priority

- [ ] **Shuffle partition routing** ← _tracked in `roadmap.md` §1.1_
  - `__target_partition` column produced by ShuffleOperator is not yet used for routing
  - Current dedup works without it (shard-side band_hash index handles cross-batch matching)
  - Proper shuffle routing needed for general shuffle operators (GroupBy, Join, etc.)
  - Implementation: wire `split_by_partition()` into `StageWorker._serialize_outputs()`, create per-partition queues in `ray_runner.py`

- [ ] **PayloadStore S3 production hardening**
  - `FsspecSplitPayloadStore` already supports `s3://` URIs
  - Pending: large-payload throughput benchmark, recovery validation, TTL/cleanup policy
  - Align with `runtime-prod-hardening.md` durability/recovery items

### Medium Priority

- [ ] **Large bucket auto-splitting**
  - When a band_hash has >500K docs, secondary hash split
  - Prevents single-shard memory hotspot
  - Implement in `UFShard.batch_match_and_union()`

- [ ] **Embedding dedup**
  - Add `EmbeddingEncoderOperator` (model-based encoding)
  - Add ANN search stage (FAISS/ScaNN)
  - Reuse UFService `batch_union()` + DedupFilter

- [ ] **Benchmark: Nurion Runtime vs Datatrove**
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
| CandidatePairOperator | legacy minhash candidates module (removed) | O(n^2) pairwise comparison replaced by O(n) shard-side index |
| CCInitOperator | legacy connected-components module (removed) | CC label propagation replaced by Union-Find |
| CCIterateOperator | legacy connected-components module (removed) | Multi-round iteration replaced by one-pass Union-Find |
| CCMessageOperator | legacy connected-components module (removed) | No message generation needed |
| DedupeByClusterOperator | legacy connected-components module (removed) | Replaced by DedupFilterOperator |
| CCIterateMaster | legacy cc master module (removed) | No iterative master needed |
| Old workflow (v1) | `workflows/minhash_dedup.py` | 7-stage pipeline replaced by 3-stage |

See `dedup-and-fault-tolerance-deprecated.md` for the old implementation status.
