# MinHash Deduplication: Union-Find Service Architecture

## Status

**Status**: ✅ IMPLEMENTED
**Author**: AI Assistant
**Created**: 2026-02-09
**Supersedes**: Connected Components label propagation design (see `../todo/dedup-and-fault-tolerance-deprecated.md`)

### Implementation Status

| Component | Status | Location |
|-----------|--------|----------|
| UnionFind data structure | ✅ Done | `utils/union_find.py` |
| UFShard actor | ✅ Done | `serve/union_find/shard.py` |
| UFClient | ✅ Done | `serve/union_find/client.py` |
| UnionFindServiceManager | ✅ Done | `serve/union_find/manager.py` |
| MinHashEncoderOperator | ✅ Done | `operators/dedup/encoder.py` |
| BucketUnionOperator | ✅ Done | `operators/dedup/bucket_union.py` |
| DedupFilterOperator | ✅ Done | `operators/dedup/filter.py` |
| Workflow orchestrator | ✅ Done | `workflows/minhash_dedup.py` |
| Unit tests (48) | ✅ Done | `tests/test_union_find.py`, `tests/test_dedup_operators.py` |
| Workflow integration test | ✅ Done | `tests/test_minhash_dedup_workflow.py` |

---

## Problem Statement

The original MinHash dedup design used Connected Components (CC) label propagation
with O(n^2) candidate pair generation and multi-round iterative label propagation
through Anvil. At 10B+ document scale, this had several critical issues:

1. **Data amplification**: Each document expanded to 16 rows (one per band), each
   carrying the full 1KB signature. 10B docs = ~160TB flowing through the queue.
2. **O(n^2) candidate pairs**: Large buckets (100K+ docs sharing a band hash)
   required 5 billion pairwise comparisons.
3. **CC iteration not implemented**: The `recompute_labels` method passed empty
   data for iteration 2+, producing incorrect results for long-chain components.
4. **Edges stored as comma-separated strings**: Inefficient parsing/serialization
   in every iteration round.
5. **No cross-batch matching**: Without shuffle partition routing, documents from
   different source splits with the same band hash never met in the same batch.

---

## Design

### Core Insight

MinHash dedup is decomposed into three concerns:

1. **Encode**: Convert documents to MinHash signatures and band hashes
2. **Match**: Find documents with identical band hashes (LSH candidates)
3. **Cluster**: Group matched documents via Union-Find

The key architectural decision: **matching happens at the shard level, not at
the operator level**. The Union-Find Service maintains a persistent `band_hash
-> doc_id` index. When a new document arrives with a band hash already in the
index, it is immediately union'd with the existing document. This enables
cross-batch matching without shuffle partition routing.

### Architecture

```
                    ┌──────────────────────────┐
                    │ UnionFindServiceManager   │  Control plane
                    │  deploy() / shutdown()    │
                    └────────────┬─────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │ UFShard[] (Ray actors)     │  Data plane
                    │  band_hash_index: {h→doc}  │
                    │  uf: UnionFind             │
                    │  cross_shard_edges: [...]   │
                    └────────────▲─────────────┘
                                 │ Ray RPC
                    ┌────────────┴─────────────┐
                    │ UFClient                  │  Used by operators
                    │  batch_match_and_union()   │
                    │  batch_find()              │
                    └──────────────────────────┘
```

### Pipeline (3 stages)

```
[Pre-pipeline] Deploy UnionFindService (N shard actors)

Job 1: Union
  Source ──► MinHashEncoder ──► BucketUnionOperator
              │                       │
              │ (doc_id, bucket_id,   │ sends (band_hash, doc_id)
              │  band_hash)           │ to UFClient → UFShard
              │                       │ returns None (side-effect only)

[Orchestration] resolve_cross_shard() + export_clusters()

Job 2: Filter
  Source ──► DedupFilter ──► Sink
              │
              │ uses cluster_table to keep
              │ only representative docs
```

### How Cross-Batch Matching Works

The critical correctness property: documents from different source splits with
the same band hash must be identified as duplicates.

**Old design (broken)**: BucketUnionOperator groups by band_hash within each
batch. Documents from split 1 and split 2 never appear in the same batch, so
matching fails.

**New design (correct)**: Each UFShard maintains a `band_hash_index: dict[int, str]`.
When `batch_match_and_union([(hash, doc_id)])` is called:

1. If `hash` not in index: register `hash → doc_id` (new entry)
2. If `hash` already in index: union `doc_id` with `index[hash]` (match!)

This works regardless of batch boundaries because the index persists across
all `batch_match_and_union` calls for the lifetime of the shard.

### Sharding Strategy

Documents are routed to shards by `band_hash % num_shards`. This ensures:
- All entries with the same `band_hash` go to the same shard (required for matching)
- The `band_hash_index` is shard-local (no distributed locking)

When two documents with the same `band_hash` have `doc_id` values that hash
to different shards (cross-shard edge), the shard records the edge. After all
buckets are processed, `resolve_cross_shard()` merges these edges globally.

### Cross-Shard Resolution

After all bucket processing is complete:

1. Collect cross-shard edges from all shards
2. Resolve local roots for all involved doc_ids via `batch_find()`
3. Build a global UnionFind over the local roots
4. Broadcast resolution mappings back to each shard

This is a single-pass operation with data proportional to the number of
cross-shard edges (typically small compared to total documents).

---

## Key Files

| File | Purpose |
|------|---------|
| `utils/union_find.py` | Union-Find with path compression, rank, Arrow serialization |
| `serve/union_find/config.py` | `UFClusterConfig` (num_shards, checkpoint settings) |
| `serve/union_find/shard.py` | `UFShard` Ray actor with band_hash_index |
| `serve/union_find/client.py` | `UFClient` for routing operations to shards |
| `serve/union_find/manager.py` | `UnionFindServiceManager` lifecycle management |
| `operators/dedup/encoder.py` | MinHash signature with xxhash + numpy vectorization |
| `operators/dedup/bucket_union.py` | Sends (band_hash, doc_id) to UFService |
| `operators/dedup/filter.py` | Filters duplicates using cluster table |
| `workflows/minhash_dedup.py` | Full pipeline orchestration |

---

## Comparison with Previous Design

| Aspect | Old (CC Label Propagation) | New (Union-Find Service) |
|--------|---------------------------|-------------------------|
| Pipeline stages | 7 (source, minhash, candidates, cc_init, cc_iterate, dedupe_cluster, sink) | 3 (encode, bucket_union, filter) |
| Clustering algorithm | CC label propagation (O(n*k) iterations) | Union-Find (O(n * alpha(n)), one-pass) |
| Candidate generation | O(n^2) pairwise within bucket | O(n) chain union via band_hash_index |
| Cross-batch matching | Requires shuffle partition routing | Shard-side band_hash index |
| State management | Edges in payload (comma-separated strings) | Centralized in UFShard actors |
| Hash function | SHA-256 (cryptographic, slow) | xxhash64 (~50x faster) |
| Signature in shuffle | Full 1KB signature per band row | Only band_hash (8 bytes per row) |
| Fault tolerance | Operator OOM loses CC state | UFShard state independent of operators |
| Iteration correctness | recompute_labels was a no-op (TODO) | No iteration needed (Union-Find is one-pass) |

## Comparison with Datatrove

| Aspect | Datatrove | Solstice |
|--------|-----------|---------|
| Architecture | File-based sort-merge | Service-based (Ray actors) |
| Matching | Sorted signature files + heap merge | Shard-side band_hash index |
| Clustering | Single-process Union-Find over .dups files | Distributed Union-Find across shards |
| Cross-worker matching | Sort + merge across all worker files | Cross-shard resolution phase |
| Scaling model | SLURM / filesystem | Ray cluster + Anvil |
| Fault tolerance | Re-run from files | UFShard checkpoint + worker restart |

Both approaches achieve the same result: they find documents with identical band
hash signatures and cluster them via Union-Find. The key difference is the
matching mechanism: datatrove sorts and merge-joins files, while Solstice uses
a persistent in-memory index in long-lived Ray actors.

---

## Extensibility: Embedding Dedup

The Union-Find Service is designed to support future embedding-based dedup:

```
MinHash dedup:
  Encode(MinHash) → BucketUnion(UFService) → Filter

Embedding dedup (future):
  Encode(Embedding) → ANNSearch → PairUnion(UFService) → Filter
```

The UFService `batch_union()` and `batch_find()` methods are algorithm-agnostic.
Only the upstream stages (encoder + matcher) change; the clustering and filter
stages are fully reused.

---

## Checkpoint and Fault Tolerance

### Design Principles

1. **The shard is a pure state machine** -- it serializes/deserializes its own
   state but has zero knowledge of where checkpoints are stored. No broker
   endpoints, no queue clients, no storage details.
2. **The manager owns checkpoint orchestration** -- it decides when to checkpoint,
   calls shard RPCs to get state, and persists via `SplitPayloadStore`.
3. **PayloadStore is the checkpoint backend** -- not Anvil State API.
   State API is for small metadata (offsets, counters). UF checkpoint data
   can be GBs at billion-doc scale and needs a storage layer designed for
   large Arrow tables. PayloadStore (with future S3 backend) is the right fit.
4. **Operator-to-shard RPC is idempotent** -- if the operator crashes between
   calling `batch_match_and_union()` and acking the upstream message, the
   message is re-delivered and the same entries are sent again. Union-Find
   union is idempotent (re-unioning connected nodes is a no-op).

### Checkpoint Data Model

Each shard checkpoints three pieces of state:

| State | Content | Scale |
|-------|---------|-------|
| UnionFind | `(key, parent_key, rank)` | 1 row per doc in shard |
| Band hash index | `(band_hash, doc_id)` | 1 row per unique band_hash in shard |
| Cross-shard edges | `(doc_id_a, doc_id_b)` | Varies; typically small |

Each is an Arrow Table. The manager stores them as SplitPayloads with
deterministic keys:

```
uf_ckpt:{cluster_id}:{shard_id}:uf
uf_ckpt:{cluster_id}:{shard_id}:band_index
uf_ckpt:{cluster_id}:{shard_id}:cross_edges
```

### Checkpoint Flow

The manager passes a `PayloadStore` reference to each shard at deploy time.
Shards write checkpoints directly -- no data round-trips through the manager.

```
deploy(payload_store):
  shard = UFShard(..., payload_store=payload_store)
  # Shard auto-restores from PayloadStore in __init__

During processing:
  shard.batch_match_and_union(entries)
  # After checkpoint_interval ops, shard auto-writes:
  #   payload_store.store("uf_ckpt:cluster:0:uf", ...)
  #   payload_store.store("uf_ckpt:cluster:0:band_index", ...)
  #   payload_store.store("uf_ckpt:cluster:0:cross_edges", ...)

Manager can force checkpoint:
  manager.force_checkpoint()  # triggers shard.save_checkpoint() on all shards

Shutdown:
  shard.clear_checkpoint()  # shard deletes its own keys from PayloadStore
```

This is the same pattern as `StageWorker`: it receives a `PayloadStore` handle
and stores payloads directly without routing through the master.

### OOM Scenarios

| Component | OOM Impact | Recovery |
|-----------|-----------|----------|
| MinHashEncoder worker | Batch not encoded | Stateless; Anvil re-delivers message |
| BucketUnion worker | Batch not sent to UFService | Stateless; re-delivery. Shard state unaffected |
| UFShard actor | In-memory state lost | Ray restarts actor; manager restores from PayloadStore checkpoint |
| DedupFilter worker | Batch not filtered | Stateless; re-delivery |

Key design property: operator workers are stateless. All clustering state lives
in UFShard actors, which are independent of the pipeline workers. An operator
OOM does not lose any union results. A shard OOM loses state since the last
checkpoint, but re-delivered messages (from operators that haven't acked yet)
will re-apply the lost operations idempotently.
