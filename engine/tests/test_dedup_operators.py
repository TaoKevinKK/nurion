# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for new dedup operators (encoder, filter) and UFShard."""

import pyarrow as pa
import pytest

from tests.conftest import make_operator_runtime
from _internal.core.models import Split, SplitPayload
from _internal.operators.dedup.encoder import (
    MinHashEncoderConfig,
    _tokenize_ngrams,
    _xxhash64,
)
from _internal.operators.dedup.filter import DedupFilterOperatorConfig
from _internal.serve.union_find.shard import UFShard


# =============================================================================
# MinHash Encoder Tests
# =============================================================================


class TestTokenizeNgrams:
    """Tests for word-level n-gram tokenization."""

    def test_basic(self):
        result = _tokenize_ngrams("the quick brown fox jumps", 3)
        assert len(result) == 3
        assert result[0] == "the quick brown"
        assert result[1] == "quick brown fox"
        assert result[2] == "brown fox jumps"

    def test_short_text(self):
        result = _tokenize_ngrams("hello world", 5)
        assert len(result) == 1
        assert result[0] == "hello world"

    def test_empty(self):
        assert _tokenize_ngrams("", 5) == []

    def test_single_word(self):
        result = _tokenize_ngrams("hello", 5)
        assert len(result) == 1
        assert result[0] == "hello"

    def test_lowercasing(self):
        result = _tokenize_ngrams("The Quick Brown", 2)
        assert result[0] == "the quick"


class TestXxhash:
    """Tests for xxhash64 wrapper."""

    def test_deterministic(self):
        assert _xxhash64("hello") == _xxhash64("hello")

    def test_different_strings(self):
        assert _xxhash64("hello") != _xxhash64("world")


class TestMinHashEncoderOperator:
    """Tests for MinHashEncoderOperator."""

    @pytest.fixture
    def sample_split(self):
        return Split(split_id="test", stage_id="encoder", data_range={})

    def test_encode_basic(self, sample_split):
        """Test basic MinHash encoding."""
        table = pa.table(
            {
                "id": ["doc1", "doc2", "doc3"],
                "content": [
                    "the quick brown fox jumps over the lazy dog",
                    "the quick brown fox jumps over the lazy cat",
                    "a completely different document about something else entirely",
                ],
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = MinHashEncoderConfig(
            content_column="content",
            id_column="id",
            num_buckets=8,
            hashes_per_bucket=4,
            ngram_size=3,
            num_partitions=4,
        )
        op = config.setup(make_operator_runtime())

        result = op.process_split(sample_split, payload)
        assert result is not None
        result_table = result.to_table()

        # Should have 3 docs * 8 buckets = 24 rows
        # (minus __target_partition column handling)
        assert "doc_id" in result_table.column_names
        assert "bucket_id" in result_table.column_names
        assert "band_hash" in result_table.column_names

        # No signature column! This is the key improvement.
        assert "signature" not in result_table.column_names

        op.close()

    def test_encode_empty_content(self, sample_split):
        """Test handling of empty content."""
        table = pa.table(
            {
                "id": ["doc1", "doc2"],
                "content": ["some content here for testing", ""],
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = MinHashEncoderConfig(
            content_column="content",
            id_column="id",
            num_buckets=4,
            hashes_per_bucket=4,
            num_partitions=2,
        )
        op = config.setup(make_operator_runtime())

        result = op.process_split(sample_split, payload)
        assert result is not None
        result_table = result.to_table()

        # Only doc1 should produce output (4 buckets)
        doc_ids = result_table.column("doc_id").to_pylist()
        unique_docs = set(doc_ids)
        assert "doc1" in unique_docs
        assert "doc2" not in unique_docs

        op.close()

    def test_encode_deterministic(self, sample_split):
        """Test that encoding is deterministic across runs."""
        table = pa.table(
            {
                "id": ["doc1"],
                "content": ["the quick brown fox jumps over the lazy dog"],
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = MinHashEncoderConfig(
            content_column="content",
            id_column="id",
            num_buckets=4,
            hashes_per_bucket=4,
            seed=42,
            num_partitions=2,
        )

        op1 = config.setup(make_operator_runtime())
        result1 = op1.process_split(sample_split, payload)

        op2 = config.setup(make_operator_runtime())
        result2 = op2.process_split(sample_split, payload)

        hashes1 = result1.to_table().column("band_hash").to_pylist()
        hashes2 = result2.to_table().column("band_hash").to_pylist()
        assert hashes1 == hashes2

        op1.close()
        op2.close()

    def test_similar_docs_share_buckets(self, sample_split):
        """Test that similar documents share some band hashes."""
        # Use longer texts with high overlap for reliable detection
        base = (
            "the quick brown fox jumps over the lazy dog in the park "
            "and then runs around the big tree near the blue river "
            "before going home for a nice warm dinner with family"
        )
        # Only change one word -> very high Jaccard similarity
        variant = base.replace("dog", "cat")

        table = pa.table(
            {
                "id": ["doc1", "doc2"],
                "content": [base, variant],
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = MinHashEncoderConfig(
            content_column="content",
            id_column="id",
            num_buckets=14,
            hashes_per_bucket=4,  # Fewer hashes per bucket = more sensitive
            ngram_size=3,
            seed=1,
            num_partitions=4,
        )
        op = config.setup(make_operator_runtime())
        result = op.process_split(sample_split, payload)
        result_table = result.to_table()

        # Group band_hashes by doc_id and bucket_id
        doc1_hashes = {}
        doc2_hashes = {}
        for i in range(result_table.num_rows):
            doc_id = result_table.column("doc_id")[i].as_py()
            bucket_id = result_table.column("bucket_id")[i].as_py()
            band_hash = result_table.column("band_hash")[i].as_py()
            if doc_id == "doc1":
                doc1_hashes[bucket_id] = band_hash
            else:
                doc2_hashes[bucket_id] = band_hash

        # Similar docs should share at least some band hashes
        shared = sum(1 for bid in doc1_hashes if doc1_hashes[bid] == doc2_hashes.get(bid))
        assert shared > 0, "Similar docs should share at least one band hash"

        op.close()


# =============================================================================
# DedupFilter Tests
# =============================================================================


class TestDedupFilterOperator:
    """Tests for DedupFilterOperator."""

    @pytest.fixture
    def sample_split(self):
        return Split(split_id="test", stage_id="filter", data_range={})

    def test_filter_with_cluster_table(self, sample_split):
        """Test filtering using a pre-exported cluster table."""
        # Cluster table: doc1 is representative, doc2 is duplicate
        cluster_table = pa.table(
            {
                "doc_id": ["doc1", "doc2", "doc3"],
                "cluster_id": ["doc1", "doc1", "doc3"],  # doc2 -> doc1 cluster
            }
        )

        # Input documents
        table = pa.table(
            {
                "id": ["doc1", "doc2", "doc3"],
                "content": ["text1", "text2", "text3"],
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = DedupFilterOperatorConfig(
            id_column="id",
            cluster_table=cluster_table,
        )
        op = config.setup(make_operator_runtime())

        result = op.process_split(sample_split, payload)
        assert result is not None
        result_table = result.to_table()

        # Should keep doc1 (representative) and doc3, drop doc2
        assert result_table.num_rows == 2
        kept_ids = set(result_table.column("id").to_pylist())
        assert "doc1" in kept_ids
        assert "doc3" in kept_ids
        assert "doc2" not in kept_ids

    def test_filter_all_unique(self, sample_split):
        """Test when all docs are unique (no duplicates)."""
        cluster_table = pa.table(
            {
                "doc_id": ["doc1", "doc2", "doc3"],
                "cluster_id": ["doc1", "doc2", "doc3"],
            }
        )

        table = pa.table(
            {
                "id": ["doc1", "doc2", "doc3"],
                "content": ["a", "b", "c"],
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = DedupFilterOperatorConfig(
            id_column="id",
            cluster_table=cluster_table,
        )
        op = config.setup(make_operator_runtime())

        result = op.process_split(sample_split, payload)
        assert result is not None
        # All kept (same payload returned)
        assert result.to_table().num_rows == 3

    def test_filter_all_duplicates(self, sample_split):
        """Test when all docs in batch are duplicates."""
        cluster_table = pa.table(
            {
                "doc_id": ["doc1", "doc2", "doc3"],
                "cluster_id": ["doc0", "doc0", "doc0"],  # All point to doc0
            }
        )

        table = pa.table(
            {
                "id": ["doc1", "doc2", "doc3"],
                "content": ["a", "b", "c"],
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = DedupFilterOperatorConfig(
            id_column="id",
            cluster_table=cluster_table,
        )
        op = config.setup(make_operator_runtime())

        result = op.process_split(sample_split, payload)
        assert result is None  # All filtered out

    def test_filter_unknown_docs_pass_through(self, sample_split):
        """Test that docs not in cluster table pass through."""
        cluster_table = pa.table(
            {
                "doc_id": ["doc1"],
                "cluster_id": ["doc1"],
            }
        )

        table = pa.table(
            {
                "id": ["doc1", "doc_unknown"],
                "content": ["a", "b"],
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = DedupFilterOperatorConfig(
            id_column="id",
            cluster_table=cluster_table,
        )
        op = config.setup(make_operator_runtime())

        result = op.process_split(sample_split, payload)
        assert result is not None
        # Both kept: doc1 is representative, doc_unknown defaults to self
        assert result.to_table().num_rows == 2


# =============================================================================
# UFShard Tests (unit-level, no Ray)
# =============================================================================


class TestUFShard:
    """Tests for UFShard without Ray (direct instantiation)."""

    def test_basic_union(self):
        shard = UFShard(shard_id=0, num_shards=1)
        result = shard.batch_union([("a", "b"), ("b", "c")])
        assert result["local_unions"] == 2
        assert result["cross_shard"] == 0

    def test_batch_find(self):
        shard = UFShard(shard_id=0, num_shards=1)
        shard.batch_union([("a", "b"), ("c", "d")])
        results = shard.batch_find(["a", "b", "c", "d"])
        assert results[0] == results[1]
        assert results[2] == results[3]

    def test_cross_shard_detection(self):
        """Test that cross-shard edges are detected."""
        shard = UFShard(shard_id=0, num_shards=2)

        # Create pairs where one key hashes to shard 0, the other doesn't
        # We test by checking the result counts
        pairs = []
        for i in range(20):
            pairs.append((f"key_{i}", f"key_{i + 100}"))

        result = shard.batch_union(pairs)
        # Some should be local, some cross-shard
        total = result["local_unions"] + result["cross_shard"]
        assert total == 20

    def test_export_clusters(self):
        shard = UFShard(shard_id=0, num_shards=1)
        shard.batch_union([("a", "b"), ("c", "d")])

        table = shard.export_clusters()
        assert table.num_rows == 4
        assert "doc_id" in table.column_names
        assert "cluster_id" in table.column_names

    def test_get_status(self):
        shard = UFShard(shard_id=0, num_shards=1)
        shard.batch_union([("a", "b")])
        status = shard.get_status()
        assert status["shard_id"] == 0
        assert status["num_elements"] == 2
        assert status["total_unions"] == 1

    def test_checkpoint_restore(self):
        shard = UFShard(shard_id=0, num_shards=1)
        shard.batch_union([("a", "b"), ("c", "d"), ("b", "c")])

        # Checkpoint (returns dict of tables)
        tables = shard.checkpoint()
        assert tables["uf"].num_rows == 4

        # Create new shard and restore
        shard2 = UFShard(shard_id=0, num_shards=1)
        shard2.restore(tables)

        # Verify state
        results = shard2.batch_find(["a", "b", "c", "d"])
        assert results[0] == results[1] == results[2] == results[3]

    def test_clear(self):
        shard = UFShard(shard_id=0, num_shards=1)
        shard.batch_union([("a", "b")])
        shard.clear()
        status = shard.get_status()
        assert status["num_elements"] == 0

    def test_ping(self):
        shard = UFShard(shard_id=0, num_shards=1)
        assert shard.ping() is True

    def test_batch_match_and_union_same_hash(self):
        """Test that docs with the same band_hash get union'd."""
        shard = UFShard(shard_id=0, num_shards=1)
        # First doc registers band_hash=100
        # Second doc with same band_hash=100 gets union'd with first
        result = shard.batch_match_and_union(
            [
                (100, "doc_a"),
                (100, "doc_b"),
                (200, "doc_c"),
            ]
        )
        assert result["new_hashes"] == 2  # 100 and 200
        assert result["matches"] == 1  # doc_a & doc_b matched on hash 100

        # Verify they're in the same cluster
        roots = shard.batch_find(["doc_a", "doc_b", "doc_c"])
        assert roots[0] == roots[1]  # doc_a and doc_b same cluster
        assert roots[2] != roots[0]  # doc_c different cluster

    def test_batch_match_and_union_cross_batch(self):
        """Test cross-batch matching: docs from separate batches with same hash."""
        shard = UFShard(shard_id=0, num_shards=1)

        # Batch 1: register doc_a with hash 100
        r1 = shard.batch_match_and_union([(100, "doc_a"), (200, "doc_x")])
        assert r1["new_hashes"] == 2
        assert r1["matches"] == 0

        # Batch 2: doc_b arrives with same hash 100 -> matched with doc_a
        r2 = shard.batch_match_and_union([(100, "doc_b"), (300, "doc_y")])
        assert r2["new_hashes"] == 1  # 300 is new
        assert r2["matches"] == 1  # doc_b matched doc_a on hash 100

        # Verify cross-batch union worked
        roots = shard.batch_find(["doc_a", "doc_b"])
        assert roots[0] == roots[1]

    def test_batch_match_and_union_chain(self):
        """Test that transitive matching works across 3+ batches."""
        shard = UFShard(shard_id=0, num_shards=1)

        shard.batch_match_and_union([(100, "doc_a")])
        shard.batch_match_and_union([(100, "doc_b")])  # b matches a
        shard.batch_match_and_union([(100, "doc_c")])  # c matches a (first registered)

        roots = shard.batch_find(["doc_a", "doc_b", "doc_c"])
        assert roots[0] == roots[1] == roots[2]  # All in same cluster

    def test_batch_match_and_union_multiple_bands(self):
        """Test that docs matching on different bands get union'd transitively."""
        shard = UFShard(shard_id=0, num_shards=1)

        # doc_a and doc_b share band 100
        # doc_b and doc_c share band 200
        # => all three should be in the same cluster
        shard.batch_match_and_union(
            [
                (100, "doc_a"),
                (200, "doc_b"),
            ]
        )
        shard.batch_match_and_union(
            [
                (100, "doc_b"),  # matches doc_a on band 100
                (200, "doc_c"),  # matches doc_b on band 200
            ]
        )

        roots = shard.batch_find(["doc_a", "doc_b", "doc_c"])
        assert roots[0] == roots[1] == roots[2]

    def test_batch_match_idempotent(self):
        """Test that re-sending the same entries is idempotent (crash recovery)."""
        shard = UFShard(shard_id=0, num_shards=1)

        # First call
        shard.batch_match_and_union([(100, "doc_a"), (100, "doc_b")])
        roots_1 = shard.batch_find(["doc_a", "doc_b"])

        # Simulate re-delivery (same entries sent again)
        shard.batch_match_and_union([(100, "doc_a"), (100, "doc_b")])
        roots_2 = shard.batch_find(["doc_a", "doc_b"])

        # State should be identical
        assert roots_1 == roots_2
        assert shard.get_status()["num_components"] == 1

    def test_checkpoint_roundtrip_full_state(self):
        """Test that checkpoint/restore preserves all shard state."""
        shard = UFShard(shard_id=0, num_shards=1)

        # Build some state: UF + band_hash_index + cross_shard_edges
        shard.batch_match_and_union(
            [
                (100, "doc_a"),
                (100, "doc_b"),  # match
                (200, "doc_c"),
            ]
        )

        # Checkpoint
        tables = shard.checkpoint()
        assert "uf" in tables
        assert "band_index" in tables
        assert "cross_edges" in tables
        assert tables["uf"].num_rows == 3  # a, b, c
        assert tables["band_index"].num_rows == 2  # hash 100, 200

        # Restore into fresh shard
        shard2 = UFShard(shard_id=0, num_shards=1)
        shard2.restore(tables)

        # Verify UF state
        roots = shard2.batch_find(["doc_a", "doc_b", "doc_c"])
        assert roots[0] == roots[1]  # a,b still connected
        assert roots[2] != roots[0]  # c still separate

        # Verify band_hash_index: new doc with hash 100 should match
        r = shard2.batch_match_and_union([(100, "doc_d")])
        assert r["matches"] == 1  # doc_d matched existing hash 100

    def test_ops_since_checkpoint_counter(self):
        """Test that ops counter tracks operations and resets on checkpoint."""
        shard = UFShard(shard_id=0, num_shards=1)
        assert shard.get_ops_since_checkpoint() == 0

        shard.batch_match_and_union([(1, "a"), (2, "b")])
        assert shard.get_ops_since_checkpoint() == 2

        shard.checkpoint()
        assert shard.get_ops_since_checkpoint() == 0

    def test_clear_resets_all_state(self):
        """Test that clear resets everything."""
        shard = UFShard(shard_id=0, num_shards=1)
        shard.batch_match_and_union([(100, "doc_a")])
        shard.clear()
        assert shard.get_status()["checkpoint_count"] == 0
        assert shard.get_status()["band_hash_index_size"] == 0
        assert shard.get_status()["num_elements"] == 0
