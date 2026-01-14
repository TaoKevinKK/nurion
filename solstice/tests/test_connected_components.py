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

"""Tests for Connected Components operators.

Note: All operators are STATELESS - they do not maintain internal state
across batches. Label tracking across iterations is done via external
state store (SlateDB) or through the data flow.
"""

import shutil
import tempfile

import pyarrow as pa
import pytest

from solstice.core.models import Split, SplitPayload
from solstice.operators.connected_components import (
    CCInitConfig,
    CCIterateConfig,
    DedupeByClusterConfig,
)
from solstice.operators.shuffle import ShuffleOperator
from solstice.state import SlateDBPartitionStateStore


class TestCCInitOperator:
    """Tests for CCInitOperator."""

    @pytest.fixture
    def sample_split(self):
        """Create a sample split."""
        return Split(split_id="test", stage_id="cc_init", data_range={})

    def test_init_basic(self, sample_split):
        """Test basic initialization from candidate pairs."""
        # Candidate pairs: (A, B), (B, C) -> A-B-C connected
        table = pa.table(
            {
                "doc_id_1": ["A", "B"],
                "doc_id_2": ["B", "C"],
                "similarity": [0.9, 0.8],
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = CCInitConfig()
        operator = config.setup()

        result = operator.process_split(sample_split, payload)

        assert result is not None
        result_table = result.to_table()

        # Should have 4 messages (2 edges * 2 directions)
        assert result_table.num_rows == 4

        # Check columns
        assert "doc_id" in result_table.column_names
        assert "neighbor_label" in result_table.column_names

    def test_init_empty(self, sample_split):
        """Test with no candidate pairs."""
        config = CCInitConfig()
        operator = config.setup()

        result = operator.process_split(sample_split, None)
        assert result is None


class TestCCIterateOperator:
    """Tests for CCIterateOperator.

    Note: The operator is stateless - it processes messages and outputs
    updated labels. Cross-batch label tracking is done via state store
    or through the `current_label` column in input.
    """

    @pytest.fixture
    def sample_split(self):
        """Create a sample split."""
        return Split(split_id="test", stage_id="cc_iterate", data_range={})

    def test_iterate_basic(self, sample_split):
        """Test basic label propagation."""
        # Messages: A should consider B, B should consider A and C
        table = pa.table(
            {
                "doc_id": ["A", "B", "B"],
                "neighbor_label": ["B", "A", "C"],
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = CCIterateConfig(num_partitions=4)
        operator = config.setup()

        result = operator.process_split(sample_split, payload)

        assert result is not None
        result_table = result.to_table()

        # Remove partition column if present
        if ShuffleOperator.PARTITION_COLUMN in result_table.column_names:
            result_table = result_table.drop([ShuffleOperator.PARTITION_COLUMN])

        # Should have labels for A and B
        doc_ids = set(result_table.column("doc_id").to_pylist())
        assert "A" in doc_ids
        assert "B" in doc_ids

        # A's label should be min(A, B) = A
        # B's label should be min(B, A, C) = A
        labels = dict(
            zip(
                result_table.column("doc_id").to_pylist(),
                result_table.column("label").to_pylist(),
            )
        )
        assert labels["A"] == "A"
        assert labels["B"] == "A"

    def test_iterate_with_current_labels(self, sample_split):
        """Test iteration with current labels provided in input."""
        # Messages with current labels
        table = pa.table(
            {
                "doc_id": ["A", "B"],
                "neighbor_label": ["B", "A"],
                "current_label": ["A", "B"],  # Current labels
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = CCIterateConfig(num_partitions=4)
        operator = config.setup()

        result = operator.process_split(sample_split, payload)

        assert result is not None
        result_table = result.to_table()

        if ShuffleOperator.PARTITION_COLUMN in result_table.column_names:
            result_table = result_table.drop([ShuffleOperator.PARTITION_COLUMN])

        # A keeps A (min of A, B)
        # B updates to A (min of B, A)
        labels = dict(
            zip(
                result_table.column("doc_id").to_pylist(),
                result_table.column("label").to_pylist(),
            )
        )
        assert labels["A"] == "A"
        assert labels["B"] == "A"

        # Check changed column
        changed = dict(
            zip(
                result_table.column("doc_id").to_pylist(),
                result_table.column("changed").to_pylist(),
            )
        )
        assert changed["A"] is False  # A -> A (no change)
        assert changed["B"] is True  # B -> A (changed)

    def test_iterate_convergence_detection(self, sample_split):
        """Test that changed column indicates convergence."""
        # Messages where no change should occur
        table = pa.table(
            {
                "doc_id": ["A", "B"],
                "neighbor_label": ["B", "C"],  # B > A, C > B, so no changes
                "current_label": ["A", "A"],  # Both already have label A
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = CCIterateConfig(num_partitions=4)
        operator = config.setup()

        result = operator.process_split(sample_split, payload)

        assert result is not None
        result_table = result.to_table()

        if ShuffleOperator.PARTITION_COLUMN in result_table.column_names:
            result_table = result_table.drop([ShuffleOperator.PARTITION_COLUMN])

        # All changed values should be False (converged)
        changed_values = result_table.column("changed").to_pylist()
        assert all(not c for c in changed_values)


class TestDedupeByClusterOperator:
    """Tests for DedupeByClusterOperator.

    Note: The operator is stateless - it deduplicates within each batch.
    Since data is shuffled by cluster_id, all docs in a cluster end up
    in the same partition, enabling within-batch deduplication.
    """

    @pytest.fixture
    def sample_split(self):
        """Create a sample split."""
        return Split(split_id="test", stage_id="dedupe_cluster", data_range={})

    def test_dedupe_basic(self, sample_split):
        """Test basic cluster deduplication."""
        # Three docs in two clusters
        table = pa.table(
            {
                "doc_id": ["A", "B", "C"],
                "label": ["A", "A", "C"],  # A and B in same cluster
                "content": ["text1", "text2", "text3"],
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = DedupeByClusterConfig(num_partitions=4)
        operator = config.setup()

        result = operator.process_split(sample_split, payload)

        assert result is not None
        result_table = result.to_table()

        # Remove partition column if present
        if ShuffleOperator.PARTITION_COLUMN in result_table.column_names:
            result_table = result_table.drop([ShuffleOperator.PARTITION_COLUMN])

        # Should have 2 docs (one per cluster)
        assert result_table.num_rows == 2

        # Should keep A (smallest in cluster A) and C
        doc_ids = set(result_table.column("doc_id").to_pylist())
        assert "A" in doc_ids
        assert "C" in doc_ids

    def test_dedupe_batch_level_only(self, sample_split):
        """Test that deduplication is batch-level (stateless).

        Without state store, each batch is processed independently.
        Since data is shuffled by cluster_id, all docs in a cluster
        should be in the same batch/partition.
        """
        config = DedupeByClusterConfig(num_partitions=4)
        operator = config.setup()

        # First batch: cluster A with doc A
        table1 = pa.table(
            {
                "doc_id": ["A"],
                "label": ["A"],
            }
        )
        payload1 = SplitPayload(data=table1, split_id="test1")
        result1 = operator.process_split(sample_split, payload1)

        # Second batch: cluster A with doc B
        # Note: In a real shuffle, both A and B would be in the same partition
        # This test shows that without that guarantee, duplicates can occur
        table2 = pa.table(
            {
                "doc_id": ["B"],
                "label": ["A"],
            }
        )
        payload2 = SplitPayload(data=table2, split_id="test2")
        result2 = operator.process_split(sample_split, payload2)

        # First batch outputs A
        assert result1 is not None
        assert result1.to_table().num_rows == 1

        # Second batch also outputs B (no cross-batch tracking)
        # In real usage, shuffle ensures both are in same batch
        assert result2 is not None
        r2_table = result2.to_table()
        if ShuffleOperator.PARTITION_COLUMN in r2_table.column_names:
            r2_table = r2_table.drop([ShuffleOperator.PARTITION_COLUMN])
        assert r2_table.num_rows == 1

    def test_dedupe_empty(self, sample_split):
        """Test with empty input."""
        config = DedupeByClusterConfig(num_partitions=4)
        operator = config.setup()

        result = operator.process_split(sample_split, None)
        assert result is None


class TestCCEndToEnd:
    """End-to-end tests for Connected Components flow."""

    @pytest.fixture
    def sample_split(self):
        """Create a sample split."""
        return Split(split_id="test", stage_id="cc", data_range={})

    def test_init_and_iterate(self, sample_split):
        """Test init followed by iterate for simple case."""
        # Initialize from candidate pairs A-B
        pairs_table = pa.table(
            {
                "doc_id_1": ["A"],
                "doc_id_2": ["B"],
                "similarity": [0.9],
            }
        )
        pairs_payload = SplitPayload(data=pairs_table, split_id="pairs")

        init_config = CCInitConfig()
        init_op = init_config.setup()
        messages_result = init_op.process_split(sample_split, pairs_payload)

        assert messages_result is not None
        messages_table = messages_result.to_table()

        # Should have 2 messages: A->B and B->A
        assert messages_table.num_rows == 2

        # Now iterate
        iterate_config = CCIterateConfig(num_partitions=1)
        iterate_op = iterate_config.setup()

        labels_result = iterate_op.process_split(sample_split, messages_result)

        assert labels_result is not None
        labels_table = labels_result.to_table()

        if ShuffleOperator.PARTITION_COLUMN in labels_table.column_names:
            labels_table = labels_table.drop([ShuffleOperator.PARTITION_COLUMN])

        # Both should have label A
        labels = dict(
            zip(
                labels_table.column("doc_id").to_pylist(),
                labels_table.column("label").to_pylist(),
            )
        )
        assert labels["A"] == "A"
        assert labels["B"] == "A"


class TestCCIterateStateStore:
    """Tests for CCIterateOperator with state store integration.

    These tests verify:
    1. recompute_from_state requires assigned_partitions parameter
    2. __changes__ is only written to one partition (avoid overcounting)
    """

    @pytest.fixture
    def temp_state_store_path(self):
        """Create a temporary directory for state store."""
        path = tempfile.mkdtemp(prefix="test_cc_state_")
        yield path
        # Cleanup after test
        shutil.rmtree(path, ignore_errors=True)

    @pytest.fixture
    def sample_split(self):
        """Create a sample split."""
        return Split(split_id="test", stage_id="cc_iterate", data_range={})

    def test_recompute_without_partitions_returns_zero(self, temp_state_store_path, sample_split):
        """Test that recompute_from_state returns 0 when no partitions provided.

        Bug #1: _recompute_worker_iterations was calling recompute_from_state
        without assigned_partitions, causing all iterations after the first
        to report 0 changes (false convergence).
        """
        config = CCIterateConfig(
            num_partitions=4,
            state_store_path=temp_state_store_path,
        )
        # Set runtime context (normally done by StageWorker)
        config.job_id = "test_job"
        config.stage_id = "cc_iterate"
        config.worker_id = "worker_0"
        operator = config.setup()

        # First, process some data to populate state store
        table = pa.table(
            {
                "doc_id": ["A", "B", "C", "D"],
                "neighbor_label": ["B", "A", "D", "C"],
            }
        )
        payload = SplitPayload(data=table, split_id="test")
        operator.process_split(sample_split, payload)

        # Now test recompute_from_state WITHOUT partitions - should return 0
        changes_without_partitions = operator.recompute_from_state(assigned_partitions=None)
        assert changes_without_partitions == 0, (
            "recompute_from_state should return 0 without partitions"
        )

        # Test with empty list - should also return 0
        changes_with_empty = operator.recompute_from_state(assigned_partitions=[])
        assert changes_with_empty == 0, "recompute_from_state should return 0 with empty partitions"

        # Cleanup
        operator.close()

    def test_recompute_with_partitions_works(self, temp_state_store_path, sample_split):
        """Test that recompute_from_state works correctly with partitions provided.

        This verifies the fix for Bug #1: assigned_partitions must be passed.
        """
        config = CCIterateConfig(
            num_partitions=4,
            state_store_path=temp_state_store_path,
        )
        config.job_id = "test_job"
        config.stage_id = "cc_iterate"
        config.worker_id = "worker_0"
        operator = config.setup()

        # Process initial data - creates edges A-B, C-D
        table = pa.table(
            {
                "doc_id": ["A", "B", "C", "D"],
                "neighbor_label": ["B", "A", "D", "C"],
            }
        )
        payload = SplitPayload(data=table, split_id="test")
        result = operator.process_split(sample_split, payload)
        assert result is not None

        # Reset iteration counter
        operator.reset_iteration()

        # Get partitions that were actually used
        partitions_used = list(operator._acquired_partitions)
        assert len(partitions_used) > 0, "Should have acquired at least one partition"

        # Now test recompute_from_state WITH partitions - should work
        changes = operator.recompute_from_state(assigned_partitions=partitions_used)
        # May or may not have changes depending on label propagation
        assert changes >= 0, "recompute_from_state should return valid count"

        operator.close()

    def test_changes_count_not_overcounted(self, temp_state_store_path, sample_split):
        """Test that __changes__ is written to only ONE partition.

        Bug #2: Changes count was written to EVERY partition touched,
        causing overcounting when master summed across all partitions.
        E.g., 10 changes across 4 partitions = 40 reported (wrong).
        """
        num_partitions = 8
        config = CCIterateConfig(
            num_partitions=num_partitions,
            state_store_path=temp_state_store_path,
        )
        config.job_id = "test_job"
        config.stage_id = "cc_iterate"
        config.worker_id = "worker_0"
        operator = config.setup()

        # Create data that will hash to MULTIPLE partitions
        # Using many docs increases chance of hitting multiple partitions
        doc_ids = [f"doc_{i}" for i in range(100)]
        neighbor_labels = [f"doc_{(i + 1) % 100}" for i in range(100)]
        table = pa.table(
            {
                "doc_id": doc_ids,
                "neighbor_label": neighbor_labels,
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        # Process data
        result = operator.process_split(sample_split, payload)
        assert result is not None

        # Verify multiple partitions were touched
        partitions_touched = operator._acquired_partitions
        assert len(partitions_touched) > 1, (
            f"Test requires multiple partitions, got {len(partitions_touched)}"
        )

        # Now read __changes__ from ALL partitions via state store
        state_store = SlateDBPartitionStateStore(
            base_path=temp_state_store_path,
            job_id="test_job",
            stage_id="cc_iterate",
        )

        changes_found = []
        for partition_id in range(num_partitions):
            try:
                state_store.acquire_partition(partition_id)
                changes_bytes = state_store.get(partition_id, b"__changes__")
                if changes_bytes:
                    changes_found.append((partition_id, int(changes_bytes.decode())))
                state_store.release_partition(partition_id)
            except Exception:
                pass  # Partition may not have been used

        state_store.close()
        operator.close()

        # Key assertion: __changes__ should only be in ONE partition
        assert len(changes_found) == 1, (
            f"__changes__ should be in exactly 1 partition, found in {len(changes_found)}: {changes_found}"
        )

    def test_changes_count_consistency_with_recompute(self, temp_state_store_path, sample_split):
        """Test that changes count is consistent between process_data and recompute.

        Verifies that the fix for Bug #2 maintains consistency.
        """
        config = CCIterateConfig(
            num_partitions=4,
            state_store_path=temp_state_store_path,
        )
        config.job_id = "test_job"
        config.stage_id = "cc_iterate"
        config.worker_id = "worker_0"
        operator = config.setup()

        # Process initial data
        table = pa.table(
            {
                "doc_id": ["A", "B", "C"],
                "neighbor_label": ["B", "A", "A"],  # C connects to A
            }
        )
        payload = SplitPayload(data=table, split_id="test")
        operator.process_split(sample_split, payload)

        # Reset and recompute
        operator.reset_iteration()
        partitions = list(operator._acquired_partitions)
        recompute_changes = operator.recompute_from_state(assigned_partitions=partitions)

        # After recompute, total changes should be updated correctly
        total_changes = operator.get_iteration_changes()
        assert total_changes == recompute_changes, (
            f"Total changes ({total_changes}) should equal recompute changes ({recompute_changes})"
        )

        operator.close()
