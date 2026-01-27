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

"""Unit tests for PartitionManager.

Tests cover:
- Partition count calculation
- Worker assignment (round-robin)
- Partition rebalancing
- Orphaned partition handling
"""

from unittest.mock import MagicMock

from solstice.core.managers.partition_manager import PartitionManager
from solstice.core.stage import StageRuntime
from solstice.core.operator import SemanticGuarantee
from solstice.queue import QueueType


def _make_mock_stage(
    max_parallelism: int = 4,
    output_partitions: int | None = None,
) -> MagicMock:
    """Create a mock Stage for testing."""
    stage = MagicMock()
    stage.max_parallelism = max_parallelism
    stage.output_partitions = output_partitions
    return stage


def _make_runtime(
    upstream_endpoint=None,
    upstream_topic=None,
) -> StageRuntime:
    """Create a StageRuntime for testing."""
    return StageRuntime(
        queue_type=QueueType.MEMORY,
        shared_broker_endpoint=None,
        upstream_endpoint=upstream_endpoint,
        upstream_topic=upstream_topic,
        state_endpoint=None,
        state_topic=None,
        semantic_guarantee=SemanticGuarantee.AT_LEAST_ONCE,
    )


class TestPartitionCountCalculation:
    """Tests for partition count calculation logic."""

    def test_single_worker_returns_one_partition(self):
        """Test that single worker scenario uses 1 partition."""
        stage = _make_mock_stage(max_parallelism=1, output_partitions=None)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        assert manager.partition_count == 1

    def test_explicit_partition_count(self):
        """Test that explicit partition_count is respected."""
        stage = _make_mock_stage(max_parallelism=4, output_partitions=8)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        assert manager.partition_count == 8

    def test_auto_partition_count_from_max_workers(self):
        """Test that partition count equals max_workers when auto."""
        stage = _make_mock_stage(max_parallelism=4, output_partitions=None)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        assert manager.partition_count == 4

    def test_partition_count_minimum_one(self):
        """Test that partition count is always at least 1."""
        stage = _make_mock_stage(max_parallelism=0, output_partitions=0)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        assert manager.partition_count >= 1


class TestPartitionCountEdgeCases:
    """Tests for edge cases in partition count calculation."""

    def test_partition_count_with_zero_max_workers(self):
        """Test partition count when max_workers is 0."""
        stage = _make_mock_stage(max_parallelism=0, output_partitions=None)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        # Should default to 1 (minimum)
        assert manager.partition_count == 1

    def test_partition_count_with_negative_value(self):
        """Test partition count with negative explicit value."""
        stage = _make_mock_stage(max_parallelism=4, output_partitions=-5)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        # Should be clamped to minimum 1
        assert manager.partition_count == 1

    def test_partition_count_large_value(self):
        """Test partition count with very large value."""
        stage = _make_mock_stage(max_parallelism=4, output_partitions=1000)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        # Should accept large value (no upper limit)
        assert manager.partition_count == 1000


class TestWorkerAssignment:
    """Tests for worker partition assignment."""

    def test_round_robin_assignment(self):
        """Test that partitions are assigned round-robin."""
        stage = _make_mock_stage(max_parallelism=3, output_partitions=6)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        # Assign 3 workers to 6 partitions
        p0 = manager.assign_worker("w0", 0, 3, 6)
        p1 = manager.assign_worker("w1", 1, 3, 6)
        p2 = manager.assign_worker("w2", 2, 3, 6)

        # Round-robin: w0->[0,3], w1->[1,4], w2->[2,5]
        assert p0 == [0, 3]
        assert p1 == [1, 4]
        assert p2 == [2, 5]

    def test_more_workers_than_partitions(self):
        """Test assignment when workers > partitions."""
        stage = _make_mock_stage(max_parallelism=4, output_partitions=2)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        # 4 workers, 2 partitions -> some workers get empty assignments
        p0 = manager.assign_worker("w0", 0, 4, 2)
        p1 = manager.assign_worker("w1", 1, 4, 2)
        p2 = manager.assign_worker("w2", 2, 4, 2)
        p3 = manager.assign_worker("w3", 3, 4, 2)

        assert p0 == [0]
        assert p1 == [1]
        assert p2 == []  # No partition for this worker
        assert p3 == []  # No partition for this worker

    def test_get_assignment(self):
        """Test getting assignment for a worker."""
        stage = _make_mock_stage(max_parallelism=2, output_partitions=4)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        manager.assign_worker("w0", 0, 2, 4)
        manager.assign_worker("w1", 1, 2, 4)

        assert manager.get_assignment("w0") == [0, 2]
        assert manager.get_assignment("w1") == [1, 3]
        assert manager.get_assignment("unknown") == []


class TestRebalancing:
    """Tests for partition rebalancing."""

    def test_rebalance_after_worker_removal(self):
        """Test rebalancing when a worker is removed."""
        stage = _make_mock_stage(max_parallelism=3, output_partitions=6)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        # Initial assignment
        manager.assign_worker("w0", 0, 3, 6)
        manager.assign_worker("w1", 1, 3, 6)
        manager.assign_worker("w2", 2, 3, 6)

        # Remove w1
        orphaned = manager.remove_worker("w1")
        assert orphaned == [1, 4]

        # Rebalance with remaining workers
        manager.rebalance(["w0", "w2"], 6)

        # Now 2 workers for 6 partitions: w0->[0,2,4], w2->[1,3,5]
        assert manager.get_assignment("w0") == [0, 2, 4]
        assert manager.get_assignment("w2") == [1, 3, 5]

    def test_collect_orphaned_partitions(self):
        """Test collecting orphaned partitions from multiple workers."""
        stage = _make_mock_stage(max_parallelism=3, output_partitions=6)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        manager.assign_worker("w0", 0, 3, 6)
        manager.assign_worker("w1", 1, 3, 6)
        manager.assign_worker("w2", 2, 3, 6)

        # Collect from w0 and w2
        orphaned = manager.collect_orphaned_partitions(["w0", "w2"])

        # Should get [0, 2, 3, 5] sorted
        assert orphaned == [0, 2, 3, 5]
        # w0 and w2 should be removed
        assert manager.get_assignment("w0") == []
        assert manager.get_assignment("w2") == []
        # w1 should still have its partitions
        assert manager.get_assignment("w1") == [1, 4]

    def test_assign_orphaned_partition(self):
        """Test assigning a single orphaned partition to a worker."""
        stage = _make_mock_stage(max_parallelism=2, output_partitions=4)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        # Initial assignment: w0->[0,2], w1->[1,3]
        manager.assign_worker("w0", 0, 2, 4)
        manager.assign_worker("w1", 1, 2, 4)

        # w1 crashes, remove it - partition 1 and 3 become orphaned
        orphaned = manager.remove_worker("w1")
        assert orphaned == [1, 3]

        # Assign orphaned partition 3 to w0
        result = manager.assign_orphaned_partition("w0", 3)

        assert result is True
        assert manager.get_assignment("w0") == [0, 2, 3]
        # w1 no longer has any partitions
        assert manager.get_assignment("w1") == []

    def test_cannot_assign_partition_to_multiple_workers(self):
        """Test that a partition cannot be assigned to multiple workers."""
        stage = _make_mock_stage(max_parallelism=2, output_partitions=4)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        # Assign partitions to w0 and w1
        manager.assign_worker("w0", 0, 2, 4)  # [0, 2]
        manager.assign_worker("w1", 1, 2, 4)  # [1, 3]

        # Try to assign partition 1 (already assigned to w1) to w0
        result = manager.assign_orphaned_partition("w0", 1)

        # Should fail - partition 1 is already assigned to w1
        assert result is False
        assert manager.get_assignment("w0") == [0, 2]  # Unchanged
        assert manager.get_assignment("w1") == [1, 3]  # Unchanged

    def test_validate_no_duplicate_assignments(self):
        """Test validation detects no duplicates after proper assignment."""
        stage = _make_mock_stage(max_parallelism=3, output_partitions=6)
        manager = PartitionManager(
            stage=stage,
            runtime=_make_runtime(),
        )

        manager.rebalance(["w0", "w1", "w2"], 6)

        # Should be valid
        assert manager.validate_no_duplicate_assignments() is True
