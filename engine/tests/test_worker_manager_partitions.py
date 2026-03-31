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

"""Tests for WorkerManager._assign_partition_ids().

Regression: upstream_num_partitions=1 was incorrectly treated as a shuffle
stage (only `<= 0` was checked), causing workers to get partition assignments
in non-shuffle pipelines.  Fixed to `<= 1`.
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock


from _internal.core.managers.worker_manager import WorkerManager
from _internal.core.stage_worker import OutputRouting


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class _FakeStageRuntime:
    upstream_num_partitions: int = 0
    broker_endpoint: object = None


@dataclass
class _FakeStage:
    stage_id: str = "test_stage"
    min_parallelism: int = 1
    max_parallelism: int = 4
    num_cpus: float = 1.0
    num_gpus: float = 0.0
    memory_mb: int = 0
    operator_config: object = None
    worker_ready_timeout_seconds: float = 5.0
    worker_spawn_retry_delay_seconds: float = 1.0
    batch_size: int = 100


def _make_worker_manager(
    upstream_num_partitions: int = 0,
    max_parallelism: int = 4,
) -> WorkerManager:
    """Create a WorkerManager with fake stage and runtime for partition tests."""
    stage = _FakeStage(max_parallelism=max_parallelism)
    runtime = _FakeStageRuntime(upstream_num_partitions=upstream_num_partitions)
    return WorkerManager(
        job_id="test_job",
        stage=stage,
        runtime=runtime,
        payload_store=MagicMock(),
        output=OutputRouting(),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAssignPartitionIds:
    """Tests for the partition assignment boundary conditions."""

    def test_zero_partitions_returns_none(self):
        """upstream_num_partitions=0 (source stage) → no partition assignment."""
        wm = _make_worker_manager(upstream_num_partitions=0)
        assert wm._assign_partition_ids(worker_index=0) is None
        assert wm._assign_partition_ids(worker_index=3) is None

    def test_one_partition_assigns_partition_zero(self):
        """upstream_num_partitions=1 → all workers get (0,).

        Single-partition QueueGroup: explicitly assign partition 0 so
        claim_from_group gets a concrete partition list (not None).
        """
        wm = _make_worker_manager(upstream_num_partitions=1)
        assert wm._assign_partition_ids(worker_index=0) == (0,)
        assert wm._assign_partition_ids(worker_index=1) == (0,)

    def test_multiple_partitions_assigned_round_robin(self):
        """upstream_num_partitions=8, max_parallelism=4 → 2 partitions per worker."""
        wm = _make_worker_manager(upstream_num_partitions=8, max_parallelism=4)

        p0 = wm._assign_partition_ids(worker_index=0)
        p1 = wm._assign_partition_ids(worker_index=1)
        p2 = wm._assign_partition_ids(worker_index=2)
        p3 = wm._assign_partition_ids(worker_index=3)

        assert p0 == (0, 4)
        assert p1 == (1, 5)
        assert p2 == (2, 6)
        assert p3 == (3, 7)

    def test_two_partitions(self):
        """upstream_num_partitions=2, max_parallelism=4 → each worker gets 0 or 1 partition."""
        wm = _make_worker_manager(upstream_num_partitions=2, max_parallelism=4)

        p0 = wm._assign_partition_ids(worker_index=0)
        p1 = wm._assign_partition_ids(worker_index=1)
        p2 = wm._assign_partition_ids(worker_index=2)
        p3 = wm._assign_partition_ids(worker_index=3)

        # Workers 0 and 1 each get one partition
        assert p0 == (0,)
        assert p1 == (1,)
        # Workers 2 and 3 have no partitions (more workers than partitions)
        assert p2 is None
        assert p3 is None

    def test_partitions_cover_all_indices(self):
        """All partition indices must be assigned to exactly one worker."""
        n_partitions = 12
        max_parallelism = 5
        wm = _make_worker_manager(
            upstream_num_partitions=n_partitions,
            max_parallelism=max_parallelism,
        )

        all_assigned = set()
        for worker_idx in range(max_parallelism):
            ids = wm._assign_partition_ids(worker_idx)
            if ids is not None:
                all_assigned.update(ids)

        assert all_assigned == set(range(n_partitions))
