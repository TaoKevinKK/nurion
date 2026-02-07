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

"""End-to-end tests for v2 pipeline architecture.

Tests the complete flow:
- Source operator generating data
- Transform operators processing data
- Queue-based communication between stages
- Worker pull model
"""

import pytest
from dataclasses import dataclass
from typing import Dict, List, Optional

import pyarrow as pa

from solstice.core.job import Job
from solstice.core.stage import Stage
from solstice.core.operator import Operator, OperatorConfig, OperatorRuntime
from solstice.core.models import Split, SplitPayload
from solstice.runtime.ray_runner import RayJobRunner

pytestmark = pytest.mark.asyncio(loop_scope="function")


# ============================================================================
# Test Operators
# ============================================================================


class MockSourceOperator(Operator):
    """Source operator that generates test data."""

    def __init__(self, config: "MockSourceConfig", runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self._generated = 0

    def generate_splits(self) -> List[Split]:
        """Generate splits for the source."""
        splits = []
        num_batches = self.config.num_records // self.config.batch_size
        for i in range(num_batches):
            splits.append(
                Split(
                    split_id=f"split_{i}",
                    stage_id="source",
                    data_range={
                        "start": i * self.config.batch_size,
                        "end": (i + 1) * self.config.batch_size,
                    },
                )
            )
        return splits

    def process_split(
        self, split: Split, payload: Optional[SplitPayload]
    ) -> Optional[SplitPayload]:
        """Generate data for a split."""
        start = split.data_range["start"]
        end = split.data_range["end"]

        # Generate test data
        data = pa.table(
            {
                "id": list(range(start, end)),
                "value": [f"record_{i}" for i in range(start, end)],
            }
        )

        self._generated += end - start
        return SplitPayload(data=data, split_id=split.split_id)

    def close(self) -> None:
        pass


@dataclass
class MockSourceConfig(OperatorConfig):
    """Config for test source operator."""

    num_records: int = 100
    batch_size: int = 10

    def create_source(self) -> "MockSplitPlanner":
        return MockSplitPlanner(self)


# Set operator_class after class definition
MockSourceConfig.operator_class = MockSourceOperator


class MockSplitPlanner:
    """Test split planner that generates splits from config."""

    def __init__(self, config: MockSourceConfig):
        self._config = config

    def cleanup(self) -> None:
        pass

    def plan_splits(self, stage_id: str):
        """Generate splits based on operator config."""
        num_batches = self._config.num_records // self._config.batch_size

        for i in range(num_batches):
            yield Split(
                split_id=f"split_{i}",
                stage_id=stage_id,
                data_range={
                    "start": i * self._config.batch_size,
                    "end": (i + 1) * self._config.batch_size,
                },
            )


class MockTransformOperator(Operator):
    """Transform operator that modifies data."""

    def __init__(self, config: "MockTransformConfig", runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self._processed = 0

    def process_split(
        self, split: Split, payload: Optional[SplitPayload]
    ) -> Optional[SplitPayload]:
        """Transform data by adding suffix to values."""
        if payload is None:
            return None

        table = payload.to_table()

        # Transform: add suffix to value column
        values = table.column("value").to_pylist()
        new_values = [v + self.config.suffix for v in values]

        new_table = pa.table(
            {
                "id": table.column("id"),
                "value": new_values,
            }
        )

        self._processed += table.num_rows
        return SplitPayload(data=new_table, split_id=split.split_id)

    def close(self) -> None:
        pass


@dataclass
class MockTransformConfig(OperatorConfig):
    """Config for test transform operator."""

    suffix: str = "_transformed"


# Set operator_class after class definition
MockTransformConfig.operator_class = MockTransformOperator


class MockSinkOperator(Operator):
    """Sink operator that collects results."""

    # Shared storage for test verification
    collected_records: List[Dict] = []

    def __init__(self, config: "MockSinkConfig", runtime: OperatorRuntime):
        super().__init__(config, runtime)

    def process_split(
        self, split: Split, payload: Optional[SplitPayload]
    ) -> Optional[SplitPayload]:
        """Collect records from payload."""
        if payload is None:
            return None

        records = payload.to_pylist()
        MockSinkOperator.collected_records.extend(records)

        # Sink doesn't produce output
        return None

    def close(self) -> None:
        pass

    @classmethod
    def reset(cls):
        cls.collected_records = []


@dataclass
class MockSinkConfig(OperatorConfig):
    """Config for test sink operator."""

    pass


# Set operator_class after class definition
MockSinkConfig.operator_class = MockSinkOperator


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def simple_job():
    """Create a simple single-stage job."""
    job = Job(job_id="test_simple")

    source_stage = Stage(
        stage_id="source",
        operator_config=MockSourceConfig(num_records=50, batch_size=10),
        parallelism=(1, 2),  # (min, max)
    )
    job.add_stage(source_stage)

    return job


# ============================================================================
# Tests
# ============================================================================


class TestRayJobRunner:
    """Tests for RayJobRunner."""

    @pytest.mark.asyncio
    async def test_initialization(self, simple_job, ray_cluster):
        """Test runner initialization."""
        runner = RayJobRunner(simple_job)

        assert not runner.is_initialized
        assert not runner.is_running

        await runner.initialize()

        assert runner.is_initialized
        assert "source" in runner._masters

    @pytest.mark.asyncio
    async def test_get_status(self, simple_job, ray_cluster):
        """Test getting pipeline status."""
        runner = RayJobRunner(simple_job)
        await runner.initialize()

        status = runner.get_status()

        assert status.job_id == "test_simple"
        assert not status.is_running
        assert "source" in status.stages

        await runner.stop()

    @pytest.mark.asyncio
    async def test_stop_before_run(self, simple_job, ray_cluster):
        """Test stopping before running."""
        runner = RayJobRunner(simple_job)
        await runner.initialize()
        await runner.stop()  # Should not raise

        assert not runner.is_running


# ============================================================================
