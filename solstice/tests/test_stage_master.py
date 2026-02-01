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

"""Tests for Stage Master v2 architecture.

Tests the new queue-based architecture with:
- Worker pull model
- Simplified master (output queue only)
- Queue broker/client integration
"""

import pytest
import pytest_asyncio
from dataclasses import dataclass
from typing import List
from unittest.mock import MagicMock

from solstice.core.stage_master import (
    StageMaster,
    QueueMessage,
)
from solstice.core.operator import OperatorConfig, Operator, OperatorRuntime
from solstice.core.stage import StageRuntime

# Note: Only async test classes/functions should use @pytest.mark.asyncio decorator


# ============================================================================
# Test Fixtures
# ============================================================================


class MockOperator(Operator):
    """Mock operator that passes through data."""

    def __init__(self, config: "MockOperatorConfig", runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self._closed = False

    def process_split(self, split, payload):
        # Just pass through for testing
        return payload

    def generate_splits(self):
        from solstice.core.models import Split

        # Generate some test splits
        return [
            Split(split_id=f"split_{i}", stage_id="test_stage", data_range={"index": i})
            for i in range(5)
        ]

    def close(self):
        self._closed = True


@dataclass
class MockOperatorConfig(OperatorConfig):
    """Mock operator config for testing."""

    pass


# Set operator_class after class definition
MockOperatorConfig.operator_class = MockOperator


@dataclass
class MockStage:
    """Mock stage for testing."""

    stage_id: str = "test_stage"
    operator_config: MockOperatorConfig = None
    upstream_stages: List[str] = None
    # Parallelism settings
    min_parallelism: int = 1
    max_parallelism: int = 2
    output_partitions: int = None
    # Processing configuration
    batch_size: int = 100
    commit_batch_size: int = 5
    # Backpressure thresholds
    backpressure_threshold_lag: int = 5000
    backpressure_threshold_queue_size: int = 1000
    # Worker lifecycle
    worker_ready_timeout_seconds: float = 30.0
    worker_spawn_retry_delay_seconds: float = 2.0
    # Worker resources
    num_cpus: float = 1.0
    num_gpus: float = 0.0
    memory_mb: int = 0

    def __post_init__(self):
        if self.operator_config is None:
            self.operator_config = MockOperatorConfig()
        if self.upstream_stages is None:
            self.upstream_stages = []


@pytest.fixture
def mock_stage():
    """Provide a mock stage."""
    return MockStage()


@pytest.fixture
def stage_runtime():
    """Provide default stage runtime for unit tests."""
    return StageRuntime(
        broker_endpoint=None,
        upstream_queue_name=None,
        state_queue_name=None,
    )


@pytest.fixture
def payload_store():
    """Provide a mock payload store."""

    store = MagicMock()
    store.store = MagicMock()
    store.get = MagicMock(return_value=None)
    store.delete = MagicMock()
    store.clear = MagicMock()
    yield store


# ============================================================================
# QueueMessage Tests
# ============================================================================


class TestQueueMessage:
    """Tests for QueueMessage serialization."""

    def test_to_bytes_from_bytes(self):
        """Test message round-trip serialization."""
        msg = QueueMessage(
            message_id="msg_001",
            split_id="split_001",
            payload_key="abc123",
            metadata={"key": "value"},
        )

        data = msg.to_bytes()
        restored = QueueMessage.from_bytes(data)

        assert restored.message_id == msg.message_id
        assert restored.split_id == msg.split_id
        assert restored.payload_key == msg.payload_key
        assert restored.metadata == msg.metadata

    def test_empty_metadata(self):
        """Test message with empty metadata."""
        msg = QueueMessage(
            message_id="msg_001",
            split_id="split_001",
            payload_key="abc123",
        )

        data = msg.to_bytes()
        restored = QueueMessage.from_bytes(data)

        assert restored.metadata == {}


# ============================================================================
# StageMaster Tests
# ============================================================================


class TestStageMaster:
    """Tests for StageMaster."""

    @pytest.mark.asyncio
    async def test_create_output_queue(self, mock_stage, stage_runtime, payload_store, ray_cluster):
        """Test that master creates output queue."""
        master = StageMaster(
            job_id="test_job",
            stage=mock_stage,
            runtime=stage_runtime,
            payload_store=payload_store,
        )

        await master.start()

        assert master._output_queue is not None
        assert master._output_queue_name == "test_job_test_stage_output"

        await master.stop()

    @pytest.mark.asyncio
    async def test_get_status(self, mock_stage, stage_runtime, payload_store, ray_cluster):
        """Test getting stage status."""
        master = StageMaster(
            job_id="test_job",
            stage=mock_stage,
            runtime=stage_runtime,
            payload_store=payload_store,
        )

        # Before start
        status = master.get_status()
        assert not status.is_running
        assert not status.is_finished

        await master.start()

        # After start
        status = master.get_status()
        assert status.is_running
        assert status.worker_count >= 1

        await master.stop()

    @pytest.mark.asyncio
    async def test_stop_idempotent(self, mock_stage, stage_runtime, payload_store, ray_cluster):
        """Test that stop can be called multiple times."""
        master = StageMaster(
            job_id="test_job",
            stage=mock_stage,
            runtime=stage_runtime,
            payload_store=payload_store,
        )

        await master.start()
        await master.stop()
        await master.stop()  # Should not raise

    @pytest.mark.asyncio
    async def test_get_output_queue(self, mock_stage, stage_runtime, payload_store, ray_cluster):
        """Test getting output queue for downstream."""
        from solstice.queue import WorkQueueQueueClient

        master = StageMaster(
            job_id="test_job",
            stage=mock_stage,
            runtime=stage_runtime,
            payload_store=payload_store,
        )

        assert master.get_output_queue() is None

        await master.start()

        queue = master.get_output_queue()
        assert queue is not None
        assert isinstance(queue, WorkQueueQueueClient)

        await master.stop()


