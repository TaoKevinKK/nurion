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

import asyncio

import pyarrow as pa
import pytest
from dataclasses import dataclass
from typing import List
from unittest.mock import MagicMock

from _internal.core.models import (
    DataQueueMessage,
    QueueEndpoint,
    SplitPayload,
    queue_message_from_bytes,
)
from _internal.core.stage_master import StageMaster
from _internal.core.operator import OperatorConfig, Operator, OperatorRuntime
from _internal.core.stage import StageRuntime
from _internal.runtime.queue_stats import QueueRef

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
        from _internal.core.models import Split

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
    """Provide stage runtime with a real broker for unit tests."""
    from _internal.queue import AnvilBrokerManager
    from _internal.core.models import QueueEndpoint

    # Create a real broker for tests
    broker = AnvilBrokerManager(db_path="memory://")
    broker.start()

    broker_url = broker.get_broker_url()
    host, port_str = broker_url.split(":")

    runtime = StageRuntime(
        broker_endpoint=QueueEndpoint(
            host=host,
            port=int(port_str),
            storage_url="memory://",
        ),
    )

    yield runtime

    # Cleanup
    broker.stop()


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
# DataQueueMessage Tests
# ============================================================================


class TestDataQueueMessage:
    """Tests for DataQueueMessage serialization."""

    def test_to_bytes_from_bytes(self):
        """Test message round-trip serialization."""
        msg = DataQueueMessage(
            message_id="msg_001",
            split_id="split_001",
            payload_key="abc123",
            metadata={"key": "value"},
        )

        data = msg.to_bytes()
        restored = queue_message_from_bytes(data)

        assert isinstance(restored, DataQueueMessage)
        assert restored.message_id == msg.message_id
        assert restored.split_id == msg.split_id
        assert restored.payload_key == msg.payload_key
        assert restored.metadata == msg.metadata

    def test_empty_metadata(self):
        """Test message with empty metadata."""
        msg = DataQueueMessage(
            message_id="msg_001",
            split_id="split_001",
            payload_key="abc123",
        )

        data = msg.to_bytes()
        restored = queue_message_from_bytes(data)

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

        assert master._queue_client is not None
        assert master._output_group_name == "test_job_test_stage_output"

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
    async def test_get_queue_client(self, mock_stage, stage_runtime, payload_store, ray_cluster):
        """Test getting output queue for downstream."""
        from _internal.queue import AnvilQueueClient

        master = StageMaster(
            job_id="test_job",
            stage=mock_stage,
            runtime=stage_runtime,
            payload_store=payload_store,
        )

        assert master.get_queue_client() is None

        await master.start()

        queue = master.get_queue_client()
        assert queue is not None
        assert isinstance(queue, AnvilQueueClient)

        await master.stop()


# ============================================================================
# Bug-regression: backpressure must not drop the current split (Bug #1)
# ============================================================================


class TestSourceManagerBackpressure:
    """Regression tests for the backpressure-drops-split bug.

    Previously, ``continue`` in the for-loop advanced the iterator to the next
    split while the current one was silently discarded.  The fix uses an inner
    ``while True`` loop that re-checks backpressure without advancing the
    iterator.
    """

    @pytest.mark.asyncio
    async def test_backpressure_does_not_drop_split(self, anvil_backend):
        """When backpressure fires, the paused split must still be produced."""
        from _internal.core.managers.source_manager import SourceManager
        from _internal.core.models import Split

        NUM_SPLITS = 3

        class _StubPlanner:
            def plan_splits(self, stage_id):
                for i in range(NUM_SPLITS):
                    yield Split(split_id=f"s{i}", stage_id=stage_id, data_range={"idx": i})

            def cleanup(self) -> None:
                pass

        # Returns True (pause) for the first 3 calls at idx=0, then False.
        call_count = 0
        running_flag = [True]

        async def backpressure_fn():
            nonlocal call_count
            call_count += 1
            return call_count <= 3

        manager = SourceManager(_StubPlanner(), "job_bp", "stage_bp")

        manager.start_split_production(
            queue_client=anvil_backend.client,
            backpressure_fn=backpressure_fn,
            running_fn=lambda: running_flag[0],
        )

        # _production_task completes once all splits are pushed + queue marked finished.
        await asyncio.wait_for(manager._production_task, timeout=5.0)

        stats = anvil_backend.client.get_stats(manager.planner_queue_name)
        total_pushed = stats.get("pending_count", 0) + stats.get("claimed_count", 0)
        assert total_pushed == NUM_SPLITS, (
            f"Expected {NUM_SPLITS} splits after backpressure, got {total_pushed}. "
            "A split was likely dropped by the old `continue` bug."
        )

        running_flag[0] = False
        await asyncio.sleep(0.15)
        await manager.stop()


# ============================================================================
# Bug-regression: production task exception must surface in run-loop (Bug #2)
# ============================================================================


class TestSourceManagerProductionFailure:
    """Regression tests for the production-task exception propagation bug.

    Previously, a background task failure (e.g. schema mismatch inside
    plan_splits) was only observable at stop()-time.  Now
    raise_if_production_failed() re-raises immediately in the StageMaster
    run-loop.
    """

    @pytest.mark.asyncio
    async def test_raise_if_production_failed_re_raises_exception(self):
        """raise_if_production_failed() re-raises a failed production task's exception."""
        from _internal.core.managers.source_manager import SourceManager

        class _StubPlanner:
            def plan_splits(self, stage_id):
                return iter([])

        manager = SourceManager(_StubPlanner(), "job_fail", "stage_fail")

        async def _fail():
            raise ValueError("Schema mismatch detected")

        task = asyncio.create_task(_fail())
        try:
            await task
        except ValueError:
            pass

        manager._production_task = task

        with pytest.raises(ValueError, match="Schema mismatch detected"):
            manager.raise_if_production_failed()

    @pytest.mark.asyncio
    async def test_raise_if_production_failed_silent_while_running(self):
        """raise_if_production_failed() does nothing while the task is still running."""
        from _internal.core.managers.source_manager import SourceManager

        class _StubPlanner:
            def plan_splits(self, stage_id):
                return iter([])

        manager = SourceManager(_StubPlanner(), "job_run", "stage_run")

        event = asyncio.Event()

        async def _hang():
            await event.wait()

        task = asyncio.create_task(_hang())
        manager._production_task = task

        # Must not raise while task is in progress.
        manager.raise_if_production_failed()

        event.set()
        await task

    def test_raise_if_production_failed_silent_when_no_task(self):
        """raise_if_production_failed() is a no-op when no task exists."""
        from _internal.core.managers.source_manager import SourceManager

        class _StubPlanner:
            def plan_splits(self, stage_id):
                return iter([])

        manager = SourceManager(_StubPlanner(), "job_none", "stage_none")
        assert manager._production_task is None
        manager.raise_if_production_failed()  # must not raise


# ============================================================================
# Bug-regression: consumed input payloads must be deleted after ack (Bug #3)
# ============================================================================


class TestStageWorkerPayloadCleanup:
    """Regression tests for the payload-store memory-leak bug.

    Previously, StageWorker fetched input payloads but never deleted them,
    causing unbounded growth in the payload store over long pipelines.
    After a successful ack the worker must call payload_store.delete(key).
    """

    @pytest.mark.asyncio
    async def test_payload_deleted_after_successful_ack(self, anvil_backend):
        """payload_store.delete(key) is called once per consumed payload after ack."""
        from _internal.core.stage_worker import StageWorker, WorkerRuntime

        # Access the underlying Python class directly to avoid Ray actor overhead.
        WorkerClass = StageWorker.__ray_actor_class__

        payload_key = "input_payload_abc"
        mock_payload_store = MagicMock()
        test_payload = SplitPayload(
            data=pa.table({"x": [1, 2, 3]}),
            split_id="s1",
        )
        mock_payload_store.get.return_value = test_payload
        mock_payload_store.get_with_hint.return_value = test_payload
        mock_payload_store.delete.return_value = True
        mock_payload_store.store.return_value = payload_key
        mock_payload_store.get_location.return_value = None
        mock_payload_store.flush_pending_writes.return_value = None

        runtime = WorkerRuntime(
            worker_id="w_cleanup",
            job_id="job_cleanup",
            stage_id="stage_cleanup",
            broker_endpoint=QueueEndpoint(
                host=anvil_backend.host,
                port=anvil_backend.port,
                storage_url="memory://",
            ),
            upstream=QueueRef.queue("cleanup_upstream"),
            # no downstream — ack-only path (default OutputRouting has group_name=None)
        )

        worker = WorkerClass(runtime, MockStage(), mock_payload_store)
        worker._init_operator()
        # Re-use the test backend's already-started client.
        worker.queue_client = anvil_backend.client

        # Push a DataQueueMessage that references a payload.
        anvil_backend.client.create_queue("cleanup_upstream")
        msg = DataQueueMessage(
            message_id="msg_del_001",
            split_id="s1",
            payload_key=payload_key,
            metadata={},
        )
        anvil_backend.client.push("cleanup_upstream", msg.to_bytes())

        records, _ = anvil_backend.client.claim("cleanup_upstream", batch_size=1, timeout_ms=1000)
        assert len(records) == 1, "Expected to claim 1 record"

        await worker._process_and_ack(records)

        # The input payload must have been fetched (via get_with_hint), then deleted.
        mock_payload_store.get_with_hint.assert_called_once()
        assert mock_payload_store.get_with_hint.call_args[0][0] == payload_key
        mock_payload_store.delete.assert_called_once_with(payload_key)

    @pytest.mark.asyncio
    async def test_payload_unreachable_raises_runtime_error(self, anvil_backend):
        """When payload_store returns None, worker must raise RuntimeError (fail fast).

        Regression: previously the worker would nack and return None, causing the
        message to be re-enqueued endlessly.  The job would hang forever instead of
        surfacing the error.
        """
        from _internal.core.stage_worker import StageWorker, WorkerRuntime

        WorkerClass = StageWorker.__ray_actor_class__

        payload_key = "unreachable_payload"
        mock_payload_store = MagicMock()
        # Simulate payload unreachable (Flight timeout, Ray object lost, S3 down)
        mock_payload_store.get_with_hint.return_value = None
        mock_payload_store.get.return_value = None
        mock_payload_store.get_location.return_value = None
        mock_payload_store.flush_pending_writes.return_value = None

        runtime = WorkerRuntime(
            worker_id="w_fail_fast",
            job_id="job_fail_fast",
            stage_id="stage_fail_fast",
            broker_endpoint=QueueEndpoint(
                host=anvil_backend.host,
                port=anvil_backend.port,
                storage_url="memory://",
            ),
            upstream=QueueRef.queue("fail_fast_upstream"),
        )

        worker = WorkerClass(runtime, MockStage(), mock_payload_store)
        worker._init_operator()
        worker.queue_client = anvil_backend.client

        anvil_backend.client.create_queue("fail_fast_upstream")
        msg = DataQueueMessage(
            message_id="msg_unreachable_001",
            split_id="s1",
            payload_key=payload_key,
            metadata={},
        )
        anvil_backend.client.push("fail_fast_upstream", msg.to_bytes())

        records, _ = anvil_backend.client.claim("fail_fast_upstream", batch_size=1, timeout_ms=1000)
        assert len(records) == 1

        with pytest.raises(RuntimeError, match="Payload unreachable"):
            await worker._process_and_ack(records)
