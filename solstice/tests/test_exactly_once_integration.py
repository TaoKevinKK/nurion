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

"""Integration tests for exactly-once semantics.

These tests verify:
1. Offset-based deduplication in StageWorker
2. State persistence and recovery
3. Fault injection before mark_processed

Tests use real components (StageWorker, StateStore) without full pipeline.
"""

import tempfile
import threading
from dataclasses import dataclass
from typing import ClassVar, Dict, Optional, Set, Type

import pyarrow as pa
import pytest

from solstice.core import (
    Job,
    JobConfig,
    Stage,
    OperatorConfig,
    SemanticGuarantee,
)
from solstice.core.models import Split, SplitPayload
from solstice.core.sink_operator import SinkOperator
from solstice.queue import QueueType
from solstice.testing import (
    FaultInjector,
    set_fault_injector,
    FAULT_BEFORE_MARK_PROCESSED,
)


# Mark all tests as integration tests
pytestmark = pytest.mark.integration


# =============================================================================
# Test Operators (prefixed with _ to avoid pytest collection)
# =============================================================================


# Global storage for sinks (simulates external storage)
_sink_storage: Dict[str, Set[int]] = {}
_sink_storage_lock = threading.Lock()


def _get_sink_storage(storage_id: str) -> Set[int]:
    """Get the storage for a sink."""
    with _sink_storage_lock:
        if storage_id not in _sink_storage:
            _sink_storage[storage_id] = set()
        return _sink_storage[storage_id]


def _clear_sink_storage(storage_id: str) -> None:
    """Clear storage for a sink."""
    with _sink_storage_lock:
        _sink_storage[storage_id] = set()


@dataclass
class _IdempotentSinkConfig(OperatorConfig):
    """Config for idempotent sink that tracks unique values."""

    storage_id: str = "default"
    operator_class: ClassVar[Type["_IdempotentSink"]]


class _IdempotentSink(SinkOperator):
    """Sink that stores unique values (idempotent by value)."""

    def __init__(self, config: _IdempotentSinkConfig):
        super().__init__(config)
        self._config = config

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        if payload is None:
            return None

        table = payload.to_table()
        values = table.column("value").to_pylist()

        storage = _get_sink_storage(self._config.storage_id)
        with _sink_storage_lock:
            for v in values:
                storage.add(v)

        return None


_IdempotentSinkConfig.operator_class = _IdempotentSink


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture(scope="function")
def clean_storage():
    """Clean up storage before and after each test."""
    _sink_storage.clear()
    yield
    _sink_storage.clear()


@pytest.fixture(scope="function")
def temp_state_dir():
    """Create temp directory for state store."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


# =============================================================================
# Integration Tests - Operator Level
# =============================================================================


class TestOperatorExactlyOnce:
    """Test exactly-once at operator level with real state store."""

    def test_offset_dedup_with_state_store(self, clean_storage, temp_state_dir):
        """Offset-based dedup with real SlateDB state store."""
        storage_id = "test_dedup"
        _clear_sink_storage(storage_id)

        # Create operator with state store
        config = _IdempotentSinkConfig(
            storage_id=storage_id,
            state_store_path=temp_state_dir,
        )
        config.job_id = "test"
        config.stage_id = "sink"
        config.partition_id = 0
        config.semantic_guarantee = SemanticGuarantee.EXACTLY_ONCE

        op = config.setup()
        op.init_from_state_store()

        # Process messages 0-4
        for offset in range(5):
            if not op.is_duplicate(offset):
                payload = SplitPayload(
                    data=pa.table({"value": [offset]}),
                    split_id=f"s{offset}",
                )
                op.process_split(
                    Split(split_id=f"s{offset}", stage_id="sink", data_range={}),
                    payload,
                )
                op.mark_processed(offset)

        assert op.last_offset == 4
        storage = _get_sink_storage(storage_id)
        assert len(storage) == 5
        assert storage == {0, 1, 2, 3, 4}

        op.close()

    def test_recovery_from_state_store(self, clean_storage, temp_state_dir):
        """After crash, operator recovers offset from state store."""
        storage_id = "test_recovery"
        _clear_sink_storage(storage_id)

        # First run: process 0-4
        config1 = _IdempotentSinkConfig(
            storage_id=storage_id,
            state_store_path=temp_state_dir,
        )
        config1.job_id = "test"
        config1.stage_id = "sink"
        config1.partition_id = 0
        config1.semantic_guarantee = SemanticGuarantee.EXACTLY_ONCE

        op1 = config1.setup()
        op1.init_from_state_store()

        for offset in range(5):
            if not op1.is_duplicate(offset):
                payload = SplitPayload(
                    data=pa.table({"value": [offset]}),
                    split_id=f"s{offset}",
                )
                op1.process_split(
                    Split(split_id=f"s{offset}", stage_id="sink", data_range={}),
                    payload,
                )
                op1.mark_processed(offset)

        op1.close()
        storage_after_run1 = _get_sink_storage(storage_id).copy()
        assert len(storage_after_run1) == 5

        # Second run: simulate recovery and process 0-9
        config2 = _IdempotentSinkConfig(
            storage_id=storage_id,
            state_store_path=temp_state_dir,
        )
        config2.job_id = "test"
        config2.stage_id = "sink"
        config2.partition_id = 0
        config2.semantic_guarantee = SemanticGuarantee.EXACTLY_ONCE

        op2 = config2.setup()
        op2.init_from_state_store()

        # Should have recovered last_offset = 4
        assert op2.last_offset == 4, f"Expected last_offset=4, got {op2.last_offset}"

        processed_in_run2 = 0
        for offset in range(10):
            if not op2.is_duplicate(offset):
                payload = SplitPayload(
                    data=pa.table({"value": [offset]}),
                    split_id=f"s{offset}",
                )
                op2.process_split(
                    Split(split_id=f"s{offset}", stage_id="sink", data_range={}),
                    payload,
                )
                op2.mark_processed(offset)
                processed_in_run2 += 1

        op2.close()

        # Run 2 should only process 5-9 (5 new messages)
        assert processed_in_run2 == 5, f"Expected 5 new messages, got {processed_in_run2}"

        # Final storage should have all 10
        storage = _get_sink_storage(storage_id)
        assert len(storage) == 10
        assert storage == set(range(10))


class TestFaultInjection:
    """Test fault injection with exactly-once recovery."""

    def test_fault_before_mark_processed(self, clean_storage, temp_state_dir):
        """Fault before mark_processed: message is reprocessed on recovery.
        
        Scenario:
        1. Process messages 0-4 successfully
        2. On message 5: process succeeds, fault before mark_processed
        3. Recovery: message 5 is reprocessed (idempotent sink handles it)
        4. Continue with 6-9
        5. Final count = 10 (exactly once)
        """
        storage_id = "test_fault"
        _clear_sink_storage(storage_id)

        # Set up fault injection - fail on 6th mark_processed call
        injector = FaultInjector(enabled=True)
        injector.fail_after(FAULT_BEFORE_MARK_PROCESSED, count=5)
        set_fault_injector(injector)

        try:
            # First run: will crash on offset 5
            config1 = _IdempotentSinkConfig(
                storage_id=storage_id,
                state_store_path=temp_state_dir,
            )
            config1.job_id = "test"
            config1.stage_id = "sink"
            config1.partition_id = 0
            config1.semantic_guarantee = SemanticGuarantee.EXACTLY_ONCE

            op1 = config1.setup()
            op1.init_from_state_store()

            processed_before_crash = 0
            try:
                for offset in range(10):
                    if not op1.is_duplicate(offset):
                        payload = SplitPayload(
                            data=pa.table({"value": [offset]}),
                            split_id=f"s{offset}",
                        )
                        op1.process_split(
                            Split(split_id=f"s{offset}", stage_id="sink", data_range={}),
                            payload,
                        )
                        # Simulate what StageWorker does
                        from solstice.testing.fault_injection import check_fault
                        check_fault(FAULT_BEFORE_MARK_PROCESSED)
                        op1.mark_processed(offset)
                        processed_before_crash += 1
            except RuntimeError:
                pass  # Expected - fault injected

            op1.close()

            # Should have processed 0-4 (5 messages), crashed on 5
            assert processed_before_crash == 5
            assert op1.last_offset == 4  # Only 0-4 were marked processed

            # Storage has 0-5 (5 was written but not marked)
            storage_after_crash = _get_sink_storage(storage_id)
            assert 5 in storage_after_crash  # Message 5 was written to sink
            assert len(storage_after_crash) == 6

            # Disable fault injection for recovery
            injector.enabled = False

            # Second run: recovery
            config2 = _IdempotentSinkConfig(
                storage_id=storage_id,
                state_store_path=temp_state_dir,
            )
            config2.job_id = "test"
            config2.stage_id = "sink"
            config2.partition_id = 0
            config2.semantic_guarantee = SemanticGuarantee.EXACTLY_ONCE

            op2 = config2.setup()
            op2.init_from_state_store()

            # last_offset should be 4 (5 was not marked)
            assert op2.last_offset == 4

            for offset in range(10):
                if not op2.is_duplicate(offset):
                    payload = SplitPayload(
                        data=pa.table({"value": [offset]}),
                        split_id=f"s{offset}",
                    )
                    op2.process_split(
                        Split(split_id=f"s{offset}", stage_id="sink", data_range={}),
                        payload,
                    )
                    op2.mark_processed(offset)

            op2.close()

            # Final verification: exactly 10 unique values
            final_storage = _get_sink_storage(storage_id)
            assert len(final_storage) == 10, f"Expected 10, got {len(final_storage)}"
            assert final_storage == set(range(10))

        finally:
            set_fault_injector(None)

    def test_at_least_once_dedup_works_same_as_exactly_once(self, clean_storage, temp_state_dir):
        """AT_LEAST_ONCE uses the same offset-based dedup as EXACTLY_ONCE within a run.

        The difference is in recovery behavior, not runtime dedup logic.
        Both modes track last_offset and skip duplicates via is_duplicate().
        """
        storage_id = "test_alo"
        _clear_sink_storage(storage_id)

        config = _IdempotentSinkConfig(
            storage_id=storage_id,
            state_store_path=temp_state_dir,
        )
        config.job_id = "test"
        config.stage_id = "sink"
        config.partition_id = 0
        config.semantic_guarantee = SemanticGuarantee.AT_LEAST_ONCE

        op = config.setup()
        op.init_from_state_store()

        processed_count = 0
        skipped_count = 0

        # Process same offsets multiple times (simulates redelivery)
        for _ in range(3):
            for offset in range(5):
                # Dedup check - same logic in both AT_LEAST_ONCE and EXACTLY_ONCE
                if op.is_duplicate(offset):
                    skipped_count += 1
                    continue

                payload = SplitPayload(
                    data=pa.table({"value": [offset]}),
                    split_id=f"s{offset}",
                )
                op.process_split(
                    Split(split_id=f"s{offset}", stage_id="sink", data_range={}),
                    payload,
                )
                # Mark as processed (like stage_worker does)
                op.mark_processed(offset)
                processed_count += 1

        op.close()

        # Only first round should process (5 unique), rounds 2-3 should be skipped (10 duplicates)
        assert processed_count == 5, f"Expected 5 processed, got {processed_count}"
        assert skipped_count == 10, f"Expected 10 skipped, got {skipped_count}"

        # Idempotent sink has 5 unique values
        storage = _get_sink_storage(storage_id)
        assert len(storage) == 5


# =============================================================================
# Config Propagation Tests (no Ray runtime)
# =============================================================================


class TestConfigPropagation:
    """Test that semantic_guarantee is properly passed through config chain.

    This verifies the fix for the bug where JobConfig.semantic_guarantee
    was never passed to StageWorker.
    """

    def test_stage_config_has_semantic_guarantee(self):
        """Verify StageConfig includes semantic_guarantee field."""
        from solstice.core.stage_config import StageConfig

        # Default should be AT_LEAST_ONCE
        config = StageConfig()
        assert config.semantic_guarantee == SemanticGuarantee.AT_LEAST_ONCE

        # Can be set to EXACTLY_ONCE
        config = StageConfig(semantic_guarantee=SemanticGuarantee.EXACTLY_ONCE)
        assert config.semantic_guarantee == SemanticGuarantee.EXACTLY_ONCE

    def test_job_config_semantic_guarantee_in_stage_config(self):
        """Verify JobConfig.semantic_guarantee flows to StageConfig."""
        # Create job with EXACTLY_ONCE
        job = Job(
            job_id="test_config_flow",
            config=JobConfig(
                queue_type=QueueType.MEMORY,
                semantic_guarantee=SemanticGuarantee.EXACTLY_ONCE,
            ),
        )

        job.add_stage(
            Stage(
                stage_id="sink",
                operator_config=_IdempotentSinkConfig(storage_id="test"),
                parallelism=1,
            )
        )

        # Create runner and check _build_stage_config
        runner = job.create_ray_runner()
        stage = job.stages["sink"]
        stage_config = runner._build_stage_config(stage)

        # Verify semantic_guarantee was passed
        assert stage_config.semantic_guarantee == SemanticGuarantee.EXACTLY_ONCE

    def test_at_least_once_default(self):
        """Verify AT_LEAST_ONCE is the default."""
        job = Job(
            job_id="test_default",
            config=JobConfig(queue_type=QueueType.MEMORY),
        )

        job.add_stage(
            Stage(
                stage_id="sink",
                operator_config=_IdempotentSinkConfig(storage_id="test"),
                parallelism=1,
            )
        )

        runner = job.create_ray_runner()
        stage = job.stages["sink"]
        stage_config = runner._build_stage_config(stage)

        assert stage_config.semantic_guarantee == SemanticGuarantee.AT_LEAST_ONCE


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
