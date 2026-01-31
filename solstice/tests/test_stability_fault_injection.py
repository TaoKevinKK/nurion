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

"""Deterministic fault injection tests.

These tests use FaultInjector for precise, reproducible failure scenarios.
Unlike chaos tests (random kills), these tests guarantee:
1. Reproducibility - same config = same behavior
2. Precision - failures at exact operation points
3. Stability - no timing-dependent flakiness

Use `pytest -m fault_injection` to run these tests.
"""

import asyncio
import os
import pytest
import ray

from solstice.runtime.ray_runner import RayJobRunner
from solstice.testing.fault_injection import (
    reset_fault_injector,
    FAULT_QUEUE_PRODUCE,
    FAULT_QUEUE_FETCH,
    FAULT_QUEUE_COMMIT,
    FAULT_STATE_STORE_PUT,
    FAULT_STATE_STORE_GET,
    FAULT_BEFORE_PROCESS,
    FAULT_AFTER_PROCESS,
)
from tests.utils import (
    DataValidator,
    ExplodeConfig,
    FilterConfig,
    create_collector,
    create_test_pipeline,
    generate_test_data_with_checksum,
    get_sink_records,
)

# Mapping from fault point to env var suffix
_POINT_TO_SUFFIX = {
    "queue.produce": "QUEUE_PRODUCE",
    "queue.fetch": "QUEUE_FETCH",
    "queue.commit": "QUEUE_COMMIT",
    "operator.before_process": "BEFORE_PROCESS",
    "operator.after_process": "AFTER_PROCESS",
    "operator.before_mark_processed": "BEFORE_MARK_PROCESSED",
    "operator.after_mark_processed": "AFTER_MARK_PROCESSED",
    "state_store.put_batch": "STATE_STORE_PUT",
    "state_store.get": "STATE_STORE_GET",
}

# Mark all tests in this module
pytestmark = [pytest.mark.stability]


class FaultInjectionTestBase:
    """Base class for fault injection tests."""

    _fault_env_vars: list[str] = []

    @pytest.fixture(autouse=True)
    async def setup(self, ray_cluster, request):
        """Setup collector and fault injector for each test."""
        import hashlib

        test_name = request.node.name.replace("[", "_").replace("]", "_")
        unique = hashlib.md5(test_name.encode()).hexdigest()[:8]
        self.collector_name = f"test_collector_{unique}"
        create_collector(self.collector_name)

        # Enable fault injection via environment variable
        self._fault_env_vars = []
        os.environ["SOLSTICE_FAULT_INJECTION"] = "1"
        self._fault_env_vars.append("SOLSTICE_FAULT_INJECTION")
        reset_fault_injector()

        yield

        # Cleanup - remove all fault env vars
        for var in self._fault_env_vars:
            os.environ.pop(var, None)
        reset_fault_injector()

        try:
            collector = ray.get_actor(self.collector_name)
            ray.kill(collector)
        except Exception:
            pass

    def set_fault(
        self, fault_point: str, after_count: int | None = None, probability: float | None = None
    ) -> None:
        """Set a fault injection via environment variable."""
        suffix = _POINT_TO_SUFFIX.get(fault_point)
        if not suffix:
            raise ValueError(f"Unknown fault point: {fault_point}")

        if after_count is not None:
            key = f"SOLSTICE_FAULT_{suffix}_AFTER"
            os.environ[key] = str(after_count)
            self._fault_env_vars.append(key)

        if probability is not None:
            key = f"SOLSTICE_FAULT_{suffix}_PROB"
            os.environ[key] = str(probability)
            self._fault_env_vars.append(key)

        reset_fault_injector()


class TestQueueFaultInjection(FaultInjectionTestBase):
    """Deterministic queue fault tests."""

    @pytest.mark.asyncio
    async def test_queue_produce_failure_recovery(self, ray_cluster):
        """Queue produce fails after N calls, then recovers.

        Verifies:
        1. System retries failed produce operations
        2. No data loss despite transient failures
        3. Exactly-once semantics maintained
        """
        NUM_RECORDS = 1000
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        # Fail produce after 5 successful calls (simulates network blip)
        self.set_fault(FAULT_QUEUE_PRODUCE, after_count=5)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=200,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=60)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Data should be complete despite produce failure
        assert validator.verify_count(sink_data, NUM_RECORDS), (
            f"Data loss after produce failure: expected {NUM_RECORDS}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates(sink_data)

    @pytest.mark.asyncio
    async def test_queue_fetch_failure_recovery(self, ray_cluster):
        """Queue fetch fails after N calls, then recovers.

        Verifies:
        1. Workers retry failed fetch operations
        2. No data loss from fetch failures
        3. Consumer reconnection works correctly
        """
        NUM_RECORDS = 1000
        FILTER_MODULO = 3
        FILTER_REMAINDER = 0
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        # Fail fetch after 10 successful calls
        self.set_fault(FAULT_QUEUE_FETCH, after_count=10)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=200,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterConfig(
                modulo=FILTER_MODULO,
                remainder=FILTER_REMAINDER,
            ),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=60)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss after fetch failure: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_filter_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

    @pytest.mark.asyncio
    async def test_queue_commit_failure_exactly_once(self, ray_cluster):
        """Queue commit fails - critical test for exactly-once semantics.

        This tests the most dangerous failure: commit fails after processing.
        System must either:
        - Retry the commit, or
        - Re-process the message idempotently

        Verifies no duplicates AND no data loss.
        """
        NUM_RECORDS = 500
        EXPLODE_FACTOR = 2
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        # Fail commit after 3 successful calls - this is the critical path
        self.set_fault(FAULT_QUEUE_COMMIT, after_count=3)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=100,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=60)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # CRITICAL: No duplicates despite commit failure
        assert validator.verify_no_duplicates_composite(sink_data, ["id", "copy_idx"]), (
            "Duplicates found - exactly-once semantics violated!"
        )
        # And no data loss
        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss after commit failure: expected {expected_count}, got {len(sink_data)}"
        )


class TestOperatorFaultInjection(FaultInjectionTestBase):
    """Deterministic operator processing fault tests."""

    @pytest.mark.asyncio
    async def test_failure_before_process_no_data_loss(self, ray_cluster):
        """Failure before processing - message should be redelivered.

        When processing fails before any work is done, the message
        should be automatically redelivered and processed.
        """
        NUM_RECORDS = 800
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        # Fail before processing on 8th message
        self.set_fault(FAULT_BEFORE_PROCESS, after_count=8)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=200,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=60)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # No data should be lost
        assert validator.verify_count(sink_data, NUM_RECORDS), (
            f"Data loss after pre-process failure: expected {NUM_RECORDS}, got {len(sink_data)}"
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_failure_after_process_idempotency(self, ray_cluster):
        """Failure after processing - tests idempotent handling.

        This is a tricky scenario: processing completed but system crashes
        before acknowledgment. On retry, the system must either:
        - Detect duplicate and skip, or
        - Process idempotently (same result)
        """
        NUM_RECORDS = 500
        FILTER_MODULO = 2
        FILTER_REMAINDER = 0
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        # Fail after processing on 5th message
        self.set_fault(FAULT_AFTER_PROCESS, after_count=5)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=100,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterConfig(
                modulo=FILTER_MODULO,
                remainder=FILTER_REMAINDER,
            ),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=60)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify idempotency: no duplicates
        assert validator.verify_no_duplicates(sink_data), "Duplicates found - idempotency violated!"
        # And completeness
        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss after post-process failure: expected {expected_count}, got {len(sink_data)}"
        )


class TestStateStoreFaultInjection(FaultInjectionTestBase):
    """Deterministic state store fault tests."""

    @pytest.mark.asyncio
    async def test_state_store_put_failure_recovery(self, ray_cluster):
        """State store write fails - checkpoint must retry.

        Tests that checkpoint operations are retried when state
        store writes fail transiently.
        """
        NUM_RECORDS = 600
        EXPLODE_FACTOR = 2
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        # Fail state put after 2 successful checkpoints
        self.set_fault(FAULT_STATE_STORE_PUT, after_count=2)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=150,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=60)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss after state put failure: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_explode_result(sink_data, NUM_RECORDS, EXPLODE_FACTOR)

    @pytest.mark.asyncio
    async def test_state_store_get_failure_recovery(self, ray_cluster):
        """State store read fails - recovery must handle gracefully.

        Tests that workers can recover even when state store reads fail
        initially (e.g., during worker restart).
        """
        NUM_RECORDS = 500
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        # Fail state get on first attempt (simulates cold start issue)
        self.set_fault(FAULT_STATE_STORE_GET, after_count=1)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=100,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=60)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, NUM_RECORDS), (
            f"Data loss after state get failure: expected {NUM_RECORDS}, got {len(sink_data)}"
        )


class TestCombinedFaultScenarios(FaultInjectionTestBase):
    """Tests combining multiple fault injection points."""

    @pytest.mark.asyncio
    async def test_multiple_fault_points(self, ray_cluster):
        """Multiple failures across different components.

        Tests system resilience when faults occur at:
        - Queue produce
        - Queue fetch
        - State store put

        All in the same pipeline run.
        """
        NUM_RECORDS = 800
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        # Configure multiple fault points
        self.set_fault(FAULT_QUEUE_PRODUCE, after_count=3)
        self.set_fault(FAULT_QUEUE_FETCH, after_count=5)
        self.set_fault(FAULT_STATE_STORE_PUT, after_count=2)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=100,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=90)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, NUM_RECORDS), (
            f"Data loss with multiple faults: expected {NUM_RECORDS}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates(sink_data)
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_probability_based_failures(self, ray_cluster):
        """Random failures with controlled probability.

        Uses probability-based fault injection for less deterministic
        but more realistic failure patterns.
        """
        NUM_RECORDS = 1000
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        # 5% chance of failure on each produce (multiple failures possible)
        self.set_fault(FAULT_QUEUE_PRODUCE, probability=0.05)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=100,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=90)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Even with random failures, data should be complete
        assert validator.verify_count(sink_data, NUM_RECORDS), (
            f"Data loss with random failures: expected {NUM_RECORDS}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates(sink_data)
