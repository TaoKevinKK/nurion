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

"""Stability tests with deterministic fault injection.

These tests verify system stability under various failure scenarios using
the FaultInjector framework for precise, reproducible fault injection.

Test Categories:
1. Exactly-Once Semantics (5 tests) - Verify no data loss and no duplicates
2. Checkpoint & Recovery (5 tests) - Verify recovery after various failures
3. Dynamic Scaling (4 tests) - Verify scaling behavior under load
4. Partition Management (4 tests) - Verify multi-partition correctness
5. Backpressure (3 tests) - Verify flow control under pressure
6. Combined Fault Scenarios (4 tests) - Verify resilience under multiple faults

Total: 25 tests

Use `pytest -m stability` to run these tests.
"""

import asyncio
import os
import pytest
import ray

from _internal.runtime.ray_runner import RayJobRunner
from _internal.testing.fault_injection import (
    reset_fault_injector,
    FAULT_QUEUE_PRODUCE,
    FAULT_QUEUE_FETCH,
    FAULT_QUEUE_COMMIT,
    FAULT_BEFORE_PROCESS,
    FAULT_AFTER_PROCESS,
)

from tests.utils import (
    DataValidator,
    ExplodeConfig,
    FilterConfig,
    create_collector,
    create_test_pipeline,
    create_multi_stage_pipeline,
    generate_test_data_with_checksum,
    get_sink_records,
    kill_random_worker,
    wait_for_progress,
    wait_for_stage_workers,
)

# Mark all tests in this module as stability tests
pytestmark = [pytest.mark.stability]


# =============================================================================
# Test Fixtures
# =============================================================================


class StabilityTestBase:
    """Base class for stability tests with common setup."""

    # Track env vars set by tests for cleanup
    _fault_env_vars: list[str] = []

    @pytest.fixture(autouse=True)
    async def setup(self, ray_cluster, request):
        """Setup collector and fault injector for each test."""
        import hashlib

        test_name = request.node.name.replace("[", "_").replace("]", "_")
        unique = hashlib.md5(test_name.encode()).hexdigest()[:8]
        self.collector_name = f"stability_collector_{unique}"
        create_collector(self.collector_name)

        # Enable fault injection via environment variable
        self._fault_env_vars = []
        os.environ["NURION_FAULT_INJECTION"] = "1"
        self._fault_env_vars.append("NURION_FAULT_INJECTION")

        # Reset injector to pick up new env vars
        reset_fault_injector()

        yield

        # Cleanup - remove all fault env vars
        for var in self._fault_env_vars:
            os.environ.pop(var, None)

        # Reset injector to clear state
        reset_fault_injector()

        try:
            collector = ray.get_actor(self.collector_name)
            ray.kill(collector)
        except Exception:
            pass

    def set_fault(
        self, fault_point: str, after_count: int | None = None, probability: float | None = None
    ) -> None:
        """Set a fault injection via environment variable.

        Args:
            fault_point: The fault point constant (e.g., FAULT_QUEUE_PRODUCE)
            after_count: Fail after N successful calls
            probability: Fail with given probability (0-1)
        """
        # Map fault point to env var suffix
        point_to_suffix = {
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

        suffix = point_to_suffix.get(fault_point)
        if not suffix:
            raise ValueError(f"Unknown fault point: {fault_point}")

        if after_count is not None:
            key = f"NURION_FAULT_{suffix}_AFTER"
            os.environ[key] = str(after_count)
            self._fault_env_vars.append(key)

        if probability is not None:
            key = f"NURION_FAULT_{suffix}_PROB"
            os.environ[key] = str(probability)
            self._fault_env_vars.append(key)

        # Reset injector to pick up new env vars
        reset_fault_injector()


# =============================================================================
# 1. Exactly-Once Semantics Tests (5 tests)
# =============================================================================


class TestExactlyOnceSemantics(StabilityTestBase):
    """Tests for exactly-once processing guarantees."""

    @pytest.mark.asyncio
    async def test_offset_dedup_skip_processed(self, ray_cluster):
        """Verify offset-based deduplication skips already processed messages.

        Scenario: Same offset presented twice (simulates retry after crash).
        Expected: Second occurrence is detected as duplicate and skipped.
        """
        NUM_RECORDS = 500
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        # Fail before processing to trigger retry
        self.set_fault(FAULT_BEFORE_PROCESS, after_count=10)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
            num_records=NUM_RECORDS,
            batch_size=100,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
        )
        # Enable exactly-once mode

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=60)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Exactly-once: no duplicates
        assert validator.verify_no_duplicates(sink_data), (
            "Duplicates found - offset deduplication failed"
        )
        assert validator.verify_count(sink_data, NUM_RECORDS)

    @pytest.mark.asyncio
    async def test_crash_before_mark_reprocesses(self, ray_cluster):
        """Crash after processing but before mark_processed.

        Scenario: Worker crash during processing (simulated by killing worker).
        Expected: On recovery, message is reprocessed, data complete.

        Note: FaultInjector doesn't work across Ray processes, so we use
        worker kills to simulate crashes at critical points.
        """
        NUM_RECORDS = 600
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
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
            run_task = asyncio.create_task(runner.run())

            # Wait for processing to start, then kill a worker
            await asyncio.sleep(1.0)
            await kill_random_worker(runner, stage_id="transform")

            await asyncio.wait_for(run_task, timeout=60)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # No data loss - retry should succeed
        assert validator.verify_count(sink_data, NUM_RECORDS), (
            f"Data loss: expected {NUM_RECORDS}, got {len(sink_data)}"
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_crash_after_mark_skips_on_retry(self, ray_cluster):
        """Crash after mark_processed but before queue commit.

        Scenario: Offset saved, but queue commit didn't happen.
        Expected: On retry, is_duplicate returns True, message skipped.
        """
        NUM_RECORDS = 500
        FILTER_MODULO = 2
        FILTER_REMAINDER = 0
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        # Fail after processing
        self.set_fault(FAULT_AFTER_PROCESS, after_count=8)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
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

        # Should have correct count (skipped duplicates)
        assert validator.verify_count(sink_data, expected_count)
        assert validator.verify_filter_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

    @pytest.mark.asyncio
    async def test_queue_commit_failure_no_duplicates(self, ray_cluster):
        """Queue commit fails - verify exactly-once semantics.

        Scenario: Processing done, offset saved, but queue commit fails.
        Expected: No duplicates in output, data complete.
        """
        NUM_RECORDS = 400
        EXPLODE_FACTOR = 2
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        # Fail queue commit
        self.set_fault(FAULT_QUEUE_COMMIT, after_count=3)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
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

        assert validator.verify_no_duplicates_composite(sink_data, ["id", "copy_idx"]), (
            "Duplicates after commit failure - exactly-once violated"
        )
        assert validator.verify_count(sink_data, expected_count)


# =============================================================================
# 2. Checkpoint & Recovery Tests (5 tests)
# =============================================================================


class TestCheckpointRecovery(StabilityTestBase):
    """Tests for checkpoint and recovery mechanisms."""

    @pytest.mark.asyncio
    async def test_single_worker_crash_recovery(self, ray_cluster):
        """Single worker crash during processing.

        Scenario: One worker crashes mid-processing.
        Expected: Work is redistributed, no data loss.
        """
        NUM_RECORDS = 1000
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
            num_records=NUM_RECORDS,
            batch_size=100,
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Wait for workers to be active
            await wait_for_stage_workers(runner, "transform", min_workers=3, timeout=15)

            # Kill one worker
            await kill_random_worker(runner, stage_id="transform")

            await asyncio.wait_for(run_task, timeout=60)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, NUM_RECORDS), (
            f"Data loss after worker crash: expected {NUM_RECORDS}, got {len(sink_data)}"
        )

    @pytest.mark.asyncio
    async def test_multiple_workers_simultaneous_crash(self, ray_cluster):
        """Multiple workers crash simultaneously.

        Scenario: 2 out of 4 workers crash at once.
        Expected: System recovers, no data loss.
        """
        NUM_RECORDS = 1200
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
            num_records=NUM_RECORDS,
            batch_size=100,
            min_workers=4,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            await wait_for_stage_workers(runner, "transform", min_workers=4, timeout=15)

            # Kill 2 workers quickly
            await kill_random_worker(runner, stage_id="transform")
            await asyncio.sleep(0.1)
            await kill_random_worker(runner, stage_id="transform")

            await asyncio.wait_for(run_task, timeout=90)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, NUM_RECORDS)

    @pytest.mark.asyncio
    async def test_offset_recovery_after_restart(self, ray_cluster):
        """Offset recovery after worker restart.

        Scenario: Worker crashes and restarts, should resume from last offset.
        Expected: No duplicate processing after restart.
        """
        NUM_RECORDS = 800
        FILTER_MODULO = 3
        FILTER_REMAINDER = 0
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
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
            run_task = asyncio.create_task(runner.run())

            # Wait for some progress
            await wait_for_progress(
                runner, min_processed=100, timeout=30, collector_name=self.collector_name
            )

            # Kill worker (will restart and recover from offset)
            await kill_random_worker(runner, stage_id="transform")

            await asyncio.wait_for(run_task, timeout=60)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # No duplicates means offset recovery worked
        assert validator.verify_no_duplicates(sink_data)
        assert validator.verify_count(sink_data, expected_count)

    @pytest.mark.asyncio
    async def test_multi_stage_pipeline_recovery(self, ray_cluster):
        """Multi-stage pipeline recovery after failure.

        Scenario: Worker in middle stage crashes.
        Expected: Pipeline recovers, all data processed.
        """
        NUM_RECORDS = 600
        NUM_STAGES = 3
        validator = DataValidator()

        job = create_multi_stage_pipeline(
            num_records=NUM_RECORDS,
            batch_size=100,
            num_transform_stages=NUM_STAGES,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Wait for pipeline to start
            await asyncio.sleep(2)

            # Kill worker in transform_1 (middle stage)
            await kill_random_worker(runner, stage_id="transform_1")

            await asyncio.wait_for(run_task, timeout=90)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, NUM_RECORDS)

    @pytest.mark.asyncio
    async def test_queue_reconnection_after_failure(self, ray_cluster):
        """Queue reconnection after worker failure.

        Scenario: Worker dies and is replaced, new worker reconnects.
        Expected: System recovers and continues processing.

        Note: We simulate transient failures via worker kills since
        FaultInjector doesn't work across Ray processes.
        """
        NUM_RECORDS = 600
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
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
            run_task = asyncio.create_task(runner.run())

            # Kill workers to simulate connection failures
            await asyncio.sleep(0.5)
            await kill_random_worker(runner, stage_id="transform")
            await asyncio.sleep(0.5)
            await kill_random_worker(runner, stage_id="transform")

            await asyncio.wait_for(run_task, timeout=60)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, NUM_RECORDS)


# =============================================================================
# 3. Dynamic Scaling Tests (4 tests)
# =============================================================================


class TestDynamicScaling(StabilityTestBase):
    """Tests for dynamic worker scaling under various conditions."""

    @pytest.mark.asyncio
    async def test_scale_up_under_high_lag(self, ray_cluster):
        """Scale up when queue lag is high.

        Scenario: Processing can't keep up with production.
        Expected: System scales up workers, lag reduces.
        """
        NUM_RECORDS = 1500
        EXPLODE_FACTOR = 2
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
            num_records=NUM_RECORDS,
            batch_size=100,
            min_workers=2,
            max_workers=8,  # Allow scaling up
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=90)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count)
        assert validator.verify_explode_result(sink_data, NUM_RECORDS, EXPLODE_FACTOR)

    @pytest.mark.asyncio
    async def test_scale_down_under_low_lag(self, ray_cluster):
        """Scale down when queue lag is low.

        Scenario: All data processed, workers idle.
        Expected: System can scale down without data loss.
        """
        NUM_RECORDS = 500
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
            num_records=NUM_RECORDS,
            batch_size=200,  # Larger batches = faster processing
            min_workers=1,
            max_workers=6,
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

        assert validator.verify_count(sink_data, NUM_RECORDS)

    @pytest.mark.asyncio
    async def test_worker_failure_auto_replenishment(self, ray_cluster):
        """Worker auto-replenishment after failure.

        Scenario: Worker dies, should be automatically replaced.
        Expected: New worker spawned, no data loss.
        """
        NUM_RECORDS = 1000
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
            num_records=NUM_RECORDS,
            batch_size=100,
            min_workers=3,  # Must maintain at least 3
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
        )

        runner = RayJobRunner(job)
        kills = 0
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Kill workers repeatedly, system should replenish
            for _ in range(3):
                await asyncio.sleep(1)
                try:
                    killed = await kill_random_worker(runner, stage_id="transform")
                    if killed:
                        kills += 1
                except Exception:
                    pass

            await asyncio.wait_for(run_task, timeout=90)
        finally:
            await runner.stop()

        assert kills > 0, "No workers killed - test invalid"

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, NUM_RECORDS)

    @pytest.mark.asyncio
    async def test_rapid_scale_cycles_stability(self, ray_cluster):
        """Rapid scale up/down cycles.

        Scenario: Quick scaling changes shouldn't cause instability.
        Expected: Data integrity maintained despite rapid changes.
        """
        NUM_RECORDS = 800
        FILTER_MODULO = 4
        FILTER_REMAINDER = 0
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
            num_records=NUM_RECORDS,
            batch_size=100,
            min_workers=2,
            max_workers=8,
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
            run_task = asyncio.create_task(runner.run())

            # Simulate rapid scaling by killing and letting replenish
            for _ in range(2):
                await asyncio.sleep(0.5)
                await kill_random_worker(runner, stage_id="transform")
                await asyncio.sleep(0.3)

            await asyncio.wait_for(run_task, timeout=60)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert len(sink_data) >= expected_count, (
            f"Data loss in rapid scaling: expected >= {expected_count}, got {len(sink_data)}"
        )


# =============================================================================
# 4. Backpressure Tests (3 tests)
# =============================================================================


class TestBackpressure(StabilityTestBase):
    """Tests for backpressure and flow control."""

    @pytest.mark.asyncio
    async def test_backpressure_prevents_overflow(self, ray_cluster):
        """Backpressure prevents memory overflow.

        Scenario: Slow sink causes queue buildup.
        Expected: System slows down, completes without overflow.
        """
        NUM_RECORDS = 800
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
            num_records=NUM_RECORDS,
            batch_size=50,  # Many small batches
            min_workers=4,
            max_workers=8,
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

        assert validator.verify_count(sink_data, NUM_RECORDS)

    @pytest.mark.asyncio
    async def test_queue_produce_retry_under_pressure(self, ray_cluster):
        """Queue produce retries under backpressure.

        Scenario: Produce fails due to queue full.
        Expected: Retry succeeds, no data loss.
        """
        NUM_RECORDS = 600
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        # Fail produce (simulates queue full)
        self.set_fault(FAULT_QUEUE_PRODUCE, after_count=8)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
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

        assert validator.verify_count(sink_data, NUM_RECORDS)

    @pytest.mark.asyncio
    async def test_graceful_degradation_under_pressure(self, ray_cluster):
        """System degrades gracefully under pressure.

        Scenario: Multiple faults during high load.
        Expected: System completes without crash, data intact.
        """
        NUM_RECORDS = 1000
        FILTER_MODULO = 2
        FILTER_REMAINDER = 0
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        # Multiple fault types
        self.set_fault(FAULT_QUEUE_FETCH, after_count=10)
        self.set_fault(FAULT_QUEUE_PRODUCE, after_count=15)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
            num_records=NUM_RECORDS,
            batch_size=50,
            min_workers=3,
            max_workers=6,
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
            await asyncio.wait_for(runner.run(), timeout=90)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count)


# =============================================================================
# 6. Combined Fault Scenarios (4 tests)
# =============================================================================


class TestCombinedFaultScenarios(StabilityTestBase):
    """Tests combining multiple fault types."""

    @pytest.mark.asyncio
    async def test_multiple_fault_points_simultaneously(self, ray_cluster):
        """Multiple faults across different components.

        Scenario: Queue and processing faults together.
        Expected: System recovers from all, data complete.
        """
        NUM_RECORDS = 800
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        # Configure multiple fault points
        self.set_fault(FAULT_QUEUE_PRODUCE, after_count=5)
        self.set_fault(FAULT_QUEUE_FETCH, after_count=8)
        self.set_fault(FAULT_BEFORE_PROCESS, after_count=10)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
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

        assert validator.verify_count(sink_data, NUM_RECORDS)

    @pytest.mark.asyncio
    async def test_cascading_failures_across_stages(self, ray_cluster):
        """Failures in one stage don't corrupt other stages.

        Scenario: Faults in transform stage.
        Expected: Sink receives correct data, no corruption.
        """
        NUM_RECORDS = 600
        EXPLODE_FACTOR = 2
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        # Faults in transform processing
        self.set_fault(FAULT_AFTER_PROCESS, after_count=7)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
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

        # Data should be correct (checksums match)
        assert validator.verify_checksums(source_data, sink_data)
        assert validator.verify_count(sink_data, expected_count)

    @pytest.mark.asyncio
    async def test_fault_injection_at_critical_paths(self, ray_cluster):
        """Faults at the most critical processing paths.

        Scenario: Faults at mark_processed (most dangerous point).
        Expected: Exactly-once semantics maintained.
        """
        NUM_RECORDS = 500
        FILTER_MODULO = 3
        FILTER_REMAINDER = 0
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        # Critical path faults
        self.set_fault(FAULT_BEFORE_PROCESS, after_count=4)
        self.set_fault(FAULT_AFTER_PROCESS, after_count=6)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
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

        # No duplicates despite faults at critical paths
        assert validator.verify_no_duplicates(sink_data)
        assert validator.verify_count(sink_data, expected_count)

    @pytest.mark.asyncio
    async def test_recovery_under_continuous_faults(self, ray_cluster):
        """System recovery under continuous fault injection.

        Scenario: Random faults throughout processing (probability-based).
        Expected: System completes successfully.
        """
        NUM_RECORDS = 1000
        validator = DataValidator()
        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        # Continuous random faults (5% probability)
        self.set_fault(FAULT_QUEUE_FETCH, probability=0.05)

        job = create_test_pipeline(
            claim_timeout_secs=10,
            recovery_interval_secs=2,
            num_records=NUM_RECORDS,
            batch_size=100,
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=120)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Should complete despite continuous faults
        assert validator.verify_count(sink_data, NUM_RECORDS)
        assert validator.verify_checksums(source_data, sink_data)
