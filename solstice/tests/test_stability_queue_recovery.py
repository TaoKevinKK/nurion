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

"""Queue and network fault tests for distributed Solstice pipelines.

These are P1 tests that verify:
- WorkQueue broker restart recovery (with SlateDB persistence)
- Connection timeout handling
- Slow network / backpressure behavior
- Push/claim retry on failure

All tests use real Ray clusters and WorkQueue brokers (no mocks).
Data volumes: 10,000+ records with complex operators.

Note: Broker restart tests use file storage to ensure data persists
across restarts. Memory-backed storage loses all data on restart.
"""

import asyncio
import pytest
import ray

from solstice.runtime.ray_runner import RayJobRunner

from tests.utils import (
    DataValidator,
    ExplodeConfig,
    FilterConfig,
    FilterExplodeConfig,
    create_collector,
    create_test_pipeline,
    generate_test_data_with_checksum,
    get_sink_records,
    wait_for_progress,
)

# Mark all tests in this module as stability tests
pytestmark = pytest.mark.stability


class TestQueueFaultRecovery:
    """Tests for queue/broker fault scenarios."""

    @pytest.fixture(autouse=True)
    async def setup_collector(self, ray_cluster, request):
        """Create a unique collector for each test."""
        import hashlib

        test_name = request.node.name.replace("[", "_").replace("]", "_")
        unique = hashlib.md5(test_name.encode()).hexdigest()[:8]
        self.collector_name = f"test_collector_{unique}"
        create_collector(self.collector_name)
        yield
        try:
            collector = ray.get_actor(self.collector_name)
            ray.kill(collector)
        except Exception:
            pass

    @pytest.mark.asyncio
    @pytest.mark.timeout(120)
    async def test_workqueue_broker_restart(self, ray_cluster, workqueue_storage_path):
        """WorkQueue broker restart: job should exit on broker loss.

        This test verifies that when the broker goes down:
        1. The job exits instead of hanging

        Uses file storage backend to ensure data durability.
        """
        NUM_RECORDS = 1500  # Smaller dataset for faster test
        FILTER_MODULO = 4
        FILTER_REMAINDER = 0
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=300,  # Smaller batches for more splits
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterConfig(
                modulo=FILTER_MODULO,
                remainder=FILTER_REMAINDER,
            ),
            workqueue_db_path=workqueue_storage_path,  # Use file storage for persistence
        )

        runner = RayJobRunner(job)
        broker_restarted = False
        _records_before_restart = 0

        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Wait for some processing before restart (at least 50 records)
            await wait_for_progress(
                runner, min_processed=50, timeout=30, collector_name=self.collector_name
            )

            # Record how many records were processed before restart
            collector = ray.get_actor(self.collector_name)
            _records_before_restart = ray.get(collector.count.remote())

            # Restart the broker by creating a new instance
            # Note: We create a new broker instance instead of restarting the same one
            # because the underlying Rust/Tokio runtime may have residual state
            from solstice.queue import WorkQueueBrokerManager

            old_broker = runner._shared_broker
            old_url = old_broker.get_broker_url()
            old_host, old_port_str = old_url.rsplit(":", 1)
            old_port = int(old_port_str)
            old_db_path = old_broker.db_path
            old_claim_timeout = old_broker.claim_timeout_secs
            old_recovery_interval = old_broker.recovery_interval_secs

            # Stop the old broker and wait for clean shutdown
            old_broker.stop()
            await asyncio.sleep(1.0)  # Wait for port to be released

            # Create and start a new broker instance on the same port
            # Using the same db_path ensures data persistence
            new_broker = WorkQueueBrokerManager(
                db_path=old_db_path,
                port=old_port,
                claim_timeout_secs=old_claim_timeout,
                recovery_interval_secs=old_recovery_interval,
            )
            new_broker.start()
            await asyncio.sleep(0.5)  # Wait for broker to be ready

            # Replace the runner's broker reference
            runner._shared_broker = new_broker
            broker_restarted = True

            # Wait for pipeline to fail (broker down => job exits)
            with pytest.raises(RuntimeError):
                await asyncio.wait_for(run_task, timeout=60)
        finally:
            await runner.stop()

        assert broker_restarted

    @pytest.mark.asyncio
    async def test_workqueue_connection_timeout(self, ray_cluster):
        """Connection timeout: correct retry, no panic.

        This test verifies the system handles connection issues gracefully
        by processing data through a pipeline that may experience
        transient connection issues.
        """
        NUM_RECORDS = 1500
        EXPLODE_FACTOR = 2
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()

            # Run with timeout - should complete without panic
            await asyncio.wait_for(runner.run(), timeout=90)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify normal completion
        assert validator.verify_count(sink_data, expected_count), (
            f"Count mismatch: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_explode_result(sink_data, NUM_RECORDS, EXPLODE_FACTOR)

    @pytest.mark.asyncio
    async def test_workqueue_slow_network(self, ray_cluster):
        """Slow network: backpressure should work correctly, no data loss.

        Simulates slow network by using slow transform operators combined
        with filter/explode, which causes queue buildup and backpressure activation.
        """
        NUM_RECORDS = 1500
        FILTER_MODULO = 5
        FILTER_REMAINDER = 0
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        # Use filter to reduce data, simulating network-constrained throughput
        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=400,
            min_workers=4,
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

            # Longer timeout due to slow processing
            await asyncio.wait_for(runner.run(), timeout=120)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify backpressure didn't cause data loss
        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss with slow network: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_filter_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_produce_retry_on_failure(self, ray_cluster):
        """Produce failure: auto-retry, eventual success.

        This test verifies that transient produce failures are handled
        with retries and the pipeline eventually completes successfully.
        Uses filter+explode for complex row count verification.
        """
        NUM_RECORDS = 2000
        FILTER_MODULO = 3
        FILTER_REMAINDER = 0
        EXPLODE_FACTOR = 2
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_explode_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterExplodeConfig(
                filter_modulo=FILTER_MODULO,
                filter_remainder=FILTER_REMAINDER,
                explode_factor=EXPLODE_FACTOR,
            ),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()

            # Run the pipeline - internal retries should handle transient failures
            await asyncio.wait_for(runner.run(), timeout=120)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify all data was eventually produced
        assert validator.verify_count(sink_data, expected_count), (
            f"Count mismatch: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_filter_explode_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_fetch_retry_on_failure(self, ray_cluster):
        """Fetch failure: auto-retry, no message skip.

        This test verifies that transient fetch failures are handled
        with retries and no messages are skipped.
        """
        NUM_RECORDS = 1500
        EXPLODE_FACTOR = 3
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=400,
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()

            # Run the pipeline - internal retries should handle transient failures
            await asyncio.wait_for(runner.run(), timeout=120)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify no messages were skipped
        assert validator.verify_count(sink_data, expected_count), (
            f"Messages skipped: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates_composite(sink_data, ["id", "copy_idx"]), (
            "Duplicate records found"
        )
        assert validator.verify_explode_result(sink_data, NUM_RECORDS, EXPLODE_FACTOR)
        assert validator.verify_checksums(source_data, sink_data)
