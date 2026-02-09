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

"""Elasticity tests for distributed Nurion engine pipelines.

These are P1 tests that verify:
- Dynamic worker scaling up during processing
- Dynamic worker scaling down (worker failures)
- Zero-worker recovery (all workers killed)
- Exactly-once semantics during scaling

All tests use real Ray clusters and WorkQueue brokers (no mocks).
WorkQueue uses a single-queue multi-consumer model where workers
compete to claim messages - no explicit partition assignment needed.
"""

import asyncio
import logging
import pytest
import ray

from _internal.runtime.ray_runner import RayJobRunner

from tests.utils import (
    DataValidator,
    ExplodeConfig,
    FilterConfig,
    FilterExplodeConfig,
    create_collector,
    create_test_pipeline,
    generate_test_data_with_checksum,
    get_sink_records,
    kill_random_worker,
    wait_for_progress,
)

logger = logging.getLogger(__name__)

# Mark all tests in this module as distributed tests
pytestmark = pytest.mark.distributed


class TestElasticScaling:
    """Tests for elastic worker scaling with WorkQueue.

    WorkQueue model:
    - Single queue per stage, multiple workers claim messages
    - No explicit partition assignment - workers compete for messages
    - Claimed messages have lease timeout for failure recovery
    - Exactly-once semantics via atomic ack_and_forward
    """

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
    async def test_scale_up_during_processing(self, ray_cluster):
        """Scale up: additional workers should help process messages faster.

        In WorkQueue model, new workers simply start claiming from the queue.
        No rebalancing needed - they compete for available messages.
        """
        NUM_RECORDS = 2000
        FILTER_MODULO = 3
        FILTER_REMAINDER = 0
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=200,
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
        workers_added = 0
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            master = runner._masters.get("transform")
            assert master is not None, "Transform master not found"

            # Wait for initial progress
            await wait_for_progress(
                runner, min_processed=100, timeout=30, collector_name=self.collector_name
            )

            # Scale up: spawn additional workers
            initial_count = len(master._workers) if master._workers else 0
            if master._worker_manager and not master._finished:
                for _ in range(3):
                    try:
                        await master._worker_manager.spawn_worker(is_min_worker=False)
                        workers_added += 1
                    except Exception:
                        pass
                    await asyncio.sleep(0.1)

            logger.info(
                f"Scaled up from {initial_count} to {initial_count + workers_added} workers"
            )

            await asyncio.wait_for(run_task, timeout=60)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Exactly-once: correct count and no duplicates
        assert validator.verify_count(sink_data, expected_count), (
            f"Count mismatch after scale up: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates(sink_data), "Duplicates found after scale up"
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_scale_down_worker_failures(self, ray_cluster):
        """Scale down via worker failures: claimed messages should be recovered.

        When workers die, their claimed messages timeout and return to queue.
        Other workers or new workers will reclaim and process them.
        """
        NUM_RECORDS = 2000
        EXPLODE_FACTOR = 2
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=200,
            min_workers=4,
            max_workers=8,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        kills = 0
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Wait for processing to start
            await wait_for_progress(
                runner, min_processed=200, timeout=30, collector_name=self.collector_name
            )

            # Scale down: kill workers sequentially
            for _ in range(2):
                try:
                    if await kill_random_worker(runner, stage_id="transform"):
                        kills += 1
                except Exception:
                    pass
                await asyncio.sleep(0.5)

            logger.info(f"Killed {kills} workers")

            await asyncio.wait_for(run_task, timeout=60)
        finally:
            await runner.stop()

        assert kills > 0, "No workers were killed - test invalid"

        sink_data = get_sink_records(self.collector_name)

        # Exactly-once: correct count and no duplicates
        assert validator.verify_count(sink_data, expected_count), (
            f"Count mismatch after scale down: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates_composite(sink_data, ["id", "copy_idx"]), (
            "Duplicates found after scale down"
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_zero_worker_recovery(self, ray_cluster):
        """Zero worker recovery: kill all workers, system should auto-recover.

        StageMaster detects zero workers with pending messages and spawns new ones.
        Messages claimed by dead workers are recovered after claim_timeout.
        """
        NUM_RECORDS = 1500
        FILTER_MODULO = 5
        FILTER_REMAINDER = 0
        EXPLODE_FACTOR = 2
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_explode_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=300,
            min_workers=2,
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
            run_task = asyncio.create_task(runner.run())

            # Wait for processing to start
            await wait_for_progress(
                runner, min_processed=100, timeout=30, collector_name=self.collector_name
            )

            # Kill ALL transform workers
            master = runner._masters.get("transform")
            if master and master._workers:
                workers = list(master._workers.values())
                logger.info(f"Killing all {len(workers)} transform workers")
                for worker in workers:
                    try:
                        ray.kill(worker)
                    except Exception:
                        pass

            # StageMaster should detect this and spawn new workers
            # Wait for completion (includes recovery time)
            await asyncio.wait_for(run_task, timeout=90)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Exactly-once semantics
        assert validator.verify_count(sink_data, expected_count), (
            f"Count mismatch after zero-worker recovery: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates_composite(sink_data, ["id", "copy_idx"]), (
            "Duplicates found after zero-worker recovery"
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_concurrent_scale_up_and_failures(self, ray_cluster):
        """Concurrent scaling: add workers while others fail.

        Tests system stability when scaling up and experiencing failures
        simultaneously. Exactly-once semantics must be maintained.
        """
        NUM_RECORDS = 2000
        FILTER_MODULO = 4
        FILTER_REMAINDER = 0
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=200,
            min_workers=3,
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
        spawns = 0
        kills = 0

        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            master = runner._masters.get("transform")

            # Wait for initial progress
            await wait_for_progress(
                runner, min_processed=100, timeout=30, collector_name=self.collector_name
            )

            # Perform concurrent scale operations
            for _ in range(3):
                if run_task.done():
                    break

                # Scale up
                if master and master._worker_manager and not master._finished:
                    try:
                        await master._worker_manager.spawn_worker(is_min_worker=False)
                        spawns += 1
                    except Exception:
                        pass

                await asyncio.sleep(0.2)

                # Scale down (kill)
                if not master._finished:
                    try:
                        if await kill_random_worker(runner, stage_id="transform"):
                            kills += 1
                    except Exception:
                        pass

                await asyncio.sleep(0.3)

            logger.info(f"Spawned {spawns} workers, killed {kills} workers")

            await asyncio.wait_for(run_task, timeout=90)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Exactly-once semantics maintained during concurrent scaling
        assert validator.verify_count(sink_data, expected_count), (
            f"Count mismatch during concurrent scaling: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates(sink_data), (
            "Duplicates found during concurrent scaling"
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_multi_stage_elasticity(self, ray_cluster):
        """Multi-stage elasticity: failures in different stages.

        Tests that failures in one stage don't corrupt data flow to others.
        Each stage operates independently with its own workers and queue.
        """
        NUM_RECORDS = 5000  # More records to give time for worker kills
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
            batch_size=300,
            min_workers=2,
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
        kills_by_stage = {"source": 0, "transform": 0, "sink": 0}

        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Wait for pipeline to warm up
            await wait_for_progress(
                runner, min_processed=100, timeout=30, collector_name=self.collector_name
            )

            # Kill workers in different stages
            for stage in ["source", "transform", "sink"]:
                if run_task.done():
                    break
                try:
                    if await kill_random_worker(runner, stage_id=stage):
                        kills_by_stage[stage] += 1
                        logger.info(f"Killed worker in {stage}")
                except Exception as e:
                    logger.debug(f"Could not kill worker in {stage}: {e}")
                await asyncio.sleep(0.5)

            logger.info(f"Kills by stage: {kills_by_stage}")

            await asyncio.wait_for(run_task, timeout=90)
        finally:
            await runner.stop()

        total_kills = sum(kills_by_stage.values())
        assert total_kills > 0, "No workers killed - test invalid"

        sink_data = get_sink_records(self.collector_name)

        # Exactly-once across all stages
        assert validator.verify_count(sink_data, expected_count), (
            f"Count mismatch in multi-stage test: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates_composite(sink_data, ["id", "copy_idx"]), (
            "Duplicates found in multi-stage test"
        )
        assert validator.verify_checksums(source_data, sink_data)
