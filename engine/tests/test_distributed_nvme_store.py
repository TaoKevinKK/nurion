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

"""Distributed tests for NvmeSplitPayloadStore.

Layer A: End-to-end pipeline tests on a local Ray cluster with NVMe store.
         Verifies that payload_loc flows through messages and Flight reads work
         across real Ray actors (not mocks).

Layer B: Multi-container cluster tests with node failure injection.
         Uses testcontainers for real network isolation and container kill/restart.

All tests use real Ray clusters and Anvil brokers (no mocks).
"""

import asyncio
import hashlib
import logging
import os
import shutil

import pytest
import ray

from _internal.runtime.ray_runner import RayJobRunner

from tests.utils import (
    create_collector,
    create_test_pipeline,
    get_sink_records,
    wait_for_progress,
)

logger = logging.getLogger(__name__)

# Mark all tests as distributed (excluded from default CI fast tests)
pytestmark = pytest.mark.distributed


# ===========================================================================
# Layer A: End-to-end pipeline with NVMe store on local Ray cluster
# ===========================================================================


class TestNvmeStoreEndToEnd:
    """Run complete source -> transform -> sink pipelines using NVMe store.

    These tests verify that:
    - NvmeSplitPayloadStore works as a drop-in replacement for RaySplitPayloadStore
    - payload_loc metadata flows through DataQueueMessage correctly
    - Arrow Flight cross-actor reads work within a Ray cluster
    - Data integrity is maintained (all records arrive at sink)
    """

    @pytest.fixture(autouse=True)
    async def setup(self, ray_cluster, request):
        """Per-test setup: unique collector + NVMe temp dir.

        Uses /tmp instead of pytest tmp_path to avoid macOS $TMPDIR symlink
        issues with long paths that can break os.rename across processes.
        """
        test_name = request.node.name.replace("[", "_").replace("]", "_")
        unique = hashlib.md5(test_name.encode()).hexdigest()[:8]
        self.collector_name = f"nvme_collector_{unique}"
        self.nvme_dir = f"/tmp/nurion_test_nvme_{unique}"
        os.makedirs(self.nvme_dir, exist_ok=True)
        create_collector(self.collector_name)
        yield
        try:
            collector = ray.get_actor(self.collector_name)
            ray.kill(collector)
        except Exception:
            pass
        shutil.rmtree(self.nvme_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_pipeline_write_back(self, ray_cluster):
        """Full pipeline with WRITE_BACK policy — NVMe only, no S3."""
        NUM_RECORDS = 200

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=50,
            max_workers=2,
            collector_name=self.collector_name,
            payload_store_uri=f"nvme://{self.nvme_dir}",
        )

        runner = RayJobRunner(job)
        await runner.run()

        records = get_sink_records(self.collector_name)
        assert len(records) == NUM_RECORDS, f"Expected {NUM_RECORDS} records, got {len(records)}"

    @pytest.mark.asyncio
    async def test_pipeline_write_through_with_s3(self, ray_cluster):
        """Full pipeline with WRITE_THROUGH + S3 fallback (file:// as S3 mock)."""
        NUM_RECORDS = 200
        s3_dir = f"{self.nvme_dir}_s3"
        os.makedirs(s3_dir, exist_ok=True)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=50,
            max_workers=2,
            collector_name=self.collector_name,
            payload_store_uri=(
                f"nvme://{self.nvme_dir}?s3_fallback=file://{s3_dir}&write_policy=write_through"
            ),
        )

        runner = RayJobRunner(job)
        await runner.run()

        records = get_sink_records(self.collector_name)
        assert len(records) == NUM_RECORDS

        # S3 directory should have payload files (WRITE_THROUGH guarantees S3 writes)
        s3_files = []
        for dirpath, dirnames, filenames in os.walk(s3_dir):
            s3_files.extend(f for f in filenames if f.endswith(".arrow"))
        assert len(s3_files) > 0, "WRITE_THROUGH should have written payloads to S3"

    @pytest.mark.asyncio
    async def test_pipeline_data_integrity(self, ray_cluster):
        """Verify all records arrive at sink with correct count."""
        NUM_RECORDS = 500

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=100,
            max_workers=2,
            collector_name=self.collector_name,
            payload_store_uri=f"nvme://{self.nvme_dir}",
        )

        runner = RayJobRunner(job)
        await runner.run()

        records = get_sink_records(self.collector_name)
        assert len(records) == NUM_RECORDS, (
            f"Data integrity check failed: expected {NUM_RECORDS}, got {len(records)}"
        )

    @pytest.mark.asyncio
    async def test_pipeline_with_multi_disk(self, ray_cluster):
        """Pipeline using multi-disk NVMe pool."""
        NUM_RECORDS = 200
        d0 = f"{self.nvme_dir}_disk0"
        d1 = f"{self.nvme_dir}_disk1"
        os.makedirs(d0, exist_ok=True)
        os.makedirs(d1, exist_ok=True)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=50,
            max_workers=2,
            collector_name=self.collector_name,
            payload_store_uri=f"nvme://{d0},{d1}",
        )

        runner = RayJobRunner(job)
        await runner.run()

        records = get_sink_records(self.collector_name)
        assert len(records) == NUM_RECORDS

    @pytest.mark.asyncio
    async def test_cleanup_after_job(self, ray_cluster):
        """NVMe files should be cleaned up after job completes."""
        job = create_test_pipeline(
            num_records=100,
            batch_size=50,
            max_workers=1,
            collector_name=self.collector_name,
            payload_store_uri=f"nvme://{self.nvme_dir}",
        )

        runner = RayJobRunner(job)
        await runner.run()

        # After job.run() completes, payload store should have called clear().
        # Check that job directory is empty or removed.
        job_dirs = [
            d for d in os.listdir(self.nvme_dir) if os.path.isdir(os.path.join(self.nvme_dir, d))
        ]
        for jd in job_dirs:
            arrow_files = list(
                f
                for dp, dn, fn in os.walk(os.path.join(self.nvme_dir, jd))
                for f in fn
                if f.endswith(".arrow")
            )
            assert len(arrow_files) == 0, f"Leftover files in {jd}: {arrow_files}"


# ===========================================================================
# Layer A+: Worker failure with NVMe store
# ===========================================================================


class TestNvmeStoreWorkerFailure:
    """Test worker failure recovery when using NVMe store.

    Verifies that:
    - Worker crash → claimed messages nacked → re-processed by new worker
    - Payloads stored by crashed worker remain readable (NVMe persists)
    - Pipeline completes despite worker failures
    """

    @pytest.fixture(autouse=True)
    async def setup(self, ray_cluster, request):
        test_name = request.node.name.replace("[", "_").replace("]", "_")
        unique = hashlib.md5(test_name.encode()).hexdigest()[:8]
        self.collector_name = f"nvme_fail_{unique}"
        self.nvme_dir = f"/tmp/nurion_test_nvme_fail_{unique}"
        os.makedirs(self.nvme_dir, exist_ok=True)
        create_collector(self.collector_name)
        yield
        try:
            collector = ray.get_actor(self.collector_name)
            ray.kill(collector)
        except Exception:
            pass
        shutil.rmtree(self.nvme_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_worker_kill_recovery(self, ray_cluster):
        """Kill a transform worker mid-pipeline, verify pipeline recovers."""
        from tests.utils import kill_random_worker, wait_for_stage_workers

        NUM_RECORDS = 500

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=50,
            min_workers=2,
            max_workers=3,
            collector_name=self.collector_name,
            payload_store_uri=f"nvme://{self.nvme_dir}",
            claim_timeout_secs=2.0,
            recovery_interval_secs=0.5,
        )

        runner = RayJobRunner(job)
        run_task = asyncio.create_task(runner.run())

        # Wait for some progress, then kill a worker
        await wait_for_progress(
            runner, min_processed=100, timeout=30, collector_name=self.collector_name
        )

        killed = kill_random_worker(runner, stage_id="transform")
        if killed:
            logger.info(f"Killed worker: {killed}")
            # Wait for replacement worker
            await wait_for_stage_workers(runner, "transform", min_workers=2, timeout=15)

        # Wait for pipeline to complete
        await asyncio.wait_for(run_task, timeout=120)

        records = get_sink_records(self.collector_name)
        assert len(records) == NUM_RECORDS, (
            f"Expected {NUM_RECORDS} records after worker failure, got {len(records)}"
        )
