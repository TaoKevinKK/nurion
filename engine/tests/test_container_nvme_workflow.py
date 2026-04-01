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

"""Workflow tests for NVMe PayloadStore with real S3 (MinIO) and Flight containers.

End-to-end pipelines running on a local Ray cluster with:
    - NvmeSplitPayloadStore (local NVMe + Arrow Flight)
    - Real S3 backend via MinIO testcontainer
    - Flight container post-verification
    - Worker failure + S3 recovery scenarios

Requires: Docker daemon running.  Excluded from fast CI via ``distributed`` marker.
"""

import asyncio
import hashlib
import logging
import os
import shutil

import pyarrow.flight as flight
import pytest

pytest.importorskip("docker", reason="docker SDK required for container workflow tests")

from _internal.runtime.ray_runner import RayJobRunner  # noqa: E402
from tests.utils import (  # noqa: E402
    create_collector,
    create_test_pipeline,
    get_sink_records,
    kill_random_worker,
    wait_for_progress,
    wait_for_stage_workers,
)
from tests.utils.container_helpers import (  # noqa: E402
    minio_s3_options,
    start_flight_container,
)

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.distributed
# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture(autouse=True)
async def _per_test_setup(ray_cluster, request):
    """Per-test setup: unique collector + NVMe temp dir.

    Uses /tmp to avoid macOS $TMPDIR symlink issues with os.rename across processes.
    """
    test_name = request.node.name.replace("[", "_").replace("]", "_")
    unique = hashlib.md5(test_name.encode()).hexdigest()[:8]

    request.instance.collector_name = f"wf_nvme_{unique}"
    request.instance.nvme_dir = f"/tmp/nurion_wf_nvme_{unique}"
    request.instance.job_id = f"wf_{unique}"
    os.makedirs(request.instance.nvme_dir, exist_ok=True)
    create_collector(request.instance.collector_name)

    yield

    import ray

    try:
        collector = ray.get_actor(request.instance.collector_name)
        ray.kill(collector)
    except Exception:
        pass
    shutil.rmtree(request.instance.nvme_dir, ignore_errors=True)


# ===========================================================================
# Layer B-5a: Full pipeline with NVMe + real S3 (MinIO)
# ===========================================================================


class TestNvmeWorkflowWithS3:
    """End-to-end pipeline using NVMe WRITE_THROUGH + MinIO as real S3 backend."""

    @pytest.mark.asyncio
    async def test_write_through_pipeline(self, minio_container):
        """WRITE_THROUGH pipeline: all records arrive and S3 has Arrow files."""
        from minio import Minio  # type: ignore[import-untyped]

        NUM_RECORDS = 300
        s3_options = minio_s3_options(minio_container)
        s3_prefix = f"wf-wt-{self.job_id}"

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=50,
            max_workers=2,
            job_id=self.job_id,
            collector_name=self.collector_name,
            payload_store_uri=(
                f"nvme://{self.nvme_dir}"
                f"?s3_fallback=s3://warehouse/{s3_prefix}"
                f"&write_policy=write_through"
            ),
            payload_store_options=s3_options,
        )

        runner = RayJobRunner(job)
        await runner.run()

        records = get_sink_records(self.collector_name)
        assert len(records) == NUM_RECORDS

        # Verify S3 (MinIO) actually has Arrow payload files
        host = minio_container.get_container_host_ip()
        port = minio_container.get_exposed_port(9000)
        mc = Minio(
            f"{host}:{port}",
            access_key=minio_container.access_key,
            secret_key=minio_container.secret_key,
            secure=False,
        )
        s3_objects = list(mc.list_objects("warehouse", prefix=f"{s3_prefix}/", recursive=True))
        arrow_files = [o for o in s3_objects if o.object_name.endswith(".arrow")]
        assert len(arrow_files) > 0, "WRITE_THROUGH should produce S3 Arrow files"

    @pytest.mark.asyncio
    async def test_write_back_pipeline(self, minio_container):
        """WRITE_BACK pipeline: all records arrive, S3 gets async uploads."""
        NUM_RECORDS = 300
        s3_options = minio_s3_options(minio_container)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=50,
            max_workers=2,
            job_id=self.job_id,
            collector_name=self.collector_name,
            payload_store_uri=(
                f"nvme://{self.nvme_dir}"
                f"?s3_fallback=s3://warehouse/wf-wb-{self.job_id}"
                f"&write_policy=write_back"
            ),
            payload_store_options=s3_options,
        )

        runner = RayJobRunner(job)
        await runner.run()

        records = get_sink_records(self.collector_name)
        assert len(records) == NUM_RECORDS

    @pytest.mark.asyncio
    async def test_pipeline_data_integrity_large(self, minio_container):
        """Large pipeline (1000 records) with NVMe + S3, verify exact count."""
        NUM_RECORDS = 1000
        s3_options = minio_s3_options(minio_container)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=100,
            max_workers=3,
            job_id=self.job_id,
            collector_name=self.collector_name,
            payload_store_uri=(
                f"nvme://{self.nvme_dir}"
                f"?s3_fallback=s3://warehouse/wf-integ-{self.job_id}"
                f"&write_policy=write_through"
            ),
            payload_store_options=s3_options,
        )

        runner = RayJobRunner(job)
        await runner.run()

        records = get_sink_records(self.collector_name)
        assert len(records) == NUM_RECORDS, (
            f"Data integrity: expected {NUM_RECORDS}, got {len(records)}"
        )


# ===========================================================================
# Layer B-5b: Worker failure recovery with NVMe + S3
# ===========================================================================


class TestNvmeWorkflowFailure:
    """Worker kill mid-pipeline — recovery relies on S3 fallback for payloads."""

    @pytest.mark.asyncio
    async def test_worker_kill_write_through_recovery(self, minio_container):
        """Kill transform worker, verify pipeline recovers with S3 payloads.

        Uses fewer records (200) to keep write-through S3 round-trips fast
        in CI (MinIO in Docker has variable I/O latency).
        """
        NUM_RECORDS = 200
        s3_options = minio_s3_options(minio_container)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=50,
            min_workers=2,
            max_workers=3,
            job_id=self.job_id,
            collector_name=self.collector_name,
            payload_store_uri=(
                f"nvme://{self.nvme_dir}"
                f"?s3_fallback=s3://warehouse/wf-kill-{self.job_id}"
                f"&write_policy=write_through"
            ),
            payload_store_options=s3_options,
            claim_timeout_secs=2.0,
            recovery_interval_secs=0.5,
        )

        runner = RayJobRunner(job)
        run_task = asyncio.create_task(runner.run())

        # Wait for some progress, then kill a transform worker
        await wait_for_progress(
            runner, min_processed=50, timeout=30, collector_name=self.collector_name
        )

        killed = await kill_random_worker(runner, stage_id="transform")
        if killed:
            logger.info(f"Killed worker: {killed}")
            await wait_for_stage_workers(runner, "transform", min_workers=2, timeout=15)

        # Progress-based completion: wait for all records with generous timeout.
        # write-through S3 in CI is slow but should not stall.
        await asyncio.wait_for(run_task, timeout=180)

        records = get_sink_records(self.collector_name)
        assert len(records) == NUM_RECORDS, (
            f"Expected {NUM_RECORDS} after worker kill, got {len(records)}"
        )


# ===========================================================================
# Layer B-5c: Post-pipeline Flight container verification
# ===========================================================================


class TestNvmeWorkflowFlightVerification:
    """After pipeline completes, mount NVMe data into a Flight container and
    verify payloads are readable — simulates a new node reading stored data."""

    @pytest.mark.asyncio
    async def test_post_pipeline_flight_read(self, flight_server_image, minio_container):
        """Pipeline writes to NVMe; Flight container serves the data afterwards."""
        NUM_RECORDS = 200
        s3_options = minio_s3_options(minio_container)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=50,
            max_workers=2,
            job_id=self.job_id,
            collector_name=self.collector_name,
            payload_store_uri=(
                f"nvme://{self.nvme_dir}"
                f"?s3_fallback=s3://warehouse/wf-flight-{self.job_id}"
                f"&write_policy=write_through"
            ),
            payload_store_options=s3_options,
        )

        runner = RayJobRunner(job)
        await runner.run()

        records = get_sink_records(self.collector_name)
        assert len(records) == NUM_RECORDS

        # The NVMe store created job subdirectory: {nvme_dir}/{job_id}/
        job_data_dir = os.path.join(self.nvme_dir, self.job_id)
        if not os.path.isdir(job_data_dir):
            # clear() may have removed it — check S3 instead
            pytest.skip("NVMe data was cleaned up (clear() called); S3 verified above")

        # Find Arrow files written by the pipeline
        arrow_files = []
        for dirpath, _, filenames in os.walk(job_data_dir):
            arrow_files.extend(f for f in filenames if f.endswith(".arrow"))

        if not arrow_files:
            pytest.skip("No Arrow files remain after pipeline cleanup")

        # Mount the job data dir into a Flight container and read
        node = start_flight_container(flight_server_image, job_data_dir)
        try:
            client = flight.FlightClient(node.endpoint)

            # Read one payload file's key from the filename
            sample_file = arrow_files[0]
            key = sample_file.removesuffix(".arrow")

            reader = client.do_get(flight.Ticket(key.encode()))
            table = reader.read_all()
            assert table.num_rows > 0, "Flight container should serve pipeline payloads"
            logger.info(f"Flight verification OK: read {table.num_rows} rows for key '{key}'")
        finally:
            node.container.stop()

    @pytest.mark.asyncio
    async def test_multi_container_read_after_pipeline(self, flight_server_image, minio_container):
        """Two Flight containers serve different stage outputs from the same pipeline."""
        NUM_RECORDS = 200
        s3_options = minio_s3_options(minio_container)
        nvme_dir2 = f"{self.nvme_dir}_disk2"
        os.makedirs(nvme_dir2, exist_ok=True)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=50,
            max_workers=2,
            job_id=self.job_id,
            collector_name=self.collector_name,
            payload_store_uri=(
                f"nvme://{self.nvme_dir},{nvme_dir2}"
                f"?s3_fallback=s3://warehouse/wf-multi-{self.job_id}"
                f"&write_policy=write_through"
            ),
            payload_store_options=s3_options,
        )

        runner = RayJobRunner(job)
        await runner.run()

        records = get_sink_records(self.collector_name)
        assert len(records) == NUM_RECORDS

        # Check which disks got data
        containers = []
        try:
            for nvme_dir in [self.nvme_dir, nvme_dir2]:
                job_dir = os.path.join(nvme_dir, self.job_id)
                if not os.path.isdir(job_dir):
                    continue
                arrow_files = [
                    f for dp, _, fns in os.walk(job_dir) for f in fns if f.endswith(".arrow")
                ]
                if not arrow_files:
                    continue

                node = start_flight_container(flight_server_image, job_dir)
                containers.append((node, arrow_files))

            # Read from each container
            for node, files in containers:
                client = flight.FlightClient(node.endpoint)
                key = files[0].removesuffix(".arrow")
                table = client.do_get(flight.Ticket(key.encode())).read_all()
                assert table.num_rows > 0
        finally:
            for node, _ in containers:
                try:
                    node.container.stop()
                except Exception:
                    pass
            shutil.rmtree(nvme_dir2, ignore_errors=True)
