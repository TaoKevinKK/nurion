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

"""Ray-based end-to-end test for the video slice workflow.

Uses public HTTPS URLs for video files (no authentication required).
Parametrized over payload store backends: ray://, nvme://, nvme://+S3 (MinIO).

Local Debug Mode:
    Set VIDEO_CACHE_DIR environment variable to preserve output:

        export VIDEO_CACHE_DIR=~/.cache/solstice_test_videos
        pytest tests/test_video_workflow.py -v -m workflow
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

import lance
import pyarrow as pa
import pytest

logger = logging.getLogger("test")

# Public HTTPS endpoint (no auth required)
PUBLIC_VIDEO_URL = "https://pub-8bc1f1d3d1984bdfb056d0bc0bf97c3d.r2.dev/videos/raw"

# Video files available at the public endpoint
TEST_VIDEOS = [
    "-qwTw3PNXDE.mp4",
    "0wJO0eqVDho.mkv",
    "1UmhvUR_wtQ.mp4",
    "2R-gGLtYmdc.mp4",
    "3EIixA3E-rI.mp4",
    "3ETxXjGlxRo.mp4",
    "3WG6fgdFV74.mp4",
    "3jRDH1hSnpM.mp4",
    "4GIuKZbwl2w.mp4",
    "4kzJHyYtNhk.mp4",
]

# Cache directory for downloaded videos and debug output.
# Videos are downloaded once and reused across all parametrize variants.
LOCAL_CACHE_DIR = os.environ.get("VIDEO_CACHE_DIR")
VIDEO_DOWNLOAD_DIR = os.path.join(LOCAL_CACHE_DIR or "/tmp/nurion_video_cache", "raw")


def _ensure_videos_cached() -> str:
    """Download test videos to local cache (skips already-cached files).

    Returns the cache directory containing the raw video files.
    """
    import urllib.request

    os.makedirs(VIDEO_DOWNLOAD_DIR, exist_ok=True)
    opener = urllib.request.build_opener()
    opener.addheaders = [("User-Agent", "nurion-test/1.0")]

    for video in TEST_VIDEOS:
        local_path = os.path.join(VIDEO_DOWNLOAD_DIR, video)
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            continue
        url = f"{PUBLIC_VIDEO_URL}/{video}"
        logger.info(f"Downloading {video} ...")
        tmp_path = local_path + ".tmp"
        with opener.open(url) as resp, open(tmp_path, "wb") as f:
            shutil.copyfileobj(resp, f)
        os.rename(tmp_path, local_path)
        logger.info(f"  cached → {local_path} ({os.path.getsize(local_path)} bytes)")
    return VIDEO_DOWNLOAD_DIR


def create_test_lance_table(table_path: str, video_dir: str | None = None) -> None:
    """Create a local Lance table with video paths for testing.

    If *video_dir* is given, ``video_path`` points to local cached files;
    otherwise it falls back to remote HTTPS URLs.
    """
    records = []
    for i, video in enumerate(TEST_VIDEOS):
        video_url = f"{PUBLIC_VIDEO_URL}/{video}"
        slug = video.rsplit(".", 1)[0]

        if video_dir:
            video_path = os.path.join(video_dir, video)
        else:
            video_path = video_url

        records.append(
            {
                "global_index": i,
                "video_uid": slug,
                "source_url": video_url,
                "video_path": video_path,
                "subset": "train" if i < 8 else "validation",
            }
        )

    table = pa.Table.from_pylist(records)
    lance.write_dataset(table, table_path, mode="overwrite")
    logger.info(f"Created test Lance table at {table_path} with {len(records)} videos")


@dataclass
class _StoreConfig:
    """Payload store config + optional cleanup path (separated to avoid leaking
    the private ``_nvme_cleanup_dir`` key into production config dicts)."""

    config: dict  # payload_store_uri + payload_store_options
    cleanup_dir: str | None = None


def _build_store_config(store_type: str, request) -> _StoreConfig:
    """Return payload store config for *store_type*."""
    from tests.utils.container_helpers import minio_s3_options

    unique = hashlib.md5(f"{store_type}_{os.getpid()}".encode()).hexdigest()[:8]

    if store_type == "ray":
        return _StoreConfig(config={})

    nvme_dir = f"/tmp/nurion_video_nvme_{unique}"
    os.makedirs(nvme_dir, exist_ok=True)

    if store_type == "nvme":
        return _StoreConfig(
            config={"payload_store_uri": f"nvme://{nvme_dir}"},
            cleanup_dir=nvme_dir,
        )

    # nvme_s3: NVMe + real MinIO S3
    minio = request.getfixturevalue("minio_container")
    s3_options = minio_s3_options(minio)
    return _StoreConfig(
        config={
            "payload_store_uri": (
                f"nvme://{nvme_dir}"
                f"?s3_fallback=s3://warehouse/video-wf-{unique}"
                f"&write_policy=write_through"
            ),
            "payload_store_options": s3_options,
        },
        cleanup_dir=nvme_dir,
    )


@pytest.mark.workflow
@pytest.mark.timeout(900)  # 15 minutes for video processing
@pytest.mark.parametrize("store_type", ["ray", "nvme", "nvme_s3"])
def test_video_slice_workflow_with_ray(ray_cluster, store_type, request):
    """Verify scene detection, slicing, filtering, and hashing on public videos.

    Parametrized over payload store backends:
        ray     — default Ray Object Store
        nvme    — NVMe SSD (local disk, WRITE_BACK)
        nvme_s3 — NVMe + real S3 via MinIO container (WRITE_THROUGH)

    Creates a local Lance table with 10 public video URLs, split_size=2 for 5 splits.
    Videos are downloaded once to a local cache and reused across all variants.
    """
    # Pre-download videos (cached across parametrize variants)
    video_dir = _ensure_videos_cached()

    sc = _build_store_config(store_type, request)
    store_cfg, nvme_cleanup = sc.config, sc.cleanup_dir

    # In local debug mode, use cache directory for output (preserved after test)
    # Otherwise use temp directory (cleaned up after test)
    if LOCAL_CACHE_DIR:
        cache_dir = Path(LOCAL_CACHE_DIR).expanduser()
        cache_dir.mkdir(parents=True, exist_ok=True)
        tmp_dir = str(cache_dir / f"test_output_{store_type}")
        Path(tmp_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Local debug mode: output will be preserved in {tmp_dir}")
    else:
        tmp_dir = tempfile.mkdtemp(prefix=f"video_workflow_{store_type}_")

    input_table_path = os.path.join(tmp_dir, "input_videos.lance")
    output_path = Path(tmp_dir) / "hashed_slices.lance"

    try:
        # Create local Lance table with public video URLs
        create_test_lance_table(input_table_path, video_dir=video_dir)

        # Verify table was created
        ds = lance.dataset(input_table_path)
        logger.info(f"Test dataset has {ds.count_rows()} rows")
        assert ds.count_rows() == 10, f"Expected 10 rows, got {ds.count_rows()}"

        from workflows.video_slice_workflow import create_job

        filter_modulo = 4  # Keep every 4th slice

        config = {
            "input": input_table_path,
            "output": str(output_path),
            "output_format": "lance",
            "filter_modulo": filter_modulo,
            "scene_threshold": 0.4,
            "split_size": 2,  # 2 rows per split = 5 splits for 10 videos
            "workqueue_db_path": "memory://",  # Use memory for WorkQueue
            # Elastic worker counts (min=2, max=4) to test multi-worker scenarios
            # with resource backoff on limited CPU environments
            "scene_parallelism": (2, 4),
            "slice_parallelism": (2, 4),
            "filter_parallelism": (2, 4),
            "hash_parallelism": (2, 4),
            "sink_buffer_size": 16,
            # Low CPU/memory for local testing (4 CPU machine)
            "worker_num_cpus": 0.25,  # 0.25 CPU per worker = 16 workers max on 4 CPUs
            "worker_memory_mb": 256,  # 256MB per worker
            # Payload store configuration (injected by parametrize)
            **store_cfg,
        }

        job = create_job(job_id=f"video_slice_{store_type}", config=config)

        # Ray already initialized by ray_cluster fixture with correct excludes
        runner = job.create_ray_runner()

        async def run_pipeline():
            try:
                await runner.run(timeout=600)
            finally:
                await runner.stop()

        asyncio.run(run_pipeline())

        assert output_path.exists(), f"Output path {output_path} does not exist"
        result_ds = lance.dataset(str(output_path))
        rows = result_ds.to_table().to_pylist()

        logger.info(f"Output has {len(rows)} rows (store={store_type})")
        assert rows, "Expected filtered slice payloads"

        for row in rows:
            # Check hash
            digest = row.get("slice_sha256")
            assert isinstance(digest, str) and len(digest) == 64, f"Invalid hash: {digest}"
            # Check filter modulo
            assert int(row["global_slice_rank"]) % filter_modulo == 0
            # Check binary slice data
            slice_binary = row.get("slice_binary")
            assert slice_binary is not None, "Missing slice_binary"
            assert len(slice_binary) > 0, "Empty slice_binary"

        logger.info(f"Test passed with {len(rows)} output slices (store={store_type})")

    finally:
        # Cleanup - skip in local debug mode to preserve output
        if LOCAL_CACHE_DIR:
            logger.info(f"Local debug mode: output preserved at {output_path}")
        elif Path(tmp_dir).exists():
            shutil.rmtree(tmp_dir)
        if nvme_cleanup:
            shutil.rmtree(nvme_cleanup, ignore_errors=True)
