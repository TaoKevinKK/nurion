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

"""
Video Slicing Workflow

This workflow extracts frames from videos at a specified FPS rate.

DAG structure:
    Source (Lance) -> VideoSlice (FlatMap) -> Sink (Lance)

Input: Lance table with `data_paths` field containing JSON like:
    {"mkv": "s3://bucket/path/to/video.mkv"}

Output: Lance table with columns:
    - frame_index: int (0-based index within video)
    - frame_timestamp: float (timestamp in seconds)
    - image: bytes (JPEG encoded frame)
    - original_video_path: str (source video path)
"""

import asyncio
import io
import json
import logging
import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Type

import pyarrow as pa

from solstice.core.job import Job, JobConfig, WebUIConfig
from solstice.core.models import Split, SplitPayload
from solstice.core.operator import Operator, OperatorConfig
from solstice.core.stage import Stage
from solstice.operators.sinks import LanceSinkConfig
from solstice.operators.sources import LanceTableSourceConfig
from solstice.queue import QueueType
from solstice.utils.remote import ensure_local_file, is_remote_path, restore_s3_object

_OUTPUT_SCHEMA = pa.schema(
    [
        pa.field("frame_index", pa.int64()),
        pa.field("frame_timestamp", pa.float64()),
        pa.field("image", pa.binary()),
        pa.field("original_video_path", pa.string()),
    ]
)
def _log_s3_head(bucket: str) -> None:
    """Best-effort S3 head check with short timeouts."""
    logger = logging.getLogger(__name__)
    try:
        import boto3
        from botocore.config import Config

        client = boto3.client(
            "s3",
            config=Config(connect_timeout=3, read_timeout=3, retries={"max_attempts": 1}),
        )
        client.head_bucket(Bucket=bucket)
        logger.info(f"S3 head bucket ok: {bucket}")
    except Exception as e:
        logger.warning(f"S3 head bucket failed for {bucket}: {e}")



def _check_file_exists(path: str) -> bool:
    """Check if a file exists (supports both local and S3 paths)."""
    if is_remote_path(path):
        # For S3, try to get file info
        try:
            import fsspec
            from solstice.utils.remote import get_s3_storage_options

            storage_options = get_s3_storage_options() if path.startswith("s3://") else {}
            fs = fsspec.filesystem(
                path.split("://")[0],
                **storage_options,
            )
            return fs.exists(path)
        except Exception:
            return False
    else:
        return Path(path).exists()


def _is_glacier_access_error(exc: Exception) -> bool:
    """Check if an exception indicates Glacier/archived access restrictions."""
    message = str(exc).lower()
    return (
        "invalidobjectstate" in message
        or "access tier" in message
        or "glacier" in message
        or "deep archive" in message
    )


def _is_ffmpeg_decode_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "ebml header parsing failed" in message
        or "invalid data found when processing input" in message
        or "ffmpeg failed" in message
    )


def _extract_frames_with_retry(
    video_path: str,
    fps: float,
    jpeg_quality: int,
    use_cache: bool,
) -> List[Dict[str, Any]]:
    try:
        with ensure_local_file(video_path, use_cache=use_cache) as local_path:
            if local_path.stat().st_size == 0:
                raise RuntimeError(f"Empty video file: {video_path}")
            return _extract_frames_at_fps(local_path, fps=fps, quality=jpeg_quality)
    except Exception as e:
        if video_path.startswith("s3://") and _is_ffmpeg_decode_error(e):
            # Retry without cache to avoid corrupted cache artifacts.
            with ensure_local_file(video_path, use_cache=False) as local_path:
                if local_path.stat().st_size == 0:
                    raise RuntimeError(f"Empty video file: {video_path}")
                return _extract_frames_at_fps(local_path, fps=fps, quality=jpeg_quality)
        raise


def _extract_frames_at_fps(
    video_path: Path,
    fps: float,
    output_format: str = "jpeg",
    quality: int = 95,
) -> List[Dict[str, Any]]:
    """Extract frames from video at specified FPS using ffmpeg.
    
    Args:
        video_path: Path to the video file
        fps: Frames per second to extract
        output_format: Output image format (jpeg, png)
        quality: JPEG quality (1-100)
    
    Returns:
        List of dicts with frame_index, frame_timestamp, image (bytes)
    """
    logger = logging.getLogger(__name__)
    
    # Create temp directory for frames
    with tempfile.TemporaryDirectory() as tmpdir:
        output_pattern = Path(tmpdir) / "frame_%06d.jpg"
        
        # Use ffmpeg to extract frames at specified fps
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "error",
            "-i", str(video_path),
            "-vf", f"fps={fps}",
            "-q:v", str(max(1, min(31, 32 - int(quality * 31 / 100)))),  # JPEG quality (1=best, 31=worst)
            "-f", "image2",
            str(output_pattern),
        ]
        
        logger.debug(f"Running ffmpeg: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        
        if result.returncode != 0:
            logger.error(f"ffmpeg failed: {result.stderr}")
            raise RuntimeError(f"ffmpeg failed: {result.stderr}")
        
        # Read extracted frames
        frames = []
        frame_files = sorted(Path(tmpdir).glob("frame_*.jpg"))
        
        for idx, frame_file in enumerate(frame_files):
            timestamp = idx / fps  # Calculate timestamp based on fps
            
            with open(frame_file, "rb") as f:
                image_bytes = f.read()
            
            frames.append({
                "frame_index": idx,
                "frame_timestamp": round(timestamp, 3),
                "image": image_bytes,
            })
        
        logger.debug(f"Extracted {len(frames)} frames from {video_path}")
        return frames


@dataclass
class VideoSliceConfig(OperatorConfig):
    """Configuration for VideoSliceOperator."""
    
    fps: float = 2.0
    """Frames per second to extract."""
    
    video_path_field: str = "data_paths"
    """Field containing video path (JSON string or direct path)."""
    
    video_path_json_key: str = "mkv"
    """Key in JSON to extract video path (if video_path_field is JSON)."""
    
    skip_missing_videos: bool = True
    """If True, skip videos that don't exist instead of raising error."""
    
    jpeg_quality: int = 95
    """JPEG quality for extracted frames (1-100)."""
    
    use_cache: bool = False
    """Whether to cache downloaded remote videos locally."""

    max_rows: Optional[int] = None
    """Maximum rows to process (for testing). None = no limit."""
    
    operator_class: ClassVar[Type["VideoSliceOperator"]]


class VideoSliceOperator(Operator):
    """Operator that extracts frames from videos at specified FPS.
    
    Each input row (video) produces multiple output rows (frames).
    """
    
    def __init__(self, config: VideoSliceConfig):
        super().__init__(config)
        self.fps = config.fps
        self.video_path_field = config.video_path_field
        self.video_path_json_key = config.video_path_json_key
        self.skip_missing = config.skip_missing_videos
        self.jpeg_quality = config.jpeg_quality
        self.use_cache = config.use_cache
        self.max_rows = config.max_rows
        self._processed_count = 0  # Track processed rows
    
    def _get_video_path(self, row: Dict[str, Any]) -> Optional[str]:
        """Extract video path from row."""
        value = row.get(self.video_path_field)
        if not value:
            return None
        
        # Try to parse as JSON
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, dict):
                    return parsed.get(self.video_path_json_key)
            except json.JSONDecodeError:
                # Not JSON, treat as direct path
                return value
        
        return str(value) if value else None
    
    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        if payload is None:
            raise ValueError("VideoSliceOperator requires a payload")
        
        rows = payload.to_table().to_pylist()
        
        # Apply max_rows limit if configured
        if self.max_rows is not None:
            remaining = self.max_rows - self._processed_count
            if remaining <= 0:
                self.logger.info(f"Reached max_rows limit ({self.max_rows}), skipping split")
                return None
            rows = rows[:remaining]
        
        self.logger.info(f"Processing {len(rows)} videos for split {split.split_id}")
        
        output_records: List[Dict[str, Any]] = []
        
        for row_idx, row in enumerate(rows):
            video_path = self._get_video_path(row)
            
            if not video_path:
                self.logger.warning(f"Row {row_idx}: No video path found, skipping")
                continue
            
            # Check if video exists
            if not _check_file_exists(video_path):
                if self.skip_missing:
                    self.logger.warning(f"Video not found, skipping: {video_path}")
                    continue
                else:
                    raise FileNotFoundError(f"Video not found: {video_path}")
            
            try:
                frames = _extract_frames_with_retry(
                    video_path,
                    fps=self.fps,
                    jpeg_quality=self.jpeg_quality,
                    use_cache=self.use_cache,
                )
                for frame in frames:
                    image_bytes = frame["image"]
                    if isinstance(image_bytes, memoryview):
                        image_bytes = image_bytes.tobytes()
                    if not isinstance(image_bytes, (bytes, bytearray)):
                        raise ValueError(
                            f"Expected binary image bytes, got {type(image_bytes)}"
                        )
                    output_records.append(
                        {
                            "frame_index": frame["frame_index"],
                            "frame_timestamp": frame["frame_timestamp"],
                            "image": bytes(image_bytes),
                            "original_video_path": video_path,
                        }
                    )

                self.logger.info(
                    f"Extracted {len(frames)} frames from {video_path}"
                )
                    
            except Exception as e:
                if self.skip_missing:
                    if _is_glacier_access_error(e) and video_path.startswith("s3://"):
                        restored = restore_s3_object(video_path, days=2)
                        if restored:
                            self.logger.warning(
                                f"Requested Glacier restore (2 days) for {video_path}"
                            )
                        else:
                            self.logger.warning(
                                f"Glacier restore already in progress or not needed for {video_path}"
                            )
                        continue
                    self.logger.error(f"Error processing {video_path}: {e}, skipping")
                    continue
                else:
                    raise
        
        # Update processed count
        self._processed_count += len(rows)
        
        if not output_records:
            self.logger.warning(f"No frames extracted for split {split.split_id}")
            return None
        
        self.logger.info(
            f"Produced {len(output_records)} frames for split {payload.split_id}"
        )
        
        return SplitPayload.from_arrow(
            pa.Table.from_pylist(output_records, schema=_OUTPUT_SCHEMA),
            split_id=f"{payload.split_id}:video-slice-{self.worker_id}",
        )


# Link config to operator class
VideoSliceConfig.operator_class = VideoSliceOperator


def create_job(
    job_id: str,
    config: Dict[str, Any],
) -> Job:
    """
    Create a video slicing job.
    
    DAG structure:
        Source (Lance) -> VideoSlice -> Sink (Lance)
    
    Required config parameters:
        - input: Input Lance table path (required)
        - output: Output Lance table path (required)
    
    Optional config parameters:
        - fps: Frames per second to extract (default: 2.0)
        - source_parallelism: Number of source workers reading Lance (default: 4)
        - slice_parallelism: Number of slice workers (default: 150)
        - split_size: Number of rows per split from source (default: 10)
        - max_rows: Maximum rows to process, for testing (default: None = unlimited)
        - video_path_field: Field containing video path (default: "data_paths")
        - video_path_json_key: JSON key for video path (default: "mkv")
        - skip_missing_videos: Skip missing videos (default: True)
        - jpeg_quality: JPEG quality 1-100 (default: 95)
        - use_cache: Cache downloaded remote videos locally (default: False)
        - sink_parallelism: Number of sink workers (default: auto)
        - ray_address: Ray cluster address (default: "ray://localhost:8265")
        - webui_storage_path: SlateDB root path for WebUI (optional)
    
    Args:
        job_id: Unique job identifier
        config: Job configuration dictionary
    
    Returns:
        Configured Job instance
    """
    logger = logging.getLogger(__name__)
    logger.info("Creating Video Slice job")
    
    # Validate required parameters
    input_path = config.get("input")
    output_path = config.get("output")
    
    if not input_path:
        raise ValueError("'input' parameter is required (Lance table path)")
    if not output_path:
        raise ValueError("'output' parameter is required (output path)")
    
    # Extract optional parameters with defaults
    fps = config.get("fps", 2.0)
    source_parallelism = config.get("source_parallelism", 4)  # Parallel Lance readers
    slice_parallelism = config.get("slice_parallelism", 150)
    split_size = config.get("split_size", 10)  # Smaller splits for video processing
    max_rows = config.get("max_rows")  # None = unlimited
    video_path_field = config.get("video_path_field", "data_paths")
    video_path_json_key = config.get("video_path_json_key", "mkv")
    skip_missing_videos = config.get("skip_missing_videos", True)
    jpeg_quality = config.get("jpeg_quality", 95)
    use_cache = config.get("use_cache", False)
    webui_storage_path = config.get("webui_storage_path")
    sink_parallelism = config.get("sink_parallelism", 0)
    if not sink_parallelism:
        sink_parallelism = max(4, min(32, slice_parallelism // 4))
    
    # Ray init kwargs - use "auto" to connect to existing cluster
    # when running as a Ray job, the cluster is already initialized
    ray_init_kwargs = {
        "address": "auto",
    }
    
    # Create job with configuration
    # Use TANSU queue for distributed execution on Ray cluster
    job = Job(
        job_id=job_id,
        config=JobConfig(
            queue_type=QueueType.TANSU,
            ray_init_kwargs=ray_init_kwargs,
            webui=WebUIConfig(
                enabled=True,
                storage_path=webui_storage_path or WebUIConfig.storage_path,
            ),
        ),
    )
    
    # Stage 1: Source - Read from Lance table
    # Multiple source workers to read Lance fragments in parallel
    source_stage = Stage(
        stage_id="source",
        operator_config=LanceTableSourceConfig(
            dataset_uri=input_path,
            split_size=split_size,
            max_rows=max_rows,  # Limit at source level for efficiency
        ),
        parallelism=source_parallelism,  # Parallel Lance readers
        output_partitions=slice_parallelism,  # Match downstream for parallel consumption
        worker_resources={
            "num_cpus": 1,
            "memory": 4 * 1024**3,
        },
    )
    
    # Stage 2: VideoSlice - Extract frames at specified FPS
    slice_stage = Stage(
        stage_id="video_slice",
        operator_config=VideoSliceConfig(
            fps=fps,
            video_path_field=video_path_field,
            video_path_json_key=video_path_json_key,
            skip_missing_videos=skip_missing_videos,
            jpeg_quality=jpeg_quality,
            use_cache=use_cache,
            max_rows=max_rows,
        ),
        parallelism=slice_parallelism,
        worker_resources={
            "num_cpus": 2,
            "memory": 8 * 1024**3,  # 8GB per worker for video processing
        },
    )
    
    # Stage 3: Sink - Write to Lance table
    sink_stage = Stage(
        stage_id="sink",
        operator_config=LanceSinkConfig(
            table_path=output_path,
            buffer_size=10000,
            blob_columns=[],  # Inline binary bytes for image column
        ),
        parallelism=sink_parallelism,
        worker_resources={
            "num_cpus": 1,
            "memory": 4 * 1024**3,
        },
    )
    
    # Build DAG: Source -> VideoSlice -> Sink
    job.add_stage(source_stage)
    job.add_stage(slice_stage, upstream_stages=["source"])
    job.add_stage(sink_stage, upstream_stages=["video_slice"])
    
    logger.info(f"Created Video Slice job with {len(job.stages)} stages")
    logger.info(
        f"FPS: {fps}, Source parallelism: {source_parallelism}, "
        f"Slice parallelism: {slice_parallelism}, Sink parallelism: {sink_parallelism}"
    )
    logger.info(f"Input: {input_path}")
    logger.info(f"Output: {output_path}")
    
    return job


async def run_video_slice_job(
    input_path: str,
    output_path: str,
    fps: float = 2.0,
    source_parallelism: int = 4,
    slice_parallelism: int = 150,
    sink_parallelism: int = 0,
    ray_address: str = "ray://localhost:8265",
    webui_storage_path: Optional[str] = None,
    **kwargs,
) -> None:
    """
    Convenience function to run a video slicing job.
    
    Args:
        input_path: Input Lance table path
        output_path: Output Lance table path
        fps: Frames per second to extract
        source_parallelism: Number of source workers reading Lance
        slice_parallelism: Number of slice workers
        sink_parallelism: Number of sink workers (0 = auto)
        ray_address: Ray cluster address
        **kwargs: Additional config options (see create_job)
    
    Example:
        >>> import asyncio
        >>> asyncio.run(run_video_slice_job(
        ...     input_path="s3://bucket/videos.lance",
        ...     output_path="s3://bucket/frames.lance",
        ...     fps=2.0,
        ...     source_parallelism=4,
        ...     slice_parallelism=8,
        ... ))
    """
    import uuid
    
    config = {
        "input": input_path,
        "output": output_path,
        "fps": fps,
        "source_parallelism": source_parallelism,
        "slice_parallelism": slice_parallelism,
        "sink_parallelism": sink_parallelism,
        "ray_address": ray_address,
        "webui_storage_path": webui_storage_path,
        **kwargs,
    }
    
    job_id = f"video_slice_{uuid.uuid4().hex[:8]}"
    job = create_job(job_id, config)
    
    runner = job.create_ray_runner()
    await runner.run()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Video Slicing Workflow")
    parser.add_argument("--input", required=False, help="Input Lance table path")
    parser.add_argument("--output", required=False, help="Output Lance table path")
    parser.add_argument("--fps", type=float, default=2.0, help="Frames per second")
    parser.add_argument("--source-parallelism", type=int, default=4, help="Source workers")
    parser.add_argument("--slice-parallelism", type=int, default=150, help="Slice workers")
    parser.add_argument(
        "--sink-parallelism",
        type=int,
        default=0,
        help="Sink workers (0=auto)",
    )
    parser.add_argument("--split-size", type=int, default=10, help="Rows per split")
    parser.add_argument("--max-rows", type=int, help="Max rows to process (for testing)")
    parser.add_argument("--ray-address", default="ray://localhost:8265", help="Ray cluster")
    parser.add_argument("--video-path-field", default="data_paths", help="Video path field")
    parser.add_argument("--video-path-json-key", default="mkv", help="JSON key for path")
    parser.add_argument("--jpeg-quality", type=int, default=95, help="JPEG quality (1-100)")
    parser.add_argument("--no-skip-missing", action="store_true", help="Fail on missing videos")
    parser.add_argument(
        "--test-video-path",
        default=None,
        help="Run a single-video ffmpeg test and exit",
    )
    parser.add_argument(
        "--webui-storage-path",
        default=None,
        help="SlateDB root path for WebUI (e.g. s3://bucket/solstice/)",
    )
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info("AWS_ACCESS_KEY_ID set: %s", bool(os.getenv("AWS_ACCESS_KEY_ID")))
    logger.info("AWS_ENDPOINT_URL: %s", os.getenv("AWS_ENDPOINT_URL"))
    if args.webui_storage_path and args.webui_storage_path.startswith("s3://"):
        bucket = args.webui_storage_path[5:].split("/", 1)[0]
        _log_s3_head(bucket)
    
    if args.test_video_path:
        try:
            frames = _extract_frames_with_retry(
                args.test_video_path, fps=args.fps, jpeg_quality=args.jpeg_quality
            )
            logger.info(
                f"Test extracted {len(frames)} frames from {args.test_video_path}"
            )
        except Exception as e:
            if _is_glacier_access_error(e) and args.test_video_path.startswith("s3://"):
                restored = restore_s3_object(args.test_video_path, days=2)
                if restored:
                    logger.warning(
                        f"Requested Glacier restore (2 days) for {args.test_video_path}"
                    )
                else:
                    logger.warning(
                        f"Glacier restore already in progress or not needed for {args.test_video_path}"
                    )
            raise
        raise SystemExit(0)

    if not args.input or not args.output:
        parser.error("--input and --output are required unless --test-video-path is set")

    asyncio.run(
        run_video_slice_job(
            input_path=args.input,
            output_path=args.output,
            fps=args.fps,
            source_parallelism=args.source_parallelism,
            slice_parallelism=args.slice_parallelism,
            sink_parallelism=args.sink_parallelism,
            split_size=args.split_size,
            max_rows=args.max_rows,
            ray_address=args.ray_address,
            video_path_field=args.video_path_field,
            video_path_json_key=args.video_path_json_key,
            jpeg_quality=args.jpeg_quality,
            skip_missing_videos=not args.no_skip_missing,
            webui_storage_path=args.webui_storage_path,
        )
    )
