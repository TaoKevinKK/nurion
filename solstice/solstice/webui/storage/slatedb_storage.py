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

"""SlateDB storage backend for WebUI data persistence."""

import json
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

from solstice.utils.logging import create_ray_logger


def _parse_s3_path(path: str) -> Tuple[str, str]:
    """Parse s3://bucket/prefix into (bucket, prefix)."""
    without_scheme = path[5:]
    bucket, _, prefix = without_scheme.partition("/")
    return bucket, prefix


def _write_s3_env_file(bucket: str) -> str:
    """Write a .env file for SlateDB S3 config and return its path."""
    # SlateDB uses object_store::AmazonS3Builder::from_env which only
    # reads AWS_* uppercase variables.
    lines = ["CLOUD_PROVIDER=aws", f"AWS_BUCKET={bucket}"]

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    default_region = os.getenv("AWS_DEFAULT_REGION")
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    imds_disabled = os.getenv("AWS_EC2_METADATA_DISABLED")
    shared_credentials = os.getenv("AWS_SHARED_CREDENTIALS_FILE")
    profile = os.getenv("AWS_PROFILE")

    if access_key:
        lines.append(f"AWS_ACCESS_KEY_ID={access_key}")
    if secret_key:
        lines.append(f"AWS_SECRET_ACCESS_KEY={secret_key}")
    if session_token:
        lines.append(f"AWS_SESSION_TOKEN={session_token}")
    if region:
        lines.append(f"AWS_REGION={region}")
    if default_region:
        lines.append(f"AWS_DEFAULT_REGION={default_region}")
    if endpoint:
        lines.append(f"AWS_ENDPOINT_URL={endpoint}")
    if imds_disabled:
        lines.append(f"AWS_EC2_METADATA_DISABLED={imds_disabled}")
    if shared_credentials:
        lines.append(f"AWS_SHARED_CREDENTIALS_FILE={shared_credentials}")
    if profile:
        lines.append(f"AWS_PROFILE={profile}")

    env_file = tempfile.NamedTemporaryFile(
        mode="w",
        delete=False,
        prefix="slatedb_s3_",
        suffix=".env",
    )
    env_file.write("\n".join(lines) + "\n")
    env_file.flush()
    env_file.close()
    return env_file.name


def _get_settings_path() -> Optional[str]:
    """Resolve SlateDB settings file path if configured."""
    override = os.getenv("SOLSTICE_SLATEDB_SETTINGS")
    if override:
        return override
    default_path = Path(__file__).with_name("slatedb_settings.json")
    if default_path.exists():
        return str(default_path)
    return None


def _create_slatedb_reader(path: str):
    """Create a SlateDBReader for the given path."""
    from slatedb import SlateDBReader

    if path.startswith("s3://"):
        bucket, prefix = _parse_s3_path(path)
        env_file = _write_s3_env_file(bucket)
        db_path = prefix or "slatedb"
        return SlateDBReader(db_path, env_file=env_file)
    return SlateDBReader("db", url=f"file://{path}/")


class JobStorage:
    """Per-job SlateDB storage for writing WebUI data.

    Each running job creates its own JobStorage instance to write metrics,
    events, and archives. This ensures SlateDB's single-writer constraint
    is satisfied.

    Storage path format: {base_path}/{job_id}/{attempt_id}/
    - Local: /tmp/solstice-webui/my_job/20250101_120000_abc1/
    - S3: s3://bucket/solstice/my_job/20250101_120000_abc1/

    Key schema (no job_id in keys since path already contains job_id):
    - job -> JobArchive JSON (single entry per SlateDB instance)
    - metrics:{stage_id}:{timestamp} -> Metrics JSON
    - exception:{exception_id} -> Exception JSON
    - lineage:{split_id} -> Lineage JSON
    - worker:{worker_id} -> Worker history JSON
    - worker_event:{worker_id}:{timestamp} -> Event JSON
    - ray_event:{event_id} -> Ray event JSON

    See also: PortalStorage for read-only access across all jobs.
    """

    def __init__(
        self,
        path: str = "/tmp/solstice-webui/",
        db: Any | None = None,
        job_id: Optional[str] = None,
    ):
        """Initialize SlateDB storage.

        Args:
            path: Storage path (local or S3)
                - Local: /tmp/solstice-webui/
                - S3: s3://bucket/path/
            db: Optional pre-created DB handle (reader or writer)
            job_id: Optional job_id override for read-only usage
        """
        self.path = path
        self.logger = create_ray_logger("JobStorage")
        self._job_id_override = job_id
        self._read_only = db is not None

        if db is not None:
            self.db = db
            self.logger.info(f"Initialized read-only storage at {path}")
            return

        from slatedb import SlateDB

        if path.startswith("s3://"):
            bucket, prefix = _parse_s3_path(path)
            env_file = _write_s3_env_file(bucket)
            db_path = prefix or "slatedb"
            settings_path = _get_settings_path()
            self.db = SlateDB(db_path, env_file=env_file, settings=settings_path)
        else:
            Path(path).mkdir(parents=True, exist_ok=True)
            url = f"file://{path}/"
            settings_path = _get_settings_path()
            self.db = SlateDB("db", url=url, settings=settings_path)
        self.logger.info(f"Initialized SlateDB storage at {path}")

    # === Job Configuration ===

    def store_configuration(self, config_data: Dict[str, Any]) -> None:
        """Store job configuration.

        Should be called at job start with complete configuration.

        Args:
            config_data: Configuration dictionary with:
                - job_config: Job-level settings (job_id, queue_type, etc.)
                - stage_configs: Per-stage settings (operator_type, parallelism, etc.)
                - environment: Environment variables
        """
        key = "config"
        self.db.put(key.encode(), json.dumps(config_data).encode())
        self.db.flush()
        self.logger.debug("Stored job configuration")

    def _get_configuration_data(self) -> Optional[Dict[str, Any]]:
        """Retrieve raw configuration from this storage."""
        key = "config"
        data = self.db.get(key.encode())
        if data:
            result: Dict[str, Any] = json.loads(data.decode())
            return result
        return None

    def _get_job_archive_data(self) -> Optional[Dict[str, Any]]:
        """Retrieve raw job archive from this storage."""
        key = "job"
        data = self.db.get(key.encode())
        if data:
            result: Dict[str, Any] = json.loads(data.decode())
            return result
        return None

    def _resolve_job_id(self) -> Optional[str]:
        """Best-effort job_id resolution from stored data."""
        if self._job_id_override:
            return self._job_id_override
        job_data = self._get_job_archive_data()
        if job_data:
            return job_data.get("job_id")
        config_data = self._get_configuration_data()
        if config_data:
            return config_data.get("job_config", {}).get("job_id")
        return None

    def _matches_job_id(self, job_id: Optional[str]) -> bool:
        if job_id is None:
            return True
        return self._resolve_job_id() == job_id

    def get_configuration(self, job_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Retrieve job configuration from this storage."""
        if not self._matches_job_id(job_id):
            return None
        config_data = self._get_configuration_data()
        if config_data:
            return config_data
        job_archive = self._get_job_archive_data()
        if not job_archive:
            return None
        return self._extract_config_from_archive(job_archive)

    def _extract_config_from_archive(self, job_archive: Dict[str, Any]) -> Dict[str, Any]:
        """Extract configuration from job archive data."""
        result: Dict[str, Any] = {
            "job_config": job_archive.get("config", {}),
            "stage_configs": {},
            "environment": {},
        }

        for stage in job_archive.get("stages", []):
            stage_id = stage.get("stage_id", "")
            if stage_id:
                result["stage_configs"][stage_id] = {
                    "operator_type": stage.get("operator_type", "N/A"),
                    "min_parallelism": stage.get("min_parallelism", 1),
                    "max_parallelism": stage.get("max_parallelism", 1),
                    "num_cpus": stage.get("num_cpus", 0),
                    "num_gpus": stage.get("num_gpus", 0),
                    "memory_mb": stage.get("memory_mb", 0),
                }

        return result

    def flush(self) -> None:
        """Flush pending writes to storage."""
        self.db.flush()

    def close(self) -> None:
        """Close underlying DB handle if supported."""
        close_fn = getattr(self.db, "close", None)
        if callable(close_fn):
            close_fn()

    # === Job Archive ===

    def store_job_archive(self, archive_data: Dict[str, Any]) -> None:
        """Store archived job data."""
        key = "job"
        self.db.put(key.encode(), json.dumps(archive_data).encode())

        # Flush to ensure data is persisted to disk
        self.db.flush()

        status = archive_data.get("status", "UNKNOWN")
        job_id = archive_data.get("job_id", "unknown")
        self.logger.info(f"Archived job {job_id} with status {status}")

    def get_job_archive(self, job_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Retrieve archived job data from this storage."""
        if not self._matches_job_id(job_id):
            return None
        return self._get_job_archive_data()

    def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List jobs for this storage (single job)."""
        job_data = self._get_job_archive_data()
        if not job_data:
            return []
        if status and job_data.get("status") != status:
            return []
        return [job_data][offset : offset + limit]

    def _scan_prefix(self, prefix: bytes, limit: int = 1000) -> List[tuple]:
        """Scan keys with prefix using SlateDB scan API.

        Args:
            prefix: Key prefix to scan
            limit: Maximum number of results

        Returns:
            List of (key, value) tuples
        """
        results = []
        for key, value in self.db.scan(prefix):
            results.append((key, value))
            if len(results) >= limit:
                break
        return results

    # === Metrics Snapshots ===

    def store_metrics_snapshot(
        self,
        stage_id: str,
        timestamp: float,
        metrics: Dict[str, Any],
    ) -> None:
        """Store a metrics snapshot."""
        key = f"metrics:{stage_id}:{int(timestamp)}"
        # Ensure timestamp is included in the data
        data = {**metrics, "timestamp": timestamp}
        self.db.put(key.encode(), json.dumps(data).encode())
        self.logger.debug(f"Stored metrics snapshot for {stage_id}")

    def get_metrics_history(
        self,
        job_id: Optional[str],
        stage_id: str,
        start_time: float,
        end_time: float,
    ) -> List[Dict[str, Any]]:
        """Query metrics history for a stage.

        Aggregates from split metrics stored by JobStateManager.
        """
        if not self._matches_job_id(job_id):
            return []

        # First try legacy metrics:{stage_id}: format
        prefix = f"metrics:{stage_id}:"
        results = self._scan_prefix(prefix.encode())

        metrics_list = []
        for key, value in results:
            parts = key.decode().split(":")
            if len(parts) >= 3:
                ts = float(parts[2])
                if start_time <= ts <= end_time:
                    metrics_list.append(json.loads(value.decode()))

        if metrics_list:
            return sorted(metrics_list, key=lambda x: x.get("timestamp", 0))

        # Aggregate from split metrics: split:{stage_id}:{partition}:{offset}
        split_prefix = f"split:{stage_id}:"
        split_results = self._scan_prefix(split_prefix.encode())

        if not split_results:
            return []

        # Group by time buckets (10 second intervals)
        bucket_size = 10.0
        buckets: Dict[int, Dict[str, Any]] = {}

        for _, value in split_results:
            try:
                data = json.loads(value.decode())
                ts = data.get("ts", 0)
                if start_time <= ts <= end_time:
                    bucket_key = int(ts / bucket_size)
                    if bucket_key not in buckets:
                        buckets[bucket_key] = {
                            "timestamp": bucket_key * bucket_size,
                            "input_records": 0,
                            "output_records": 0,
                            "process_time_ms": 0,
                            "split_count": 0,
                        }
                    buckets[bucket_key]["input_records"] += data.get("input_records", 0)
                    buckets[bucket_key]["output_records"] += data.get("output_records", 0)
                    buckets[bucket_key]["process_time_ms"] += data.get("process_time_ms", 0)
                    buckets[bucket_key]["split_count"] += 1
            except Exception:
                continue

        return sorted(buckets.values(), key=lambda x: x.get("timestamp", 0))

    def get_latest_stage_metrics(self, stage_id: str) -> Optional[Dict[str, Any]]:
        """Get the best metrics snapshot for a stage.

        This returns the snapshot with the highest input_records + output_records,
        since later snapshots may show 0 after workers stop.

        Returns:
            Best metrics dict or None if no metrics found
        """
        prefix = f"metrics:{stage_id}:"
        results = self._scan_prefix(prefix.encode())

        if not results:
            return None

        # Find the snapshot with highest input + output records
        # (later snapshots may be 0 after workers stop)
        best = None
        best_total = -1
        for key, value in results:
            metrics = json.loads(value.decode())
            total = metrics.get("input_records", 0) + metrics.get("output_records", 0)
            if total > best_total:
                best_total = total
                best = metrics

        return best

    # === Exceptions ===

    def store_exception(
        self,
        exception_id: str,
        exception_data: Dict[str, Any],
    ) -> None:
        """Store exception data."""
        key = f"exception:{exception_id}"
        self.db.put(key.encode(), json.dumps(exception_data).encode())
        self.logger.debug(f"Stored exception {exception_id}")

    def list_exceptions(
        self,
        job_id: Optional[str],
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List exceptions in this storage."""
        if not self._matches_job_id(job_id):
            return []
        prefix = b"exception:"
        results = self._scan_prefix(prefix, limit=limit + offset)
        # Apply offset
        results = results[offset : offset + limit]
        return [json.loads(value.decode()) for _, value in results]

    # === Split Lineage ===

    def store_split_lineage(
        self,
        split_id: str,
        lineage_data: Dict[str, Any],
    ) -> None:
        """Store split lineage data."""
        key = f"lineage:{split_id}"
        self.db.put(key.encode(), json.dumps(lineage_data).encode())

        # Also create index by stage for efficient stage-scoped queries
        stage_id = lineage_data.get("stage_id", "")
        if stage_id:
            index_key = f"lineage_by_stage:{stage_id}:{split_id}"
            self.db.put(index_key.encode(), split_id.encode())

        self.logger.debug(f"Stored lineage for split {split_id}")

    def store_split_lineage_with_children(
        self,
        split_id: str,
        lineage_data: Dict[str, Any],
    ) -> None:
        """Store split lineage data and update parent→child indexes atomically.

        All writes are batched and flushed together to ensure consistency.
        SlateDB ensures atomicity of the flush operation.
        """
        # Batch all writes together before flush
        # Main lineage record
        main_key = f"lineage:{split_id}"
        self.db.put(main_key.encode(), json.dumps(lineage_data).encode())

        # Stage index
        stage_id = lineage_data.get("stage_id", "")
        if stage_id:
            index_key = f"lineage_by_stage:{stage_id}:{split_id}"
            self.db.put(index_key.encode(), split_id.encode())

        # Parent→child reverse indexes
        for parent_id in lineage_data.get("parent_split_ids", []):
            parent_index_key = f"lineage_by_parent:{parent_id}:{split_id}"
            self.db.put(parent_index_key.encode(), split_id.encode())

        # Flush all writes atomically
        self.db.flush()

        self.logger.debug(f"Stored lineage with indexes for split {split_id}")

    def get_split_lineage(self, job_id: Optional[str], split_id: str) -> Optional[Dict[str, Any]]:
        """Get split lineage data."""
        if not self._matches_job_id(job_id):
            return None
        key = f"lineage:{split_id}"
        data = self.db.get(key.encode())
        if data:
            result: Dict[str, Any] = json.loads(data.decode())
            return result
        return None

    def get_lineage_graph(self) -> Dict[str, Any]:
        """Get complete lineage graph.

        Returns:
            Graph data with nodes and edges
        """
        prefix = b"lineage:"
        results = self._scan_prefix(prefix)

        nodes = []
        edges = []

        for _, value in results:
            lineage = json.loads(value.decode())
            split_id = lineage["split_id"]

            # Add node
            nodes.append(
                {
                    "id": split_id,
                    "split_id": split_id,
                    "worker_id": lineage.get("worker_id"),
                    "timestamp": lineage.get("timestamp"),
                }
            )

            # Add edges from parents
            for parent_id in lineage.get("parent_split_ids", []):
                edges.append(
                    {
                        "source": parent_id,
                        "target": split_id,
                    }
                )

        return {
            "nodes": nodes,
            "edges": edges,
        }

    def list_splits_by_stage(
        self,
        job_id: Optional[str],
        stage_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List splits for a stage."""
        if not self._matches_job_id(job_id):
            return []
        prefix = f"lineage_by_stage:{stage_id}:".encode()
        splits: List[Dict[str, Any]] = []
        for _, split_id_bytes in self.db.scan(prefix):
            split_id = split_id_bytes.decode()
            lineage_data = self.db.get(f"lineage:{split_id}".encode())
            if lineage_data:
                splits.append(json.loads(lineage_data.decode()))
            if len(splits) >= offset + limit:
                break

        splits = sorted(splits, key=lambda x: x.get("timestamp", 0), reverse=True)
        return splits[offset : offset + limit]

    def get_lineage_overview(self, job_id: Optional[str] = None) -> Dict[str, Any]:
        """Get stage-level lineage overview with aggregated statistics.

        Returns:
            Dict with 'stages', 'edges', and 'dag_edges'
        """
        if not self._matches_job_id(job_id):
            return {"stages": [], "edges": [], "dag_edges": {}}
        job_data = self._get_job_archive_data()
        if not job_data:
            return {"stages": [], "edges": [], "dag_edges": {}}

        dag_edges = job_data.get("dag_edges", {})
        stages_list = job_data.get("stages", [])
        stage_order = [s.get("stage_id") for s in stages_list]

        # Collect all lineage records grouped by stage
        stage_splits: Dict[str, List[Dict[str, Any]]] = {}
        for _, value in self.db.scan(b"lineage:"):
            lineage = json.loads(value.decode())
            stage_id = lineage.get("stage_id", "")
            if stage_id not in stage_splits:
                stage_splits[stage_id] = []
            stage_splits[stage_id].append(lineage)

        # Calculate edge statistics
        edges = []
        for from_stage, to_stages in dag_edges.items():
            for to_stage in to_stages:
                to_splits = stage_splits.get(to_stage, [])
                if not to_splits:
                    edges.append(
                        {
                            "from_stage": from_stage,
                            "to_stage": to_stage,
                            "splits_count": 0,
                            "total_rows": 0,
                            "total_bytes": 0,
                        }
                    )
                    continue

                total_rows = sum(s.get("output_records", 0) for s in to_splits)
                total_bytes = sum(s.get("output_bytes", 0) for s in to_splits)
                rows_list = [s.get("output_records", 0) for s in to_splits]
                bytes_list = [s.get("output_bytes", 0) for s in to_splits]
                proc_times = [s.get("processing_time_ms", 0) for s in to_splits]

                edges.append(
                    {
                        "from_stage": from_stage,
                        "to_stage": to_stage,
                        "splits_count": len(to_splits),
                        "total_rows": total_rows,
                        "total_bytes": total_bytes,
                        "min_rows": min(rows_list) if rows_list else 0,
                        "max_rows": max(rows_list) if rows_list else 0,
                        "min_bytes": min(bytes_list) if bytes_list else 0,
                        "max_bytes": max(bytes_list) if bytes_list else 0,
                        "min_processing_ms": min(proc_times) if proc_times else 0,
                        "max_processing_ms": max(proc_times) if proc_times else 0,
                        "avg_processing_ms": sum(proc_times) / len(proc_times) if proc_times else 0,
                    }
                )

        # Stage stats
        stage_stats = []
        for stage_id in stage_order:
            splits = stage_splits.get(stage_id, [])
            if not splits:
                stage_stats.append(
                    {
                        "stage_id": stage_id,
                        "splits_count": 0,
                        "total_output_rows": 0,
                        "total_output_bytes": 0,
                    }
                )
                continue

            total_rows = sum(s.get("output_records", 0) for s in splits)
            total_bytes = sum(s.get("output_bytes", 0) for s in splits)

            stage_stats.append(
                {
                    "stage_id": stage_id,
                    "splits_count": len(splits),
                    "total_output_rows": total_rows,
                    "total_output_bytes": total_bytes,
                }
            )

        return {"stages": stage_stats, "edges": edges, "dag_edges": dag_edges}

    def get_split_trace(self, job_id: Optional[str], split_id: str) -> Dict[str, Any]:
        """Get complete lineage trace for a split (both upstream and downstream).

        Returns:
            Dict with 'splits' (ordered by stage), 'edges', and 'root_split_id'
        """
        if not self._matches_job_id(job_id):
            return {"splits": [], "edges": [], "root_split_id": split_id}

        visited: set = set()
        splits: list = []
        edges: list = []

        def collect_upstream(current_id: str) -> None:
            if current_id in visited:
                return
            visited.add(current_id)

            lineage_data = self.db.get(f"lineage:{current_id}".encode())
            if not lineage_data:
                return

            lineage = json.loads(lineage_data.decode())
            splits.append(lineage)

            for parent_id in lineage.get("parent_split_ids", []):
                edges.append({"source": parent_id, "target": current_id})
                collect_upstream(parent_id)

        def collect_downstream(current_id: str) -> None:
            if current_id in visited:
                return
            visited.add(current_id)

            lineage_data = self.db.get(f"lineage:{current_id}".encode())
            if not lineage_data:
                return

            lineage = json.loads(lineage_data.decode())
            if current_id not in [s.get("split_id") for s in splits]:
                splits.append(lineage)

            for _, child_id_bytes in self.db.scan(f"lineage_by_parent:{current_id}:".encode()):
                child_id = child_id_bytes.decode()
                edges.append({"source": current_id, "target": child_id})
                visited.discard(current_id)
                collect_downstream(child_id)

        collect_upstream(split_id)
        visited.clear()
        collect_downstream(split_id)

        # Sort splits by stage order
        job_data = self._get_job_archive_data()
        stage_order = {}
        if job_data:
            for i, s in enumerate(job_data.get("stages", [])):
                stage_order[s.get("stage_id")] = i

        splits.sort(key=lambda x: stage_order.get(x.get("stage_id"), 999))

        return {"splits": splits, "edges": edges, "root_split_id": split_id}

    # === Worker History ===

    def store_worker_history(
        self,
        worker_id: str,
        worker_data: Dict[str, Any],
    ) -> None:
        """Store worker history snapshot.

        Args:
            worker_id: Worker identifier
            worker_data: Worker data including:
                - stage_id: Stage the worker belongs to
                - status: RUNNING, COMPLETED, FAILED
                - start_time: When worker started
                - end_time: When worker finished (if completed)
                - input_records: Total input records processed
                - output_records: Total output records produced
                - processed_splits: List of split IDs processed
                - actor_id, node_id, pid: Ray actor info
        """
        key = f"worker:{worker_id}"
        self.db.put(key.encode(), json.dumps(worker_data).encode())
        self.logger.debug(f"Stored worker history for {worker_id}")

    def get_worker_history(self, job_id: Optional[str], worker_id: str) -> Optional[Dict[str, Any]]:
        """Get worker history."""
        if not self._matches_job_id(job_id):
            return None
        key = f"worker:{worker_id}"
        data = self.db.get(key.encode())
        if data:
            result: Dict[str, Any] = json.loads(data.decode())
            return result
        return None

    def list_workers(
        self,
        job_id: Optional[str],
        stage_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List all workers with optional filtering."""
        if not self._matches_job_id(job_id):
            return []
        prefix = b"worker:"
        results = self._scan_prefix(prefix, limit=1000)  # Get all workers
        workers = [json.loads(value.decode()) for _, value in results]

        # Filter by stage_id if specified
        if stage_id:
            workers = [w for w in workers if w.get("stage_id") == stage_id]

        # Filter by status if specified
        if status:
            workers = [w for w in workers if w.get("status") == status]

        # Sort by start_time descending (newest first)
        sorted_workers = sorted(workers, key=lambda x: x.get("start_time", 0), reverse=True)

        return sorted_workers[offset : offset + limit]

    # === Time-Series Metrics (Prometheus-style) ===

    def get_metrics_samples(
        self,
        job_id: Optional[str],
        worker_id: str,
        start_time: float,
        end_time: float,
    ) -> List[Dict[str, Any]]:
        """Query raw time-series samples for a worker.

        Aggregates from split metrics (split:{stage_id}:{partition}:{offset})
        filtered by worker_id.

        Args:
            job_id: Job identifier (for validation)
            worker_id: Worker identifier
            start_time: Start timestamp (Unix seconds)
            end_time: End timestamp (Unix seconds)

        Returns:
            List of samples sorted by timestamp
        """
        if not self._matches_job_id(job_id):
            return []

        # Get worker's stage_id for more efficient prefix scan
        worker_data = self.get_worker_history(job_id, worker_id)
        if worker_data:
            stage_id = worker_data.get("stage_id", "")
            prefix = f"split:{stage_id}:".encode() if stage_id else b"split:"
        else:
            prefix = b"split:"

        results = self._scan_prefix(prefix, limit=10000)

        samples = []
        for _, value in results:
            try:
                data = json.loads(value.decode())
                # Filter by worker_id
                if data.get("worker_id") != worker_id:
                    continue
                ts = data.get("ts", 0)
                if start_time <= ts <= end_time:
                    samples.append(data)
            except Exception:
                continue

        return sorted(samples, key=lambda x: x.get("ts", 0))

    def rate(
        self,
        job_id: Optional[str],
        worker_id: str,
        metric_name: str,
        time_range_s: float = 60.0,
    ) -> float:
        """Calculate rate for a metric from split data.

        Since split metrics are per-split increments (not cumulative counters),
        we sum all values in the time range and divide by the duration.

        rate = sum(values) / (last_ts - first_ts)

        Args:
            job_id: Job identifier
            worker_id: Worker identifier
            metric_name: Metric name (e.g., "input_records", "output_records")
                        Use "processed_count" to count splits processed.
            time_range_s: Time range to look back

        Returns:
            Rate per second, or 0.0 if insufficient data
        """
        now = time.time()
        samples = self.get_metrics_samples(job_id, worker_id, now - time_range_s, now)

        if len(samples) < 1:
            return 0.0

        # Get time range from samples
        first_ts = samples[0].get("ts", 0)
        last_ts = samples[-1].get("ts", 0)

        if last_ts <= first_ts:
            # Single sample or no time range - return 0
            return 0.0

        # Sum all values in the time range
        # For "processed_count", count the number of samples (each sample = 1 split)
        if metric_name == "processed_count":
            total = len(samples)
        else:
            total = sum(s.get(metric_name, 0) for s in samples)

        duration = last_ts - first_ts
        return total / duration if duration > 0 else 0.0

    def get_partition_offsets(
        self,
        job_id: Optional[str],
        stage_id: Optional[str] = None,
    ) -> Dict[str, Dict[int, int]]:
        """Get latest partition offsets for all workers (Gauge metric).

        Args:
            job_id: Job identifier
            stage_id: Optional stage filter

        Returns:
            Dict mapping worker_id -> {partition_id: offset}
        """
        if not self._matches_job_id(job_id):
            return {}

        # Get latest worker state from worker: prefix
        prefix = b"worker:"
        results = self._scan_prefix(prefix, limit=1000)

        offsets: Dict[str, Dict[int, int]] = {}
        for _, value in results:
            worker = json.loads(value.decode())
            if stage_id and worker.get("stage_id") != stage_id:
                continue
            worker_id = worker.get("worker_id", "")
            partition_offsets = worker.get("partition_offsets", {})
            if partition_offsets:
                offsets[worker_id] = {int(k): v for k, v in partition_offsets.items()}

        return offsets

    def get_throughput(
        self,
        job_id: Optional[str],
        stage_id: Optional[str] = None,
        time_range_s: float = 60.0,
    ) -> Dict[str, Any]:
        """Calculate throughput from split metrics.

        Args:
            job_id: Job identifier
            stage_id: Optional stage filter
            time_range_s: Time range for rate calculation

        Returns:
            Summary with per-worker and aggregated rates
        """
        if not self._matches_job_id(job_id):
            return {"workers": [], "total": {}}

        now = time.time()
        start_time = now - time_range_s

        # Get all workers
        prefix = b"worker:"
        results = self._scan_prefix(prefix, limit=1000)

        # Build worker info map
        worker_info: Dict[str, Dict[str, Any]] = {}
        for _, value in results:
            worker = json.loads(value.decode())
            if stage_id and worker.get("stage_id") != stage_id:
                continue
            worker_id = worker.get("worker_id", "")
            worker_info[worker_id] = {
                "stage_id": worker.get("stage_id"),
                "input_records": 0,
                "output_records": 0,
                "split_count": 0,
                "first_ts": now,
                "last_ts": start_time,
            }

        # Aggregate from split metrics
        if stage_id:
            split_prefix = f"split:{stage_id}:".encode()
        else:
            split_prefix = b"split:"

        split_results = self._scan_prefix(split_prefix, limit=10000)

        for _, value in split_results:
            try:
                data = json.loads(value.decode())
                ts = data.get("ts", 0)
                if ts < start_time:
                    continue

                worker_id = data.get("worker_id", "")
                if worker_id not in worker_info:
                    # Worker not in our filter, skip
                    continue

                info = worker_info[worker_id]
                info["input_records"] += data.get("input_records", 0)
                info["output_records"] += data.get("output_records", 0)
                info["split_count"] += 1
                info["first_ts"] = min(info["first_ts"], ts)
                info["last_ts"] = max(info["last_ts"], ts)
            except Exception:
                continue

        # Calculate rates
        workers = []
        total_input_rate = 0.0
        total_output_rate = 0.0
        total_splits_rate = 0.0

        for worker_id, info in worker_info.items():
            duration = info["last_ts"] - info["first_ts"]
            if duration > 0:
                input_rate = info["input_records"] / duration
                output_rate = info["output_records"] / duration
                splits_rate = info["split_count"] / duration
            else:
                input_rate = 0.0
                output_rate = 0.0
                splits_rate = 0.0

            workers.append(
                {
                    "worker_id": worker_id,
                    "stage_id": info["stage_id"],
                    "input_records_per_sec": input_rate,
                    "output_records_per_sec": output_rate,
                    "splits_per_sec": splits_rate,
                }
            )

            total_input_rate += input_rate
            total_output_rate += output_rate
            total_splits_rate += splits_rate

        return {
            "workers": workers,
            "total": {
                "input_records_per_sec": total_input_rate,
                "output_records_per_sec": total_output_rate,
                "splits_per_sec": total_splits_rate,
                "worker_count": len(workers),
            },
        }

    # === Worker Events ===

    def store_worker_event(
        self,
        worker_id: str,
        timestamp: float,
        event_data: Dict[str, Any],
    ) -> None:
        """Store worker lifecycle event."""
        key = f"worker_event:{worker_id}:{int(timestamp * 1000)}"
        self.db.put(key.encode(), json.dumps(event_data).encode())
        self.logger.debug(f"Stored worker event for {worker_id}")

    def list_worker_events(
        self,
        job_id: Optional[str],
        worker_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List worker events."""
        if not self._matches_job_id(job_id):
            return []
        if worker_id:
            prefix = f"worker_event:{worker_id}:"
        else:
            prefix = "worker_event:"

        results = self._scan_prefix(prefix.encode(), limit=limit + offset)
        events = [json.loads(value.decode()) for _, value in results]
        # Sort by timestamp descending (newest first)
        sorted_events = sorted(events, key=lambda x: x.get("timestamp", 0), reverse=True)
        # Apply offset and limit
        return sorted_events[offset : offset + limit]

    # === Ray Events ===

    def store_ray_event(
        self,
        event_id: str,
        event_data: Dict[str, Any],
    ) -> None:
        """Store Ray event."""
        key = f"ray_event:{event_id}"
        self.db.put(key.encode(), json.dumps(event_data).encode())

    def list_ray_events(
        self,
        event_types: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List Ray events."""
        prefix = b"ray_event:"

        # Fetch more to account for filtering and offset
        fetch_limit = (limit + offset) * 2 if event_types else (limit + offset)
        results = self._scan_prefix(prefix, limit=fetch_limit)
        events = [json.loads(value.decode()) for _, value in results]

        # Filter by event types if specified
        if event_types:
            events = [e for e in events if e.get("event_type") in event_types]

        # Sort by timestamp descending (newest first)
        sorted_events = sorted(events, key=lambda x: x.get("timestamp", 0), reverse=True)

        # Apply offset and limit
        return sorted_events[offset : offset + limit]


class PortalStorage:
    """Read-only storage for Portal to scan completed job archives."""

    def __init__(self, base_path: str):
        """Initialize portal storage.

        Args:
            base_path: Base storage path containing job directories.
                       e.g., /tmp/solstice-webui/ or s3://bucket/solstice/
        """
        self.base_path = base_path.rstrip("/")
        self.logger = create_ray_logger("PortalStorage")
        self._is_s3 = base_path.startswith("s3://")
        self._reader_cache: Dict[str, Tuple[str, JobStorage]] = {}
        self._s3_bucket: Optional[str] = None
        self._s3_prefix: str = ""
        self._s3_base_prefix: str = ""
        self._s3_client = None

        if self._is_s3:
            bucket, prefix = _parse_s3_path(self.base_path)
            self._s3_bucket = bucket
            self._s3_prefix = prefix.rstrip("/")
            self._s3_base_prefix = f"{self._s3_prefix}/" if self._s3_prefix else ""

        self.logger.info(f"PortalStorage initialized at {self.base_path}")

    def _get_s3_client(self):
        if self._s3_client:
            return self._s3_client
        import boto3

        region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
        endpoint = os.getenv("AWS_ENDPOINT_URL")
        self._s3_client = boto3.client(
            "s3",
            region_name=region,
            endpoint_url=endpoint,
        )
        return self._s3_client

    @contextmanager
    def _open_storage_for_path(
        self, job_id: str, attempt_path: str
    ) -> Generator[JobStorage, None, None]:
        cached = self._reader_cache.get(job_id)
        if cached and cached[0] == attempt_path:
            yield cached[1]
            return

        if cached:
            try:
                cached[1].close()
            except Exception:
                pass

        db = _create_slatedb_reader(attempt_path)
        storage = JobStorage(path=attempt_path, db=db, job_id=job_id)
        self._reader_cache[job_id] = (attempt_path, storage)
        yield storage

    @contextmanager
    def _open_job_storage(self, job_id: str) -> Generator[Optional[JobStorage], None, None]:
        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            yield None
            return
        with self._open_storage_for_path(job_id, str(latest_attempt)) as storage:
            yield storage

    def close(self) -> None:
        """Close any cached readers."""
        for _, reader in self._reader_cache.values():
            try:
                reader.close()
            except Exception:
                pass
        self._reader_cache.clear()

    def _list_s3_prefixes(self, prefix: str) -> List[str]:
        if not self._s3_bucket:
            return []
        s3 = self._get_s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        prefixes: List[str] = []
        for page in paginator.paginate(Bucket=self._s3_bucket, Prefix=prefix, Delimiter="/"):
            for item in page.get("CommonPrefixes", []):
                prefixes.append(item["Prefix"])
        return prefixes

    def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List archived jobs by scanning job directories."""
        if self._is_s3:
            jobs = self._list_jobs_s3(status)
        else:
            jobs = self._list_jobs_local(status)

        jobs.sort(key=lambda x: x.get("end_time") or 0, reverse=True)
        return jobs[offset : offset + limit]

    def _list_jobs_local(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List jobs from local filesystem."""
        jobs = []
        base_dir = Path(self.base_path)

        if not base_dir.exists():
            return []

        for job_dir in base_dir.iterdir():
            if not job_dir.is_dir():
                continue

            job_id = job_dir.name
            attempts = sorted(job_dir.iterdir(), reverse=True)
            if not attempts:
                continue

            latest_attempt = attempts[0]
            if not latest_attempt.is_dir():
                continue

            try:
                job_data = self._read_job_archive(str(latest_attempt), job_id)
                if job_data and (status is None or job_data.get("status") == status):
                    jobs.append(job_data)
            except Exception as e:
                self.logger.debug(f"Skipping job {job_id}: {e}")

        return jobs

    def _list_jobs_s3(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List jobs from S3 storage."""
        jobs = []
        job_prefixes = self._list_s3_prefixes(self._s3_base_prefix)

        for job_prefix in job_prefixes:
            job_id = job_prefix[len(self._s3_base_prefix) :].rstrip("/")
            latest_attempt = self._get_latest_attempt_path(job_id)
            if not latest_attempt:
                continue
            try:
                job_data = self._read_job_archive(latest_attempt, job_id)
                if job_data and (status is None or job_data.get("status") == status):
                    jobs.append(job_data)
            except Exception as e:
                self.logger.debug(f"Skipping job {job_id}: {e}")

        return jobs

    def _read_job_archive(self, attempt_path: str, job_id: str) -> Optional[Dict[str, Any]]:
        """Read job archive from an attempt directory using SlateDB."""
        with self._open_storage_for_path(job_id, attempt_path) as storage:
            return storage.get_job_archive(job_id)

    def get_job_archive(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get archived job data by job_id."""
        if self._is_s3:
            return self._get_job_archive_s3(job_id)
        return self._get_job_archive_local(job_id)

    def _get_job_archive_local(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job archive from local filesystem."""
        job_dir = Path(self.base_path) / job_id

        if not job_dir.exists():
            return None

        attempts = sorted(job_dir.iterdir(), reverse=True)
        if not attempts:
            return None

        latest_attempt = attempts[0]
        if not latest_attempt.is_dir():
            return None

        return self._read_job_archive(str(latest_attempt), job_id)

    def _get_job_archive_s3(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job archive from S3."""
        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            return None
        return self._read_job_archive(latest_attempt, job_id)

    def _get_latest_attempt_path(self, job_id: str) -> Optional[str]:
        """Get the path to the latest attempt directory for a job."""
        if self._is_s3:
            if not self._s3_bucket:
                return None
            job_prefix = f"{self._s3_base_prefix}{job_id}/"
            attempts = self._list_s3_prefixes(job_prefix)
            if not attempts:
                return None
            latest_attempt = sorted(attempts)[-1].rstrip("/")
            return f"s3://{self._s3_bucket}/{latest_attempt}"

        job_dir = Path(self.base_path) / job_id
        if not job_dir.exists():
            return None

        attempts = sorted(job_dir.iterdir(), reverse=True)
        if not attempts:
            return None

        latest_attempt = attempts[0]
        if not latest_attempt.is_dir():
            return None

        return str(latest_attempt)

    def get_configuration(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job configuration from storage."""
        with self._open_job_storage(job_id) as storage:
            if not storage:
                return None
            return storage.get_configuration(job_id)

    def list_exceptions(
        self,
        job_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List exceptions for a job by scanning its SlateDB."""
        with self._open_job_storage(job_id) as storage:
            if not storage:
                return []
            return storage.list_exceptions(job_id, limit=limit, offset=offset)

    def get_metrics_history(
        self,
        job_id: str,
        stage_id: str,
        start_time: float,
        end_time: float,
    ) -> List[Dict[str, Any]]:
        """Get metrics history for a stage."""
        with self._open_job_storage(job_id) as storage:
            if not storage:
                return []
            return storage.get_metrics_history(job_id, stage_id, start_time, end_time)

    def get_split_lineage(self, job_id: str, split_id: str) -> Optional[Dict[str, Any]]:
        """Get lineage for a specific split."""
        with self._open_job_storage(job_id) as storage:
            if not storage:
                return None
            return storage.get_split_lineage(job_id, split_id)

    def list_splits_by_stage(
        self,
        job_id: str,
        stage_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List splits for a stage."""
        with self._open_job_storage(job_id) as storage:
            if not storage:
                return []
            return storage.list_splits_by_stage(job_id, stage_id, limit, offset)

    def list_workers(
        self,
        job_id: str,
        stage_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List all workers for a job."""
        with self._open_job_storage(job_id) as storage:
            if not storage:
                return []
            return storage.list_workers(job_id, stage_id=stage_id, limit=limit, offset=offset)

    def get_worker_history(
        self,
        job_id: str,
        worker_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get worker history."""
        with self._open_job_storage(job_id) as storage:
            if not storage:
                return None
            return storage.get_worker_history(job_id, worker_id)

    def get_lineage_overview(self, job_id: str) -> Dict[str, Any]:
        """Get stage-level lineage overview with aggregated statistics."""
        with self._open_job_storage(job_id) as storage:
            if not storage:
                return {"stages": [], "edges": [], "dag_edges": {}}
            return storage.get_lineage_overview(job_id)

    def get_split_trace(self, job_id: str, split_id: str) -> Dict[str, Any]:
        """Get complete lineage trace for a split (both upstream and downstream)."""
        with self._open_job_storage(job_id) as storage:
            if not storage:
                return {"splits": [], "edges": [], "root_split_id": split_id}
            return storage.get_split_trace(job_id, split_id)

    def list_worker_events(
        self,
        job_id: str,
        worker_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List worker events for a job."""
        with self._open_job_storage(job_id) as storage:
            if not storage:
                return []
            return storage.list_worker_events(
                job_id, worker_id=worker_id, limit=limit, offset=offset
            )
