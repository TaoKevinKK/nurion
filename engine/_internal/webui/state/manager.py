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

"""Job state reader backed by WorkQueue storage (pyO3).

v2: Worker metadata read from persistent state (not event scanning).
    Serve models/workers read from serve namespace.
    O(N) scan methods removed.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from _internal.queue import WorkQueueStorageReader
from _internal.utils.logging import create_ray_logger
from _internal.webui.state.schema import (
    config_key,
    decode_json,
    job_key,
    job_namespace,
    jobs_namespace,
    parse_event_key,
    serve_namespace,
    split_key,
)


class JobStateManager:
    """Read-only state access for WebUI (no state queue, no SlateDB)."""

    def __init__(
        self, db_path: Optional[str] = None, storage: Optional[WorkQueueStorageReader] = None
    ):
        if storage is None:
            if db_path is None:
                raise ValueError("db_path is required when storage is not provided")
            storage = WorkQueueStorageReader(db_path)
        self.db_path = db_path or ""
        self._storage = storage
        self.logger = create_ray_logger("JobStateManager")

    def close(self) -> None:
        self._storage.close()

    # ---------------------------------------------------------------------
    # Job & Configuration
    # ---------------------------------------------------------------------

    def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        entries = self._storage.state_scan_prefix(
            jobs_namespace(),
            prefix="job:",
            limit=0,
        )
        jobs: List[Dict[str, Any]] = []
        for entry in entries:
            try:
                data = decode_json(entry["value"])
                if status and data.get("status") != status:
                    continue
                jobs.append(data)
            except Exception:
                continue
        jobs.sort(key=lambda x: x.get("start_time", 0), reverse=True)
        return jobs[offset : offset + limit]

    def get_job_archive(self, job_id: str) -> Optional[Dict[str, Any]]:
        namespace = job_namespace(job_id)
        data = self._storage.state_get_batch(namespace, [job_key(), config_key()])
        if job_key() not in data:
            return None
        job_data = decode_json(data[job_key()])
        if config_key() in data:
            job_data["config"] = decode_json(data[config_key()])

        stage_entries = self._storage.state_scan_prefix(namespace, prefix="stage:", limit=0)
        stage_state: Dict[str, Dict[str, Any]] = {}
        for entry in stage_entries:
            try:
                stage = decode_json(entry["value"])
                stage_id = stage.get("stage_id") or entry["key"].split(":", 1)[-1]
                stage_state[stage_id] = stage
            except Exception:
                continue

        stages = job_data.get("stages", [])
        for stage in stages:
            stage_id = stage.get("stage_id")
            if not stage_id:
                continue
            if stage_id in stage_state:
                stage.update(stage_state[stage_id])
            output_queue = f"{job_id}_{stage_id}_output"
            try:
                stats = self._storage.get_queue_stats(output_queue)
                stage["queue_stats"] = {
                    "pending_count": stats.pending_count,
                    "claimed_count": stats.claimed_count,
                    "total_pushed": stats.total_pushed,
                    "total_acked": stats.total_acked,
                }
            except Exception:
                stage["queue_stats"] = {
                    "pending_count": 0,
                    "claimed_count": 0,
                    "total_pushed": 0,
                    "total_acked": 0,
                }

        job_data["stages"] = stages
        return job_data

    def get_configuration(self, job_id: str) -> Optional[Dict[str, Any]]:
        namespace = job_namespace(job_id)
        data = self._storage.state_get_batch(namespace, [config_key()])
        if config_key() not in data:
            return None
        return decode_json(data[config_key()])

    # ---------------------------------------------------------------------
    # Events
    # ---------------------------------------------------------------------

    def list_events(
        self,
        job_id: str,
        stage_id: Optional[str] = None,
        worker_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """List events for a job with optional filters.

        Args:
            job_id: Job identifier
            stage_id: Filter by stage
            worker_id: Filter by worker
            event_type: Comma-separated event types (e.g. "ack,nack,timeout")
            limit: Maximum events to return
        """
        namespace = job_namespace(job_id)
        prefix = f"event:{stage_id}:" if stage_id else "event:"
        entries = self._storage.state_scan_prefix(namespace, prefix=prefix, limit=0)

        type_filter = set(event_type.split(",")) if event_type else None

        events: List[Dict[str, Any]] = []
        for entry in entries:
            try:
                event = decode_json(entry["value"])
            except Exception:
                continue
            parsed = parse_event_key(entry["key"])
            if parsed:
                parsed_stage_id, ts_ns, msg_id = parsed
                event.setdefault("timestamp_ns", ts_ns)
                event.setdefault("stage_id", parsed_stage_id)
                event.setdefault("msg_id", msg_id)
            if worker_id and event.get("worker_id") != worker_id:
                continue
            if type_filter and event.get("event_type") not in type_filter:
                continue
            events.append(event)

        # Merge timeout events from recovery (global namespace)
        if not type_filter or "timeout" in type_filter:
            try:
                job_data = self.get_job_archive(job_id) or {}
                job_stages = job_data.get("stages", [])
                queue_map = {
                    f"{job_id}_{s.get('stage_id')}_output": s.get("stage_id")
                    for s in job_stages
                    if s.get("stage_id")
                }
                queues = [f"{job_id}_{stage_id}_output"] if stage_id else list(queue_map.keys())
                for queue in queues:
                    timeout_entries = self._storage.state_scan_prefix(
                        "wq_events",
                        prefix=f"timeout:{queue}:",
                        limit=0,
                    )
                    for te in timeout_entries:
                        try:
                            event = decode_json(te["value"])
                        except Exception:
                            continue
                        event["stage_id"] = event.get("stage_id") or queue_map.get(queue)
                        if worker_id and event.get("worker_id") != worker_id:
                            continue
                        events.append(event)
            except Exception:
                pass

        events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        return events[:limit]

    # ---------------------------------------------------------------------
    # Workers (from persistent metadata, not event scanning)
    # ---------------------------------------------------------------------

    def list_workers(
        self,
        job_id: str,
        stage_id: Optional[str] = None,
        worker_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List workers from persistent worker metadata.

        Reads worker:{stage_id}:{worker_id} keys written by StageMaster.
        """
        namespace = job_namespace(job_id)
        prefix = f"worker:{stage_id}:" if stage_id else "worker:"
        entries = self._storage.state_scan_prefix(namespace, prefix=prefix, limit=0)

        workers: List[Dict[str, Any]] = []
        for entry in entries:
            try:
                worker = decode_json(entry["value"])
            except Exception:
                continue
            if worker_id and worker.get("worker_id") != worker_id:
                continue
            workers.append(worker)

        workers.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        return workers[offset : offset + limit]

    # ---------------------------------------------------------------------
    # Lineage
    # ---------------------------------------------------------------------

    def get_split_lineage(self, job_id: str, split_id: str) -> Optional[Dict[str, Any]]:
        namespace = job_namespace(job_id)
        data = self._storage.state_get_batch(namespace, [split_key(split_id)])
        if split_key(split_id) not in data:
            return None
        return decode_json(data[split_key(split_id)])

    def get_split_trace(self, job_id: str, split_id: str) -> Dict[str, Any]:
        visited: set[str] = set()
        splits: List[Dict[str, Any]] = []
        edges: List[Dict[str, Any]] = []

        def _walk(current_id: str) -> None:
            if current_id in visited:
                return
            visited.add(current_id)
            record = self.get_split_lineage(job_id, current_id)
            if not record:
                return
            splits.append(record)
            parent_id = record.get("parent_message_id")
            if parent_id:
                edges.append({"from": parent_id, "to": current_id})
                _walk(parent_id)

        _walk(split_id)
        return {"splits": splits, "edges": edges, "root_split_id": split_id}

    # ---------------------------------------------------------------------
    # Serve (from serve namespace)
    # ---------------------------------------------------------------------

    def list_serve_models(self, limit: int = 100) -> List[Dict[str, Any]]:
        """List deployed models from serve namespace."""
        entries = self._storage.state_scan_prefix(serve_namespace(), prefix="model:", limit=0)
        models: List[Dict[str, Any]] = []
        for entry in entries:
            try:
                models.append(decode_json(entry["value"]))
            except Exception:
                continue
        models.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        return models[:limit]

    def list_serve_workers(
        self,
        model_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List serve workers from serve namespace."""
        prefix = f"worker:{model_id}:" if model_id else "worker:"
        entries = self._storage.state_scan_prefix(serve_namespace(), prefix=prefix, limit=0)
        workers: List[Dict[str, Any]] = []
        for entry in entries:
            try:
                workers.append(decode_json(entry["value"]))
            except Exception:
                continue
        workers.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        return workers[:limit]

    def list_serve_events(
        self,
        model_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List serve events from serve namespace."""
        prefix = f"event:{model_id}:" if model_id else "event:"
        entries = self._storage.state_scan_prefix(serve_namespace(), prefix=prefix, limit=0)
        events: List[Dict[str, Any]] = []
        for entry in entries:
            try:
                events.append(decode_json(entry["value"]))
            except Exception:
                continue
        events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        return events[:limit]
