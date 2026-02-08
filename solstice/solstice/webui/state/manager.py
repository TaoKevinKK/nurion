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

"""Job state reader backed by WorkQueue storage (pyO3)."""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

from solstice.queue import WorkQueueStorageReader
from solstice.utils.logging import create_ray_logger
from solstice.webui.state.schema import (
    config_key,
    decode_json,
    event_key,
    job_key,
    job_namespace,
    jobs_namespace,
    parse_event_key,
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
                stage["output_queue_size"] = stats.pending_count
                stage["output_queue_claimed"] = stats.claimed_count
            except Exception:
                stage["output_queue_size"] = 0
                stage["output_queue_claimed"] = 0

        job_data["stages"] = stages
        return job_data

    def get_configuration(self, job_id: str) -> Optional[Dict[str, Any]]:
        namespace = job_namespace(job_id)
        data = self._storage.state_get_batch(namespace, [config_key()])
        if config_key() not in data:
            return None
        return decode_json(data[config_key()])

    # ---------------------------------------------------------------------
    # Events & Metrics
    # ---------------------------------------------------------------------

    def list_events(
        self,
        job_id: str,
        stage_id: Optional[str] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        namespace = job_namespace(job_id)
        prefix = f"event:{stage_id}:" if stage_id else "event:"
        entries = self._storage.state_scan_prefix(namespace, prefix=prefix, limit=0)
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
                event.setdefault("split_id", f"{job_id}:{parsed_stage_id}:{msg_id}")
            ts = event.get("timestamp") or 0
            if start_time and ts < start_time:
                continue
            if end_time and ts > end_time:
                continue
            events.append(event)

        # Merge timeout events written by recovery (global namespace)
        try:
            job_data = self.get_job_archive(job_id) or {}
            stages = job_data.get("stages", [])
            queue_map = {
                f"{job_id}_{s.get('stage_id')}_output": s.get("stage_id")
                for s in stages
                if s.get("stage_id")
            }
            queues = [f"{job_id}_{stage_id}_output"] if stage_id else list(queue_map.keys())
            for queue in queues:
                timeout_entries = self._storage.state_scan_prefix(
                    "wq_events",
                    prefix=f"timeout:{queue}:",
                    limit=0,
                )
                for entry in timeout_entries:
                    try:
                        event = decode_json(entry["value"])
                    except Exception:
                        continue
                    event["stage_id"] = event.get("stage_id") or queue_map.get(queue)
                    ts = event.get("timestamp") or 0
                    if start_time and ts < start_time:
                        continue
                    if end_time and ts > end_time:
                        continue
                    events.append(event)
        except Exception:
            pass

        events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        return events[:limit]

    def get_metrics_samples(
        self,
        job_id: str,
        worker_id: str,
        start_time: float,
        end_time: float,
    ) -> List[Dict[str, Any]]:
        events = self.list_events(
            job_id,
            start_time=start_time,
            end_time=end_time,
            limit=100000,
        )
        samples = []
        for event in events:
            if event.get("event_type") != "ack":
                continue
            if event.get("worker_id") != worker_id:
                continue
            samples.append(
                {
                    "ts": event.get("timestamp", 0),
                    "input_records": event.get("input_rows", 0),
                    "output_records": event.get("output_rows", 0),
                    "process_time_ms": event.get("processing_ms", 0),
                }
            )
        return sorted(samples, key=lambda x: x.get("ts", 0))

    def get_metrics_history(
        self,
        job_id: str,
        stage_id: str,
        start_time: float,
        end_time: float,
    ) -> List[Dict[str, Any]]:
        events = self.list_events(
            job_id,
            stage_id=stage_id,
            start_time=start_time,
            end_time=end_time,
            limit=100000,
        )
        if not events:
            return []
        bucket_size = 10.0
        buckets: Dict[int, Dict[str, Any]] = {}
        for event in events:
            if event.get("event_type") != "ack":
                continue
            ts = event.get("timestamp", 0)
            bucket_key = int(ts / bucket_size)
            if bucket_key not in buckets:
                buckets[bucket_key] = {
                    "timestamp": bucket_key * bucket_size,
                    "input_records": 0,
                    "output_records": 0,
                    "process_time_ms": 0,
                    "split_count": 0,
                }
            buckets[bucket_key]["input_records"] += event.get("input_rows", 0)
            buckets[bucket_key]["output_records"] += event.get("output_rows", 0)
            buckets[bucket_key]["process_time_ms"] += event.get("processing_ms", 0)
            buckets[bucket_key]["split_count"] += 1
        return sorted(buckets.values(), key=lambda x: x.get("timestamp", 0))

    def rate(
        self,
        job_id: str,
        worker_id: str,
        metric_name: str,
        time_range_s: float = 60.0,
    ) -> float:
        now = time.time()
        samples = self.get_metrics_samples(job_id, worker_id, now - time_range_s, now)
        if not samples:
            return 0.0
        if metric_name == "input_records":
            total = sum(s.get("input_records", 0) for s in samples)
        elif metric_name == "output_records":
            total = sum(s.get("output_records", 0) for s in samples)
        elif metric_name == "processed_count":
            total = len(samples)
        else:
            total = 0
        return total / time_range_s if time_range_s > 0 else 0.0

    def get_throughput(
        self,
        job_id: str,
        stage_id: Optional[str] = None,
        time_range_s: float = 60.0,
    ) -> Dict[str, Any]:
        now = time.time()
        events = self.list_events(
            job_id,
            stage_id=stage_id,
            start_time=now - time_range_s,
            end_time=now,
            limit=100000,
        )
        input_records = sum(e.get("input_rows", 0) for e in events if e.get("event_type") == "ack")
        output_records = sum(
            e.get("output_rows", 0) for e in events if e.get("event_type") == "ack"
        )
        return {
            "input_records_per_sec": input_records / time_range_s if time_range_s > 0 else 0.0,
            "output_records_per_sec": output_records / time_range_s if time_range_s > 0 else 0.0,
            "splits_per_sec": len([e for e in events if e.get("event_type") == "ack"])
            / time_range_s
            if time_range_s > 0
            else 0.0,
        }

    # ---------------------------------------------------------------------
    # Workers
    # ---------------------------------------------------------------------

    def list_workers(
        self,
        job_id: str,
        stage_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        job_data = self.get_job_archive(job_id) or {"status": "UNKNOWN"}
        job_status = job_data.get("status", "UNKNOWN")
        events = self.list_events(job_id, limit=100000)
        workers: Dict[str, Dict[str, Any]] = {}
        for event in events:
            worker_id = event.get("worker_id")
            if not worker_id:
                continue
            worker = workers.setdefault(
                worker_id,
                {
                    "worker_id": worker_id,
                    "stage_id": event.get("stage_id", ""),
                    "start_time": event.get("timestamp", 0),
                    "end_time": None,
                    "last_seen": event.get("timestamp", 0),
                    "event_count": 0,
                },
            )
            worker["stage_id"] = event.get("stage_id", worker.get("stage_id"))
            worker["start_time"] = min(worker.get("start_time", 0), event.get("timestamp", 0))
            worker["last_seen"] = max(worker.get("last_seen", 0), event.get("timestamp", 0))
            worker["event_count"] += 1

        now = time.time()
        results = []
        for worker in workers.values():
            if job_status in ("COMPLETED", "FAILED"):
                worker_status = "COMPLETED" if job_status == "COMPLETED" else "FAILED"
                worker["end_time"] = job_data.get("end_time")
            else:
                worker_status = "RUNNING" if now - worker.get("last_seen", 0) < 60 else "IDLE"
            worker["status"] = worker_status
            results.append(worker)

        if stage_id:
            results = [w for w in results if w.get("stage_id") == stage_id]
        if status:
            results = [w for w in results if w.get("status") == status]

        results.sort(key=lambda x: x.get("start_time", 0), reverse=True)
        return results[offset : offset + limit]

    def get_worker_history(self, job_id: str, worker_id: str) -> Optional[Dict[str, Any]]:
        workers = self.list_workers(job_id, limit=1000)
        for worker in workers:
            if worker.get("worker_id") == worker_id:
                return worker
        return None

    def list_worker_events(
        self,
        job_id: str,
        worker_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        events = self.list_events(job_id, limit=100000)
        if worker_id:
            events = [e for e in events if e.get("worker_id") == worker_id]
        return events[:limit]

    # ---------------------------------------------------------------------
    # Exceptions
    # ---------------------------------------------------------------------

    def list_exceptions(
        self,
        job_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        events = self.list_events(job_id, limit=100000)
        exceptions: List[Dict[str, Any]] = []
        for event in events:
            if event.get("event_type") not in ("nack", "timeout"):
                continue
            exception_id = event_key(
                event.get("stage_id", ""),
                int(event.get("timestamp_ns", 0)),
                event.get("msg_id", ""),
            )
            exceptions.append(
                {
                    "exception_id": exception_id,
                    "timestamp": event.get("timestamp", 0),
                    "exception_type": event.get("event_type", ""),
                    "message": event.get("reason", ""),
                    "stage_id": event.get("stage_id"),
                    "worker_id": event.get("worker_id"),
                    "split_id": event.get("split_id"),
                    "stacktrace": "",
                }
            )
        exceptions.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        return exceptions[offset : offset + limit]

    # ---------------------------------------------------------------------
    # Lineage
    # ---------------------------------------------------------------------

    def get_split_lineage(self, job_id: str, split_id: str) -> Optional[Dict[str, Any]]:
        namespace = job_namespace(job_id)
        data = self._storage.state_get_batch(namespace, [split_key(split_id)])
        if split_key(split_id) not in data:
            return None
        return decode_json(data[split_key(split_id)])

    def list_splits_by_stage(
        self,
        job_id: str,
        stage_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        events = self.list_events(job_id, stage_id=stage_id, limit=100000)
        splits = [e for e in events if e.get("event_type") == "ack"]
        splits.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        return splits[offset : offset + limit]

    def get_lineage_overview(self, job_id: str) -> Dict[str, Any]:
        job_data = self.get_job_archive(job_id) or {}
        dag_edges = job_data.get("dag_edges", {})
        stages_list = job_data.get("stages", [])
        stage_order = [s.get("stage_id") for s in stages_list]

        stage_splits: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        events = self.list_events(job_id, limit=100000)
        for event in events:
            if event.get("event_type") != "ack":
                continue
            stage_splits[event.get("stage_id", "")].append(event)

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
                total_rows = sum(s.get("output_rows", 0) for s in to_splits)
                total_bytes = sum(s.get("output_bytes", 0) for s in to_splits)
                proc_times = [s.get("processing_ms", 0) for s in to_splits]
                rows_list = [s.get("output_rows", 0) for s in to_splits]
                bytes_list = [s.get("output_bytes", 0) for s in to_splits]
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
            total_rows = sum(s.get("output_rows", 0) for s in splits)
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
    # Partition offsets (not applicable in WorkQueue)
    # ---------------------------------------------------------------------

    def get_partition_offsets(self, job_id: str, stage_id: Optional[str] = None) -> Dict[str, Any]:
        return {}
