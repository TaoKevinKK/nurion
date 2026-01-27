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

"""Job state manager - stateless message consumer with async writes.

JobStateManager consumes state messages from Tansu and writes directly to storage.
Uses SlateDB async API and WriteBatch for high throughput.

Design principles:
1. Stateless: No in-memory accumulation, direct write to storage
2. Async: Uses SlateDB async API for non-blocking writes
3. Batched: Uses WriteBatch for efficient bulk writes
4. Idempotent: Re-processing same message produces same result
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING, Optional

from slatedb import WriteBatch

from solstice.webui.state.messages import StateMessage, StateMessageType
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.queue import QueueClient
    from solstice.webui.storage import JobStorage


class JobStateManager:
    """Stateless message consumer with async writes to storage.

    Uses SlateDB async API and WriteBatch for high throughput.
    """

    def __init__(
        self,
        job_id: str,
        queue_client: "QueueClient",
        state_topic: str,
        storage: "JobStorage",
    ):
        self.job_id = job_id
        self.queue_client = queue_client
        self.state_topic = state_topic
        self.storage = storage

        self.logger = create_ray_logger(f"JobStateManager-{job_id}")

        self._running = False
        self._consume_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start consuming from state topic."""
        if self._running:
            return

        self._running = True
        self._consume_task = asyncio.create_task(self._consume_loop())
        # Yield to allow the task to start
        await asyncio.sleep(0)
        self.logger.info("JobStateManager started")

    async def stop(self) -> None:
        """Stop consuming."""
        self._running = False

        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
            self._consume_task = None

        self.logger.info("JobStateManager stopped")

    async def _consume_loop(self) -> None:
        """Main consumption loop - batch process and async write."""
        message_count = 0
        last_log_time = time.time()
        fetch_count = 0

        self.logger.info(f"Starting consume loop for topic {self.state_topic}")

        while self._running:
            try:
                fetch_count += 1
                if fetch_count <= 5:
                    self.logger.info(f"Fetch #{fetch_count}: starting...")
                # Use short timeout and yield control frequently
                records = self.queue_client.fetch(
                    self.state_topic,
                    None,  # offset
                    100,  # max_records
                    100,  # timeout_ms - short to avoid blocking
                )

                if fetch_count <= 5:
                    self.logger.info(
                        f"Fetch #{fetch_count}: got {len(records) if records else 0} records"
                    )

                if records:
                    # Process all records into a single batch
                    batch = WriteBatch()
                    for record in records:
                        try:
                            message = StateMessage.from_bytes(record.value)
                            self._add_to_batch(batch, message)
                            message_count += 1
                        except Exception as e:
                            self.logger.warning(f"Failed to parse message: {e}")

                    # Write batch async (non-blocking, don't wait for durable)
                    await self.storage.db.write_with_options_async(batch, await_durable=False)

                # Log progress every 30 seconds
                now = time.time()
                if now - last_log_time >= 30.0:
                    self.logger.info(f"Consumed {message_count} messages")
                    last_log_time = now

                # Yield control to other tasks
                await asyncio.sleep(0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in consume loop: {e}")
                await asyncio.sleep(0.1)

    def _add_to_batch(self, batch: WriteBatch, msg: StateMessage) -> None:
        """Add message writes to batch."""
        match msg.message_type:
            case StateMessageType.JOB_STARTED:
                self._batch_job_event(batch, msg, "RUNNING")

            case StateMessageType.JOB_COMPLETED:
                self._batch_job_event(batch, msg, "COMPLETED")

            case StateMessageType.JOB_FAILED:
                self._batch_job_event(batch, msg, "FAILED")

            case StateMessageType.STAGE_STARTED:
                self._batch_stage_event(batch, msg, "RUNNING")

            case StateMessageType.STAGE_COMPLETED:
                self._batch_stage_event(batch, msg, "COMPLETED")

            case StateMessageType.WORKER_STARTED:
                self._batch_worker_event(batch, msg, "RUNNING")

            case StateMessageType.WORKER_STOPPED:
                self._batch_worker_event(batch, msg, "STOPPED")

            case StateMessageType.WORKER_STATE:
                self._batch_worker_state(batch, msg)

            case StateMessageType.SPLIT_METRICS_BATCH:
                self._batch_split_metrics(batch, msg)

            case StateMessageType.EXCEPTION:
                self._batch_exception(batch, msg)

            case StateMessageType.BACKPRESSURE:
                self._batch_backpressure(batch, msg)

    def _batch_job_event(self, batch: WriteBatch, msg: StateMessage, status: str) -> None:
        """Add job event to batch."""
        # Key is just "job" - each storage instance is per-job
        key = "job"
        data = {
            "job_id": self.job_id,
            "status": status,
            "timestamp": msg.timestamp,
            "dag_edges": msg.payload.get("dag_edges", {}),
            "stages": msg.payload.get("stages", []),
            "config": msg.payload.get("config", {}),
        }
        if status in ("COMPLETED", "FAILED"):
            data["end_time"] = msg.timestamp
        else:
            data["start_time"] = msg.timestamp

        batch.put(key.encode(), json.dumps(data).encode())

    def _batch_stage_event(self, batch: WriteBatch, msg: StateMessage, status: str) -> None:
        """Add stage event to batch."""
        stage_id = msg.source_id
        key = f"stage:{stage_id}"
        data = {
            "stage_id": stage_id,
            "status": status,
            "timestamp": msg.timestamp,
            "operator_type": msg.payload.get("operator_type", ""),
            "min_parallelism": msg.payload.get("min_parallelism", 1),
            "max_parallelism": msg.payload.get("max_parallelism", 1),
        }
        if status == "COMPLETED":
            data["end_time"] = msg.timestamp
        else:
            data["start_time"] = msg.timestamp

        batch.put(key.encode(), json.dumps(data).encode())

    def _batch_worker_event(self, batch: WriteBatch, msg: StateMessage, status: str) -> None:
        """Add worker event to batch."""
        worker_id = msg.source_id
        stage_id = msg.payload.get("stage_id", "")
        key = f"worker:{worker_id}"
        data = {
            "worker_id": worker_id,
            "stage_id": stage_id,
            "status": status,
            "timestamp": msg.timestamp,
            "assigned_partitions": msg.payload.get("assigned_partitions", []),
            "reason": msg.payload.get("reason", ""),
        }
        if status == "STOPPED":
            data["end_time"] = msg.timestamp
        else:
            data["start_time"] = msg.timestamp

        batch.put(key.encode(), json.dumps(data).encode())

    def _batch_worker_state(self, batch: WriteBatch, msg: StateMessage) -> None:
        """Add worker state to batch."""
        worker_id = msg.source_id
        stage_id = msg.payload.get("stage_id", "")

        # Store current state
        key = f"worker:{worker_id}"
        data = {
            "worker_id": worker_id,
            "stage_id": stage_id,
            "status": msg.payload.get("status", "RUNNING"),
            "timestamp": msg.timestamp,
            "assigned_partitions": msg.payload.get("assigned_partitions", []),
            "partition_offsets": msg.payload.get("partition_offsets", {}),
        }
        batch.put(key.encode(), json.dumps(data).encode())

        # Store partition offsets as time-series
        partition_offsets = msg.payload.get("partition_offsets", {})
        for partition_id, offset in partition_offsets.items():
            offset_key = f"offset:{stage_id}:{partition_id}:{int(msg.timestamp * 1000)}"
            offset_data = {
                "ts": msg.timestamp,
                "stage_id": stage_id,
                "partition_id": int(partition_id),
                "offset": offset,
                "worker_id": worker_id,
            }
            batch.put(offset_key.encode(), json.dumps(offset_data).encode())

    def _batch_split_metrics(self, batch: WriteBatch, msg: StateMessage) -> None:
        """Add split metrics to batch."""
        stage_id = msg.payload.get("stage_id", "")
        metrics = msg.payload.get("metrics", [])

        for metric in metrics:
            partition_id = metric.get("partition_id", 0)
            offset = metric.get("offset", 0)
            timestamp = metric.get("timestamp", msg.timestamp)

            key = f"split:{stage_id}:{partition_id}:{offset}"
            data = {
                "ts": timestamp,
                "stage_id": stage_id,
                "partition_id": partition_id,
                "offset": offset,
                "worker_id": metric.get("worker_id", ""),
                "process_time_ms": metric.get("process_time_ms", 0),
                "input_records": metric.get("input_records", 0),
                "output_records": metric.get("output_records", 0),
            }
            batch.put(key.encode(), json.dumps(data).encode())

    def _batch_exception(self, batch: WriteBatch, msg: StateMessage) -> None:
        """Add exception to batch."""
        key = f"exception:{msg.source_id}:{int(msg.timestamp * 1000)}"
        data = {
            "ts": msg.timestamp,
            "stage_id": msg.payload.get("stage_id"),
            "worker_id": msg.payload.get("worker_id"),
            "exception_type": msg.payload.get("exception_type"),
            "message": msg.payload.get("message"),
            "stacktrace": msg.payload.get("stacktrace"),
            "split_id": msg.payload.get("split_id"),
        }
        batch.put(key.encode(), json.dumps(data).encode())

    def _batch_backpressure(self, batch: WriteBatch, msg: StateMessage) -> None:
        """Add backpressure event to batch."""
        stage_id = msg.source_id
        key = f"backpressure:{stage_id}:{int(msg.timestamp * 1000)}"
        data = {
            "ts": msg.timestamp,
            "stage_id": stage_id,
            "active": msg.payload.get("active", False),
            "queue_lag": msg.payload.get("queue_lag", 0),
        }
        batch.put(key.encode(), json.dumps(data).encode())
