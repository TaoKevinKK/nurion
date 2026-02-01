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

"""StageWorker - Claim-based streaming worker.

This worker implements the WorkQueue claim-based model:

1. **Claim**: Atomically grab messages from the upstream queue
2. **Process**: Execute the operator on each message
3. **Ack/Forward**: Acknowledge processed messages (or forward to downstream)

No partitions or consumer groups - workers compete for messages from a single queue.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray

from solstice.queue import WorkQueueQueueClient, WorkQueueRecord
from solstice.webui.state.producer import StateProducer
from solstice.utils.logging import create_ray_logger
from solstice.core.models import (
    QueueEndpoint,
    QueueMessage,
    make_split_id,
)
from solstice.core.split_payload_store import SplitPayloadStore
from solstice.core.operator import Operator, OperatorRuntime
from solstice.testing.fault_injection import (
    check_fault,
    FAULT_BEFORE_MARK_PROCESSED,
    FAULT_AFTER_MARK_PROCESSED,
    FAULT_BEFORE_PROCESS,
    FAULT_AFTER_PROCESS,
)

if TYPE_CHECKING:
    from solstice.core.stage import Stage


@dataclass(frozen=True)
class WorkerRuntime:
    """Runtime parameters for StageWorker initialization."""

    worker_id: str
    job_id: str
    stage_id: str

    # Single broker endpoint (all queues use the same broker)
    broker_endpoint: Optional[QueueEndpoint] = None
    upstream_queue_name: Optional[str] = None
    output_queue_name: Optional[str] = None
    state_queue_name: Optional[str] = None

    # Processing config
    batch_size: int = 100


@ray.remote
class StageWorker:
    """Worker with claim-based processing model."""

    def __init__(
        self,
        runtime: WorkerRuntime,
        stage: "Stage",
        payload_store: SplitPayloadStore,
    ):
        """Initialize worker with runtime parameters."""
        self.worker_id = runtime.worker_id
        self.job_id = runtime.job_id
        self.stage_id = runtime.stage_id

        # Single broker endpoint for all queues
        self.broker_endpoint = runtime.broker_endpoint
        self.upstream_queue_name = runtime.upstream_queue_name
        self.output_queue_name = runtime.output_queue_name
        self.state_queue_name = runtime.state_queue_name

        # Processing config
        self._batch_size = runtime.batch_size

        # Store references
        self.stage = stage
        self.payload_store = payload_store

        self._state_producer: Optional[StateProducer] = None

        # Queue connections (created lazily)
        self.upstream_queue: Optional[WorkQueueQueueClient] = None
        self.output_queue: Optional[WorkQueueQueueClient] = None

        self.logger = create_ray_logger(f"Worker-{self.stage_id}-{self.worker_id}")

        # Single operator per worker (no partitions)
        self._operator: Optional[Operator] = None
        self._init_operator()

        # Worker-level state
        self._running = False
        self._upstream_finished = False

        # Buffer for split metrics (batch produce)
        self._pending_split_metrics: List[Any] = []

    def _init_operator(self) -> None:
        """Initialize Operator instance."""
        runtime = OperatorRuntime(
            job_id=self.job_id,
            stage_id=self.stage_id,
            worker_id=self.worker_id,
        )

        self._operator = self.stage.operator_config.setup(runtime)
        self.logger.debug("Initialized Operator")

    def _create_queue_client(self) -> WorkQueueQueueClient:
        """Create a queue connection to the broker."""
        if not self.broker_endpoint:
            raise RuntimeError("broker_endpoint is required")
        broker_url = f"{self.broker_endpoint.host}:{self.broker_endpoint.port}"
        client = WorkQueueQueueClient(broker_url, worker_id=self.worker_id)
        client.start()
        return client

    async def run(self) -> Dict[str, Any]:
        """Main entry point - runs the claim-process-ack loop."""
        self._running = True
        self.logger.info(f"Worker {self.worker_id} starting")

        if not self.broker_endpoint or not self.upstream_queue_name:
            raise RuntimeError(
                f"Worker {self.worker_id} requires broker_endpoint and upstream_queue_name."
            )

        try:
            # Create queue connections (single client for all queues)
            self.upstream_queue = self._create_queue_client()
            if self.output_queue_name:
                self.output_queue = self.upstream_queue  # Same client, different queue

            # Initialize state producer for WebUI
            await self._init_state_producer()
            await self._emit_worker_started()

            # Start periodic metrics reporter
            metrics_task = asyncio.create_task(
                self._periodic_metrics_loop(),
                name=f"metrics_{self.worker_id}",
            )

            try:
                await self._run_claim_loop()
                self.logger.info(f"Worker {self.worker_id} claim loop completed")
            finally:
                metrics_task.cancel()
                try:
                    await metrics_task
                except asyncio.CancelledError:
                    pass

            self.logger.info(f"Worker {self.worker_id} emitting stopped event")
            await self._emit_worker_stopped(reason="completed")

            # Collect stats
            op = self._operator
            return {
                "worker_id": self.worker_id,
                "processed_count": op.processed_count if op else 0,
                "error_count": op.error_count if op else 0,
            }

        except Exception as e:
            self.logger.error(f"Worker {self.worker_id} failed: {e}")
            await self._emit_exception(e)
            await self._emit_worker_stopped(reason="failed")
            raise
        finally:
            self._running = False
            await self._cleanup()

    async def _run_claim_loop(self) -> None:
        """Run the claim-process-ack loop.

        Exit conditions (unified):
        - upstream_finished flag is set AND
        - queue is empty (pending_count == 0 AND claimed_count == 0)
        """
        assert self.upstream_queue is not None
        assert self.upstream_queue_name is not None
        assert self._operator is not None

        self.logger.info(
            f"Worker {self.worker_id} starting claim loop on queue {self.upstream_queue_name}"
        )

        while self._running:
            try:
                # Claim messages from the queue
                records = self.upstream_queue.claim(
                    self.upstream_queue_name,
                    batch_size=self._batch_size,
                    timeout_ms=1000,
                )

                if not records:
                    # Queue returned empty, check if we should exit
                    if self._upstream_finished and self._is_queue_drained():
                        self.logger.info(
                            f"Worker {self.worker_id} done: upstream finished and queue drained"
                        )
                        break
                    await asyncio.sleep(0.05)
                    continue

                # Process each claimed message
                for record in records:
                    message = QueueMessage.from_bytes(record.value)
                    split_id = make_split_id(self.job_id, self.stage_id, record.msg_id)

                    check_fault(FAULT_BEFORE_PROCESS)
                    await self._process_message(message, record, split_id)
                    check_fault(FAULT_AFTER_PROCESS)

                    check_fault(FAULT_BEFORE_MARK_PROCESSED)
                    self._operator.processed_count += 1
                    check_fault(FAULT_AFTER_MARK_PROCESSED)

                    # Ack the message after successful processing
                    self.upstream_queue.ack(self.upstream_queue_name, [record.msg_id])

            except asyncio.CancelledError:
                self.logger.info(f"Worker {self.worker_id} claim loop cancelled")
                raise
            except Exception as e:
                if self._operator:
                    self._operator.error_count += 1
                self.logger.error(f"Error in worker {self.worker_id}: {e}")
                await asyncio.sleep(0.1)

        self.logger.info(
            f"Worker {self.worker_id} finished: processed={self._operator.processed_count if self._operator else 0}"
        )

    def _is_queue_drained(self) -> bool:
        """Check if queue is fully drained (no pending, no in-flight messages)."""
        if not self.upstream_queue or not self.upstream_queue_name:
            return True

        try:
            stats = self.upstream_queue.get_stats(self.upstream_queue_name)
            pending = stats.get("pending_count", 0)
            claimed = stats.get("claimed_count", 0)
            return pending == 0 and claimed == 0
        except Exception:
            # If we can't get stats, assume not drained
            return False

    async def _process_message(
        self,
        message: QueueMessage,
        record: WorkQueueRecord,
        split_id: str,
    ) -> None:
        """Process a single message using the operator."""
        from solstice.core.models import Split, SplitPayload

        assert self._operator is not None

        payload: Optional[SplitPayload] = None
        is_source_message = not message.payload_key

        if is_source_message:
            data_range = message.metadata.get("data_range", {})
            split = Split(
                split_id=message.split_id,
                stage_id=self.stage_id,
                data_range=data_range,
                parent_split_ids=[],
            )
        else:
            payload = self.payload_store.get(message.payload_key)
            if payload is None:
                raise RuntimeError(f"Payload not found for key: {message.payload_key}")

            split = Split(
                split_id=message.split_id,
                stage_id=self.stage_id,
                data_range={"message_id": message.message_id},
                parent_split_ids=[message.split_id],
            )

        # Process with operator
        start_time = time.time()
        output_payload = self._operator.process_split(split, payload)
        process_time_ms = (time.time() - start_time) * 1000

        # Update metrics
        input_records = len(payload) if payload else 0
        output_records = len(output_payload) if output_payload else 0
        self._operator.total_input_records += input_records
        self._operator.total_output_records += output_records
        self._operator.total_processing_time += process_time_ms / 1000

        # Record split metric for batch sending
        self._record_split_metric(
            msg_id=record.msg_id,
            process_time_ms=process_time_ms,
            input_records=input_records,
            output_records=output_records,
        )

        # Produce output if any
        if output_payload and self.output_queue and self.output_queue_name:
            payload_key = split_id
            self.payload_store.store(payload_key, output_payload)

            output_message = QueueMessage(
                message_id=split_id,
                split_id=split_id,
                payload_key=payload_key,
                metadata={
                    "source_stage": self.stage_id,
                    "parent_message_id": message.message_id,
                },
            )

            self.output_queue.push(
                self.output_queue_name,
                output_message.to_bytes(),
                metadata={"source_stage": self.stage_id},
            )

    async def _cleanup(self) -> None:
        """Clean up resources."""
        if self._operator:
            try:
                self._operator.close()
            except Exception as e:
                self.logger.warning(f"Error closing operator: {e}")
            self._operator = None

        if self._state_producer:
            try:
                await self._state_producer.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping state producer: {e}")

        # Only stop upstream_queue (output_queue is the same client)
        if self.upstream_queue:
            self.upstream_queue.stop()

    # === Status and Control ===

    def notify_upstream_finished(self) -> None:
        """Called by master when upstream stage(s) have finished."""
        self._upstream_finished = True
        self.logger.info(f"Worker {self.worker_id} notified: upstream finished")

    def get_status(self) -> Dict[str, Any]:
        """Get current worker status."""
        import os

        op = self._operator

        return {
            "worker_id": self.worker_id,
            "stage_id": self.stage_id,
            "pid": os.getpid(),
            "running": self._running,
            "upstream_finished": self._upstream_finished,
            "processed_count": op.processed_count if op else 0,
            "error_count": op.error_count if op else 0,
            "input_records": op.total_input_records if op else 0,
            "output_records": op.total_output_records if op else 0,
            "processing_time_s": op.total_processing_time if op else 0,
        }

    def stop(self) -> None:
        """Stop the worker."""
        self._running = False
        self.logger.info(f"Worker {self.worker_id} stopping")

    def invoke_operator(self, method_name: str, *args, **kwargs) -> Any:
        """Invoke an operator method by name."""
        from solstice.core.operator import is_master_callable

        if not self._operator:
            return None

        method = getattr(self._operator, method_name, None)
        if method is None:
            return None

        if not is_master_callable(method):
            raise ValueError(f"Method '{method_name}' is not marked @master_callable.")

        return method(*args, **kwargs)

    # === State Producer and Events ===

    async def _init_state_producer(self) -> None:
        """Initialize state producer for metrics push."""
        if not self.broker_endpoint or not self.state_queue_name:
            return

        try:
            state_queue = self._create_queue_client()
            self._state_producer = StateProducer(
                job_id=self.job_id,
                queue_client=state_queue,
                state_queue_name=self.state_queue_name,
            )
            await self._state_producer.start()
            self.logger.debug("State producer initialized")
        except Exception as e:
            self.logger.warning(f"Failed to init state producer: {e}")
            self._state_producer = None

    async def _emit_worker_started(self) -> None:
        """Emit WORKER_STARTED event."""
        if not self._state_producer:
            return

        try:
            from solstice.webui.state.messages import worker_started_message

            msg = worker_started_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                worker_id=self.worker_id,
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit worker started: {e}")

    async def _emit_worker_stopped(self, reason: str = "completed") -> None:
        """Emit WORKER_STOPPED event."""
        if not self._state_producer:
            return

        try:
            from solstice.webui.state.messages import worker_stopped_message

            msg = worker_stopped_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                worker_id=self.worker_id,
                reason=reason,
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit worker stopped: {e}")

    async def _periodic_metrics_loop(self, interval_s: float = 5.0) -> None:
        """Background task to emit worker state and split metrics periodically."""
        while self._running:
            try:
                await asyncio.sleep(interval_s)
                if self._running:
                    await self._emit_worker_state()
                    await self._emit_split_metrics_batch()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.debug(f"Error in periodic metrics loop: {e}")

    def _record_split_metric(
        self,
        msg_id: str,
        process_time_ms: float,
        input_records: int = 0,
        output_records: int = 0,
    ) -> None:
        """Record a split metric for later batching."""
        from solstice.webui.state.messages import SplitMetric

        self._pending_split_metrics.append(
            SplitMetric(
                stage_id=self.stage_id,
                msg_id=msg_id,
                worker_id=self.worker_id,
                process_time_ms=process_time_ms,
                input_records=input_records,
                output_records=output_records,
            )
        )

    async def _emit_worker_state(self) -> None:
        """Emit WORKER_STATE message."""
        if not self._state_producer:
            return

        try:
            from solstice.webui.state.messages import worker_state_message

            msg = worker_state_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                worker_id=self.worker_id,
                status="RUNNING" if self._running else "STOPPED",
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit worker state: {e}")

    async def _emit_split_metrics_batch(self) -> None:
        """Emit SPLIT_METRICS_BATCH message."""
        if not self._state_producer:
            return

        if not self._pending_split_metrics:
            return

        try:
            from solstice.webui.state.messages import split_metrics_batch_message

            metrics = self._pending_split_metrics
            self._pending_split_metrics = []

            msg = split_metrics_batch_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                worker_id=self.worker_id,
                metrics=metrics,
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit split metrics batch: {e}")

    async def _emit_exception(self, exception: Exception) -> None:
        """Emit EXCEPTION event."""
        if not self._state_producer:
            return

        try:
            import traceback
            from solstice.webui.state.messages import exception_message

            msg = exception_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                worker_id=self.worker_id,
                exception_type=type(exception).__name__,
                message=str(exception),
                stacktrace=traceback.format_exc(),
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit exception: {e}")
