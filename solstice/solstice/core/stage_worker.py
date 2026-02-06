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
3. **Ack/Forward**: Atomically acknowledge upstream + push downstream (ack_and_forward)

Key design: Uses `ack_and_forward` for atomic ack + push to prevent duplicates.
If worker crashes between processing and ack, message returns to pending queue.
With atomic ack_and_forward, downstream only receives data after successful commit.

No partitions or consumer groups - workers compete for messages from a single queue.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional

import ray

from solstice.queue import WorkQueueQueueClient, WorkQueueRecord
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
from solstice.webui.state.schema import encode_json, event_key, job_namespace, split_key

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

    # Processing config
    batch_size: int = 100
    claim_timeout_secs: float = 60.0


@dataclass(frozen=True)
class ProcessResult:
    """Processing result for a single message.

    output_messages_bytes can contain 0, 1, or N messages:
    - [] or None: operator filtered/dropped this message
    - [bytes]: operator produced one output (map, 1:1)
    - [bytes, bytes, ...]: operator produced multiple outputs (explode, 1:N)
    """

    output_messages_bytes: list[bytes]
    input_rows: int
    input_bytes: int
    output_rows: int
    output_bytes: int


class PayloadMissingError(RuntimeError):
    """Raised when required payload is missing for a claimed message."""

    def __init__(self, msg_id: str, payload_key: str) -> None:
        super().__init__(f"Payload not found for key: {payload_key}")
        self.msg_id = msg_id
        self.payload_key = payload_key


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

        # Processing config
        self._batch_size = runtime.batch_size
        self._claim_timeout_secs = runtime.claim_timeout_secs

        # Store references
        self.stage = stage
        self.payload_store = payload_store

        # Queue connection (single client for all queues)
        self.queue_client: Optional[WorkQueueQueueClient] = None

        self.logger = create_ray_logger(f"Worker-{self.stage_id}-{self.worker_id}")

        # Single operator per worker (no partitions)
        self._operator: Optional[Operator] = None
        self._init_operator()

        # Worker-level state
        self._running = False
        self._safe_to_exit = False  # Set by master when queue is confirmed drained

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
        from solstice.queue.workqueue import _compute_heartbeat_interval

        client = WorkQueueQueueClient(
            broker_url,
            worker_id=self.worker_id,
            heartbeat_interval_secs=_compute_heartbeat_interval(self._claim_timeout_secs),
        )
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
            # Create queue connection (single client for all queues)
            self.queue_client = self._create_queue_client()
            await self._run_claim_loop()
            self.logger.info(f"Worker {self.worker_id} claim loop completed")
            return {"worker_id": self.worker_id}

        except Exception as e:
            self.logger.error(f"Worker {self.worker_id} failed: {e}")
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
        assert self.queue_client is not None
        assert self.upstream_queue_name is not None
        assert self._operator is not None

        self.logger.info(
            f"Worker {self.worker_id} starting claim loop on queue {self.upstream_queue_name}"
        )

        while self._running:
            try:
                # Claim messages from the queue
                records = self.queue_client.claim(
                    self.upstream_queue_name,
                    batch_size=self._batch_size,
                    timeout_ms=1000,
                )

                if not records:
                    # Queue returned empty, check if we should exit
                    if self._should_exit():
                        self.logger.info(
                            f"Worker {self.worker_id} done: upstream finished and queue drained"
                        )
                        break
                    await asyncio.sleep(0.05)
                    continue

                # Process each claimed message
                for record in records:
                    if not record.claim_token:
                        raise RuntimeError(f"Missing claim_token for message {record.msg_id}")

                    message = QueueMessage.from_bytes(record.value)
                    split_id = make_split_id(self.job_id, self.stage_id, record.msg_id)

                    try:
                        check_fault(FAULT_BEFORE_PROCESS)
                        process_start = time.time()
                        result = await self._process_message(message, record, split_id)
                        processing_ms = max(0.0, (time.time() - process_start) * 1000.0)
                        check_fault(FAULT_AFTER_PROCESS)
                    except PayloadMissingError as e:
                        if self.upstream_queue_name:
                            self.logger.error(
                                f"Payload missing for msg_id={e.msg_id}, "
                                f"nacking for retry: {e.payload_key}"
                            )
                            event_puts = self._build_event_puts(
                                event_type="nack",
                                record=record,
                                split_id=split_id,
                                message=message,
                                processing_ms=0.0,
                                input_rows=0,
                                input_bytes=0,
                                output_rows=0,
                                output_bytes=0,
                                reason="payload_missing",
                            )
                            self.queue_client.nack(
                                self.upstream_queue_name,
                                [record.msg_id],
                                claim_tokens=[record.claim_token],
                                reason="payload_missing",
                                state_namespace=job_namespace(self.job_id),
                                state_puts=event_puts,
                            )
                            continue
                        raise

                    check_fault(FAULT_BEFORE_MARK_PROCESSED)
                    check_fault(FAULT_AFTER_MARK_PROCESSED)

                    event_puts = self._build_event_puts(
                        event_type="ack",
                        record=record,
                        split_id=split_id,
                        message=message,
                        processing_ms=processing_ms,
                        input_rows=result.input_rows,
                        input_bytes=result.input_bytes,
                        output_rows=result.output_rows,
                        output_bytes=result.output_bytes,
                        reason="completed",
                    )

                    # Atomic ack (+ forward if output exists)
                    if result.output_messages_bytes and self.output_queue_name:
                        # Atomic: ack upstream + push downstream
                        self.queue_client.ack_and_forward(
                            upstream_queue=self.upstream_queue_name,
                            upstream_msg_ids=[record.msg_id],
                            upstream_claim_tokens=[record.claim_token],
                            downstream_queue=self.output_queue_name,
                            downstream_payloads=result.output_messages_bytes,
                            state_namespace=job_namespace(self.job_id),
                            state_puts=event_puts,
                        )
                    else:
                        # No output, just ack
                        self.queue_client.ack(
                            self.upstream_queue_name,
                            [record.msg_id],
                            claim_tokens=[record.claim_token],
                            state_namespace=job_namespace(self.job_id),
                            state_puts=event_puts,
                        )

            except asyncio.CancelledError:
                self.logger.info(f"Worker {self.worker_id} claim loop cancelled")
                raise
            except Exception as e:
                try:
                    import grpc
                except Exception:
                    grpc = None  # type: ignore[assignment]

                is_broker_error = False
                if grpc is not None and isinstance(e, grpc.RpcError):
                    is_broker_error = True
                elif isinstance(e, RuntimeError) and "Client not started" in str(e):
                    is_broker_error = True

                if is_broker_error:
                    self.logger.error(f"Worker {self.worker_id} broker error, stopping: {e}")
                    raise RuntimeError("broker_unavailable") from e
                self.logger.error(f"Error in worker {self.worker_id}: {e}")
                await asyncio.sleep(0.1)

        self.logger.info(f"Worker {self.worker_id} finished")

    def _should_exit(self) -> bool:
        """Check if worker should exit.

        Returns True when master has confirmed it's safe to exit, meaning:
        1. Upstream has finished (queue marked as finished)
        2. Queue is drained (pending==0 && claimed==0)

        The master handles the RPC check and notifies workers via
        notify_safe_to_exit() when these conditions are met.
        """
        return self._safe_to_exit

    async def _process_message(
        self,
        message: QueueMessage,
        record: WorkQueueRecord,
        split_id: str,
    ) -> ProcessResult:
        """Process a single message using the operator.

        Returns:
            ProcessResult containing output bytes and metrics.
            The caller is responsible for atomic ack_and_forward.
        """
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
                raise PayloadMissingError(record.msg_id, message.payload_key)

            split = Split(
                split_id=message.split_id,
                stage_id=self.stage_id,
                data_range={"message_id": message.message_id},
                parent_split_ids=[message.split_id],
            )

        # Process with operator (supports sync, async, iterator, async iterator)
        result = self._operator.process_split(split, payload)

        # Normalize result into list[SplitPayload]
        output_payloads = await self._collect_outputs(result)

        input_rows = len(payload) if payload else 0
        input_bytes = int(payload.data.nbytes) if payload else 0
        output_rows = sum(len(p) for p in output_payloads)
        output_bytes = sum(int(p.data.nbytes) for p in output_payloads)

        # Prepare output messages for atomic ack_and_forward
        output_messages_bytes: list[bytes] = []
        if output_payloads and self.output_queue_name:
            for idx, out_payload in enumerate(output_payloads):
                out_split_id = split_id if len(output_payloads) == 1 else f"{split_id}_{idx}"
                payload_key = out_split_id
                self.payload_store.store(payload_key, out_payload)

                output_message = QueueMessage(
                    message_id=out_split_id,
                    split_id=out_split_id,
                    payload_key=payload_key,
                    metadata={
                        "source_stage": self.stage_id,
                        "parent_message_id": message.message_id,
                    },
                )
                output_messages_bytes.append(output_message.to_bytes())

        return ProcessResult(
            output_messages_bytes=output_messages_bytes,
            input_rows=input_rows,
            input_bytes=input_bytes,
            output_rows=output_rows,
            output_bytes=output_bytes,
        )

    @staticmethod
    async def _collect_outputs(result: Any) -> list:
        """Normalize process_split return value into list[SplitPayload].

        Supports:
            None                        → []
            SplitPayload                → [payload]
            Coroutine → await → recurse
            Iterator[SplitPayload]      → list(iter)
            AsyncIterator[SplitPayload] → [p async for p in iter]
        """
        from solstice.core.models import SplitPayload

        # Coroutine (async def process_split)
        if asyncio.iscoroutine(result):
            result = await result
            return await StageWorker._collect_outputs(result)

        # None → drop
        if result is None:
            return []

        # Single payload
        if isinstance(result, SplitPayload):
            return [result]

        # Async iterator/generator
        if hasattr(result, "__aiter__"):
            return [p async for p in result]

        # Sync iterator/generator
        if hasattr(result, "__iter__"):
            return list(result)

        raise TypeError(
            f"process_split returned unsupported type {type(result).__name__}. "
            "Expected None, SplitPayload, Iterator[SplitPayload], or async variants."
        )

    def _build_event_puts(
        self,
        event_type: str,
        record: WorkQueueRecord,
        split_id: str,
        message: QueueMessage,
        processing_ms: float,
        input_rows: int,
        input_bytes: int,
        output_rows: int,
        output_bytes: int,
        reason: str,
    ) -> Dict[str, bytes]:
        ts_ns = time.time_ns()
        queue_wait_ms = max(0.0, (time.time() - record.created_at) * 1000.0)
        parent_message_id = message.metadata.get("parent_message_id")
        source_stage = message.metadata.get("source_stage")

        base_event = {
            "event_type": event_type,
            "timestamp": time.time(),
            "worker_id": self.worker_id,
            "processing_ms": processing_ms,
            "queue_wait_ms": queue_wait_ms,
            "input_rows": input_rows,
            "input_bytes": input_bytes,
            "output_rows": output_rows,
            "output_bytes": output_bytes,
            "reason": reason,
        }
        split_event = dict(base_event)
        split_event.update(
            {
                "timestamp_ns": ts_ns,
                "stage_id": self.stage_id,
                "split_id": split_id,
                "parent_message_id": parent_message_id,
                "source_stage": source_stage,
            }
        )
        if parent_message_id is None:
            split_event.pop("parent_message_id", None)
        if source_stage is None:
            split_event.pop("source_stage", None)

        puts = {
            event_key(self.stage_id, ts_ns, record.msg_id): encode_json(base_event),
            split_key(split_id): encode_json(split_event),
        }
        return puts

    async def _cleanup(self) -> None:
        """Clean up resources."""
        if self._operator:
            try:
                self._operator.close()
            except Exception as e:
                self.logger.warning(f"Error closing operator: {e}")
            self._operator = None

        if self.queue_client:
            self.queue_client.stop()

    # === Status and Control ===

    def notify_safe_to_exit(self) -> None:
        """Called by master when queue is confirmed drained and safe to exit.

        This is the authoritative signal that:
        1. Upstream has finished (queue marked as finished)
        2. Queue is drained (pending==0 && claimed==0)
        """
        self._safe_to_exit = True
        self.logger.info(f"Worker {self.worker_id} notified: safe to exit")

    def get_status(self) -> Dict[str, Any]:
        """Get current worker status."""
        import os

        return {
            "worker_id": self.worker_id,
            "stage_id": self.stage_id,
            "pid": os.getpid(),
            "running": self._running,
            "safe_to_exit": self._safe_to_exit,
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
