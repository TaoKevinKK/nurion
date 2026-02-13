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

Claim-process-ack loop with optional merge:
1. Claim messages from upstream queue
2. Merge payloads if merge_upstream > 1 (Arrow table concatenation)
3. Call operator.process_split() once per group
4. Atomic ack_and_forward (ack all upstream + push output downstream)

merge_upstream=1: each message processed individually (default).
merge_upstream=N: N messages merged before processing (e.g., Lance sink).
Both use the same code path -- single record is just a group of size 1.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional

import ray

from _internal.core.models import (
    QueueEndpoint,
    QueueMessage,
    RawOutputBytes,
    make_split_id,
)
from _internal.core.operator import Operator, OperatorRuntime
from _internal.core.split_payload_store import SplitPayloadStore
from _internal.queue import WorkQueueQueueClient, WorkQueueRecord
from _internal.testing.fault_injection import (
    FAULT_AFTER_PROCESS,
    FAULT_BEFORE_PROCESS,
    check_fault,
)
from _internal.utils.logging import create_ray_logger
from _internal.webui.state.schema import encode_json, event_key, job_namespace, split_key

if TYPE_CHECKING:
    from _internal.core.stage import Stage


@dataclass(frozen=True)
class WorkerRuntime:
    """Runtime parameters for StageWorker initialization."""

    worker_id: str
    job_id: str
    stage_id: str

    broker_endpoint: Optional[QueueEndpoint] = None
    upstream_queue_name: Optional[str] = None
    output_queue_name: Optional[str] = None

    batch_size: int = 100
    claim_timeout_secs: float = 60.0


class PayloadMissingError(RuntimeError):
    """Raised when required payload is missing for a claimed message."""

    def __init__(self, msg_id: str, payload_key: str) -> None:
        super().__init__(f"Payload not found for key: {payload_key}")
        self.msg_id = msg_id
        self.payload_key = payload_key


@ray.remote
class StageWorker:
    """Worker with claim-based processing model and optional merge."""

    def __init__(
        self,
        runtime: WorkerRuntime,
        stage: "Stage",
        payload_store: SplitPayloadStore,
    ):
        self.worker_id = runtime.worker_id
        self.job_id = runtime.job_id
        self.stage_id = runtime.stage_id

        self.broker_endpoint = runtime.broker_endpoint
        self.upstream_queue_name = runtime.upstream_queue_name
        self.output_queue_name = runtime.output_queue_name

        self._batch_size = runtime.batch_size
        self._claim_timeout_secs = runtime.claim_timeout_secs
        self._merge_upstream = stage.operator_config.get_merge_upstream()

        self.stage = stage
        self.payload_store = payload_store
        self.queue_client: Optional[WorkQueueQueueClient] = None

        self.logger = create_ray_logger(f"Worker-{self.stage_id}-{self.worker_id}")

        self._operator: Optional[Operator] = None
        self._init_operator()

        self._running = False
        self._safe_to_exit = False

    def _init_operator(self) -> None:
        runtime = OperatorRuntime(
            job_id=self.job_id,
            stage_id=self.stage_id,
            worker_id=self.worker_id,
            broker_endpoint=self.broker_endpoint,
        )
        self._operator = self.stage.operator_config.setup(runtime)

    def _create_queue_client(self) -> WorkQueueQueueClient:
        if not self.broker_endpoint:
            raise RuntimeError("broker_endpoint is required")
        broker_url = f"{self.broker_endpoint.host}:{self.broker_endpoint.port}"
        from _internal.queue.workqueue import _compute_heartbeat_interval

        client = WorkQueueQueueClient(
            broker_url,
            worker_id=self.worker_id,
            heartbeat_interval_secs=_compute_heartbeat_interval(self._claim_timeout_secs),
        )
        client.start()
        return client

    # =========================================================================
    # Main loop
    # =========================================================================

    async def run(self) -> Dict[str, Any]:
        """Main entry point."""
        self._running = True
        self.logger.info(f"Worker {self.worker_id} starting")

        if not self.broker_endpoint or not self.upstream_queue_name:
            raise RuntimeError(
                f"Worker {self.worker_id} requires broker_endpoint and upstream_queue_name."
            )

        try:
            self.queue_client = self._create_queue_client()
            await self._run_claim_loop()
            return {"worker_id": self.worker_id}
        except Exception as e:
            self.logger.error(f"Worker {self.worker_id} failed: {e}")
            raise
        finally:
            self._running = False
            await self._cleanup()

    async def _run_claim_loop(self) -> None:
        """Claim-process-ack loop.

        Always uses group processing. When merge_upstream=1,
        each record is a group of size 1. No special case needed.
        """
        assert self.queue_client is not None
        assert self.upstream_queue_name is not None

        merge = self._merge_upstream
        pending: list[WorkQueueRecord] = []

        while self._running:
            try:
                records = self.queue_client.claim(
                    self.upstream_queue_name,
                    batch_size=self._batch_size,
                    timeout_ms=1000,
                )

                if records:
                    pending.extend(records)
                else:
                    if self._should_exit():
                        if pending:
                            await self._process_and_ack(pending)
                            pending.clear()
                        break
                    # Flush partial group if queue is idle
                    if pending:
                        await self._process_and_ack(pending)
                        pending.clear()
                    await asyncio.sleep(0.05)
                    continue

                # Process complete groups
                while len(pending) >= merge:
                    group = pending[:merge]
                    pending = pending[merge:]
                    await self._process_and_ack(group)

            except asyncio.CancelledError:
                self.logger.info(f"Worker {self.worker_id} cancelled")
                raise
            except Exception as e:
                if self._is_broker_error(e):
                    self.logger.error(f"Worker {self.worker_id} broker error: {e}")
                    raise RuntimeError("broker_unavailable") from e
                self.logger.error(f"Error in worker {self.worker_id}: {e}")
                await asyncio.sleep(0.1)

    # =========================================================================
    # Process and ack (unified for single and merge)
    # =========================================================================

    async def _process_and_ack(self, records: list[WorkQueueRecord]) -> None:
        """Process one or more records and ack atomically.

        When len(records) == 1: equivalent to the old single-record path.
        When len(records) > 1: merges payloads via Arrow concat, processes once.
        All upstream messages are acked atomically via ack_and_forward.
        """
        assert self.queue_client is not None
        assert self.upstream_queue_name is not None
        assert self._operator is not None

        import pyarrow as pa

        from _internal.core.models import Split, SplitPayload

        # Collect msg_ids, claim_tokens, and payloads
        msg_ids: list[str] = []
        claim_tokens: list[str] = []
        tables: list[pa.Table] = []
        parent_split_ids: list[str] = []

        for record in records:
            if not record.claim_token:
                raise RuntimeError(f"Missing claim_token for message {record.msg_id}")
            msg_ids.append(record.msg_id)
            claim_tokens.append(record.claim_token)

            message = QueueMessage.from_bytes(record.value)
            if message.payload_key:
                payload = self.payload_store.get(message.payload_key)
                if payload is None:
                    self.logger.error(
                        f"Payload missing for key {message.payload_key}, "
                        f"nacking {len(records)} records"
                    )
                    self.queue_client.nack(
                        self.upstream_queue_name,
                        msg_ids,
                        claim_tokens=claim_tokens,
                        reason="payload_missing",
                        state_namespace=job_namespace(self.job_id),
                    )
                    return
                tables.append(payload.data)
                parent_split_ids.append(message.split_id)
            else:
                # Source message (no payload) -- only valid for single records
                pass

        # Build merged split + payload
        split_id = make_split_id(self.job_id, self.stage_id, msg_ids[0])

        if tables:
            merged_table = (
                tables[0]
                if len(tables) == 1
                else pa.concat_tables(tables, promote_options="default")
            )
            merged_payload: Optional[SplitPayload] = SplitPayload(
                data=merged_table, split_id=split_id
            )
        else:
            merged_table = None
            merged_payload = None

        # For source messages, use data_range from the first message
        first_message = QueueMessage.from_bytes(records[0].value)
        if not first_message.payload_key:
            data_range = first_message.metadata.get("data_range", {})
        else:
            data_range = {"merged_count": len(records)} if len(records) > 1 else {}

        split = Split(
            split_id=split_id,
            stage_id=self.stage_id,
            data_range=data_range,
            parent_split_ids=parent_split_ids,
        )

        # Process
        check_fault(FAULT_BEFORE_PROCESS)
        process_start = time.time()
        result = self._operator.process_split(split, merged_payload)
        check_fault(FAULT_AFTER_PROCESS)

        input_rows = merged_table.num_rows if merged_table is not None else 0
        input_bytes = merged_table.nbytes if merged_table is not None else 0

        # Build output
        output_bytes_list: list[bytes] = []

        if isinstance(result, RawOutputBytes):
            if self.output_queue_name:
                output_bytes_list = result.payloads
        else:
            output_payloads = await self._collect_outputs(result)
            if output_payloads and self.output_queue_name:
                for idx, out_payload in enumerate(output_payloads):
                    out_id = split_id if len(output_payloads) == 1 else f"{split_id}_{idx}"
                    self.payload_store.store(out_id, out_payload)
                    out_msg = QueueMessage(
                        message_id=out_id,
                        split_id=out_id,
                        payload_key=out_id,
                        metadata={"source_stage": self.stage_id},
                    )
                    output_bytes_list.append(out_msg.to_bytes())

        # For async operators, include await time in processing latency.
        processing_ms = max(0.0, (time.time() - process_start) * 1000.0)

        # Build WebUI event
        event_puts = self._build_event_puts(
            record=records[0],
            split_id=split_id,
            message=first_message,
            processing_ms=processing_ms,
            input_rows=input_rows,
            input_bytes=input_bytes,
        )

        # Atomic ack (+ forward if output exists)
        if output_bytes_list and self.output_queue_name:
            self.queue_client.ack_and_forward(
                upstream_queue=self.upstream_queue_name,
                upstream_msg_ids=msg_ids,
                upstream_claim_tokens=claim_tokens,
                downstream_queue=self.output_queue_name,
                downstream_payloads=output_bytes_list,
                state_namespace=job_namespace(self.job_id),
                state_puts=event_puts,
            )
        else:
            self.queue_client.ack(
                self.upstream_queue_name,
                msg_ids,
                claim_tokens=claim_tokens,
                state_namespace=job_namespace(self.job_id),
                state_puts=event_puts,
            )

    # =========================================================================
    # Helpers
    # =========================================================================

    @staticmethod
    async def _collect_outputs(result: Any) -> list:
        """Normalize process_split return value into list[SplitPayload]."""
        from _internal.core.models import SplitPayload

        if asyncio.iscoroutine(result):
            result = await result
            return await StageWorker._collect_outputs(result)
        if result is None:
            return []
        if isinstance(result, SplitPayload):
            return [result]
        if hasattr(result, "__aiter__"):
            return [p async for p in result]
        if hasattr(result, "__iter__"):
            return list(result)
        raise TypeError(
            f"process_split returned unsupported type {type(result).__name__}. "
            "Expected None, SplitPayload, Iterator, or async variants."
        )

    @staticmethod
    def _is_broker_error(e: Exception) -> bool:
        try:
            import grpc
        except Exception:
            grpc = None  # type: ignore[assignment]
        if grpc is not None and isinstance(e, grpc.RpcError):
            return True
        if isinstance(e, RuntimeError) and "Client not started" in str(e):
            return True
        return False

    def _build_event_puts(
        self,
        record: WorkQueueRecord,
        split_id: str,
        message: QueueMessage,
        processing_ms: float,
        input_rows: int,
        input_bytes: int,
    ) -> Dict[str, bytes]:
        ts_ns = time.time_ns()
        queue_wait_ms = max(0.0, (time.time() - record.created_at) * 1000.0)

        event = {
            "event_type": "ack",
            "timestamp": time.time(),
            "worker_id": self.worker_id,
            "processing_ms": processing_ms,
            "queue_wait_ms": queue_wait_ms,
            "input_rows": input_rows,
            "input_bytes": input_bytes,
        }
        split_event = {
            **event,
            "timestamp_ns": ts_ns,
            "stage_id": self.stage_id,
            "split_id": split_id,
        }
        source_stage = message.metadata.get("source_stage")
        if source_stage:
            split_event["source_stage"] = source_stage

        return {
            event_key(self.stage_id, ts_ns, record.msg_id): encode_json(event),
            split_key(split_id): encode_json(split_event),
        }

    def _should_exit(self) -> bool:
        return self._safe_to_exit

    async def _cleanup(self) -> None:
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
        self._safe_to_exit = True

    def get_status(self) -> Dict[str, Any]:
        import os

        return {
            "worker_id": self.worker_id,
            "stage_id": self.stage_id,
            "pid": os.getpid(),
            "running": self._running,
            "safe_to_exit": self._safe_to_exit,
        }

    def stop(self) -> None:
        self._running = False

    def invoke_operator(self, method_name: str, *args, **kwargs) -> Any:
        from _internal.core.operator import is_master_callable

        if not self._operator:
            return None
        method = getattr(self._operator, method_name, None)
        if method is None:
            return None
        if not is_master_callable(method):
            raise ValueError(f"Method '{method_name}' is not marked @master_callable.")
        return method(*args, **kwargs)
