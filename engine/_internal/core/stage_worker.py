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
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, NamedTuple, Optional

import ray

from _internal.core.models import (
    DataQueueMessage,
    QueueEndpoint,
    RawOutputBytes,
    SourceQueueMessage,
    make_split_id,
    queue_message_from_bytes,
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
    import pyarrow as pa

    from _internal.core.stage import Stage


class _ParsedBatch(NamedTuple):
    """Intermediate result of parsing claimed records."""

    msg_ids: list[str]
    claim_tokens: list[str]
    records: list[WorkQueueRecord]
    tables: "list[pa.Table]"
    parent_split_ids: list[str]
    consumed_payload_keys: list[str]
    source_message: Optional[SourceQueueMessage]
    source_stage: Optional[str]


@dataclass(frozen=True)
class OutputRouting:
    """Where to send processed output."""

    queue_name: Optional[str] = None
    partition_queue_names: Optional[tuple[str, ...]] = None
    partition_column: Optional[str] = None


@dataclass(frozen=True)
class WorkerRuntime:
    """Runtime parameters for StageWorker initialization."""

    worker_id: str
    job_id: str
    stage_id: str

    broker_endpoint: Optional[QueueEndpoint] = None
    upstream_queue_name: Optional[str] = None
    output: OutputRouting = field(default_factory=OutputRouting)

    batch_size: int = 100
    claim_timeout_secs: float = 60.0

    # Partition support: assigned partition queue names to claim from
    assigned_partition_queue_names: Optional[tuple[str, ...]] = None


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
        self._runtime = runtime
        self._output = runtime.output  # shortcut for frequently accessed output routing

        self.worker_id = runtime.worker_id
        self.job_id = runtime.job_id
        self.stage_id = runtime.stage_id

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
            broker_endpoint=self._runtime.broker_endpoint,
            payload_store=self.payload_store,
        )
        self._operator = self.stage.operator_config.setup(runtime)

    def _create_queue_client(self) -> WorkQueueQueueClient:
        if not self._runtime.broker_endpoint:
            raise RuntimeError("broker_endpoint is required")
        broker_url = f"{self._runtime.broker_endpoint.host}:{self._runtime.broker_endpoint.port}"
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

        if not self._runtime.broker_endpoint or not self._runtime.upstream_queue_name:
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

        If this worker is downstream of a shuffle stage, it claims from
        its assigned partition queues in round-robin instead of one queue.
        """
        assert self.queue_client is not None

        if self._runtime.assigned_partition_queue_names:
            await self._run_partition_claim_loop()
        else:
            await self._run_single_queue_claim_loop()

    async def _run_single_queue_claim_loop(self) -> None:
        """Standard claim loop from a single upstream queue."""
        assert self.queue_client is not None
        assert self._runtime.upstream_queue_name is not None

        upstream_queue = self._runtime.upstream_queue_name
        merge = self._merge_upstream
        pending: list[WorkQueueRecord] = []

        while self._running:
            try:
                records = self.queue_client.claim(
                    upstream_queue,
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

    async def _run_partition_claim_loop(self) -> None:
        """Claim from assigned partition queues in round-robin.

        Each partition queue is processed independently to maintain partition
        affinity — all records in a single _process_and_ack call come from
        the same partition queue.
        """
        assert self.queue_client is not None
        assert self._runtime.assigned_partition_queue_names is not None

        queues = list(self._runtime.assigned_partition_queue_names)
        merge = self._merge_upstream

        while self._running:
            any_records = False
            try:
                for queue_name in queues:
                    records = self.queue_client.claim(
                        queue_name,
                        batch_size=self._batch_size,
                        timeout_ms=200,
                    )
                    if not records:
                        continue

                    any_records = True
                    pending: list[WorkQueueRecord] = list(records)

                    # Process complete groups from this partition queue
                    while len(pending) >= merge:
                        group = pending[:merge]
                        pending = pending[merge:]
                        await self._process_and_ack(group, upstream_queue_override=queue_name)

                    # Flush partial group
                    if pending:
                        await self._process_and_ack(pending, upstream_queue_override=queue_name)

                if not any_records:
                    if self._should_exit():
                        break
                    await asyncio.sleep(0.05)

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

    async def _process_and_ack(
        self,
        records: list[WorkQueueRecord],
        upstream_queue_override: Optional[str] = None,
    ) -> None:
        """Process one or more records and ack atomically.

        When len(records) == 1: equivalent to the old single-record path.
        When len(records) > 1: merges payloads via Arrow concat, processes once.
        All upstream messages are acked atomically via ack_and_forward.

        Args:
            records: Claimed records to process.
            upstream_queue_override: If set, use this queue name for ack
                instead of self.upstream_queue_name. Used by partition claim
                loop where records come from different partition queues.
        """
        assert self.queue_client is not None
        assert self._operator is not None

        upstream_queue = upstream_queue_override or self._runtime.upstream_queue_name
        assert upstream_queue is not None

        batch = self._parse_records(records, upstream_queue=upstream_queue)
        if batch is None:
            return  # already nacked

        split_id, split, merged_payload = self._merge_and_build_split(batch)

        check_fault(FAULT_BEFORE_PROCESS)
        process_start = time.time()
        result = self._operator.process_split(split, merged_payload)
        check_fault(FAULT_AFTER_PROCESS)

        # Resolve async results now so processing_ms includes actual work
        # (e.g., LLM API calls in async operators).
        collected: Any = (
            result if isinstance(result, RawOutputBytes) else await self._collect_outputs(result)
        )

        processing_ms = max(0.0, (time.time() - process_start) * 1000.0)

        input_rows = merged_payload.data.num_rows if merged_payload else 0
        input_bytes = merged_payload.data.nbytes if merged_payload else 0
        event_puts = self._build_event_puts(
            records=batch.records,
            split_id=split_id,
            source_stage=batch.source_stage,
            processing_ms=processing_ms,
            input_rows=input_rows,
            input_bytes=input_bytes,
        )

        # Shuffle output: split by partition and push to partition queues
        if self._output.partition_queue_names and not isinstance(collected, RawOutputBytes):
            await self._shuffle_output_and_ack(
                collected,
                split_id,
                batch,
                upstream_queue,
                event_puts,
            )
        else:
            # Standard path: atomic ack_and_forward to single output queue
            output_bytes_list = await self._serialize_outputs(collected, split_id)
            if output_bytes_list and self._output.queue_name:
                self.queue_client.ack_and_forward(
                    upstream_queue=upstream_queue,
                    upstream_msg_ids=batch.msg_ids,
                    upstream_claim_tokens=batch.claim_tokens,
                    downstream_queue=self._output.queue_name,
                    downstream_payloads=output_bytes_list,
                    state_namespace=job_namespace(self.job_id),
                    state_puts=event_puts,
                )
            else:
                self.queue_client.ack(
                    upstream_queue,
                    batch.msg_ids,
                    claim_tokens=batch.claim_tokens,
                    state_namespace=job_namespace(self.job_id),
                    state_puts=event_puts,
                )

        # Eagerly free input payloads now that ack succeeded.
        for key in batch.consumed_payload_keys:
            try:
                self.payload_store.delete(key)
            except Exception as e:
                self.logger.warning(f"Failed to delete consumed payload {key}: {e}")

    # =========================================================================
    # _process_and_ack helpers
    # =========================================================================

    def _parse_records(
        self,
        records: list[WorkQueueRecord],
        upstream_queue: Optional[str] = None,
    ) -> Optional[_ParsedBatch]:
        """Parse claimed records, fetch payloads. Returns None if nacked."""
        nack_queue = upstream_queue or self._runtime.upstream_queue_name

        msg_ids: list[str] = []
        claim_tokens: list[str] = []
        tables: list = []  # pa.Table items
        parent_split_ids: list[str] = []
        consumed_payload_keys: list[str] = []
        source_message: Optional[SourceQueueMessage] = None
        source_stage: Optional[str] = None

        for record in records:
            if not record.claim_token:
                raise RuntimeError(f"Missing claim_token for message {record.msg_id}")
            msg_ids.append(record.msg_id)
            claim_tokens.append(record.claim_token)

            message = queue_message_from_bytes(record.value)
            if source_stage is None:
                source_stage = message.metadata.get("source_stage")
            if isinstance(message, SourceQueueMessage):
                source_message = message
            else:
                payload = self.payload_store.get(message.payload_key)
                if payload is None:
                    self.logger.error(
                        f"Payload missing for key {message.payload_key}, "
                        f"nacking {len(records)} records"
                    )
                    self._nack_all(
                        msg_ids,
                        claim_tokens,
                        reason="payload_missing",
                        upstream_queue_override=nack_queue,
                    )
                    return None
                tables.append(payload.data)
                parent_split_ids.append(message.split_id)
                consumed_payload_keys.append(message.payload_key)

        return _ParsedBatch(
            msg_ids=msg_ids,
            claim_tokens=claim_tokens,
            records=records,
            tables=tables,
            parent_split_ids=parent_split_ids,
            consumed_payload_keys=consumed_payload_keys,
            source_message=source_message,
            source_stage=source_stage,
        )

    def _nack_all(
        self,
        msg_ids: list[str],
        claim_tokens: list[str],
        reason: str,
        upstream_queue_override: Optional[str] = None,
    ) -> None:
        """Nack all messages with WebUI nack events."""
        assert self.queue_client is not None
        queue = upstream_queue_override or self._runtime.upstream_queue_name
        assert queue is not None

        ts_ns = time.time_ns()
        nack_puts: Dict[str, bytes] = {}
        for mid in msg_ids:
            nack_puts[event_key(self.stage_id, ts_ns, mid)] = encode_json(
                {
                    "event_type": "nack",
                    "timestamp": time.time(),
                    "worker_id": self.worker_id,
                    "stage_id": self.stage_id,
                    "reason": reason,
                }
            )
            ts_ns += 1
        self.queue_client.nack(
            queue,
            msg_ids,
            claim_tokens=claim_tokens,
            reason=reason,
            state_namespace=job_namespace(self.job_id),
            state_puts=nack_puts,
        )

    def _merge_and_build_split(self, batch: _ParsedBatch) -> "tuple[str, Any, Any]":
        """Merge Arrow tables and build Split + SplitPayload.

        Returns (split_id, Split, Optional[SplitPayload]).
        """
        import pyarrow as pa

        from _internal.core.models import Split, SplitPayload

        split_id = make_split_id(self.job_id, self.stage_id, batch.msg_ids[0])

        if batch.tables:
            merged_table = (
                batch.tables[0]
                if len(batch.tables) == 1
                else pa.concat_tables(batch.tables, promote_options="default")
            )
            merged_payload: Optional[SplitPayload] = SplitPayload(
                data=merged_table, split_id=split_id
            )
        else:
            merged_payload = None

        if batch.source_message is not None:
            data_range = batch.source_message.data_range
            source_split_id = batch.source_message.split_id or split_id
        else:
            data_range = {"merged_count": len(batch.records)} if len(batch.records) > 1 else {}
            source_split_id = split_id

        split = Split(
            split_id=source_split_id,
            stage_id=self.stage_id,
            data_range=data_range,
            parent_split_ids=batch.parent_split_ids,
        )

        return split_id, split, merged_payload

    async def _serialize_outputs(self, result: Any, split_id: str) -> list[bytes]:
        """Serialize process_split results into output messages.

        Args:
            result: Either RawOutputBytes or a pre-collected list[SplitPayload].
        """
        output_bytes_list: list[bytes] = []

        if isinstance(result, RawOutputBytes):
            if self._output.queue_name:
                output_bytes_list = result.payloads
        else:
            # result is already collected (list[SplitPayload]) by _process_and_ack.
            output_payloads = (
                result if isinstance(result, list) else await self._collect_outputs(result)
            )
            if output_payloads and self._output.queue_name:
                for idx, out_payload in enumerate(output_payloads):
                    out_id = split_id if len(output_payloads) == 1 else f"{split_id}_{idx}"
                    self.payload_store.store(out_id, out_payload)
                    out_msg = DataQueueMessage(
                        message_id=out_id,
                        split_id=out_id,
                        payload_key=out_id,
                        metadata={"source_stage": self.stage_id},
                    )
                    output_bytes_list.append(out_msg.to_bytes())

        return output_bytes_list

    # =========================================================================
    # Partitioned output
    # =========================================================================

    async def _shuffle_output_and_ack(
        self,
        result: Any,
        split_id: str,
        batch: _ParsedBatch,
        upstream_queue: str,
        event_puts: Dict[str, bytes],
    ) -> None:
        """Split output by partition column and push to partition queues.

        Uses at-least-once semantics: push all partition messages first, then
        ack upstream. If the worker crashes between push and ack, upstream
        messages will be reprocessed and partition queues may receive duplicates.
        This matches Spark's shuffle write semantics.
        """
        from _internal.core.partition import split_table_by_column

        assert self.queue_client is not None
        assert self._output.partition_queue_names is not None
        assert self._output.partition_column is not None

        # result is already collected (list[SplitPayload]) by _process_and_ack.
        output_payloads = (
            result if isinstance(result, list) else await self._collect_outputs(result)
        )
        if not output_payloads:
            # No output: just ack upstream
            self.queue_client.ack(
                upstream_queue,
                batch.msg_ids,
                claim_tokens=batch.claim_tokens,
                state_namespace=job_namespace(self.job_id),
                state_puts=event_puts,
            )
            return

        partition_column = self._output.partition_column

        # Split each output payload by partition and push to partition queues
        for idx, out_payload in enumerate(output_payloads):
            table = out_payload.data
            if partition_column not in table.column_names:
                # No partition column (e.g., filtered to None then returned).
                # Push to first partition queue as fallback.
                out_id = split_id if len(output_payloads) == 1 else f"{split_id}_{idx}"
                self.payload_store.store(out_id, out_payload)
                out_msg = DataQueueMessage(
                    message_id=out_id,
                    split_id=out_id,
                    payload_key=out_id,
                    metadata={"source_stage": self.stage_id},
                )
                self.queue_client.push(self._output.partition_queue_names[0], out_msg.to_bytes())
                continue

            partition_tables = split_table_by_column(table, partition_column)
            for partition_id, partition_table in partition_tables.items():
                if partition_id < 0 or partition_id >= len(self._output.partition_queue_names):
                    self.logger.warning(
                        f"Partition ID {partition_id} out of range "
                        f"[0, {len(self._output.partition_queue_names)}), skipping"
                    )
                    continue

                out_id = f"{split_id}_{idx}_p{partition_id}"
                from _internal.core.models import SplitPayload

                partition_payload = SplitPayload(data=partition_table, split_id=out_id)
                self.payload_store.store(out_id, partition_payload)
                out_msg = DataQueueMessage(
                    message_id=out_id,
                    split_id=out_id,
                    payload_key=out_id,
                    metadata={
                        "source_stage": self.stage_id,
                        "partition_id": str(partition_id),
                    },
                )
                partition_queue = self._output.partition_queue_names[partition_id]
                self.queue_client.push(partition_queue, out_msg.to_bytes())

        # After all pushes succeed, ack upstream (at-least-once)
        self.queue_client.ack(
            upstream_queue,
            batch.msg_ids,
            claim_tokens=batch.claim_tokens,
            state_namespace=job_namespace(self.job_id),
            state_puts=event_puts,
        )

    # =========================================================================
    # General helpers
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
        records: list[WorkQueueRecord],
        split_id: str,
        source_stage: Optional[str],
        processing_ms: float,
        input_rows: int,
        input_bytes: int,
    ) -> Dict[str, bytes]:
        now = time.time()
        ts_ns = time.time_ns()
        puts: Dict[str, bytes] = {}

        # One ack event per record (each has its own queue_wait_ms)
        for i, record in enumerate(records):
            queue_wait_ms = max(0.0, (now - record.created_at) * 1000.0)
            event = {
                "event_type": "ack",
                "timestamp": now,
                "worker_id": self.worker_id,
                "processing_ms": processing_ms,
                "queue_wait_ms": queue_wait_ms,
                "input_rows": input_rows,
                "input_bytes": input_bytes,
            }
            puts[event_key(self.stage_id, ts_ns + i, record.msg_id)] = encode_json(event)

        # One split event (represents this merged processing unit)
        split_event = {
            "event_type": "ack",
            "timestamp": now,
            "timestamp_ns": ts_ns,
            "worker_id": self.worker_id,
            "stage_id": self.stage_id,
            "split_id": split_id,
            "processing_ms": processing_ms,
            "input_rows": input_rows,
            "input_bytes": input_bytes,
        }
        if source_stage:
            split_event["source_stage"] = source_stage
        puts[split_key(split_id)] = encode_json(split_event)

        return puts

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
