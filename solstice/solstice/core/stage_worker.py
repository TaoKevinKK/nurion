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

"""StageWorker - Pull-based streaming worker with partition-per-operator model.

This worker implements the Simple Exactly-Once v4 architecture:

1. **Partition-per-Operator**: Each partition has its own dedicated Operator instance
2. **Concurrent Processing**: Partitions are processed in parallel via asyncio.gather
3. **Deterministic Split ID**: split_id = f(job, stage, partition, offset)
4. **At-Least-Once + Dedup**: Produce before commit, downstream deduplicates
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import ray

from solstice.queue import QueueType, QueueClient, MemoryClient, TansuQueueClient
from solstice.webui.state.producer import StateProducer
from solstice.utils.logging import create_ray_logger
from solstice.core.models import (
    QueueEndpoint,
    QueueMessage,
    make_split_id,
)
from solstice.core.split_payload_store import SplitPayloadStore
from solstice.core.operator import Operator, OperatorRuntime, SemanticGuarantee
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
    assigned_partitions: Tuple[int, ...]
    consumer_group: str
    semantic_guarantee: SemanticGuarantee

    # Queue endpoints
    upstream_endpoint: Optional[QueueEndpoint] = None
    upstream_topic: Optional[str] = None
    output_endpoint: Optional[QueueEndpoint] = None
    output_topic: Optional[str] = None

    # State push (WebUI)
    state_endpoint: Optional[QueueEndpoint] = None
    state_topic: Optional[str] = None

    # Processing config
    batch_size: int = 100
    commit_batch_size: int = 5


@ray.remote
class StageWorker:
    """Worker with partition-per-operator model for exactly-once semantics."""

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
        self.semantic_guarantee = runtime.semantic_guarantee
        self.assigned_partitions = list(runtime.assigned_partitions)
        self.consumer_group = runtime.consumer_group

        # Store endpoints
        self.upstream_endpoint = runtime.upstream_endpoint
        self.upstream_topic = runtime.upstream_topic
        self.output_endpoint = runtime.output_endpoint
        self.output_topic = runtime.output_topic

        # State push configuration
        self.state_endpoint = runtime.state_endpoint
        self.state_topic = runtime.state_topic

        # Processing config
        self._batch_size = runtime.batch_size
        self._commit_batch_size = runtime.commit_batch_size

        # Store references
        self.stage = stage
        self.payload_store = payload_store

        self._state_producer: Optional[StateProducer] = None

        # Queue connections (created lazily)
        self.upstream_queue: Optional[QueueClient] = None
        self.output_queue: Optional[QueueClient] = None

        self.logger = create_ray_logger(f"Worker-{self.stage_id}-{self.worker_id}")

        # Partition-per-Operator: Create one Operator per assigned partition
        self._partition_operators: Dict[int, Operator] = {}
        self._init_partition_operators()

        # Worker-level state
        self._running = False
        self._upstream_finished = False
        self._partition_update_event = asyncio.Event()

        # Counter for output partition distribution
        self._output_counter = 0

        # Buffer for split metrics (batch produce)
        self._pending_split_metrics: List[Any] = []

    def _init_partition_operators(self) -> None:
        """Initialize Operator instances for assigned partitions."""
        for partition_id in self.assigned_partitions:
            self._create_partition_operator(partition_id)

    def _create_partition_operator(self, partition_id: int) -> Operator:
        """Create a new Operator for the given partition."""
        runtime = OperatorRuntime(
            job_id=self.job_id,
            stage_id=self.stage_id,
            worker_id=f"{self.worker_id}_p{partition_id}",
            partition_id=partition_id,
            semantic_guarantee=self.semantic_guarantee,
        )

        op = self.stage.operator_config.setup(runtime)
        self._partition_operators[partition_id] = op
        op.init_from_state_store()

        self.logger.debug(f"Created Operator for partition {partition_id}")
        return op

    async def _create_queue_from_endpoint(self, endpoint: QueueEndpoint) -> QueueClient:
        """Create a queue connection from endpoint info."""
        queue: QueueClient
        if endpoint.queue_type == QueueType.TANSU:
            broker_url = f"{endpoint.host}:{endpoint.port}"
            queue = TansuQueueClient(broker_url)
        else:
            queue = MemoryClient(endpoint.storage_url)
        queue.start()
        return queue

    async def run(self) -> Dict[str, Any]:
        """Main entry point - runs all partition loops concurrently."""
        self._running = True
        self.logger.info(
            f"Worker {self.worker_id} starting with {len(self.assigned_partitions)} partitions"
        )

        if not self.upstream_endpoint or not self.upstream_topic:
            raise RuntimeError(
                f"Worker {self.worker_id} requires upstream_endpoint and upstream_topic."
            )

        try:
            # Create queue connections
            self.output_queue = await self._create_queue_from_endpoint(self.output_endpoint)
            self.upstream_queue = await self._create_queue_from_endpoint(self.upstream_endpoint)

            # Initialize state producer for WebUI
            await self._init_state_producer()
            await self._emit_worker_started()

            # Start periodic metrics reporter
            metrics_task = asyncio.create_task(
                self._periodic_metrics_loop(),
                name=f"metrics_{self.worker_id}",
            )

            try:
                await self._run_partition_loops()
                self.logger.info(f"Worker {self.worker_id} partition loops completed")
            finally:
                metrics_task.cancel()
                try:
                    await metrics_task
                except asyncio.CancelledError:
                    pass

            self.logger.info(f"Worker {self.worker_id} emitting stopped event")
            await self._emit_worker_stopped(reason="completed")

            # Aggregate stats from all partitions
            total_processed = sum(p.processed_count for p in self._partition_operators.values())
            total_errors = sum(p.error_count for p in self._partition_operators.values())

            return {
                "worker_id": self.worker_id,
                "processed_count": total_processed,
                "error_count": total_errors,
            }

        except Exception as e:
            self.logger.error(f"Worker {self.worker_id} failed: {e}")
            await self._emit_exception(e)
            await self._emit_worker_stopped(reason="failed")
            raise
        finally:
            self._running = False
            await self._cleanup()

    async def _run_partition_loops(self) -> None:
        """Run processing loops for all partitions concurrently."""
        for partition_id, pop in self._partition_operators.items():
            pop.task = asyncio.create_task(
                self._process_partition(partition_id),
                name=f"partition-{partition_id}",
            )

        while self._running and self._partition_operators:
            tasks = [pop.task for pop in self._partition_operators.values() if pop.task]
            if not tasks:
                break

            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=1.0,
            )

            for task in done:
                try:
                    task.result()
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    self.logger.error(f"Partition task failed: {e}")

            if self._partition_update_event.is_set():
                self._partition_update_event.clear()
                await self._handle_partition_update()

            all_done = all(
                pop.task is None or pop.task.done() for pop in self._partition_operators.values()
            )
            if all_done:
                self.logger.info(f"Worker {self.worker_id} all partitions done")
                break

    async def _process_partition(self, partition_id: int) -> None:
        """Process messages from a single partition."""
        assert self.upstream_queue is not None
        assert self.output_queue is not None
        assert self.upstream_topic is not None

        pop = self._partition_operators.get(partition_id)
        if pop is None:
            return

        eof_received = False
        empty_fetch_count = 0
        MAX_EMPTY_FETCHES_WHEN_UPSTREAM_DONE = 10

        self.logger.info(
            f"Partition {partition_id} loop starting, consumer_group={self.consumer_group}"
        )

        while self._running and not eof_received:
            try:
                records = self.upstream_queue.fetch(
                    self.upstream_topic,
                    max_records=self._batch_size,
                    timeout_ms=1000,
                    partition=partition_id,
                    group_id=self.consumer_group,
                )

                if not records:
                    empty_fetch_count += 1
                    if (
                        self._upstream_finished
                        and empty_fetch_count >= MAX_EMPTY_FETCHES_WHEN_UPSTREAM_DONE
                    ):
                        self.logger.info(f"Partition {partition_id} done (upstream finished)")
                        break
                    await asyncio.sleep(0.05)
                    continue

                empty_fetch_count = 0

                for record in records:
                    message = QueueMessage.from_bytes(record.value)

                    if message.is_eof():
                        eof_received = True
                        self.logger.info(f"Partition {partition_id} received EOF")
                        self.upstream_queue.commit_offset(
                            self.consumer_group,
                            self.upstream_topic,
                            record.offset + 1,
                            partition=partition_id,
                        )
                        break

                    if pop.is_duplicate(record.offset):
                        self.logger.debug(f"Skipping duplicate offset: {record.offset}")
                        self.upstream_queue.commit_offset(
                            self.consumer_group,
                            self.upstream_topic,
                            record.offset + 1,
                            partition=partition_id,
                        )
                        continue

                    split_id = make_split_id(
                        self.job_id, self.stage_id, partition_id, record.offset
                    )

                    check_fault(FAULT_BEFORE_PROCESS)
                    await self._process_message(pop, message, record.offset, partition_id, split_id)
                    check_fault(FAULT_AFTER_PROCESS)
                    check_fault(FAULT_BEFORE_MARK_PROCESSED)
                    pop.mark_processed(record.offset)
                    check_fault(FAULT_AFTER_MARK_PROCESSED)

                    self.upstream_queue.commit_offset(
                        self.consumer_group,
                        self.upstream_topic,
                        record.offset + 1,
                        partition=partition_id,
                    )

            except asyncio.CancelledError:
                self.logger.info(f"Partition {partition_id} loop cancelled")
                raise
            except Exception as e:
                pop.error_count += 1
                self.logger.error(f"Error in partition {partition_id}: {e}")
                await asyncio.sleep(0.1)

        self.logger.info(f"Partition {partition_id} finished: processed={pop.processed_count}")

    async def _process_message(
        self,
        op: Operator,
        message: QueueMessage,
        offset: int,
        partition_id: int,
        split_id: str,
    ) -> None:
        """Process a single message using the partition's operator."""
        from solstice.core.models import Split, SplitPayload

        assert self.output_queue is not None

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

        # Process with partition's operator
        start_time = time.time()
        output_payload = op.process_split(split, payload)
        process_time_ms = (time.time() - start_time) * 1000

        # Update metrics
        input_records = len(payload) if payload else 0
        output_records = len(output_payload) if output_payload else 0
        op.total_input_records += input_records
        op.total_output_records += output_records
        op.total_processing_time += process_time_ms / 1000

        # Record split metric for batch sending
        self._record_split_metric(
            partition_id=partition_id,
            offset=offset,
            process_time_ms=process_time_ms,
            input_records=input_records,
            output_records=output_records,
        )

        # Produce output if any
        if output_payload:
            # Use a per-worker counter for even distribution across output partitions
            # This ensures all output partitions receive data regardless of how
            # splits are distributed in the source queue
            output_partition = self._get_output_partition(self._output_counter)
            self._output_counter += 1
            payload_key = split_id

            self.payload_store.store(payload_key, output_payload)

            output_message = QueueMessage(
                message_id=split_id,
                split_id=split_id,
                payload_key=payload_key,
                metadata={
                    "source_stage": self.stage_id,
                    "parent_message_id": message.message_id,
                    "partition": partition_id,
                    "offset": offset,
                },
            )

            self.output_queue.produce(
                self.output_topic,
                output_message.to_bytes(),
                partition=output_partition,
            )

    def _get_output_partition(self, routing_key: int) -> int:
        """Map a routing key to a valid output partition."""
        partition_count = self._get_output_partition_count()
        if partition_count <= 1:
            return 0
        return routing_key % partition_count

    def _get_output_partition_count(self) -> int:
        """Compute output partition count."""
        if self.stage.output_partitions is not None:
            return max(1, self.stage.output_partitions)
        if self.stage.max_parallelism <= 1:
            return 1
        return self.stage.max_parallelism

    async def _cleanup(self) -> None:
        """Clean up resources."""
        for pop in self._partition_operators.values():
            try:
                pop.close()
            except Exception as e:
                self.logger.warning(f"Error closing partition operator: {e}")
        self._partition_operators.clear()

        if self._state_producer:
            try:
                await self._state_producer.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping state producer: {e}")

        if self.upstream_queue:
            self.upstream_queue.stop()
        if self.output_queue:
            self.output_queue.stop()

    # === Partition Rebalancing ===

    def update_partitions(self, partitions: List[int]) -> None:
        """Update the partition assignment for this worker."""
        old_partitions = set(self.assigned_partitions)
        new_partitions = set(partitions)

        added = new_partitions - old_partitions
        removed = old_partitions - new_partitions

        self.assigned_partitions = list(partitions)
        self._partition_update_event.set()

        self.logger.info(
            f"Worker {self.worker_id} partition update: "
            f"added={list(added)}, removed={list(removed)}"
        )

    async def _handle_partition_update(self) -> None:
        """Handle partition update during runtime."""
        current_partitions = set(self._partition_operators.keys())
        target_partitions = set(self.assigned_partitions)

        for partition_id in current_partitions - target_partitions:
            pop = self._partition_operators.pop(partition_id, None)
            if pop:
                if pop.task and not pop.task.done():
                    pop.task.cancel()
                    try:
                        await pop.task
                    except asyncio.CancelledError:
                        pass
                pop.close()
                self.logger.info(f"Removed partition {partition_id}")

        for partition_id in target_partitions - current_partitions:
            pop = self._create_partition_operator(partition_id)
            pop.task = asyncio.create_task(
                self._process_partition(partition_id),
                name=f"partition-{partition_id}",
            )
            self.logger.info(f"Added partition {partition_id}")

    # === Status and Control ===

    def notify_upstream_finished(self) -> None:
        """Called by master when upstream stage(s) have finished."""
        self._upstream_finished = True
        self.logger.info(f"Worker {self.worker_id} notified: upstream finished")

    def get_status(self) -> Dict[str, Any]:
        """Get current worker status."""
        import os

        operators = self._partition_operators.values()

        return {
            "worker_id": self.worker_id,
            "stage_id": self.stage_id,
            "pid": os.getpid(),
            "running": self._running,
            "upstream_finished": self._upstream_finished,
            "assigned_partitions": self.assigned_partitions,
            "partition_count": len(self._partition_operators),
            "processed_count": sum(op.processed_count for op in operators),
            "error_count": sum(op.error_count for op in operators),
            "input_records": sum(op.total_input_records for op in operators),
            "output_records": sum(op.total_output_records for op in operators),
            "processing_time_s": sum(op.total_processing_time for op in operators),
        }

    def stop(self) -> None:
        """Stop the worker."""
        self._running = False
        self.logger.info(f"Worker {self.worker_id} stopping")

    def invoke_operator(
        self, method_name: str, *args, partition_id: Optional[int] = None, **kwargs
    ) -> Any:
        """Invoke an operator method by name."""
        from solstice.core.operator import is_master_callable

        if partition_id is not None:
            operator = self._partition_operators.get(partition_id)
            if operator is None:
                return None
        else:
            if not self._partition_operators:
                return None
            operator = next(iter(self._partition_operators.values()))

        method = getattr(operator, method_name, None)
        if method is None:
            return None

        if not is_master_callable(method):
            raise ValueError(f"Method '{method_name}' is not marked @master_callable.")

        return method(*args, **kwargs)

    # === State Producer and Events ===

    async def _init_state_producer(self) -> None:
        """Initialize state producer for metrics push."""
        if not self.state_endpoint or not self.state_topic:
            return

        try:
            state_queue = await self._create_queue_from_endpoint(self.state_endpoint)
            self._state_producer = StateProducer(
                job_id=self.job_id,
                queue_client=state_queue,
                state_topic=self.state_topic,
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
                assigned_partitions=self.assigned_partitions,
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
        """Background task to emit worker state and split metrics periodically.

        - WORKER_STATE: Immediate, lightweight status
        - SPLIT_METRICS_BATCH: Batch of atomic split metrics
        """
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
        partition_id: int,
        offset: int,
        process_time_ms: float,
        input_records: int = 0,
        output_records: int = 0,
    ) -> None:
        """Record a split metric for later batching."""
        from solstice.webui.state.messages import SplitMetric

        self._pending_split_metrics.append(
            SplitMetric(
                stage_id=self.stage_id,
                partition_id=partition_id,
                offset=offset,
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

            partition_offsets = {
                partition_id: op.last_offset
                for partition_id, op in self._partition_operators.items()
                if op.last_offset >= 0
            }

            msg = worker_state_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                worker_id=self.worker_id,
                status="RUNNING" if self._running else "STOPPED",
                assigned_partitions=list(self.assigned_partitions),
                partition_offsets=partition_offsets,
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
