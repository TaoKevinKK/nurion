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

Key design principles:
- State isolation: Each operator manages only its own partition's state
- True parallelism: Multiple partitions processed concurrently
- Simple recovery: Re-process from last committed offset, downstream dedup handles duplicates
"""

from __future__ import annotations

import asyncio
import copy
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray

from solstice.queue import QueueType, QueueClient, MemoryClient, TansuQueueClient
from solstice.webui.state.producer import StateProducer
from solstice.utils.logging import create_ray_logger
from solstice.core.stage_config import (
    StageConfig,
    QueueEndpoint,
    QueueMessage,
    make_split_id,
)
from solstice.core.split_payload_store import SplitPayloadStore
from solstice.core.operator import Operator, SemanticGuarantee
from solstice.testing.fault_injection import check_fault, FAULT_BEFORE_MARK_PROCESSED

if TYPE_CHECKING:
    from solstice.core.stage import Stage


@ray.remote
class StageWorker:
    """Worker with partition-per-operator model for exactly-once semantics.

    Each assigned partition gets its own Operator instance, allowing:
    - Independent state management per partition
    - True concurrent processing via asyncio.gather
    - Simplified recovery (re-process + dedup)

    Exactly-once semantics (At-Least-Once + Downstream Dedup):
    1. Fetch from partition
    2. Check for duplicate (offset-based or split_id-based)
    3. Process with partition's dedicated operator
    4. Produce output with deterministic split_id
    5. Commit upstream offset
    6. Downstream worker deduplicates using split_id
    """

    def __init__(
        self,
        worker_id: str,
        job_id: str,
        stage: "Stage",
        upstream_endpoint: Optional[QueueEndpoint],
        upstream_topic: Optional[str],
        output_endpoint: QueueEndpoint,
        output_topic: str,
        consumer_group: str,
        assigned_partitions: List[int],
        config: StageConfig,
        payload_store: SplitPayloadStore,
        state_endpoint: Optional[QueueEndpoint] = None,
        state_topic: Optional[str] = None,
        lineage_sample_rate: float = 0.0,
        semantic_guarantee: SemanticGuarantee = SemanticGuarantee.AT_LEAST_ONCE,
    ):
        self.worker_id = worker_id
        self.job_id = job_id
        self.stage_id = stage.stage_id
        self.stage = stage
        self.config = config
        self.semantic_guarantee = semantic_guarantee

        # SplitPayloadStore for storing SplitPayload data across workers
        self.payload_store = payload_store

        # Store endpoints (will create connections in run())
        self.upstream_endpoint = upstream_endpoint
        self.upstream_topic = upstream_topic
        self.output_endpoint = output_endpoint
        self.output_topic = output_topic
        self.consumer_group = consumer_group
        self.assigned_partitions = list(assigned_partitions)

        # State push configuration (optional, for WebUI)
        self.state_endpoint = state_endpoint
        self.state_topic = state_topic
        self._state_producer: Optional[StateProducer] = None

        # Lineage tracking configuration
        self._lineage_sample_rate = lineage_sample_rate

        # Queue connections (created lazily)
        self.upstream_queue: Optional[QueueClient] = None
        self.output_queue: Optional[QueueClient] = None

        self.logger = create_ray_logger(f"Worker-{self.stage_id}-{worker_id}")

        # Partition-per-Operator: Create one Operator per assigned partition
        self._partition_operators: Dict[int, Operator] = {}
        self._init_partition_operators()

        # Worker-level state
        self._running = False
        self._upstream_finished = False
        self._partition_update_event = asyncio.Event()

    def _init_partition_operators(self) -> None:
        """Initialize Operator instances for assigned partitions."""
        for partition_id in self.assigned_partitions:
            self._create_partition_operator(partition_id)

    def _create_partition_operator(self, partition_id: int) -> Operator:
        """Create a new Operator for the given partition.

        Each partition gets its own Operator instance with isolated state.
        """
        # Deep copy the config to avoid shared state
        op_config = copy.deepcopy(self.stage.operator_config)
        op_config.job_id = self.job_id
        op_config.stage_id = self.stage_id
        op_config.worker_id = f"{self.worker_id}_p{partition_id}"
        op_config.partition_id = partition_id
        op_config.semantic_guarantee = self.semantic_guarantee

        # Create operator instance
        operator = op_config.setup()
        self._partition_operators[partition_id] = operator

        # Initialize from state store for recovery (if operator has one)
        operator.init_from_state_store()

        self.logger.debug(f"Created Operator for partition {partition_id}")
        return operator

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

            # Run all partition loops concurrently
            await self._run_partition_loops()
            self.logger.info(f"Worker {self.worker_id} partition loops completed")

            # Emit completion
            self.logger.info(f"Worker {self.worker_id} emitting stopped event")
            await self._emit_worker_stopped(reason="completed")
            self.logger.info(f"Worker {self.worker_id} stopped event emitted")

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
        """Run processing loops for all partitions concurrently using asyncio.gather."""
        # Create tasks for each partition
        for partition_id, pop in self._partition_operators.items():
            pop.task = asyncio.create_task(
                self._process_partition(partition_id),
                name=f"partition-{partition_id}",
            )

        # Wait for all partition tasks to complete
        # This will also handle partition rebalancing via task cancellation/creation
        while self._running and self._partition_operators:
            # Get current tasks
            tasks = [pop.task for pop in self._partition_operators.values() if pop.task]

            if not tasks:
                break

            # Wait for any task to complete or for partition update
            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=1.0,  # Check for partition updates periodically
            )

            # Handle completed tasks
            for task in done:
                try:
                    task.result()  # Raise any exceptions
                except asyncio.CancelledError:
                    pass  # Task was cancelled during rebalance
                except Exception as e:
                    self.logger.error(f"Partition task failed: {e}")

            # Check if partition update was requested
            if self._partition_update_event.is_set():
                self._partition_update_event.clear()
                await self._handle_partition_update()

            # Check if all partitions are done
            all_done = all(
                pop.task is None or pop.task.done() for pop in self._partition_operators.values()
            )
            if all_done:
                self.logger.info(f"Worker {self.worker_id} all partitions done, exiting loop")
                break

            # Debug: log task states periodically
            task_states = {
                pid: ("done" if pop.task and pop.task.done() else "running" if pop.task else "none")
                for pid, pop in self._partition_operators.items()
            }
            self.logger.debug(f"Worker {self.worker_id} task states: {task_states}")

    async def _process_partition(self, partition_id: int) -> None:
        """Process messages from a single partition.

        Each partition runs its own independent processing loop with:
        - Dedicated operator instance
        - Independent offset tracking
        - Deduplication via split_id
        """
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
                # Fetch from this partition
                records = self.upstream_queue.fetch(
                    self.upstream_topic,
                    max_records=self.config.batch_size,
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
                        self.logger.info(
                            f"Partition {partition_id} done (upstream finished, {empty_fetch_count} empty fetches)"
                        )
                        break
                    await asyncio.sleep(0.05)
                    continue

                empty_fetch_count = 0

                # Process records
                for record in records:
                    message = QueueMessage.from_bytes(record.value)

                    # Check for EOF
                    if message.is_eof():
                        eof_received = True
                        self.logger.info(f"Partition {partition_id} received EOF")
                        # Commit EOF offset
                        self.upstream_queue.commit_offset(
                            self.consumer_group,
                            self.upstream_topic,
                            record.offset + 1,
                            partition=partition_id,
                        )
                        break

                    # Deduplication check (offset-based for sequential consumption)
                    if pop.is_duplicate(record.offset):
                        self.logger.debug(f"Skipping duplicate offset: {record.offset}")
                        self.upstream_queue.commit_offset(
                            self.consumer_group,
                            self.upstream_topic,
                            record.offset + 1,
                            partition=partition_id,
                        )
                        continue

                    # Generate deterministic split_id for downstream
                    split_id = make_split_id(
                        self.job_id, self.stage_id, partition_id, record.offset
                    )

                    # Process the message
                    await self._process_message(pop, message, record.offset, partition_id, split_id)

                    # Fault injection point (no-op in production)
                    check_fault(FAULT_BEFORE_MARK_PROCESSED)

                    # Mark as processed
                    pop.mark_processed(record.offset)

                    # Commit offset (at-least-once: commit after produce)
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
                await asyncio.sleep(0.1)  # Brief pause before retry

        self.logger.info(
            f"Partition {partition_id} loop finished: processed={pop.processed_count}, errors={pop.error_count}"
        )

    async def _process_message(
        self,
        op: Operator,
        message: QueueMessage,
        offset: int,
        partition_id: int,
        split_id: str,
    ) -> None:
        """Process a single message using the partition's operator.

        Args:
            op: The Operator for this partition
            message: The queue message
            offset: The message offset
            partition_id: The partition being processed
            split_id: The deterministic split ID for this message
        """
        from solstice.core.models import Split, SplitPayload

        assert self.output_queue is not None

        payload: Optional[SplitPayload] = None
        is_source_message = not message.payload_key

        if is_source_message:
            # Source message: data_range is in metadata
            data_range = message.metadata.get("data_range", {})
            split = Split(
                split_id=message.split_id,
                stage_id=self.stage_id,
                data_range=data_range,
                parent_split_ids=[],
            )
        else:
            # Regular message: get payload from store
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
        dequeue_time = time.time()
        output_payload = op.process_split(split, payload)
        complete_time = time.time()
        processing_time = complete_time - dequeue_time

        # Update metrics
        input_records = len(payload) if payload else 0
        output_records = len(output_payload) if output_payload else 0
        op.total_input_records += input_records
        op.total_output_records += output_records
        op.total_processing_time += processing_time

        # Produce output if any
        payload_key = ""
        if output_payload:
            routing_key = partition_id
            if is_source_message:
                raw_split_index = message.metadata.get("split_index")
                if isinstance(raw_split_index, int):
                    routing_key = raw_split_index
            output_partition = self._get_output_partition(routing_key)
            # Use deterministic split_id as payload_key
            payload_key = split_id

            # Store in PayloadStore (cache, not persistence layer)
            self.payload_store.store(payload_key, output_payload)

            output_message = QueueMessage(
                message_id=split_id,
                split_id=split_id,  # Deterministic split_id for downstream dedup
                payload_key=payload_key,
                metadata={
                    "source_stage": self.stage_id,
                    "parent_message_id": message.message_id,
                    "partition": partition_id,
                    "offset": offset,
                },
            )

            # Produce to output queue (at-least-once)
            self.output_queue.produce(
                self.output_topic,
                output_message.to_bytes(),
                partition=output_partition,
            )

        # Emit lineage if configured
        if self._should_track_lineage():
            input_bytes = payload.data.nbytes if payload and hasattr(payload.data, "nbytes") else 0
            output_bytes = (
                output_payload.data.nbytes
                if output_payload and hasattr(output_payload.data, "nbytes")
                else 0
            )
            parent_ids = [] if is_source_message else [message.split_id]

            await self._emit_split_lineage(
                output_split_id=split_id,
                parent_split_ids=parent_ids,
                partition_id=partition_id,
                enqueue_time=message.timestamp,
                dequeue_time=dequeue_time,
                complete_time=complete_time,
                input_records=input_records,
                output_records=output_records,
                input_bytes=input_bytes,
                output_bytes=output_bytes,
                payload_key=payload_key,
            )

        # Emit metrics periodically
        await self._emit_worker_metrics()

    def _get_output_partition(self, routing_key: int) -> int:
        """Map a routing key to a valid output partition."""
        partition_count = self._get_output_partition_count()
        if partition_count <= 1:
            return 0
        return routing_key % partition_count

    def _get_output_partition_count(self) -> int:
        """Compute output partition count to avoid out-of-range publishes."""
        if self.config.partition_count is not None:
            return max(1, self.config.partition_count)
        if self.config.max_workers <= 1:
            return 1
        return self.config.max_workers

    async def _cleanup(self) -> None:
        """Clean up resources."""
        # Close all partition operators
        for pop in self._partition_operators.values():
            try:
                pop.close()
            except Exception as e:
                self.logger.warning(f"Error closing partition operator: {e}")
        self._partition_operators.clear()

        # Stop state producer
        if self._state_producer:
            try:
                await self._state_producer.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping state producer: {e}")

        # Cleanup queue connections
        if self.upstream_queue:
            self.upstream_queue.stop()
        if self.output_queue:
            self.output_queue.stop()

    # === Partition Rebalancing ===

    def update_partitions(self, partitions: List[int]) -> None:
        """Update the partition assignment for this worker.

        Called by master when partition rebalance occurs (e.g., scale up/down).
        """
        old_partitions = set(self.assigned_partitions)
        new_partitions = set(partitions)

        added = new_partitions - old_partitions
        removed = old_partitions - new_partitions

        self.assigned_partitions = list(partitions)
        self._partition_update_event.set()

        self.logger.info(
            f"Worker {self.worker_id} partition update: "
            f"added={list(added)}, removed={list(removed)}, "
            f"now handling {partitions}"
        )

    async def _handle_partition_update(self) -> None:
        """Handle partition update during runtime."""
        current_partitions = set(self._partition_operators.keys())
        target_partitions = set(self.assigned_partitions)

        # Remove partitions no longer assigned
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

        # Add newly assigned partitions
        for partition_id in target_partitions - current_partitions:
            pop = self._create_partition_operator(partition_id)
            pop.task = asyncio.create_task(
                self._process_partition(partition_id),
                name=f"partition-{partition_id}",
            )
            self.logger.info(f"Added partition {partition_id}")

    # === Status and Metrics ===

    def notify_upstream_finished(self) -> None:
        """Called by master when upstream stage(s) have finished."""
        self._upstream_finished = True
        self.logger.info(f"Worker {self.worker_id} notified: upstream finished")

    def get_status(self) -> Dict[str, Any]:
        """Get current worker status including metrics.

        Returns a dict with:
        - Identity: worker_id, stage_id, pid
        - State: running, upstream_finished
        - Partitions: assigned_partitions, partition_count
        - Counts: processed_count, error_count
        - Metrics: input_records, output_records, processing_time_s
        """
        import os

        operators = self._partition_operators.values()

        return {
            # Identity
            "worker_id": self.worker_id,
            "stage_id": self.stage_id,
            "pid": os.getpid(),
            # State
            "running": self._running,
            "upstream_finished": self._upstream_finished,
            # Partitions
            "assigned_partitions": self.assigned_partitions,
            "partition_count": len(self._partition_operators),
            # Counts
            "processed_count": sum(op.processed_count for op in operators),
            "error_count": sum(op.error_count for op in operators),
            # Metrics
            "input_records": sum(op.total_input_records for op in operators),
            "output_records": sum(op.total_output_records for op in operators),
            "processing_time_s": sum(op.total_processing_time for op in operators),
        }

    def stop(self) -> None:
        """Stop the worker."""
        self._running = False
        self.logger.info(f"Worker {self.worker_id} stopping")

    # === Operator Method Dispatch ===

    def invoke_operator(
        self, method_name: str, *args, partition_id: Optional[int] = None, **kwargs
    ) -> Any:
        """Invoke an operator method by name.

        If partition_id is specified, invokes on that partition's operator.
        Otherwise, invokes on the first partition's operator.

        Only methods marked with @master_callable decorator can be invoked.
        """
        from solstice.core.operator import is_master_callable

        # Select operator
        if partition_id is not None:
            operator = self._partition_operators.get(partition_id)
            if operator is None:
                return None
        else:
            # Use first partition's operator
            if not self._partition_operators:
                return None
            operator = next(iter(self._partition_operators.values()))

        method = getattr(operator, method_name, None)
        if method is None:
            return None

        if not is_master_callable(method):
            raise ValueError(
                f"Method '{method_name}' is not marked @master_callable. "
                f"Add the decorator to allow remote invocation."
            )

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

            total_processed = sum(p.processed_count for p in self._partition_operators.values())
            total_errors = sum(p.error_count for p in self._partition_operators.values())
            total_input = sum(p.total_input_records for p in self._partition_operators.values())
            total_output = sum(p.total_output_records for p in self._partition_operators.values())
            total_time = sum(p.total_processing_time for p in self._partition_operators.values())

            msg = worker_stopped_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                worker_id=self.worker_id,
                reason=reason,
                processed_count=total_processed,
                error_count=total_errors,
                input_records=total_input,
                output_records=total_output,
                processing_time=total_time,
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit worker stopped: {e}")

    async def _emit_worker_metrics(self) -> None:
        """Emit WORKER_METRICS event (rate limited)."""
        if not self._state_producer:
            return

        try:
            from solstice.webui.state.messages import worker_metrics_message

            total_processed = sum(p.processed_count for p in self._partition_operators.values())
            total_input = sum(p.total_input_records for p in self._partition_operators.values())
            total_output = sum(p.total_output_records for p in self._partition_operators.values())
            total_time = sum(p.total_processing_time for p in self._partition_operators.values())

            msg = worker_metrics_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                worker_id=self.worker_id,
                input_records=total_input,
                output_records=total_output,
                processing_time=total_time,
                processed_count=total_processed,
                assigned_partitions=self.assigned_partitions,
                is_running=self._running,
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit worker metrics: {e}")

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

    def _should_track_lineage(self) -> bool:
        """Check if this split should be tracked based on sample rate."""
        rate = self._lineage_sample_rate
        if rate <= 0.0:
            return False
        if rate >= 1.0:
            return True
        import random

        return random.random() < rate

    async def _emit_split_lineage(
        self,
        output_split_id: str,
        parent_split_ids: list[str],
        partition_id: int,
        enqueue_time: float,
        dequeue_time: float,
        complete_time: float,
        input_records: int,
        output_records: int,
        input_bytes: int,
        output_bytes: int,
        payload_key: str,
    ) -> None:
        """Emit SPLIT_PROCESSED event for lineage tracking."""
        if not self._state_producer:
            return

        try:
            from solstice.webui.state.messages import split_processed_message

            msg = split_processed_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                worker_id=self.worker_id,
                split_id=output_split_id,
                parent_split_ids=parent_split_ids,
                partition_id=partition_id,
                enqueue_time=enqueue_time,
                dequeue_time=dequeue_time,
                complete_time=complete_time,
                input_records=input_records,
                output_records=output_records,
                input_bytes=input_bytes,
                output_bytes=output_bytes,
                payload_store_key=payload_key,
                payload_storage_path=None,
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit split lineage: {e}")
