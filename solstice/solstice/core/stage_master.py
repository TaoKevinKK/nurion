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

"""Stage Master - orchestrates workers for a pipeline stage.

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                     Stage Master                            │
    │                                                             │
    │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐   │
    │  │ PartitionMgr  │  │  WorkerMgr    │  │ RecoveryMgr   │   │
    │  │ - assignment  │  │ - lifecycle   │  │ - failures    │   │
    │  │ - rebalance   │  │ - spawn/stop  │  │ - recovery    │   │
    │  └───────────────┘  └───────────────┘  └───────────────┘   │
    │                                                             │
    │  ┌───────────────┐  ┌─────────────────────────────────┐    │
    │  │BackpressureMon│  │        Output Queue             │    │
    │  │ - lag/skew    │  │  (Tansu or Memory)              │    │
    │  │ - scaling     │  └─────────────────────────────────┘    │
    │  └───────────────┘                                          │
    │                           ▲                                 │
    │  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
    │  │  Worker 1  │  │  Worker 2  │  │  Worker N  │            │
    │  └────────────┘  └────────────┘  └────────────┘            │
    └─────────────────────────────────────────────────────────────┘

Responsibilities:
1. Create and manage output queue
2. Coordinate managers (partition, worker, recovery, backpressure)
3. Run the main processing loop
4. Track stage completion and emit state events
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Dict, Optional

from solstice.queue import (
    QueueType,
    QueueClient,
    MemoryBroker,
    MemoryClient,
    TansuQueueClient,
)
from solstice.utils.logging import create_ray_logger
from solstice.core.split_payload_store import SplitPayloadStore
from solstice.core.models import (
    FailurePolicy,
    FailureTracker,
    QueueEndpoint,
    QueueMessage,
    StageStatus,
    create_queue_endpoint,
)
from solstice.core.stage_worker import StageWorker
from solstice.core.managers import (
    PartitionManager,
    WorkerManager,
    RecoveryManager,
    BackpressureMonitor,
)

if TYPE_CHECKING:
    from solstice.core.stage import Stage, StageRuntime
    from solstice.webui.state.producer import StateProducer

# Re-export for backward compatibility
__all__ = [
    "StageMaster",
    "StageWorker",
    "QueueEndpoint",
    "create_queue_endpoint",
    "QueueMessage",
    "StageStatus",
    "FailurePolicy",
    "FailureTracker",
]


class StageMaster:
    """Orchestrates workers for a pipeline stage.

    Uses component managers for specific concerns:
    - PartitionManager: Partition assignment and rebalancing
    - WorkerManager: Worker lifecycle (spawn, stop, status)
    - RecoveryManager: Failure tracking and worker recovery
    - BackpressureMonitor: Backpressure detection and scaling

    NOT responsible for:
    - Pulling from upstream (workers do this)
    - Scheduling splits to workers (workers self-schedule)
    """

    def __init__(
        self,
        job_id: str,
        stage: "Stage",
        payload_store: SplitPayloadStore,
        runtime: "StageRuntime",
    ):
        self.job_id = job_id
        self.stage_id = stage.stage_id
        self.stage = stage
        self.runtime = runtime
        self.logger = create_ray_logger(f"Master-{self.stage_id}")

        # Upstream queue connection (from runtime)
        self.upstream_endpoint = runtime.upstream_endpoint
        self.upstream_topic = runtime.upstream_topic
        self.state_endpoint = runtime.state_endpoint
        self.state_topic = runtime.state_topic

        # SplitPayloadStore - shared across all stages
        self.payload_store = payload_store

        # Output queue (managed by master)
        self._output_broker: Optional[MemoryBroker] = None
        self._output_queue: Optional[QueueClient] = None
        self._output_topic = f"{job_id}_{self.stage_id}_output"
        self._output_endpoint: Optional[QueueEndpoint] = None

        # Consumer group for offset tracking
        self._consumer_group = f"{job_id}_{self.stage_id}"

        # State
        self._running = False
        self._finished = False
        self._failed = False
        self._failure_message: Optional[str] = None
        self._start_time: Optional[float] = None
        self._upstream_finished = False

        # Downstream stage refs for backpressure (backward compatibility)
        self._downstream_stage_refs: Dict[str, "StageMaster"] = {}

        # State producer for WebUI metrics
        self._state_producer: Optional["StateProducer"] = None
        self._last_metrics_emit_time = 0.0

        # Initialize managers (will be fully configured in start())
        self._partition_manager = PartitionManager(
            stage=stage,
            runtime=runtime,
        )

        # Worker and recovery managers created after output queue is ready
        self._worker_manager: Optional[WorkerManager] = None
        self._recovery_manager: Optional[RecoveryManager] = None
        self._backpressure_monitor: Optional[BackpressureMonitor] = None

    async def _create_queue(self) -> QueueClient:
        """Connect to shared broker and create output topic."""
        partition_count = self._partition_manager.partition_count
        queue: QueueClient

        if self.runtime.queue_type == QueueType.TANSU:
            endpoint = self.runtime.shared_broker_endpoint
            if not endpoint:
                raise RuntimeError(
                    f"Stage {self.stage_id}: shared_broker_endpoint is required "
                    "for TANSU queue type"
                )

            broker_url = f"{endpoint.host}:{endpoint.port}"
            queue = TansuQueueClient(broker_url)
            queue.start()

            self._output_endpoint = QueueEndpoint(
                queue_type=self.runtime.queue_type,
                host=endpoint.host,
                port=endpoint.port,
                storage_url=endpoint.storage_url,
            )
            self.logger.info(f"Connected to shared broker at {broker_url}")
        else:
            # MEMORY: Create local broker (for testing)
            if partition_count > 1:
                self.logger.warning(
                    f"Memory backend doesn't support multiple partitions. "
                    f"Using 1 partition instead of {partition_count}"
                )
                partition_count = 1

            self._output_broker = MemoryBroker()
            self._output_broker.start()

            queue = MemoryClient(self._output_broker)
            queue.start()

            self._output_endpoint = QueueEndpoint(
                queue_type=self.runtime.queue_type,
                host="memory",
                port=0,
                storage_url=self._output_broker.get_broker_url(),
            )

        queue.create_topic(self._output_topic, partitions=partition_count)
        self.logger.info(f"Created topic {self._output_topic} with {partition_count} partition(s)")
        return queue

    def _init_managers(self) -> None:
        """Initialize managers after output queue is created."""
        self._worker_manager = WorkerManager(
            job_id=self.job_id,
            stage=self.stage,
            runtime=self.runtime,
            partition_manager=self._partition_manager,
            payload_store=self.payload_store,
            output_endpoint=self._output_endpoint,
            output_topic=self._output_topic,
            consumer_group=self._consumer_group,
            state_endpoint=self.state_endpoint,
            state_topic=self.state_topic,
        )

        self._recovery_manager = RecoveryManager(
            stage_id=self.stage_id,
            partition_manager=self._partition_manager,
            worker_manager=self._worker_manager,
            policy=FailurePolicy(),
        )

        self._backpressure_monitor = BackpressureMonitor(
            stage=self.stage,
            runtime=self.runtime,
            partition_manager=self._partition_manager,
            worker_manager=self._worker_manager,
            consumer_group=self._consumer_group,
            logger=self.logger,
        )

    async def start(self) -> None:
        """Start the stage master."""
        if self._running:
            return

        self.logger.info(f"Starting stage {self.stage_id}")
        self._start_time = time.time()

        # Create output queue
        self._output_queue = await self._create_queue()

        # Initialize managers now that we have the output endpoint
        self._init_managers()

        # Assert managers are initialized (for type checker)
        assert self._worker_manager is not None
        assert self._recovery_manager is not None

        # Set target worker count for correct partition assignment
        self._worker_manager.set_target_worker_count(self.stage.min_parallelism)

        # Get partition count for worker assignment
        if self.upstream_endpoint and self.upstream_topic:
            partition_count = await self._partition_manager.get_upstream_partition_count()
        else:
            partition_count = self._partition_manager.partition_count

        # Spawn minimum required workers
        for _ in range(self.stage.min_parallelism):
            worker_id = await self._worker_manager.spawn_worker(
                partition_count=partition_count,
                is_min_worker=True,
            )
            if worker_id is None:
                raise RuntimeError(
                    f"Stage {self.stage_id}: Failed to spawn minimum required workers"
                )

        # Initialize state producer and emit stage started event
        await self._init_state_producer()
        await self._emit_stage_started()

        # Mark as running only after all initialization succeeds
        self._running = True

        self.logger.info(
            f"Stage {self.stage_id} started with {self._worker_manager.worker_count} workers"
        )

    async def run(self) -> bool:
        """Run the stage until completion.

        Uses event-driven approach:
        1. Start all workers
        2. Wait for worker completion/failure via ray.wait()
        3. Handle failures with recovery
        4. Send EOF when all workers done
        """
        if not self._running:
            await self.start()

        # Assert managers are initialized (for type checker)
        assert self._worker_manager is not None
        assert self._recovery_manager is not None

        try:
            while self._running and not self._finished:
                # Check if all workers done
                if self._worker_manager.worker_count == 0:
                    self._finished = True
                    break

                # Event-driven wait for any worker to complete
                completed, failed = await self._worker_manager.wait_for_completion(timeout=1.0)

                # Clean up completed/failed workers from tracking
                self._worker_manager.cleanup_workers(completed + failed)

                # Handle failures with recovery
                if failed:
                    self._recovery_manager.record_failures(
                        len(failed), self._worker_manager.worker_count
                    )

                    partition_count = await self._partition_manager.get_upstream_partition_count()
                    result = await self._recovery_manager.recover_failed_workers(
                        failed_worker_ids=failed,
                        partition_count=partition_count,
                    )

                    if result.should_give_up:
                        self._failed = True
                        self._failure_message = result.give_up_reason
                        self.logger.error(
                            f"Stage {self.stage_id} giving up: {result.give_up_reason}"
                        )
                        break

                elif completed:
                    self._recovery_manager.record_success()

                    # Check if there are pending orphaned partitions that need recovery
                    # This can happen when workers were cancelled due to resource constraints
                    if self._recovery_manager.has_pending_orphaned_partitions:
                        self.logger.info(
                            f"Attempting to recover {len(self._recovery_manager.pending_orphaned_partitions)} "
                            f"pending orphaned partitions after worker completion"
                        )
                        partition_count = (
                            await self._partition_manager.get_upstream_partition_count()
                        )
                        result = await self._recovery_manager.recover_failed_workers(
                            failed_worker_ids=[],  # No failed workers, just pending partitions
                            partition_count=partition_count,
                        )

                if self._failed:
                    break

                # Emit periodic metrics
                await self._emit_stage_metrics()

            # Send EOF markers to downstream
            await self._send_eof_markers()

            # Emit completion event
            await self._emit_stage_completed()

            if self._failed:
                raise RuntimeError(self._failure_message)

            return True

        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the stage master."""
        self._running = False

        # Stop all workers
        if self._worker_manager:
            await self._worker_manager.stop_all_workers()

        # Stop backpressure monitor
        if self._backpressure_monitor:
            self._backpressure_monitor.stop()

        # Stop partition manager (closes upstream queue)
        if self._partition_manager:
            self._partition_manager.stop()

        # Stop state producer (async - has background tasks)
        if self._state_producer:
            try:
                await self._state_producer.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping state producer: {e}")
            self._state_producer = None

        self.logger.info(f"Stage {self.stage_id} stopped")

    async def _send_eof_markers(self) -> None:
        """Send EOF markers to all output partitions."""
        if not self._output_queue:
            return

        partition_count = self._partition_manager.partition_count

        for partition in range(partition_count):
            try:
                eof_message = QueueMessage.create_eof(partition)
                self._output_queue.produce(
                    self._output_topic,
                    eof_message.to_bytes(),
                    partition=partition,
                )
                self.logger.debug(f"Sent EOF marker to partition {partition}")
            except Exception as e:
                self.logger.warning(f"Failed to send EOF to partition {partition}: {e}")

        self.logger.info(f"Stage {self.stage_id} sent EOF markers to {partition_count} partitions")

    # =========================================================================
    # State/Metrics Methods
    # =========================================================================

    async def _init_state_producer(self) -> None:
        """Initialize state producer for metrics push."""
        if not self.state_endpoint or not self.state_topic:
            return

        try:
            from solstice.webui.state.producer import StateProducer

            state_queue = await self._create_queue_from_endpoint(self.state_endpoint)
            self._state_producer = StateProducer(
                job_id=self.job_id,
                queue_client=state_queue,
                state_topic=self.state_topic,
            )
            await self._state_producer.start()
            self.logger.debug("Stage state producer initialized")
        except Exception as e:
            self.logger.warning(f"Failed to init state producer: {e}")
            self._state_producer = None

    async def _create_queue_from_endpoint(self, endpoint: QueueEndpoint) -> QueueClient:
        """Create a queue client from an endpoint."""
        queue: QueueClient
        if endpoint.queue_type == QueueType.TANSU:
            broker_url = f"{endpoint.host}:{endpoint.port}"
            queue = TansuQueueClient(broker_url)
        else:
            queue = MemoryClient(endpoint.storage_url)
        queue.start()
        return queue

    async def _emit_stage_started(self) -> None:
        """Emit STAGE_STARTED event."""
        if not self._state_producer:
            return

        try:
            from solstice.webui.state.messages import stage_started_message

            operator_class = self.stage.operator_config.operator_class
            operator_name = operator_class.__name__ if operator_class else "Unknown"
            msg = stage_started_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                operator_type=operator_name,
                min_parallelism=self.stage.min_parallelism,
                max_parallelism=self.stage.max_parallelism,
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit stage started: {e}")

    async def _emit_stage_completed(self) -> None:
        """Emit STAGE_COMPLETED event."""
        if not self._state_producer:
            return

        try:
            from solstice.webui.state.messages import stage_completed_message

            msg = stage_completed_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit stage completed: {e}")

    async def _emit_stage_metrics(self) -> None:
        """Emit stage metrics (no-op, metrics come from workers)."""
        pass

    # =========================================================================
    # Public Interface (for RayJobRunner and WebUI)
    # =========================================================================

    async def notify_upstream_finished(self) -> None:
        """Notify this stage that all upstream stages have finished."""
        self._upstream_finished = True
        self.logger.info(f"Stage {self.stage_id} notified: upstream finished")

        if self._worker_manager:
            await self._worker_manager.notify_upstream_finished()

    def get_output_queue(self) -> Optional[QueueClient]:
        """Get the output queue for downstream stages."""
        return self._output_queue

    def get_output_topic(self) -> str:
        """Get the output topic name."""
        return self._output_topic

    def get_status(self) -> StageStatus:
        """Get current stage status with queue metrics."""
        output_size = 0
        if self._output_queue:
            try:
                output_size = self._output_queue.get_latest_offset(self._output_topic)
            except Exception:
                pass

        return StageStatus(
            stage_id=self.stage_id,
            worker_count=self._worker_manager.worker_count if self._worker_manager else 0,
            output_queue_size=output_size,
            is_running=self._running,
            is_finished=self._finished,
            failed=self._failed,
            failure_message=self._failure_message,
            backpressure_active=self._backpressure_monitor.is_backpressure_active
            if self._backpressure_monitor
            else False,
        )

    def get_input_queue_lag(self) -> int:
        """Get input queue lag (for autoscaler)."""
        if self._backpressure_monitor:
            return self._backpressure_monitor.get_input_lag()
        return 0

    def set_downstream_stage_refs(self, downstream_refs: Dict[str, "StageMaster"]) -> None:
        """Set downstream stage references for backpressure propagation."""
        self._downstream_stage_refs = downstream_refs
        if self._backpressure_monitor:
            self._backpressure_monitor.set_downstream_refs(downstream_refs)

    async def scale_down(self, count: int) -> int:
        """Gracefully remove workers."""
        if self._backpressure_monitor:
            return await self._backpressure_monitor.scale_down(count)
        return 0

    async def scale_up(self, count: int) -> int:
        """Scale up by spawning new workers."""
        if self._backpressure_monitor:
            return await self._backpressure_monitor.scale_up(count)
        return 0

    async def cleanup_queue(self) -> None:
        """Clean up output queue (called by runner after all consumers done)."""
        if self._output_queue:
            self._output_queue.stop()
            self._output_queue = None
        if self._output_broker:
            self._output_broker.stop()
            self._output_broker = None

    # =========================================================================
    # Backward Compatibility (delegate to managers)
    # =========================================================================

    def get_partition_assignment(self, worker_id: str) -> list:
        """Get partition assignment for a worker (backward compatibility)."""
        return self._partition_manager.get_assignment(worker_id)

    @property
    def _workers(self) -> Dict[str, Any]:
        """Access workers dict (backward compatibility for tests)."""
        if self._worker_manager:
            return self._worker_manager.workers
        return {}

    @property
    def _partition_count(self) -> int:
        """Access partition count (backward compatibility)."""
        return self._partition_manager.partition_count
