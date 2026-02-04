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
    │  │  WorkerMgr    │  │ RecoveryMgr   │  │BackpressureMon│   │
    │  │ - lifecycle   │  │ - failures    │  │ - lag         │   │
    │  │ - spawn/stop  │  │ - recovery    │  │ - scaling     │   │
    │  └───────────────┘  └───────────────┘  └───────────────┘   │
    │                                                             │
    │  ┌─────────────────────────────────────────────────────┐    │
    │  │        Output Queue (WorkQueue)                      │    │
    │  └─────────────────────────────────────────────────────┘    │
    │                           ▲                                 │
    │  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
    │  │  Worker 1  │  │  Worker 2  │  │  Worker N  │            │
    │  └────────────┘  └────────────┘  └────────────┘            │
    └─────────────────────────────────────────────────────────────┘

Responsibilities:
1. Create and manage output queue (WorkQueue)
2. Coordinate managers (worker, recovery, backpressure)
3. Run the main processing loop
4. Track stage completion and emit state events

WorkQueue Model:
- No partitions - single queue per stage
- Workers compete for messages via claim()
- Simpler worker management - just spawn N workers
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

from solstice.queue import WorkQueueQueueClient
from solstice.utils.logging import create_ray_logger
from solstice.core.split_payload_store import SplitPayloadStore
from solstice.core.models import (
    FailurePolicy,
    FailureTracker,
    QueueEndpoint,
    QueueMessage,
    StageStatus,
)
from solstice.core.stage_worker import StageWorker
from solstice.core.managers import (
    WorkerManager,
    RecoveryManager,
    BackpressureMonitor,
)

if TYPE_CHECKING:
    from solstice.core.stage import Stage, StageRuntime
    from solstice.webui.state.producer import StateProducer

# Re-export for compatibility
__all__ = [
    "StageMaster",
    "StageWorker",
    "QueueEndpoint",
    "QueueMessage",
    "StageStatus",
    "FailurePolicy",
    "FailureTracker",
]


class StageMaster:
    """Orchestrates workers for a pipeline stage.

    Uses WorkQueue for inter-stage communication:
    - Single queue per stage (no partitions)
    - Workers compete for messages via claim()
    - Simpler than Kafka partition-based model

    Managers:
    - WorkerManager: Worker lifecycle (spawn, stop, status)
    - RecoveryManager: Failure tracking and worker recovery
    - BackpressureMonitor: Backpressure detection and scaling
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

        # Queue configuration (from runtime)
        self.broker_endpoint = runtime.broker_endpoint
        self.upstream_queue_name = runtime.upstream_queue_name
        self.state_queue_name = runtime.state_queue_name

        # SplitPayloadStore - shared across all stages
        self.payload_store = payload_store

        # Queue client and output queue
        self._queue_client: Optional[WorkQueueQueueClient] = None
        self._output_queue_name = f"{job_id}_{self.stage_id}_output"

        # State
        self._running = False
        self._finished = False
        self._failed = False
        self._failure_message: Optional[str] = None
        self._start_time: Optional[float] = None
        self._upstream_finished = False

        # Downstream stage refs for backpressure (compatibility)
        self._downstream_stage_refs: Dict[str, "StageMaster"] = {}

        # State producer for WebUI metrics
        self._state_producer: Optional["StateProducer"] = None
        self._last_metrics_emit_time = 0.0

        # Worker and recovery managers created after output queue is ready
        self._worker_manager: Optional[WorkerManager] = None
        self._recovery_manager: Optional[RecoveryManager] = None
        self._backpressure_monitor: Optional[BackpressureMonitor] = None

    async def _create_queue_client(self) -> WorkQueueQueueClient:
        """Create queue client and output queue."""
        assert self.broker_endpoint is not None, "broker_endpoint is required"

        broker_url = f"{self.broker_endpoint.host}:{self.broker_endpoint.port}"
        from solstice.queue.workqueue import _compute_heartbeat_interval

        queue = WorkQueueQueueClient(
            broker_url,
            worker_id=f"master-{self.stage_id}",
            heartbeat_interval_secs=_compute_heartbeat_interval(self.runtime.claim_timeout_secs),
        )
        queue.start()
        self.logger.info(f"Connected to broker at {broker_url}")

        # Create the output queue
        queue.create_queue(self._output_queue_name)
        self.logger.info(f"Created output queue: {self._output_queue_name}")
        return queue

    def _init_managers(self) -> None:
        """Initialize managers after output queue is created."""
        self._worker_manager = WorkerManager(
            job_id=self.job_id,
            stage=self.stage,
            runtime=self.runtime,
            payload_store=self.payload_store,
            broker_endpoint=self.broker_endpoint,
            output_queue_name=self._output_queue_name,
            state_queue_name=self.state_queue_name,
        )

        self._recovery_manager = RecoveryManager(
            stage_id=self.stage_id,
            worker_manager=self._worker_manager,
            policy=FailurePolicy(),
        )

        self._backpressure_monitor = BackpressureMonitor(
            stage=self.stage,
            runtime=self.runtime,
            worker_manager=self._worker_manager,
            logger=self.logger,
        )

    def _has_unprocessed_messages(self) -> bool:
        """Check if upstream queue still has unprocessed messages.

        Returns True if there are pending or claimed (in-flight) messages,
        meaning we shouldn't finish the stage yet.
        """
        if not self.upstream_queue_name:
            # Source stages have no upstream queue
            return False

        if not self._queue_client:
            return False

        try:
            stats = self._queue_client.get_stats(self.upstream_queue_name)
            pending = stats.get("pending_count", 0)
            claimed = stats.get("claimed_count", 0)

            if pending > 0 or claimed > 0:
                self.logger.debug(
                    f"Stage {self.stage_id} upstream queue has unprocessed messages: "
                    f"pending={pending}, claimed={claimed}"
                )
                return True
            return False
        except Exception as e:
            self.logger.warning(f"Error checking upstream queue stats: {e}")
            # On error, assume there might be messages (safer)
            return True

    async def start(self) -> None:
        """Start the stage master."""
        if self._running:
            return

        self.logger.info(f"Starting stage {self.stage_id}")
        self._start_time = time.time()

        # Create output queue
        self._queue_client = await self._create_queue_client()

        # Initialize managers now that we have the output endpoint
        self._init_managers()

        # Assert managers are initialized (for type checker)
        assert self._worker_manager is not None
        assert self._recovery_manager is not None

        # Spawn minimum required workers
        for _ in range(self.stage.min_parallelism):
            worker_id = await self._worker_manager.spawn_worker(is_min_worker=True)
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
        4. Notify downstream when all workers done (via notify_upstream_finished)
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
                    # Before finishing, check if upstream queue still has messages
                    # This prevents premature exit when all workers crash
                    if self._has_unprocessed_messages():
                        self.logger.info(
                            f"Stage {self.stage_id}: no workers but queue has unprocessed messages, spawning worker"
                        )
                        # Spawn at least one worker to process remaining messages
                        worker_id = await self._worker_manager.spawn_worker(is_min_worker=False)
                        if worker_id is None:
                            self.logger.warning(
                                f"Stage {self.stage_id}: could not spawn worker for remaining messages"
                            )
                            # Wait a bit and try again
                            await asyncio.sleep(0.5)
                            continue
                    else:
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

                    result = await self._recovery_manager.recover_failed_workers(
                        failed_worker_ids=failed,
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

                if self._failed:
                    break

                # Emit periodic metrics
                await self._emit_stage_metrics()

            # Mark output queue as finished - downstream workers can now safely exit
            # when the queue is drained (pending=0, claimed=0)
            if self._queue_client:
                try:
                    self._queue_client.mark_queue_finished(self._output_queue_name)
                    self.logger.debug(
                        f"Marked output queue {self._output_queue_name} as finished"
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to mark output queue as finished: {e}")

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

        # Stop state producer (async - has background tasks)
        if self._state_producer:
            try:
                await self._state_producer.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping state producer: {e}")
            self._state_producer = None

        self.logger.info(f"Stage {self.stage_id} stopped")

    # =========================================================================
    # State/Metrics Methods
    # =========================================================================

    async def _init_state_producer(self) -> None:
        """Initialize state producer for metrics push."""
        if not self.broker_endpoint or not self.state_queue_name:
            return

        try:
            from solstice.webui.state.producer import StateProducer

            broker_url = f"{self.broker_endpoint.host}:{self.broker_endpoint.port}"
            from solstice.queue.workqueue import _compute_heartbeat_interval

            state_queue = WorkQueueQueueClient(
                broker_url,
                worker_id=f"state-{self.stage_id}",
                heartbeat_interval_secs=_compute_heartbeat_interval(
                    self.runtime.claim_timeout_secs
                ),
            )
            state_queue.start()

            self._state_producer = StateProducer(
                job_id=self.job_id,
                queue_client=state_queue,
                state_queue_name=self.state_queue_name,
            )
            await self._state_producer.start()
            self.logger.debug("Stage state producer initialized")
        except Exception as e:
            self.logger.warning(f"Failed to init state producer: {e}")
            self._state_producer = None

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
        """Notify this stage that all upstream stages have finished.

        Starts a background task to poll the upstream queue for completion.
        When the queue is drained, workers are notified they can safely exit.
        """
        self._upstream_finished = True
        self.logger.info(f"Stage {self.stage_id} notified: upstream finished")

        # Start background task to poll for queue completion
        if self.upstream_queue_name and self._queue_client:
            asyncio.create_task(
                self._poll_queue_completion(),
                name=f"poll_completion_{self.stage_id}",
            )

    async def _poll_queue_completion(self) -> None:
        """Poll upstream queue until it's safe for workers to exit.

        Checks is_queue_finished() RPC which returns safe_to_exit=True when:
        1. Queue is marked as finished (by upstream master)
        2. Queue is drained (pending==0 && claimed==0)

        When safe, notifies all workers via notify_safe_to_exit().
        """
        if not self._queue_client or not self.upstream_queue_name:
            return

        poll_interval = 0.1  # 100ms
        max_consecutive_errors = 10
        consecutive_errors = 0

        while self._running:
            try:
                result = self._queue_client.is_queue_finished(self.upstream_queue_name)
                consecutive_errors = 0  # Reset on success
                if result.get("safe_to_exit", False):
                    self.logger.debug(
                        f"Stage {self.stage_id} upstream queue drained, notifying workers"
                    )
                    if self._worker_manager:
                        await self._worker_manager.notify_safe_to_exit()
                    return
            except Exception as e:
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    self.logger.error(
                        f"Stage {self.stage_id} failed to poll queue completion "
                        f"after {max_consecutive_errors} consecutive errors: {e}"
                    )
                    raise RuntimeError(
                        f"Failed to poll upstream queue completion: {e}"
                    ) from e
                self.logger.debug(f"Error polling queue completion: {e}")

            await asyncio.sleep(poll_interval)

    def get_queue_client(self) -> Optional[WorkQueueQueueClient]:
        """Get the queue client for this stage."""
        return self._queue_client

    def get_output_queue_name(self) -> str:
        """Get the output queue name."""
        return self._output_queue_name

    def get_status(self) -> StageStatus:
        """Get current stage status with queue metrics."""
        output_size = 0
        if self._queue_client:
            try:
                stats = self._queue_client.get_stats(self._output_queue_name)
                output_size = stats.get("pending_count", 0)
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
        """Clean up queue client (called by runner after all consumers done)."""
        if self._queue_client:
            self._queue_client.stop()
            self._queue_client = None

    # =========================================================================
    # Compatibility helpers
    # =========================================================================

    @property
    def _workers(self) -> Dict[str, Any]:
        """Access workers dict (compatibility for tests)."""
        if self._worker_manager:
            return self._worker_manager.workers
        return {}
