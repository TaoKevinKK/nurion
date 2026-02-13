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

StageMaster delegates concerns to component managers:
- WorkerManager: worker lifecycle (spawn, stop, status)
- RecoveryManager: failure tracking and worker recovery
- SourceManager: SplitPlanner / DirectProducer lifecycle
- SinkManager: SinkCommitter background commit lifecycle

WorkQueue Model:
- No partitions - single queue per stage
- Workers compete for messages via claim()
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, Optional, Protocol

from _internal.core.managers import RecoveryManager, SinkManager, SourceManager, WorkerManager
from _internal.core.models import (
    FailurePolicy,
    FailureTracker,
    QueueEndpoint,
    QueueMessage,
    StageStatus,
)
from _internal.core.split_payload_store import SplitPayloadStore
from _internal.core.stage_worker import StageWorker
from _internal.queue import WorkQueueQueueClient
from _internal.utils.logging import create_ray_logger
from _internal.webui.state.schema import encode_json, job_namespace, stage_key

if TYPE_CHECKING:
    from _internal.core.stage import Stage, StageRuntime


class BackpressureProvider(Protocol):
    def is_backpressure_active(self, stage_id: str) -> bool: ...

    def should_pause(self, stage_id: str) -> bool: ...


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

    Lifecycle:
        start() -> [source produces] -> [sink commit queue + bg loop] -> spawn workers
        run()   -> worker loop -> workers done -> [sink finalize] -> mark complete
        stop()  -> cancel commit loop, stop workers
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

        # Worker and recovery managers (created in _init_managers)
        self._worker_manager: Optional[WorkerManager] = None
        self._recovery_manager: Optional[RecoveryManager] = None
        self._backpressure_provider: Optional[BackpressureProvider] = None

        # Source manager (SplitPlanner or DirectProducer)
        source = stage.operator_config.create_source()
        self._source_manager: Optional[SourceManager] = (
            SourceManager(source, job_id, self.stage_id) if source else None
        )
        # Expose _source for external checks (e.g., autoscaler)
        self._source = source

        # Sink manager (SinkCommitter)
        sink_committer = stage.operator_config.create_sink_committer()
        self._sink_manager: Optional[SinkManager] = (
            SinkManager(sink_committer, job_id, self.stage_id) if sink_committer else None
        )

    # =========================================================================
    # Queue Setup
    # =========================================================================

    async def _create_queue_client(self) -> None:
        """Create queue client and output queue."""
        assert self.broker_endpoint is not None, "broker_endpoint is required"

        broker_url = f"{self.broker_endpoint.host}:{self.broker_endpoint.port}"
        from _internal.queue.workqueue import _compute_heartbeat_interval

        self._queue_client = WorkQueueQueueClient(
            broker_url,
            worker_id=f"master-{self.stage_id}",
            heartbeat_interval_secs=_compute_heartbeat_interval(self.runtime.claim_timeout_secs),
        )
        self._queue_client.start()
        self.logger.info(f"Connected to broker at {broker_url}")

        self._queue_client.create_queue(self._output_queue_name)
        self.logger.info(f"Created output queue: {self._output_queue_name}")

    def _init_managers(self) -> None:
        """Initialize worker and recovery managers."""
        # For sink stages with a committer, workers output to the commit queue
        # (fragment metadata goes there via ack_and_forward).
        # For regular stages, workers output to the stage output queue.
        worker_output_queue = (
            self._sink_manager.commit_queue_name if self._sink_manager else self._output_queue_name
        )
        self._worker_manager = WorkerManager(
            job_id=self.job_id,
            stage=self.stage,
            runtime=self.runtime,
            payload_store=self.payload_store,
            broker_endpoint=self.broker_endpoint,
            output_queue_name=worker_output_queue,
        )

        self._recovery_manager = RecoveryManager(
            stage_id=self.stage_id,
            worker_manager=self._worker_manager,
            policy=FailurePolicy(),
        )

    def _has_unprocessed_messages(self) -> bool:
        """Check if upstream queue still has unprocessed messages."""
        if not self.upstream_queue_name or not self._queue_client:
            return False

        try:
            stats = self._queue_client.get_stats(self.upstream_queue_name)
            pending = stats.get("pending_count", 0)
            claimed = stats.get("claimed_count", 0)
            if pending > 0 or claimed > 0:
                self.logger.debug(
                    f"Stage {self.stage_id} upstream queue: pending={pending}, claimed={claimed}"
                )
                return True
            return False
        except Exception as e:
            self.logger.warning(f"Error checking upstream queue stats: {e}")
            return True

    # =========================================================================
    # Lifecycle: start / run / stop
    # =========================================================================

    async def start(self) -> None:
        """Start the stage master."""
        if self._running:
            return

        self.logger.info(f"Starting stage {self.stage_id}")
        self._start_time = time.time()

        await self._create_queue_client()
        queue_client = self._queue_client
        assert queue_client is not None
        broker_endpoint = self.broker_endpoint
        assert broker_endpoint is not None

        # --- DirectProducer: no workers ---
        if self._source_manager and self._source_manager.is_direct_producer:
            await self._source_manager.run_direct_producer(
                queue_client, self._output_queue_name, broker_endpoint
            )
            self._write_stage_state(status="RUNNING")
            self._running = True
            return

        self._running = True

        # --- Sink manager: create commit queue and start background loop ---
        if self._sink_manager:
            self._sink_manager.create_queue_and_start_loop(queue_client)

        # --- Init workers ---
        self._init_managers()
        assert self._worker_manager is not None

        # --- SplitPlanner: create planner queue, launch async production ---
        if self._source_manager and not self._source_manager.is_direct_producer:
            self.upstream_queue_name = self._source_manager.planner_queue_name
            self._worker_manager.set_upstream_queue_name(self._source_manager.planner_queue_name)
            self._source_manager.start_split_production(
                queue_client,
                self._worker_manager,
                backpressure_fn=self._check_backpressure,
                running_fn=lambda: self._running,
            )

        for _ in range(self.stage.min_parallelism):
            worker_id = await self._worker_manager.spawn_worker(is_min_worker=True)
            if worker_id is None:
                raise RuntimeError(
                    f"Stage {self.stage_id}: Failed to spawn minimum required workers"
                )

        self._write_stage_state(status="RUNNING")

        self.logger.info(
            f"Stage {self.stage_id} started with {self._worker_manager.worker_count} workers"
        )

    async def run(self) -> bool:
        """Run the stage until completion."""
        if not self._running:
            await self.start()
        queue_client = self._queue_client
        assert queue_client is not None

        # --- DirectProducer: immediate finish ---
        if self._source_manager and self._source_manager.is_direct_producer:
            self._finished = True
            try:
                queue_client.mark_queue_finished(self._output_queue_name)
            except Exception as e:
                self.logger.warning(f"Failed to mark output queue as finished: {e}")
            self._write_stage_state(status="COMPLETED")
            return True

        # --- Worker-based run loop ---
        assert self._worker_manager is not None
        assert self._recovery_manager is not None

        try:
            while self._running and not self._finished:
                if self._worker_manager.worker_count == 0:
                    if self._has_unprocessed_messages():
                        self.logger.info(
                            f"Stage {self.stage_id}: no workers but queue has "
                            f"unprocessed messages, spawning worker"
                        )
                        worker_id = await self._worker_manager.spawn_worker(is_min_worker=False)
                        if worker_id is None:
                            await asyncio.sleep(0.5)
                            continue
                    else:
                        self._finished = True
                        break

                completed, failed = await self._worker_manager.wait_for_completion(timeout=1.0)
                self._worker_manager.cleanup_workers(completed + failed)

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

            # --- Sink finalize ---
            if self._sink_manager and not self._failed:
                await self._sink_manager.finalize(queue_client)

            # Mark output queue as finished
            try:
                queue_client.mark_queue_finished(self._output_queue_name)
            except Exception as e:
                self.logger.warning(f"Failed to mark output queue as finished: {e}")

            self._write_stage_state(status="FAILED" if self._failed else "COMPLETED")

            if self._failed:
                raise RuntimeError(self._failure_message)

            return True

        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the stage master."""
        self._running = False

        if self._source_manager:
            await self._source_manager.stop()

        if self._sink_manager:
            await self._sink_manager.cancel()

        if self._worker_manager:
            await self._worker_manager.stop_all_workers()

        self.logger.info(f"Stage {self.stage_id} stopped")

    # =========================================================================
    # Helpers
    # =========================================================================

    async def _check_backpressure(self) -> bool:
        """Check if we should pause production due to downstream backpressure."""
        provider = self._backpressure_provider
        if not provider:
            return False
        try:
            return provider.should_pause(self.stage_id)
        except Exception:
            return False

    def _write_stage_state(self, status: str) -> None:
        """Write stage status into WorkQueue state."""
        if not self._queue_client:
            return
        operator_class = self.stage.operator_config.operator_class
        operator_name = operator_class.__name__ if operator_class else "Unknown"
        data = {
            "stage_id": self.stage_id,
            "status": status,
            "timestamp": time.time(),
            "operator_type": operator_name,
            "min_parallelism": self.stage.min_parallelism,
            "max_parallelism": self.stage.max_parallelism,
        }
        if status == "FAILED":
            data["failure_message"] = self._failure_message
        try:
            self._queue_client.state_put(
                job_namespace(self.job_id),
                puts={stage_key(self.stage_id): encode_json(data)},
            )
        except Exception as e:
            self.logger.debug(f"Failed to write stage state: {e}")

    # =========================================================================
    # Public Interface (for RayJobRunner, Autoscaler, WebUI)
    # =========================================================================

    async def notify_upstream_finished(self) -> None:
        """Notify this stage that all upstream stages have finished."""
        self._upstream_finished = True
        self.logger.info(f"Stage {self.stage_id} notified: upstream finished")

        if self.upstream_queue_name and self._queue_client:
            asyncio.create_task(
                self._poll_queue_completion(),
                name=f"poll_completion_{self.stage_id}",
            )

    async def _poll_queue_completion(self) -> None:
        """Poll upstream queue until it's safe for workers to exit."""
        if not self._queue_client or not self.upstream_queue_name:
            return

        poll_interval = 0.1
        max_consecutive_errors = 10
        consecutive_errors = 0

        while self._running:
            try:
                result = self._queue_client.is_queue_finished(self.upstream_queue_name)
                consecutive_errors = 0
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
                    raise RuntimeError(f"Failed to poll upstream queue completion: {e}") from e
                self.logger.debug(f"Error polling queue completion: {e}")

            await asyncio.sleep(poll_interval)

    def get_queue_client(self) -> Optional[WorkQueueQueueClient]:
        return self._queue_client

    def get_output_queue_name(self) -> str:
        return self._output_queue_name

    def get_backpressure_input_queue_name(self) -> Optional[str]:
        """Get the queue used as input lag signal for backpressure."""
        if self._source_manager and not self._source_manager.is_direct_producer:
            return self._source_manager.planner_queue_name
        return self.runtime.upstream_queue_name

    def get_backpressure_output_queue_name(self) -> str:
        """Get the queue used as output lag signal for backpressure."""
        if self._sink_manager:
            return self._sink_manager.commit_queue_name
        return self._output_queue_name

    def get_status(self) -> StageStatus:
        output_size = 0
        if self._queue_client:
            try:
                output_queue_name = self.get_backpressure_output_queue_name()
                stats = self._queue_client.get_stats(output_queue_name)
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
            backpressure_active=self._backpressure_provider.is_backpressure_active(self.stage_id)
            if self._backpressure_provider
            else False,
        )

    async def scale_down(self, count: int) -> int:
        if not self._worker_manager or count <= 0:
            return 0
        current = self._worker_manager.worker_count
        min_workers = self.stage.min_parallelism
        actual_remove = min(count, max(0, current - min_workers))
        if actual_remove == 0:
            return 0
        worker_ids = self._worker_manager.worker_ids[-actual_remove:]
        removed = 0
        for worker_id in worker_ids:
            if await self._worker_manager.stop_worker(worker_id):
                removed += 1
        self.logger.info(
            f"Scaled down {self.stage_id}: removed {removed}/{count} workers "
            f"(now {self._worker_manager.worker_count} workers)"
        )
        return removed

    async def scale_up(self, count: int) -> int:
        if not self._worker_manager or count <= 0:
            return 0
        current = self._worker_manager.worker_count
        max_workers = self.stage.max_parallelism
        actual_add = min(count, max(0, max_workers - current))
        if actual_add == 0:
            return 0
        added = 0
        for _ in range(actual_add):
            try:
                worker_id = await self._worker_manager.spawn_worker(is_min_worker=False)
                if worker_id:
                    added += 1
            except Exception as e:
                self.logger.warning(f"Failed to spawn worker: {e}")
                break
        self.logger.info(
            f"Scaled up {self.stage_id}: added {added}/{count} workers "
            f"(now {self._worker_manager.worker_count} workers)"
        )
        return added

    def set_backpressure_provider(self, provider: BackpressureProvider) -> None:
        self._backpressure_provider = provider

    async def cleanup_queue(self) -> None:
        if self._queue_client:
            self._queue_client.stop()
            self._queue_client = None

    @property
    def _workers(self) -> Dict[str, Any]:
        """Access workers dict (used by tests and helpers)."""
        if self._worker_manager:
            return self._worker_manager.workers
        return {}
