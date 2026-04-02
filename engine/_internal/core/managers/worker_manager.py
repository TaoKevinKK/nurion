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

"""Worker Manager - handles worker lifecycle.

Responsibilities:
- Spawn workers with resource checking
- Check worker readiness
- Stop/cancel workers
- Wait for worker completion (event-driven)
- Track worker tasks and handles

QueueGroup Model:
- Workers are assigned partition IDs (round-robin) for claim_from_group()
- Broker picks the best partition from the worker's assigned set
- Slot tracking ensures stable partition assignment across worker recovery
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import ray

from _internal.config import get_config
from _internal.core.stage_worker import OutputRouting, StageWorker, WorkerRuntime
from _internal.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from _internal.core.stage import Stage, StageRuntime
    from _internal.core.split_payload_store import SplitPayloadStore
    from _internal.runtime.queue_stats import QueueRef


class WorkerManager:
    """Manages worker lifecycle for a stage.

    Handles spawning, stopping, and monitoring workers. Uses event-driven
    approach (ray.wait) instead of polling for efficient completion detection.

    Thread-safe: all state modifications happen in the main asyncio loop.
    """

    def __init__(
        self,
        job_id: str,
        stage: "Stage",
        runtime: "StageRuntime",
        payload_store: "SplitPayloadStore",
        output: OutputRouting,
        upstream: Optional["QueueRef"] = None,
    ):
        self._job_id = job_id
        self._stage = stage
        self._stage_id = stage.stage_id
        self._runtime = runtime
        self._payload_store = payload_store
        self._output = output
        self._upstream = upstream
        self._logger = create_ray_logger(f"WorkerMgr-{stage.stage_id}")

        # Worker state
        self._workers: Dict[str, ray.actor.ActorHandle] = {}
        self._worker_tasks: Dict[str, ray.ObjectRef] = {}
        self._worker_index = 0

        # Slot tracking for partition assignment: when a worker dies, its slot
        # is returned to _free_slots so the replacement gets the same partitions.
        self._worker_slots: Dict[str, int] = {}
        self._free_slots: List[int] = []

    @property
    def workers(self) -> Dict[str, ray.actor.ActorHandle]:
        """Get current workers (read-only view)."""
        return self._workers.copy()

    @property
    def worker_count(self) -> int:
        """Get current number of active workers."""
        return len(self._workers)

    @property
    def worker_ids(self) -> List[str]:
        """Get list of current worker IDs."""
        return list(self._workers.keys())

    async def spawn_worker(self, is_min_worker: bool = False) -> Optional[str]:
        """Spawn a new worker.

        Args:
            is_min_worker: If True, worker is required (raises on failure)

        Returns:
            worker_id if successful, None if skipped or cancelled

        Raises:
            RuntimeError: If is_min_worker=True and worker cannot start
        """
        worker_id = await self._create_worker()

        if not is_min_worker:
            # Optional worker - check if it started successfully
            is_ready = await self._check_worker_ready(
                worker_id, self._stage.worker_ready_timeout_seconds
            )
            if not is_ready:
                self._logger.warning(
                    f"Worker {worker_id} could not start due to resource constraints. "
                    f"Cancelling worker and continuing with {len(self._workers) - 1} workers."
                )
                await self.cancel_worker(worker_id)
                return None

        return worker_id

    def _assign_partition_ids(self, worker_index: int) -> Optional[tuple[int, ...]]:
        """Assign partition IDs to a worker (round-robin distribution).

        Returns:
            Tuple of partition IDs, or None if upstream is not a group.
        """
        if self._runtime.upstream_num_partitions == 0:
            return None
        # For single-partition groups: explicitly assign partition 0 to all
        # workers so claim_from_group gets a concrete partition list (not None,
        # which some broker versions treat as "no partitions assigned").
        if self._runtime.upstream_num_partitions == 1:
            return (0,)

        n_partitions = self._runtime.upstream_num_partitions
        indices = [
            i
            for i in range(n_partitions)
            if i % self._stage.max_parallelism == worker_index % self._stage.max_parallelism
        ]
        return tuple(indices) if indices else None

    async def _create_worker(self) -> str:
        """Create a new worker actor and start its run loop.

        Returns:
            The worker_id of the spawned worker
        """
        # Reuse slot from a dead worker so partition assignment stays stable,
        # otherwise allocate a new slot.
        if self._free_slots:
            slot_index = self._free_slots.pop(0)
        else:
            slot_index = self._worker_index
            self._worker_index += 1
        worker_id = f"{self._stage_id}_w{slot_index}_{uuid.uuid4().hex[:6]}"

        # Build resource requirements
        resources = {}
        if self._stage.num_cpus > 0:
            resources["num_cpus"] = self._stage.num_cpus
        if self._stage.num_gpus > 0:
            resources["num_gpus"] = self._stage.num_gpus
        if self._stage.memory_mb > 0:
            resources["memory"] = self._stage.memory_mb * 1024 * 1024

        # Assign partition IDs for shuffle support
        assigned_partition_ids = self._assign_partition_ids(slot_index)
        self._worker_slots[worker_id] = slot_index

        # Build immutable WorkerRuntime
        runtime = WorkerRuntime(
            worker_id=worker_id,
            job_id=self._job_id,
            stage_id=self._stage_id,
            broker_endpoint=self._runtime.broker_endpoint,
            upstream=self._upstream,
            output=self._output,
            batch_size=self._stage.batch_size,
            claim_timeout_secs=self._runtime.claim_timeout_secs,
            assigned_partition_ids=assigned_partition_ids,
        )

        # Create worker actor
        worker = StageWorker.options(  # type: ignore[attr-defined]
            name=f"{self._stage_id}:{worker_id}",
            **resources,
        ).remote(
            runtime=runtime,
            stage=self._stage,
            payload_store=self._payload_store,
        )

        self._workers[worker_id] = worker

        # Start worker run loop
        task = worker.run.remote()
        self._worker_tasks[worker_id] = task

        if assigned_partition_ids:
            self._logger.info(
                f"Spawned worker {worker_id} with partitions {assigned_partition_ids}"
            )
        else:
            self._logger.info(f"Spawned worker {worker_id}")
        return worker_id

    async def _check_worker_ready(self, worker_id: str, timeout: float) -> bool:
        """Check if a worker is ready (actor has started and is responsive).

        Args:
            worker_id: The ID of the worker to check
            timeout: Maximum time to wait in seconds

        Returns:
            True if worker is ready, False if timeout or error
        """
        worker = self._workers.get(worker_id)
        if worker is None:
            return False

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                ready_refs, _ = ray.wait(
                    [worker.get_status.remote()],
                    timeout=min(1.0, timeout - (time.time() - start_time)),
                )
                if ready_refs:
                    return True
            except ray.exceptions.GetTimeoutError:
                pass
            except Exception as e:
                self._logger.debug(f"Worker {worker_id} not ready yet: {e}")

            await asyncio.sleep(self._stage.worker_spawn_retry_delay_seconds)

        return False

    async def cancel_worker(self, worker_id: str) -> None:
        """Cancel a pending worker that couldn't start due to resource constraints."""
        worker = self._workers.pop(worker_id, None)
        task = self._worker_tasks.pop(worker_id, None)
        slot = self._worker_slots.pop(worker_id, None)
        if slot is not None:
            self._free_slots.append(slot)

        if worker is not None:
            try:
                ray.kill(worker)
                self._logger.info(f"Cancelled worker {worker_id} due to resource constraints")
            except Exception as e:
                self._logger.debug(f"Error killing worker {worker_id}: {e}")

        if task is not None:
            try:
                ray.cancel(task, force=True)
            except Exception:
                pass

    async def stop_worker(self, worker_id: str, timeout: float = 10.0) -> bool:
        """Gracefully stop a worker.

        Args:
            worker_id: ID of worker to stop
            timeout: Timeout for graceful stop

        Returns:
            True if stopped successfully
        """
        worker = self._workers.get(worker_id)
        if worker is None:
            return False

        try:
            ray.get(worker.stop.remote(), timeout=timeout)
            self._workers.pop(worker_id, None)
            self._worker_tasks.pop(worker_id, None)
            slot = self._worker_slots.pop(worker_id, None)
            if slot is not None:
                self._free_slots.append(slot)
            self._logger.debug(f"Stopped worker {worker_id}")
            return True
        except Exception as e:
            self._logger.warning(f"Error stopping worker {worker_id}: {e}")
            return False

    async def stop_all_workers(self) -> None:
        """Stop all workers gracefully."""
        for worker_id, worker in list(self._workers.items()):
            try:
                ray.get(worker.stop.remote(), timeout=get_config().worker_stop_timeout_s)
            except Exception as e:
                self._logger.warning(f"Error stopping worker {worker_id}: {e}")

        self._workers.clear()
        self._worker_tasks.clear()

    async def wait_for_completion(self, timeout: float = 1.0) -> Tuple[List[str], List[str]]:
        """Wait for any worker to complete (event-driven, non-polling).

        Uses ray.wait() to efficiently wait for ANY task to complete.
        This is more efficient than polling each worker individually.

        Args:
            timeout: Maximum time to wait for a completion (seconds)

        Returns:
            (completed_worker_ids, failed_worker_ids)
        """
        if not self._worker_tasks:
            return [], []

        task_list = list(self._worker_tasks.values())
        task_to_worker = {task: wid for wid, task in self._worker_tasks.items()}

        # Use ray.wait in a thread to avoid blocking the async event loop
        ready, _ = await asyncio.to_thread(ray.wait, task_list, num_returns=1, timeout=timeout)

        if ready:
            self._logger.info(f"wait_for_completion: {len(ready)} of {len(task_list)} tasks ready")

        if not ready:
            return [], []

        # Process completed tasks
        completed, failed = [], []
        for task in ready:
            worker_id = task_to_worker[task]
            try:
                result = ray.get(task, timeout=0)
                self._logger.info(f"Worker {worker_id} completed: {result}")
                completed.append(worker_id)
            except ray.exceptions.GetTimeoutError:
                self._logger.warning(f"Unexpected: task for {worker_id} not ready")
            except Exception as e:
                if "broker_unavailable" in str(e):
                    self._logger.error(f"Worker {worker_id} failed: {e}")
                    raise
                self._logger.error(f"Worker {worker_id} failed: {e}")
                failed.append(worker_id)

        return completed, failed

    def cleanup_workers(self, worker_ids: List[str]) -> None:
        """Remove workers from tracking (after completion or failure).

        Does not actually stop workers - just removes from internal tracking.
        Returns slots to free pool so replacement workers get the same partitions.
        """
        for worker_id in worker_ids:
            self._workers.pop(worker_id, None)
            self._worker_tasks.pop(worker_id, None)
            slot = self._worker_slots.pop(worker_id, None)
            if slot is not None:
                self._free_slots.append(slot)

    def get_worker(self, worker_id: str) -> Optional[ray.actor.ActorHandle]:
        """Get a worker actor handle by ID."""
        return self._workers.get(worker_id)

    def get_worker_status(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific worker (blocking call)."""
        worker = self._workers.get(worker_id)
        if worker is None:
            return None
        try:
            return ray.get(worker.get_status.remote(), timeout=1.0)
        except Exception:
            return None
