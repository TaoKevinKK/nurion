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

"""Backpressure Monitor - handles backpressure detection and scaling.

Responsibilities:
- Monitor input queue pending count
- Detect and signal backpressure conditions
- Scale up/down workers based on load

WorkQueue Model:
- Uses pending_count for lag detection (instead of partition offsets)
- No partition-level metrics or skew detection
- Simpler scaling: just add/remove workers (no rebalancing)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Mapping, Optional, Protocol

from solstice.queue import WorkQueueQueueClient
from solstice.core.managers.worker_manager import WorkerManager

if TYPE_CHECKING:
    from solstice.core.stage import Stage, StageRuntime
    from solstice.core.stage_master import StageStatus


class StageStatusProvider(Protocol):
    """Protocol for objects that can provide stage status."""

    def get_status(self) -> "StageStatus": ...


@dataclass
class BackpressureSignal:
    """Signal for backpressure propagation."""

    from_stage: str
    to_stage: str
    slow_down_factor: float  # 0.0 = pause, 1.0 = normal
    reason: str


class BackpressureMonitor:
    """Monitors backpressure and handles scaling decisions.

    Tracks:
    - Input queue pending count (messages waiting to be claimed)
    - Output queue pending count (messages produced)

    Provides:
    - Backpressure signals for upstream stages
    - Scaling recommendations based on load

    WorkQueue Model:
    - Uses pending_count for lag (instead of partition offsets)
    - No partition-level metrics or skew detection
    - Simpler scaling: add/remove workers without partition rebalancing

    Thread-safe: all state modifications happen in the main asyncio loop.
    """

    def __init__(
        self,
        stage: "Stage",
        runtime: "StageRuntime",
        worker_manager: WorkerManager,
        logger: logging.Logger,
    ):
        self._stage = stage
        self._runtime = runtime
        self._worker_manager = worker_manager
        self._logger = logger

        # State
        self._backpressure_active = False
        self._downstream_refs: Dict[str, StageStatusProvider] = {}

        # Cached upstream queue client for metrics
        self._metrics_client: Optional[WorkQueueQueueClient] = None

    @property
    def is_backpressure_active(self) -> bool:
        """Check if backpressure is currently active."""
        return self._backpressure_active

    def set_downstream_refs(self, refs: Mapping[str, StageStatusProvider]) -> None:
        """Set references to downstream stages for backpressure propagation."""
        self._downstream_refs = dict(refs)

    def _get_metrics_client(self) -> Optional[WorkQueueQueueClient]:
        """Get or create a client for metrics."""
        endpoint = self._runtime.broker_endpoint
        if not endpoint:
            return None

        if self._metrics_client is None:
            broker_url = f"{endpoint.host}:{endpoint.port}"
            from solstice.queue.workqueue import _compute_heartbeat_interval

            self._metrics_client = WorkQueueQueueClient(
                broker_url,
                worker_id="metrics",
                heartbeat_interval_secs=_compute_heartbeat_interval(
                    self._runtime.claim_timeout_secs
                ),
            )
            self._metrics_client.start()

        return self._metrics_client

    def get_input_lag(self) -> int:
        """Get input queue lag (messages pending processing).

        Returns:
            Number of pending messages in the upstream queue.
        """
        if not self._runtime.broker_endpoint or not self._runtime.upstream_queue_name:
            return 0

        client = self._get_metrics_client()
        if client is None:
            return 0

        try:
            stats = client.get_stats(self._runtime.upstream_queue_name)
            return stats.get("pending_count", 0)
        except Exception as e:
            self._logger.debug(f"Error getting input lag: {e}")
            return 0

    def check_backpressure(
        self, output_client: Optional[WorkQueueQueueClient], output_queue_name: str
    ) -> bool:
        """Check if backpressure should be activated.

        Args:
            output_client: Output queue client (if available)
            output_queue_name: Output queue name

        Returns:
            True if backpressure should be active
        """
        # Check input queue lag
        input_lag = self.get_input_lag()
        if input_lag > self._stage.backpressure_threshold_lag:
            if not self._backpressure_active:
                self._logger.warning(
                    f"Backpressure activated for {self._stage.stage_id}: "
                    f"input_lag={input_lag} > threshold={self._stage.backpressure_threshold_lag}"
                )
            self._backpressure_active = True
            return True

        # Check output queue size
        if output_client:
            try:
                stats = output_client.get_stats(output_queue_name)
                output_size = stats.get("pending_count", 0)
                if output_size > self._stage.backpressure_threshold_queue_size:
                    if not self._backpressure_active:
                        self._logger.warning(
                            f"Backpressure activated for {self._stage.stage_id}: "
                            f"output_queue_size={output_size} > "
                            f"threshold={self._stage.backpressure_threshold_queue_size}"
                        )
                    self._backpressure_active = True
                    return True
            except Exception:
                pass

        # Deactivate with hysteresis (only when well below threshold)
        if self._backpressure_active:
            if input_lag < self._stage.backpressure_threshold_lag * 0.7:
                self._logger.info(
                    f"Backpressure deactivated for {self._stage.stage_id}: lag={input_lag}"
                )
                self._backpressure_active = False

        return self._backpressure_active

    def get_backpressure_signal(self) -> Optional[BackpressureSignal]:
        """Get backpressure signal for propagation to upstream stages.

        Returns:
            BackpressureSignal if backpressure is active, None otherwise
        """
        if not self._backpressure_active:
            return None

        return BackpressureSignal(
            from_stage=self._stage.stage_id,
            to_stage="",  # Set by caller
            slow_down_factor=0.5,  # Default: slow down by 50%
            reason="queue_lag_exceeded",
        )

    async def check_downstream_backpressure(self) -> bool:
        """Check if any downstream stage has backpressure.

        Returns:
            True if production should be paused
        """
        if not self._downstream_refs:
            return False

        for stage_id, stage_ref in self._downstream_refs.items():
            try:
                status = stage_ref.get_status()
                if status.backpressure_active:
                    self._logger.debug(f"Backpressure detected from downstream stage {stage_id}")
                    return True

                if status.output_queue_size > self._stage.backpressure_threshold_queue_size * 0.8:
                    self._logger.debug(
                        f"Downstream queue size {status.output_queue_size} approaching threshold"
                    )
                    return True
            except Exception as e:
                self._logger.debug(f"Error checking backpressure from {stage_id}: {e}")

        return False

    async def scale_down(self, count: int) -> int:
        """Scale down workers by removing the specified count.

        Args:
            count: Number of workers to remove

        Returns:
            Number of workers actually removed
        """
        if count <= 0:
            return 0

        current = self._worker_manager.worker_count
        min_workers = self._stage.min_parallelism
        safe_to_remove = max(0, current - min_workers)
        actual_remove = min(count, safe_to_remove)

        if actual_remove == 0:
            self._logger.debug(f"Cannot scale down: current={current}, min={min_workers}")
            return 0

        # Select workers to remove (last N workers)
        worker_ids = self._worker_manager.worker_ids[-actual_remove:]

        removed = 0
        for worker_id in worker_ids:
            if await self._worker_manager.stop_worker(worker_id):
                removed += 1
                self._logger.debug(f"Removed worker {worker_id}")

        self._logger.info(
            f"Scaled down {self._stage.stage_id}: removed {removed}/{count} workers "
            f"(now {self._worker_manager.worker_count} workers)"
        )
        return removed

    async def scale_up(self, count: int) -> int:
        """Scale up workers by spawning the specified count.

        Args:
            count: Number of workers to add

        Returns:
            Number of workers actually added
        """
        if count <= 0:
            return 0

        current = self._worker_manager.worker_count
        max_workers = self._stage.max_parallelism
        safe_to_add = max(0, max_workers - current)
        actual_add = min(count, safe_to_add)

        if actual_add == 0:
            self._logger.debug(f"Cannot scale up: current={current}, max={max_workers}")
            return 0

        added = 0
        for _ in range(actual_add):
            try:
                worker_id = await self._worker_manager.spawn_worker(is_min_worker=False)
                if worker_id:
                    added += 1
                    self._logger.debug(f"Spawned worker {worker_id}")
            except Exception as e:
                self._logger.warning(f"Failed to spawn worker: {e}")
                break

        self._logger.info(
            f"Scaled up {self._stage.stage_id}: added {added}/{count} workers "
            f"(now {self._worker_manager.worker_count} workers)"
        )
        return added

    def stop(self) -> None:
        """Clean up resources."""
        if self._metrics_client:
            try:
                self._metrics_client.stop()
            except Exception as e:
                self._logger.warning(f"Error stopping metrics client: {e}")
            self._metrics_client = None
