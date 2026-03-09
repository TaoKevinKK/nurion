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

"""Simple autoscaler for dynamic worker scaling.

Simple threshold-based autoscaler for offline/batch workloads.
Runs as a background task within RayJobRunner.

Design principles:
1. Single coordinator - runs within RayJobRunner, not distributed
2. In-memory state - no persistence needed
3. Slow-paced decisions - 15-30 second intervals for batch workloads
4. Simple threshold rules - no complex algorithms
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Optional

from _internal.runtime.queue_stats import QueueStatsClient, StageQueueConfig
from _internal.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from _internal.core.stage_master import StageMaster


@dataclass
class AutoscaleConfig:
    """Configuration for the autoscaler."""

    enabled: bool = True
    check_interval_s: float = 15.0

    # Scaling thresholds
    scale_up_lag_threshold: int = 1000
    scale_down_lag_threshold: int = 100

    # Damping
    cooldown_s: float = 60.0
    max_scale_step: int = 2


@dataclass
class StageMetrics:
    """Metrics collected from a stage for scaling decisions."""

    stage_id: str
    worker_count: int
    min_workers: int
    max_workers: int
    input_queue_lag: int = 0
    input_queue_claimed: int = 0
    output_queue_size: int = 0
    is_running: bool = True
    is_finished: bool = False
    is_source: bool = False


class SimpleAutoscaler:
    """Simple threshold-based autoscaler for batch workloads."""

    def __init__(
        self,
        config: Optional[AutoscaleConfig] = None,
        queue_stats_client: Optional[QueueStatsClient] = None,
        stage_queue_configs: Optional[Dict[str, StageQueueConfig]] = None,
    ):
        self.config = config or AutoscaleConfig()
        self.logger = create_ray_logger("Autoscaler")
        self._queue_stats_client = queue_stats_client
        self._stage_queue_configs = stage_queue_configs or {}

        self._last_scale_time: Dict[str, float] = {}
        self._running = False

    async def run_loop(self, masters: Dict[str, "StageMaster"]) -> None:
        """Main autoscaling loop."""
        self._running = True
        self.logger.info(f"Autoscaler started (interval={self.config.check_interval_s}s)")

        try:
            while self._running:
                await asyncio.sleep(self.config.check_interval_s)

                if not self.config.enabled:
                    continue

                try:
                    metrics = await self._collect_metrics(masters)
                    decisions = self._compute_decisions(metrics)
                    await self._execute_decisions(masters, decisions)
                except Exception as e:
                    self.logger.error(f"Autoscaler error: {e}")

        except asyncio.CancelledError:
            self.logger.info("Autoscaler stopped")
            raise

    def stop(self) -> None:
        self._running = False

    async def _collect_metrics(self, masters: Dict[str, "StageMaster"]) -> Dict[str, StageMetrics]:
        """Collect metrics from all stages."""
        if not self._queue_stats_client:
            raise RuntimeError("Queue stats client is required for autoscaling")

        metrics = {}

        for stage_id, master in masters.items():
            is_source = master._source is not None

            cfg = self._stage_queue_configs.get(stage_id)
            if not cfg:
                raise RuntimeError(f"Missing queue config for stage {stage_id}")

            input_stats = self._queue_stats_client.get_ref_stats(cfg.input)
            output_stats = self._queue_stats_client.get_ref_stats(cfg.output)

            metrics[stage_id] = StageMetrics(
                stage_id=stage_id,
                worker_count=len(master._workers),
                min_workers=master.stage.min_parallelism,
                max_workers=master.stage.max_parallelism,
                input_queue_lag=input_stats.pending_count,
                input_queue_claimed=input_stats.claimed_count,
                output_queue_size=output_stats.pending_count,
                is_running=getattr(master, "_running", True),
                is_finished=getattr(master, "_finished", False),
                is_source=is_source,
            )

        return metrics

    def _compute_decisions(self, metrics: Dict[str, StageMetrics]) -> Dict[str, int]:
        """Compute scaling decisions.

        Rules:
        1. Skip source stages (they control their own rate)
        2. Skip finished stages
        3. Scale up if input_queue_lag > threshold
        4. Scale down if lag < threshold and no in-flight work
        """
        decisions = {}

        for stage_id, m in metrics.items():
            if m.is_source or m.is_finished or not m.is_running:
                continue

            current = m.worker_count

            # Scale up on high lag
            if m.input_queue_lag > self.config.scale_up_lag_threshold:
                target = min(current + self.config.max_scale_step, m.max_workers)
                if target > current:
                    decisions[stage_id] = target
                continue

            # Scale down on low lag
            if m.input_queue_lag < self.config.scale_down_lag_threshold:
                if current > m.min_workers and m.input_queue_claimed == 0:
                    target = max(current - 1, m.min_workers)
                    decisions[stage_id] = target

        return decisions

    async def _execute_decisions(
        self,
        masters: Dict[str, "StageMaster"],
        decisions: Dict[str, int],
    ) -> None:
        """Execute scaling decisions with cooldown protection."""
        now = time.time()

        for stage_id, target in decisions.items():
            last_scale = self._last_scale_time.get(stage_id, 0)
            if now - last_scale < self.config.cooldown_s:
                continue

            master = masters.get(stage_id)
            if not master:
                continue

            current = len(master._workers)

            try:
                if target > current:
                    await master.scale_up(target - current)
                    self._last_scale_time[stage_id] = now
                    self.logger.info(f"Scaled UP {stage_id}: {current} -> {target}")
                elif target < current:
                    await master.scale_down(current - target)
                    self._last_scale_time[stage_id] = now
                    self.logger.info(f"Scaled DOWN {stage_id}: {current} -> {target}")
            except Exception as e:
                self.logger.error(f"Failed to scale {stage_id}: {e}")
