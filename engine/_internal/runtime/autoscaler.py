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

"""Resource-aware autoscaler for dynamic worker scaling.

Threshold-based autoscaler with AIMD-inspired scaling: scale UP fast
(resource-proportional step), scale DOWN slow (one worker at a time).

Key features over naive threshold scaling:
- Resource-aware step: queries ray.available_resources() to determine
  how many workers CAN be added (node returns 8 GPUs → spawn 8 workers).
- Eager fill: one-shot scale-up after startup to fill available capacity.
- AIMD cooldowns: aggressive scale-up (15s), conservative scale-down (60s).
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Optional

import ray

from _internal.runtime.queue_stats import QueueStatsClient, StageQueueConfig
from _internal.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from _internal.core.stage import Stage
    from _internal.core.stage_master import StageMaster


@dataclass
class StageAutoscaleConfig:
    """Configuration for the autoscaler."""

    enabled: bool = True
    check_interval_s: float = 10.0

    # Scaling thresholds
    scale_up_lag_threshold: int = 500
    scale_down_lag_threshold: int = 100

    # AIMD cooldowns: scale UP fast, scale DOWN slow.
    cooldown_up_s: float = 15.0
    cooldown_down_s: float = 60.0
    max_scale_step: int = 32


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
    """Resource-aware autoscaler for batch workloads."""

    def __init__(
        self,
        config: Optional[StageAutoscaleConfig] = None,
        queue_stats_client: Optional[QueueStatsClient] = None,
        stage_queue_configs: Optional[Dict[str, StageQueueConfig]] = None,
    ):
        self.config = config or StageAutoscaleConfig()
        self.logger = create_ray_logger("Autoscaler")
        self._queue_stats_client = queue_stats_client
        self._stage_queue_configs = stage_queue_configs or {}

        self._last_scale_up_time: Dict[str, float] = {}
        self._last_scale_down_time: Dict[str, float] = {}
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

    async def eager_fill(self, masters: Dict[str, "StageMaster"]) -> None:
        """One-shot scale-up after startup to fill available cluster capacity.

        Called by RayJobRunner right after all stages start, before the main
        loop. Skips source stages (they have their own rate control via
        backpressure). Tracks allocated resources across stages to avoid
        over-commitment.
        """
        # Track resources allocated so far to avoid over-committing
        allocated_cpu = 0.0
        allocated_gpu = 0.0

        for stage_id, master in masters.items():
            if master._source is not None:
                continue
            current = len(master._workers)
            headroom = master.stage.max_parallelism - current
            if headroom <= 0:
                continue
            spawnable = self._get_spawnable_count(
                master.stage,
                headroom,
                reserved_cpu=allocated_cpu,
                reserved_gpu=allocated_gpu,
            )
            if spawnable > 0:
                try:
                    added = await master.scale_up(spawnable)
                    # Track what we just allocated
                    allocated_cpu += added * (master.stage.num_cpus or 0)
                    allocated_gpu += added * (master.stage.num_gpus or 0)
                    self.logger.info(f"Eager fill {stage_id}: {current} -> {current + added}")
                except Exception as e:
                    self.logger.error(f"Eager fill failed for {stage_id}: {e}")

    # -----------------------------------------------------------------
    # Resource queries
    # -----------------------------------------------------------------

    def _get_spawnable_count(
        self,
        stage: "Stage",
        max_needed: int,
        reserved_cpu: float = 0.0,
        reserved_gpu: float = 0.0,
    ) -> int:
        """How many additional workers can the cluster support right now?

        Queries ray.available_resources() and divides by per-worker cost.
        Subtracts ``reserved_*`` (resources already allocated in the same
        eager_fill round but not yet reflected in Ray's resource accounting).
        """
        try:
            available = ray.available_resources()
        except Exception:
            return max_needed  # Optimistic fallback

        avail_gpu = max(0.0, available.get("GPU", 0) - reserved_gpu)
        avail_cpu = max(0.0, available.get("CPU", 0) - reserved_cpu)
        per_gpu = stage.num_gpus or 0
        per_cpu = stage.num_cpus or 0

        if per_gpu > 0:
            count = int(avail_gpu / per_gpu)
            if per_cpu > 0:
                count = min(count, int(avail_cpu / per_cpu))
        elif per_cpu > 0:
            count = int(avail_cpu / per_cpu)
        else:
            return max_needed

        return min(count, max_needed)

    # -----------------------------------------------------------------
    # Metrics
    # -----------------------------------------------------------------

    async def _collect_metrics(self, masters: Dict[str, "StageMaster"]) -> Dict[str, StageMetrics]:
        """Collect metrics from all stages."""
        if not self._queue_stats_client:
            raise RuntimeError("Queue stats client is required for autoscaling")

        metrics = {}
        for stage_id, master in masters.items():
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
                is_source=master._source is not None,
            )
        return metrics

    # -----------------------------------------------------------------
    # Decisions
    # -----------------------------------------------------------------

    def _compute_decisions(self, metrics: Dict[str, StageMetrics]) -> Dict[str, int]:
        """Compute scaling decisions.

        Skip source and finished stages. Scale up on high lag (resource-aware
        step), scale down on low lag (one worker at a time).
        """
        decisions = {}

        for stage_id, m in metrics.items():
            if m.is_source or m.is_finished or not m.is_running:
                continue

            current = m.worker_count

            if m.input_queue_lag > self.config.scale_up_lag_threshold:
                step = min(self.config.max_scale_step, m.max_workers - current)
                if step > 0:
                    decisions[stage_id] = current + step
                continue

            if m.input_queue_lag < self.config.scale_down_lag_threshold:
                if current > m.min_workers and m.input_queue_claimed == 0:
                    decisions[stage_id] = max(current - 1, m.min_workers)

        return decisions

    # -----------------------------------------------------------------
    # Execution
    # -----------------------------------------------------------------

    async def _execute_decisions(
        self,
        masters: Dict[str, "StageMaster"],
        decisions: Dict[str, int],
    ) -> None:
        """Execute scaling decisions with AIMD cooldowns and resource gating."""
        now = time.time()

        for stage_id, target in decisions.items():
            master = masters.get(stage_id)
            if not master:
                continue

            current = len(master._workers)
            is_scale_up = target > current

            # Directional cooldown
            if is_scale_up:
                if now - self._last_scale_up_time.get(stage_id, 0) < self.config.cooldown_up_s:
                    continue
            else:
                if now - self._last_scale_down_time.get(stage_id, 0) < self.config.cooldown_down_s:
                    continue

            try:
                if is_scale_up:
                    spawnable = self._get_spawnable_count(master.stage, target - current)
                    if spawnable <= 0:
                        self.logger.info(
                            f"Skipping scale-up for {stage_id}: no resources available "
                            f"(need cpu={master.stage.num_cpus}, gpu={master.stage.num_gpus})"
                        )
                        continue
                    added = await master.scale_up(spawnable)
                    self._last_scale_up_time[stage_id] = now
                    self.logger.info(f"Scaled UP {stage_id}: {current} -> {current + added}")
                else:
                    removed = await master.scale_down(current - target)
                    self._last_scale_down_time[stage_id] = now
                    self.logger.info(f"Scaled DOWN {stage_id}: {current} -> {current - removed}")
            except Exception as e:
                self.logger.error(f"Failed to scale {stage_id}: {e}")
