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

from __future__ import annotations

from typing import Dict, Iterable, List

from _internal.core.models import QueueStats
from _internal.runtime.queue_stats import QueueStatsClient, StageQueueConfig


class JobBackpressureController:
    """Job-level backpressure controller using WorkQueue stats."""

    def __init__(
        self,
        queue_stats: QueueStatsClient,
        stage_configs: Dict[str, StageQueueConfig],
        dag_edges: Dict[str, List[str]],
    ) -> None:
        self._queue_stats = queue_stats
        self._stage_configs = stage_configs
        self._dag_edges = dag_edges

    def is_backpressure_active(self, stage_id: str) -> bool:
        cfg = self._stage_configs.get(stage_id)
        if not cfg:
            return False

        input_stats = self._queue_stats.get_stats(cfg.input_queue_name)
        output_stats = self._queue_stats.get_stats(cfg.output_queue_name)

        return (
            input_stats.pending_count > cfg.backpressure_threshold_lag
            or output_stats.pending_count > cfg.backpressure_threshold_queue_size
        )

    def should_pause(self, stage_id: str) -> bool:
        """Check downstream queues to decide if an upstream should pause."""
        for downstream_id in self._downstream_stages(stage_id):
            if self.is_backpressure_active(downstream_id):
                return True

            cfg = self._stage_configs.get(downstream_id)
            if not cfg:
                continue

            output_stats = self._queue_stats.get_stats(cfg.output_queue_name)
            if output_stats.pending_count > cfg.backpressure_threshold_queue_size * 0.8:
                return True

        return False

    def get_input_queue_stats(self, stage_id: str) -> QueueStats:
        cfg = self._stage_configs.get(stage_id)
        if not cfg:
            return QueueStats()
        return self._queue_stats.get_stats(cfg.input_queue_name)

    def get_output_queue_stats(self, stage_id: str) -> QueueStats:
        cfg = self._stage_configs.get(stage_id)
        if not cfg:
            return QueueStats()
        return self._queue_stats.get_stats(cfg.output_queue_name)

    def _downstream_stages(self, stage_id: str) -> Iterable[str]:
        return self._dag_edges.get(stage_id, [])
