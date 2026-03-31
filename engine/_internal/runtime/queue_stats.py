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

from dataclasses import dataclass
from typing import Optional

from _internal.core.models import QueueEndpoint, QueueStats
from _internal.queue import AnvilQueueClient


@dataclass(frozen=True)
class QueueRef:
    """Reference to either a single queue or a QueueGroup.

    Single queues: source planner queue, sink commit queue (internal).
    QueueGroups: all inter-stage data (1 partition for non-shuffle, N for shuffle).
    """

    name: str
    is_group: bool = False

    @staticmethod
    def queue(name: str) -> QueueRef:
        return QueueRef(name=name, is_group=False)

    @staticmethod
    def group(name: str) -> QueueRef:
        return QueueRef(name=name, is_group=True)


@dataclass(frozen=True)
class StageQueueConfig:
    """Backpressure configuration for a stage.

    input/output are QueueRef — either a single queue (planner/commit)
    or a QueueGroup (inter-stage data). QueueStatsClient dispatches
    to the correct API based on is_group.
    """

    stage_id: str
    input: Optional[QueueRef] = None
    output: Optional[QueueRef] = None
    backpressure_threshold_lag: int = 5000
    backpressure_threshold_queue_size: int = 1000


class QueueStatsClient:
    """Thin wrapper for Anvil stats queries.

    Supports both single-queue stats and QueueGroup aggregate stats,
    dispatched automatically via QueueRef.is_group.
    """

    def __init__(self, endpoint: QueueEndpoint, claim_timeout_secs: float) -> None:
        broker_url = f"{endpoint.host}:{endpoint.port}"
        from _internal.queue.anvil import _compute_heartbeat_interval

        self._client = AnvilQueueClient(
            broker_url,
            worker_id="metrics",
            heartbeat_interval_secs=_compute_heartbeat_interval(claim_timeout_secs),
        )
        self._client.start()

    def get_ref_stats(self, ref: Optional[QueueRef]) -> QueueStats:
        """Get stats for a QueueRef (auto-dispatches to queue or group API)."""
        if not ref:
            return QueueStats()
        if ref.is_group:
            return self._get_group_stats(ref.name)
        return self._get_queue_stats(ref.name)

    def _get_queue_stats(self, queue_name: str) -> QueueStats:
        try:
            stats = self._client.get_stats(queue_name)
            return QueueStats(
                pending_count=stats.get("pending_count", 0),
                claimed_count=stats.get("claimed_count", 0),
                total_pushed=stats.get("total_pushed", 0),
                total_acked=stats.get("total_acked", 0),
            )
        except Exception:
            return QueueStats()

    def _get_group_stats(self, group_name: str) -> QueueStats:
        try:
            stats = self._client.get_group_stats(group_name)
            return QueueStats(
                pending_count=stats.get("total_pending", 0),
                claimed_count=stats.get("total_claimed", 0),
            )
        except Exception:
            return QueueStats()

    def stop(self) -> None:
        try:
            self._client.stop()
        except Exception:
            pass
