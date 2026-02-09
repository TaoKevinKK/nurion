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

"""WorkQueue storage reader (pyO3 direct access)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from workqueue_py import WorkQueueStorageReader as _WorkQueueStorageReader

from _internal.core.models import QueueStats


class WorkQueueStorageReader:
    """Direct WorkQueue storage reader (no RPC).

    Uses pyO3 bindings to access the underlying SlateDB storage.
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        reader: Optional[_WorkQueueStorageReader] = None,
    ) -> None:
        if reader is None:
            if db_path is None:
                raise ValueError("db_path is required when reader is not provided")
            self._reader = _WorkQueueStorageReader(db_path)
        else:
            self._reader = reader

    def close(self) -> None:
        close_fn = getattr(self._reader, "close", None)
        if callable(close_fn):
            close_fn()

    def get_queue_stats(self, queue: str) -> QueueStats:
        stats: Dict[str, int] = self._reader.get_queue_stats(queue)
        return QueueStats(
            pending_count=stats.get("pending_count", 0),
            claimed_count=stats.get("claimed_count", 0),
            total_pushed=stats.get("total_pushed", 0),
            total_acked=stats.get("total_acked", 0),
        )

    def list_queues(self) -> List[str]:
        return list(self._reader.list_queues())

    def scan_acked(
        self,
        queue: Optional[str] = None,
        start_ns: Optional[int] = None,
        end_ns: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        return list(
            self._reader.scan_acked(
                queue=queue,
                start_ns=start_ns,
                end_ns=end_ns,
                limit=limit,
            )
        )

    def scan_claimed(
        self,
        queue: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        return list(self._reader.scan_claimed(queue=queue, limit=limit))

    def state_get_batch(self, namespace: str, keys: List[str]) -> Dict[str, bytes]:
        return dict(self._reader.state_get_batch(namespace, keys))

    def state_scan_prefix(
        self,
        namespace: str,
        prefix: str = "",
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        return list(self._reader.state_scan_prefix(namespace, prefix, limit))
