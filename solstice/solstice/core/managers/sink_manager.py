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

"""Sink manager: handles SinkCommitter lifecycle for StageMaster.

Encapsulates commit queue creation, background commit loop, and finalize.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from solstice.core.sink import SinkCommitter
    from solstice.queue import WorkQueueQueueClient


class SinkManager:
    """Manages SinkCommitter lifecycle for a stage.

    Handles commit queue creation, background commit loop, and finalize.
    Messages are only acked after successful commit (handled by the committer).
    """

    def __init__(
        self,
        committer: "SinkCommitter",
        job_id: str,
        stage_id: str,
    ):
        self._committer = committer
        self._commit_queue_name = f"{job_id}_{stage_id}_commits"
        self._commit_task: Optional[asyncio.Task] = None
        self._logger = logging.getLogger(f"SinkManager-{stage_id}")

    @property
    def commit_queue_name(self) -> str:
        return self._commit_queue_name

    def create_queue_and_start_loop(self, queue_client: "WorkQueueQueueClient") -> None:
        """Create the commit queue and start the background commit loop."""
        queue_client.create_queue(self._commit_queue_name)
        self._logger.info(f"Created commit queue: {self._commit_queue_name}")
        self._commit_task = asyncio.create_task(
            self._run_loop_safe(queue_client),
            name=f"commit_loop_{self._commit_queue_name}",
        )

    async def finalize(self, queue_client: "WorkQueueQueueClient") -> None:
        """Cancel the background loop and do the final commit."""
        if self._commit_task:
            self._commit_task.cancel()
            try:
                await self._commit_task
            except asyncio.CancelledError:
                pass
            self._commit_task = None

        self._logger.info("Finalizing sink committer")
        await self._committer.finalize(queue_client, self._commit_queue_name)
        self._logger.info("Sink committer finalized")

    async def cancel(self) -> None:
        """Cancel the background loop without finalizing."""
        if self._commit_task:
            self._commit_task.cancel()
            try:
                await self._commit_task
            except asyncio.CancelledError:
                pass
            self._commit_task = None

    async def _run_loop_safe(self, queue_client: "WorkQueueQueueClient") -> None:
        """Wrapper with error handling for the commit loop."""
        try:
            await self._committer.run_commit_loop(queue_client, self._commit_queue_name)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._logger.error(f"Sink commit loop error: {e}")
