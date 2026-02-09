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

"""Sink committer protocol for StageMaster composition.

SinkCommitter coordinates batched commits for sink stages. It is the
symmetric counterpart to SourceStrategy:

- SourceStrategy produces data INTO the pipeline (before workers)
- SinkCommitter finalizes data OUT OF the pipeline (after workers)

StageMaster lifecycle with both:
    start() -> [source produces] -> [sink commit queue + bg loop] -> spawn workers
    run()   -> worker loop -> all workers done -> [sink finalize] -> mark complete
    stop()  -> cancel commit loop
"""

from __future__ import annotations

from typing import TYPE_CHECKING, runtime_checkable

from typing_extensions import Protocol

if TYPE_CHECKING:
    from _internal.queue import WorkQueueQueueClient


@runtime_checkable
class SinkCommitter(Protocol):
    """Coordinates batched commits for sink stages.

    Workers push commit metadata (e.g., fragment metadata) to a dedicated
    commit queue. The SinkCommitter runs a background loop that claims
    from this queue and commits on a smart schedule (time/size thresholds).

    The commit queue name is decided by StageMaster and passed to the
    run_commit_loop / finalize methods.

    StageMaster manages the lifecycle:
    1. start(): Creates the commit queue and starts the background loop
    2. run(): After all workers finish, calls finalize() for the final commit
    3. stop(): Cancels the background loop
    """

    async def run_commit_loop(
        self, queue_client: WorkQueueQueueClient, commit_queue_name: str
    ) -> None:
        """Background task: claim from commit queue, accumulate, commit on schedule.

        Runs until cancelled by StageMaster. Should handle asyncio.CancelledError
        gracefully.
        """
        ...

    async def finalize(self, queue_client: WorkQueueQueueClient, commit_queue_name: str) -> None:
        """After all workers exit: drain the commit queue and do the final commit."""
        ...
