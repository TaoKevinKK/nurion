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

"""Source manager: handles SplitPlanner and DirectProducer lifecycle for StageMaster.

Encapsulates all source-related logic: planner queue creation, split production
with backpressure, queue completion polling, and DirectProducer execution.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Optional

from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from solstice.core.models import QueueMessage, Split
from solstice.core.source import DirectProduceContext, DirectProducer, SplitPlanner
from solstice.testing.fault_injection import InjectedFaultError

if TYPE_CHECKING:
    from solstice.core.managers import WorkerManager
    from solstice.core.models import QueueEndpoint
    from solstice.queue import WorkQueueQueueClient

_RETRYABLE_EXCEPTIONS = (OSError, TimeoutError, InjectedFaultError)


class SourceManager:
    """Manages source strategy lifecycle for a stage.

    Two modes:
    - SplitPlanner: creates a planner queue, produces splits, workers consume
    - DirectProducer: external system writes directly to output queue, no workers
    """

    def __init__(
        self,
        source: SplitPlanner | DirectProducer,
        job_id: str,
        stage_id: str,
    ):
        self._source = source
        self._job_id = job_id
        self._stage_id = stage_id
        self._logger = logging.getLogger(f"SourceManager-{stage_id}")

        # SplitPlanner state
        self._planner_queue_name: Optional[str] = (
            f"{job_id}_{stage_id}_planner" if isinstance(source, SplitPlanner) else None
        )
        self._split_count = 0
        self._production_done = False

    @property
    def is_direct_producer(self) -> bool:
        return isinstance(self._source, DirectProducer)

    @property
    def planner_queue_name(self) -> Optional[str]:
        return self._planner_queue_name

    @property
    def production_done(self) -> bool:
        return self._production_done

    # =========================================================================
    # DirectProducer
    # =========================================================================

    async def run_direct_producer(
        self,
        queue_client: "WorkQueueQueueClient",
        output_queue_name: str,
        broker_endpoint: "QueueEndpoint",
    ) -> int:
        """Run the DirectProducer. Returns number of items produced."""
        assert isinstance(self._source, DirectProducer)
        ctx = DirectProduceContext(
            queue_client=queue_client,
            output_queue_name=output_queue_name,
            broker_endpoint=broker_endpoint,
            stage_id=self._stage_id,
            job_id=self._job_id,
        )
        count = await self._source.produce(ctx)
        self._logger.info(f"DirectProducer completed: {count} items written")
        return count

    def cleanup(self) -> None:
        """Clean up source resources (Spark session, etc.)."""
        self._source.cleanup()

    # =========================================================================
    # SplitPlanner
    # =========================================================================

    async def produce_splits(
        self,
        queue_client: "WorkQueueQueueClient",
        backpressure_fn: Optional[object] = None,
        running_fn: Optional[object] = None,
    ) -> None:
        """Generate splits and push to planner queue.

        Args:
            queue_client: Queue client for pushing splits
            backpressure_fn: Async callable returning True if should pause
            running_fn: Callable returning False if should stop
        """
        assert isinstance(self._source, SplitPlanner)
        assert self._planner_queue_name is not None

        self._logger.info(f"Generating splits for source {self._stage_id}")

        split_iterator = self._source.plan_splits(self._stage_id)
        backpressure_check_interval = 10
        consecutive_pauses = 0
        idx = 0

        for split in split_iterator:
            if running_fn and not running_fn():
                break

            # Backpressure check
            if backpressure_fn and idx % backpressure_check_interval == 0:
                should_pause = await backpressure_fn()
                if should_pause:
                    consecutive_pauses += 1
                    if consecutive_pauses >= 100:
                        self._logger.warning(
                            f"Source {self._stage_id} paused for {consecutive_pauses} "
                            f"consecutive checks due to backpressure"
                        )
                    await asyncio.sleep(0.1)
                    continue
                else:
                    consecutive_pauses = 0

            await self._produce_split_with_retry(queue_client, split, idx)
            idx += 1

            if idx % 100 == 0:
                self._logger.info(f"Produced {idx} splits")

        self._split_count = idx
        self._production_done = True
        self._logger.info(f"Source {self._stage_id} produced {idx} splits to queue")

    async def notify_splits_done(
        self,
        queue_client: "WorkQueueQueueClient",
        worker_manager: Optional["WorkerManager"],
        running_fn: Optional[object] = None,
    ) -> None:
        """Mark planner queue as finished and start polling for completion."""
        assert self._planner_queue_name is not None

        if queue_client:
            try:
                queue_client.mark_queue_finished(self._planner_queue_name)
                self._logger.info(f"Marked planner queue {self._planner_queue_name} as finished")
            except Exception as e:
                self._logger.warning(f"Failed to mark planner queue as finished: {e}")

            asyncio.create_task(
                self._poll_planner_queue_completion(queue_client, worker_manager, running_fn),
                name=f"poll_source_completion_{self._stage_id}",
            )

    async def _poll_planner_queue_completion(
        self,
        queue_client: "WorkQueueQueueClient",
        worker_manager: Optional["WorkerManager"],
        running_fn: Optional[object] = None,
    ) -> None:
        """Poll planner queue until safe for workers to exit."""
        assert self._planner_queue_name is not None

        poll_interval = 0.1
        max_consecutive_errors = 10
        consecutive_errors = 0

        while running_fn is None or running_fn():
            try:
                result = queue_client.is_queue_finished(self._planner_queue_name)
                consecutive_errors = 0
                if result.get("safe_to_exit", False):
                    self._logger.debug(
                        f"Source {self._stage_id} planner queue drained, notifying workers"
                    )
                    if worker_manager:
                        await worker_manager.notify_safe_to_exit()
                    return
            except Exception as e:
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    raise RuntimeError(f"Failed to poll planner queue completion: {e}") from e
                self._logger.debug(f"Error polling planner queue completion: {e}")

            await asyncio.sleep(poll_interval)

    async def _produce_split_with_retry(
        self,
        queue_client: "WorkQueueQueueClient",
        split: Split,
        idx: int,
    ) -> None:
        """Produce a split with retry logic."""

        def before_sleep_callback(retry_state: RetryCallState) -> None:
            exc = retry_state.outcome.exception() if retry_state.outcome else None
            self._logger.warning(
                f"Retry {retry_state.attempt_number}/3 producing split {split.split_id}: {exc}"
            )

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=0.1, min=0.1, max=1.0),
            retry=retry_if_exception_type(_RETRYABLE_EXCEPTIONS),
            before_sleep=before_sleep_callback,
            reraise=True,
        )
        async def _do_produce() -> None:
            assert self._planner_queue_name is not None
            message = QueueMessage(
                message_id=f"{self._stage_id}_{idx}",
                split_id=split.split_id,
                payload_key="",
                metadata={
                    "source_stage": self._stage_id,
                    "data_range": split.data_range,
                },
            )
            queue_client.push(self._planner_queue_name, message.to_bytes())

        await _do_produce()
