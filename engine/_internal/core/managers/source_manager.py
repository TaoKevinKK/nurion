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

Encapsulates all source-related logic: planner queue creation, async split
production with backpressure, queue completion polling, and DirectProducer execution.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Callable, Awaitable, Optional

from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from _internal.core.models import SourceQueueMessage, Split
from _internal.core.source import DirectProduceContext, DirectProducer, SplitPlanner
from _internal.queue.errors import QueueFullError
from _internal.testing.fault_injection import InjectedFaultError

if TYPE_CHECKING:
    from _internal.core.managers import WorkerManager
    from _internal.core.models import QueueEndpoint
    from _internal.queue import AnvilQueueClient

_RETRYABLE_EXCEPTIONS = (OSError, TimeoutError, InjectedFaultError)


class SourceManager:
    """Manages source strategy lifecycle for a stage.

    Two modes:
    - SplitPlanner: creates planner queue, produces splits async, workers consume
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
        self._production_task: Optional[asyncio.Task] = None

    @property
    def is_direct_producer(self) -> bool:
        return isinstance(self._source, DirectProducer)

    @property
    def planner_queue_name(self) -> Optional[str]:
        return self._planner_queue_name

    # =========================================================================
    # DirectProducer
    # =========================================================================

    async def run_direct_producer(
        self,
        queue_client: "AnvilQueueClient",
        output_group_name: str,
        broker_endpoint: "QueueEndpoint",
        partition: int = 0,
    ) -> int:
        """Run the DirectProducer. Returns number of items produced."""
        assert isinstance(self._source, DirectProducer)
        ctx = DirectProduceContext(
            queue_client=queue_client,
            output_queue_name=f"{output_group_name}_p{partition}",
            broker_endpoint=broker_endpoint,
            stage_id=self._stage_id,
            job_id=self._job_id,
        )
        count = await self._source.produce(ctx)
        self._logger.info(f"DirectProducer completed: {count} items written")
        return count

    def cleanup(self) -> None:
        """Clean up source resources (Spark session, etc.)."""
        if isinstance(self._source, DirectProducer):
            # DirectProducer.cleanup is async but we call from sync stop();
            # the event loop handles it via _source_manager.stop() in StageMaster.
            pass

    # =========================================================================
    # SplitPlanner: start / stop
    # =========================================================================

    def start_split_production(
        self,
        queue_client: "AnvilQueueClient",
        worker_manager: "WorkerManager",
        backpressure_fn: Callable[[], Awaitable[bool]],
        running_fn: Callable[[], bool],
    ) -> None:
        """Create planner queue and launch async split production.

        Workers can start consuming immediately while splits are being produced.
        When production finishes, the planner queue is marked as finished and
        workers are notified to exit once the queue is drained.
        """
        assert self._planner_queue_name is not None

        queue_client.create_queue(self._planner_queue_name)
        self._logger.info(f"Created planner queue {self._planner_queue_name}")

        self._production_task = asyncio.create_task(
            self._run_production(queue_client, worker_manager, backpressure_fn, running_fn),
            name=f"split_production_{self._stage_id}",
        )

    def raise_if_production_failed(self) -> None:
        """Re-raise the production task's exception if it has already failed.

        Call this from the StageMaster run-loop so that source errors (e.g.,
        schema-mismatch raised inside plan_splits) surface immediately instead
        of hanging until stop() is awaited.
        """
        if (
            self._production_task is not None
            and self._production_task.done()
            and not self._production_task.cancelled()
        ):
            exc = self._production_task.exception()
            if exc is not None:
                raise exc

    async def stop(self) -> None:
        """Cancel split production and clean up."""
        if self._production_task:
            self._production_task.cancel()
            try:
                await self._production_task
            except (asyncio.CancelledError, Exception):
                pass
            self._production_task = None

        if isinstance(self._source, DirectProducer):
            self._source.cleanup()

    # =========================================================================
    # Internal: production loop
    # =========================================================================

    async def _run_production(
        self,
        queue_client: "AnvilQueueClient",
        worker_manager: "WorkerManager",
        backpressure_fn: Callable[[], Awaitable[bool]],
        running_fn: Callable[[], bool],
    ) -> None:
        """Background task: produce splits, then mark queue as finished."""
        assert isinstance(self._source, SplitPlanner)
        assert self._planner_queue_name is not None

        try:
            await self._produce_splits(queue_client, backpressure_fn, running_fn)

            # Mark planner queue as finished so workers know no more data
            if running_fn():
                self._mark_queue_finished(queue_client)
                asyncio.create_task(
                    self._poll_queue_drained(queue_client, worker_manager, running_fn),
                    name=f"poll_source_completion_{self._stage_id}",
                )
        except asyncio.CancelledError:
            self._logger.debug("Split production cancelled")
            raise
        except Exception as e:
            self._logger.error(f"Split production failed: {e}")
            raise

    async def _produce_splits(
        self,
        queue_client: "AnvilQueueClient",
        backpressure_fn: Callable[[], Awaitable[bool]],
        running_fn: Callable[[], bool],
    ) -> None:
        """Generate splits and push to planner queue with backpressure."""
        assert isinstance(self._source, SplitPlanner)
        self._logger.info(f"Generating splits for source {self._stage_id}")

        split_iterator = self._source.plan_splits(self._stage_id)
        backpressure_check_interval = 10
        consecutive_pauses = 0
        idx = 0

        for split in split_iterator:
            if not running_fn():
                break

            # Wait out backpressure *without* advancing the iterator.
            # The previous pattern used `continue` which advanced the for-loop
            # to the next split, silently dropping the current one.
            if idx % backpressure_check_interval == 0:
                while True:
                    should_pause = await backpressure_fn()
                    if not should_pause:
                        consecutive_pauses = 0
                        break
                    consecutive_pauses += 1
                    if consecutive_pauses >= 100:
                        self._logger.warning(
                            f"Source {self._stage_id} paused for "
                            f"{consecutive_pauses} consecutive backpressure checks"
                        )
                    await asyncio.sleep(0.1)
                    if not running_fn():
                        break

            if not running_fn():
                break

            await self._produce_split_with_retry(queue_client, split, idx)
            idx += 1

            if idx % 100 == 0:
                self._logger.info(f"Produced {idx} splits")

        self._logger.info(f"Source {self._stage_id} produced {idx} splits to queue")

    def _mark_queue_finished(self, queue_client: "AnvilQueueClient") -> None:
        assert self._planner_queue_name is not None
        try:
            queue_client.mark_queue_finished(self._planner_queue_name)
            self._logger.info(f"Marked planner queue {self._planner_queue_name} as finished")
        except Exception as e:
            self._logger.warning(f"Failed to mark planner queue as finished: {e}")

    async def _poll_queue_drained(
        self,
        queue_client: "AnvilQueueClient",
        worker_manager: "WorkerManager",
        running_fn: Callable[[], bool],
    ) -> None:
        """Poll planner queue until drained, then notify workers to exit."""
        assert self._planner_queue_name is not None

        poll_interval = 0.1
        max_consecutive_errors = 10
        consecutive_errors = 0

        while running_fn():
            try:
                result = queue_client.is_queue_finished(self._planner_queue_name)
                consecutive_errors = 0
                if result.get("safe_to_exit", False):
                    self._logger.debug(
                        f"Source {self._stage_id} planner queue drained, notifying workers"
                    )
                    await worker_manager.notify_safe_to_exit()
                    return
            except Exception as e:
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    raise RuntimeError(f"Failed to poll planner queue completion: {e}") from e

            await asyncio.sleep(poll_interval)

    async def _produce_split_with_retry(
        self,
        queue_client: "AnvilQueueClient",
        split: Split,
        idx: int,
    ) -> None:
        """Produce a split with retry logic.

        Handles two categories of transient errors:
        - Network / broker errors: retried via tenacity (exponential backoff).
        - QueueFull (bounded queue): retried with a fixed 1s sleep to let
          downstream workers drain, up to 60 attempts (~ 60s).
        """

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
            message = SourceQueueMessage(
                message_id=f"{self._stage_id}_{idx}",
                split_id=split.split_id,
                data_range=split.data_range,
                metadata={"source_stage": self._stage_id},
            )
            queue_client.push(self._planner_queue_name, message.to_bytes())

        max_queue_full_retries = 60
        for attempt in range(max_queue_full_retries):
            try:
                await _do_produce()
                return
            except QueueFullError:
                if attempt % 10 == 0:
                    self._logger.info(
                        f"Source {self._stage_id}: bounded queue full, "
                        f"waiting for downstream to drain "
                        f"(attempt {attempt + 1}/{max_queue_full_retries})"
                    )
                await asyncio.sleep(1.0)
                continue
        raise RuntimeError(
            f"Source {self._stage_id}: bounded queue full for "
            f"{max_queue_full_retries}s, giving up on split {split.split_id}"
        )
