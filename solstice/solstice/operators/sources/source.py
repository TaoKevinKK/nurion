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

"""Source Master for source stages that generate splits.

SourceMaster is responsible for:
1. Generating splits via the abstract plan_splits() method
2. Writing split metadata to a planner queue (WorkQueue)
3. Spawning workers that consume from this queue and process data

Architecture:
    ┌─────────────────────────────────────────────────────────────────┐
    │                      SourceMaster                               │
    │                                                                 │
    │  ┌─────────────────────────────────────────────────────────┐   │
    │  │              Source Queue (WorkQueue)                    │   │
    │  │  - Split metadata written by plan_splits()              │   │
    │  │  - Workers compete via claim() for messages             │   │
    │  └─────────────────────────────────────────────────────────┘   │
    │                           ▲                                     │
    │                           │ push splits                         │
    │  plan_splits() ───────────┘                                    │
    │                                                                 │
    │                           │                                     │
    │                           ▼ workers claim                       │
    │  ┌────────────┐  ┌────────────┐  ┌────────────┐               │
    │  │  Worker 1  │  │  Worker 2  │  │  Worker N  │               │
    │  │ (process)  │  │ (process)  │  │ (process)  │               │
    │  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘               │
    │        │               │               │                        │
    │        └───────────────┼───────────────┘                        │
    │                        │ produce to output                      │
    │                        ▼                                        │
    │  ┌─────────────────────────────────────────────────────────┐   │
    │  │              Output Queue (for downstream)              │   │
    │  └─────────────────────────────────────────────────────────┘   │
    └─────────────────────────────────────────────────────────────────┘

Key design decisions:
- SourceMaster uses WorkQueue for planner queue
- Split metadata is pushed to planner queue, workers read actual data
- Workers claim from planner queue, produce to output queue
- No partition assignment - workers compete for messages
"""

from __future__ import annotations

import asyncio
import time
from abc import abstractmethod
from typing import TYPE_CHECKING, Iterator, Optional

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    RetryCallState,
)

from solstice.core.models import Split
from solstice.core.stage_master import (
    QueueMessage,
    StageMaster,
)
from solstice.queue import (
    WorkQueueQueueClient,
)
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.core.stage import Stage, StageRuntime
    from solstice.core.split_payload_store import SplitPayloadStore

# Import InjectedFaultError for testing - this is raised by FaultInjector
from solstice.testing.fault_injection import InjectedFaultError

# Exceptions that indicate transient failures and should be retried.
_RETRYABLE_EXCEPTIONS = (OSError, TimeoutError, InjectedFaultError)


class SourceMaster(StageMaster):
    """Master for source stages that generates splits and spawns workers.

    SourceMaster extends StageMaster with split generation capability:
    1. Generate splits via plan_splits()
    2. Write split metadata to a planner queue
    3. Spawn workers that claim from planner queue
    4. Workers produce output to output queue (for downstream stages)

    This design ensures:
    - Split planning is deterministic and persistent
    - Workers compete for messages via claim()
    - No partition assignment needed

    Subclasses must implement:
    - plan_splits() -> Iterator[Split]: Generate splits for this source
    """

    def __init__(
        self,
        job_id: str,
        stage: "Stage",
        payload_store: "SplitPayloadStore",
        runtime: "StageRuntime",
        **kwargs,
    ):
        super().__init__(
            job_id=job_id,
            stage=stage,
            payload_store=payload_store,
            runtime=runtime,
        )

        # Planner queue name (for split metadata, distinct from output queue)
        self._planner_queue_name = f"{job_id}_{self.stage_id}_planner"

        # Metrics
        self._splits_produced = 0
        self._splits_production_done = False

        # Override logger
        self.logger = create_ray_logger(f"SourceMaster-{self.stage_id}")

    async def _create_planner_queue(self) -> None:
        """Create queue client and planner queue.

        All stages use the same shared broker managed by RayJobRunner.
        """
        # Create shared queue client (also used for output queue)
        await self._create_queue_client()

        # Create planner queue
        self._queue_client.create_queue(self._planner_queue_name)
        self.logger.info(f"Created planner queue {self._planner_queue_name}")

    async def start(self) -> None:
        """Start the source master.

        1. Create planner queue for split metadata
        2. Generate splits and write to planner queue
        3. Create output queue (via parent StageMaster)
        4. Spawn workers that consume from planner queue
        """
        if self._running:
            return

        self.logger.info(f"Starting source {self.stage_id}")
        self._start_time = time.time()
        self._running = True

        # Create queue client and planner queue
        await self._create_planner_queue()

        # Generate splits and write to planner queue
        await self._produce_splits()

        # Set upstream queue name to our planner queue (workers will consume from here)
        self.upstream_queue_name = self._planner_queue_name

        # Initialize managers (must be called after output queue is created)
        self._init_managers()

        # Assert managers are initialized (for type checker)
        assert self._worker_manager is not None

        # Update worker manager with planner queue info (workers consume from planner queue)
        self._worker_manager.set_upstream_queue_name(self._planner_queue_name)

        # Spawn workers (min workers are required, so is_min_worker=True)
        for i in range(self.stage.min_parallelism):
            await self._worker_manager.spawn_worker(is_min_worker=True)

        # Notify workers that all splits have been produced
        # (workers will exit when queue is drained + this flag is set)
        if self._splits_production_done:
            await self._notify_workers_splits_done()

        self.logger.info(
            f"Source {self.stage_id} started: {self._splits_produced} splits, "
            f"{len(self._workers)} workers"
        )

    async def _produce_splits(self) -> None:
        """Generate splits and write to planner queue with backpressure awareness."""
        self.logger.info(f"Generating splits for source {self.stage_id}")

        split_iterator = self.plan_splits()
        backpressure_check_interval = 10  # Check backpressure every N splits
        consecutive_backpressure_pauses = 0
        max_consecutive_pauses = 100  # Max pauses before logging warning

        for split in split_iterator:
            if not self._running:
                break

            # Check backpressure periodically
            if self._splits_produced % backpressure_check_interval == 0:
                should_pause = await self._check_backpressure_before_produce()
                if should_pause:
                    consecutive_backpressure_pauses += 1
                    if consecutive_backpressure_pauses >= max_consecutive_pauses:
                        self.logger.warning(
                            f"Source {self.stage_id} paused for {consecutive_backpressure_pauses} "
                            f"consecutive checks due to backpressure"
                        )
                    # Wait a bit before checking again
                    await asyncio.sleep(0.1)
                    continue
                else:
                    consecutive_backpressure_pauses = 0

            try:
                await self._produce_split_with_retry(split)
                self._splits_produced += 1

                if self._splits_produced % 100 == 0:
                    self.logger.info(f"Produced {self._splits_produced} splits")

            except Exception as e:
                self.logger.error(f"Failed to produce split {split.split_id} after retries: {e}")
                self._failed = True
                self._failure_message = str(e)
                raise

        self.logger.info(f"Source {self.stage_id} produced {self._splits_produced} splits to queue")

        # Mark splits production complete - workers will be notified after they are spawned
        # (see start() method which calls _notify_workers_splits_done())
        self._splits_production_done = True

    async def _notify_workers_splits_done(self) -> None:
        """Notify workers that all splits have been produced.

        1. Marks planner queue as finished via RPC (authoritative signal)
        2. Notifies workers that upstream is finished
        3. Starts polling task to check queue completion and notify workers to exit
        """
        # Mark planner queue as finished - this is the authoritative signal
        # that no more splits will be produced
        if self._queue_client:
            try:
                self._queue_client.mark_queue_finished(self._planner_queue_name)
                self.logger.info(f"Marked planner queue {self._planner_queue_name} as finished")
            except Exception as e:
                self.logger.warning(f"Failed to mark planner queue as finished: {e}")

        # Start background task to poll for planner queue completion
        # Workers will be notified via notify_safe_to_exit when queue is drained
        if self._queue_client:
            asyncio.create_task(
                self._poll_planner_queue_completion(),
                name=f"poll_source_completion_{self.stage_id}",
            )

    async def _poll_planner_queue_completion(self) -> None:
        """Poll planner queue until it's safe for workers to exit.

        Checks is_queue_finished() RPC which returns safe_to_exit=True when:
        1. Queue is marked as finished (done above)
        2. Queue is drained (pending==0 && claimed==0)

        When safe, notifies all workers via notify_safe_to_exit().
        """
        if not self._queue_client:
            return

        poll_interval = 0.1  # 100ms
        max_consecutive_errors = 10
        consecutive_errors = 0

        while self._running:
            try:
                result = self._queue_client.is_queue_finished(self._planner_queue_name)
                consecutive_errors = 0  # Reset on success
                if result.get("safe_to_exit", False):
                    self.logger.debug(
                        f"Source {self.stage_id} planner queue drained, notifying workers"
                    )
                    if self._worker_manager:
                        await self._worker_manager.notify_safe_to_exit()
                    return
            except Exception as e:
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    self.logger.error(
                        f"Source {self.stage_id} failed to poll queue completion "
                        f"after {max_consecutive_errors} consecutive errors: {e}"
                    )
                    raise RuntimeError(f"Failed to poll planner queue completion: {e}") from e
                self.logger.debug(f"Error polling planner queue completion: {e}")

            await asyncio.sleep(poll_interval)

    async def _check_backpressure_before_produce(self) -> bool:
        """Check if we should pause production due to downstream backpressure.

        Returns:
            True if production should be paused, False otherwise
        """
        provider = self._backpressure_provider
        if not provider:
            return False

        try:
            if provider.should_pause(self.stage_id):
                self.logger.debug(
                    f"Backpressure detected for {self.stage_id}, pausing split production"
                )
                return True
        except Exception as e:
            self.logger.debug(f"Error checking backpressure for {self.stage_id}: {e}")

        return False

    async def _produce_split_with_retry(self, split: Split) -> None:
        """Produce a split with retry logic for transient failures."""

        def before_sleep_callback(retry_state: RetryCallState) -> None:
            exc = retry_state.outcome.exception() if retry_state.outcome else None
            self.logger.warning(
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
            await self._produce_split(split)

        await _do_produce()

    async def _produce_split(self, split: Split) -> None:
        """Produce a split to the planner queue.

        The split metadata is serialized and pushed to the queue.
        Workers will claim this and use the SourceOperator to read actual data.
        """
        # Create message with split metadata
        message = QueueMessage(
            message_id=f"{self.stage_id}_{self._splits_produced}",
            split_id=split.split_id,
            payload_key="",  # No payload for source splits - data will be read by operator
            metadata={
                "source_stage": self.stage_id,
                "data_range": split.data_range,
                "split_index": self._splits_produced,
            },
        )

        # Push to planner queue
        if not self._queue_client:
            raise RuntimeError("Queue client not initialized")
        self._queue_client.push(self._planner_queue_name, message.to_bytes())

        self.logger.debug(f"Produced split {split.split_id}")

    @abstractmethod
    def plan_splits(self) -> Iterator[Split]:
        """Plan and generate splits for this source.

        Subclasses must implement this to define how data is split.

        Returns:
            Iterator of Split objects, each containing metadata for one split
        """
        raise NotImplementedError("plan_splits must be implemented by subclasses")

    def get_source_client(self) -> Optional[WorkQueueQueueClient]:
        """Get the queue client (for debugging/testing)."""
        return self._queue_client

    def get_planner_queue_name(self) -> str:
        """Get the planner queue name."""
        return self._planner_queue_name
