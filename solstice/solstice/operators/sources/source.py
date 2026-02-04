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
2. Writing split metadata to a source queue (WorkQueue)
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
- SourceMaster uses WorkQueue for source queue
- Split metadata is pushed to source queue, workers read actual data
- Workers claim from source queue, produce to output queue
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
    QueueEndpoint,
    QueueMessage,
    StageStatus,
    StageMaster,
)
from solstice.queue import (
    WorkQueueBrokerManager,
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
    2. Write split metadata to a source queue
    3. Spawn workers that claim from source queue
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

        # Source queue (for split metadata, distinct from output queue)
        self._source_broker: Optional[WorkQueueBrokerManager] = None
        self._source_client: Optional[WorkQueueQueueClient] = None
        self._source_queue_name = f"{job_id}_{self.stage_id}_source"
        self._source_endpoint: Optional[QueueEndpoint] = None

        # Metrics
        self._splits_produced = 0
        self._splits_production_done = False

        # Backpressure configuration (from stage)
        self._backpressure_threshold_queue_size = stage.backpressure_threshold_queue_size

        # Override logger
        self.logger = create_ray_logger(f"SourceMaster-{self.stage_id}")

    async def _create_source_queue(self) -> WorkQueueQueueClient:
        """Connect to shared broker and create source queue.

        All stages use the same shared broker managed by RayJobRunner.

        Returns:
            WorkQueueQueueClient for pushing/claiming messages.
        """
        endpoint = self.runtime.broker_endpoint
        if not endpoint:
            raise RuntimeError(f"Source {self.stage_id}: broker_endpoint is required")

        broker_url = f"{endpoint.host}:{endpoint.port}"
        from solstice.queue.workqueue import _compute_heartbeat_interval

        client = WorkQueueQueueClient(
            broker_url,
            worker_id=f"source-{self.stage_id}",
            heartbeat_interval_secs=_compute_heartbeat_interval(self.runtime.claim_timeout_secs),
        )
        client.start()
        self._source_client = client

        self._source_endpoint = QueueEndpoint(
            host=endpoint.host,
            port=endpoint.port,
            storage_url=endpoint.storage_url,
        )

        client.create_queue(self._source_queue_name)
        self.logger.info(f"Connected to broker at {broker_url} for source {self.stage_id}")
        return client

    async def start(self) -> None:
        """Start the source master.

        1. Create source queue for split metadata
        2. Generate splits and write to source queue
        3. Create output queue (via parent StageMaster)
        4. Spawn workers that consume from source queue
        """
        if self._running:
            return

        self.logger.info(f"Starting source {self.stage_id}")
        self._start_time = time.time()
        self._running = True

        # Create source queue (broker + client for split metadata)
        await self._create_source_queue()

        # Generate splits and write to source queue
        await self._produce_splits()

        self._queue_client = await self._create_queue_client()

        # Set upstream queue name to our source queue (workers will consume from here)
        self.upstream_queue_name = self._source_queue_name

        # Initialize managers (must be called after output queue is created)
        self._init_managers()

        # Assert managers are initialized (for type checker)
        assert self._worker_manager is not None

        # Update worker manager with source queue info (workers consume from source queue)
        self._worker_manager.set_upstream_queue_name(self._source_queue_name)

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
        """Generate splits and write to source queue with backpressure awareness."""
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

        1. Marks source queue as finished via RPC (authoritative signal)
        2. Notifies workers that upstream is finished
        3. Starts polling task to check queue completion and notify workers to exit
        """
        # Mark source queue as finished - this is the authoritative signal
        # that no more splits will be produced
        if self._source_client:
            try:
                self._source_client.mark_queue_finished(self._source_queue_name)
                self.logger.info(
                    f"Marked source queue {self._source_queue_name} as finished"
                )
            except Exception as e:
                self.logger.warning(f"Failed to mark source queue as finished: {e}")

        # Start background task to poll for source queue completion
        # Workers will be notified via notify_safe_to_exit when queue is drained
        if self._source_client:
            asyncio.create_task(
                self._poll_source_queue_completion(),
                name=f"poll_source_completion_{self.stage_id}",
            )

    async def _poll_source_queue_completion(self) -> None:
        """Poll source queue until it's safe for workers to exit.

        Checks is_queue_finished() RPC which returns safe_to_exit=True when:
        1. Queue is marked as finished (done above)
        2. Queue is drained (pending==0 && claimed==0)

        When safe, notifies all workers via notify_safe_to_exit().
        """
        if not self._source_client:
            return

        poll_interval = 0.1  # 100ms
        max_consecutive_errors = 10
        consecutive_errors = 0

        while self._running:
            try:
                result = self._source_client.is_queue_finished(self._source_queue_name)
                consecutive_errors = 0  # Reset on success
                if result.get("safe_to_exit", False):
                    self.logger.debug(
                        f"Source {self.stage_id} source queue drained, notifying workers"
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
                    raise RuntimeError(
                        f"Failed to poll source queue completion: {e}"
                    ) from e
                self.logger.debug(f"Error polling source queue completion: {e}")

            await asyncio.sleep(poll_interval)

    async def _check_backpressure_before_produce(self) -> bool:
        """Check if we should pause production due to downstream backpressure.

        Returns:
            True if production should be paused, False otherwise
        """
        # Check if we have downstream stages configured
        if not self._downstream_stage_refs:
            return False

        # Check all downstream stages for backpressure
        for stage_id, stage_ref in self._downstream_stage_refs.items():
            try:
                # Get status from downstream stage (sync method)
                status = stage_ref.get_status()

                # Check if backpressure is active
                if status.backpressure_active:
                    self.logger.debug(
                        f"Backpressure detected from downstream stage {stage_id}, "
                        f"pausing split production"
                    )
                    return True

                # Also check queue size if available
                # Use a threshold (e.g., 80% of max queue size)
                queue_size = status.output_queue_size
                if queue_size > self._backpressure_threshold_queue_size * 0.8:
                    self.logger.debug(
                        f"Downstream queue size {queue_size} approaching threshold, "
                        f"slowing down production"
                    )
                    return True

            except Exception as e:
                self.logger.debug(f"Error checking backpressure from {stage_id}: {e}")
                # Continue checking other downstream stages

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
        """Produce a split to the source queue.

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

        # Push to source queue
        if not self._source_client:
            raise RuntimeError("Source client not initialized")
        self._source_client.push(self._source_queue_name, message.to_bytes())

        self.logger.debug(f"Produced split {split.split_id}")

    @abstractmethod
    def plan_splits(self) -> Iterator[Split]:
        """Plan and generate splits for this source.

        Subclasses must implement this to define how data is split.

        Returns:
            Iterator of Split objects, each containing metadata for one split
        """
        raise NotImplementedError("plan_splits must be implemented by subclasses")

    async def cleanup_queue(self) -> None:
        """Clean up queues. Called by runner after all consumers are done."""
        if self._source_client:
            self._source_client.stop()
            self._source_client = None
        await super().cleanup_queue()

    def get_source_client(self) -> Optional[WorkQueueQueueClient]:
        """Get the source queue client (for debugging/testing)."""
        return self._source_client

    def get_source_queue_name(self) -> str:
        """Get the source queue name."""
        return self._source_queue_name

    def get_source_endpoint(self) -> Optional[QueueEndpoint]:
        """Get the source endpoint (for debugging/testing)."""
        return self._source_endpoint

    def get_status(self) -> StageStatus:
        """Get current source status with queue metrics."""
        status = super().get_status()

        # Add source queue size
        if self._source_client:
            try:
                stats = self._source_client.get_stats(self._source_queue_name)
                status.metrics["source_queue_pending"] = stats.get("pending_count", 0)
            except Exception:
                pass

        status.metrics["splits_produced"] = self._splits_produced
        return status
