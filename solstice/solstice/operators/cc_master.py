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

"""Self-contained Connected Components Master.

CCIterateMaster handles iteration internally - no special logic needed
in RayJobRunner. This allows multiple iterative stages in a pipeline.

Architecture (WorkQueue-based, Jan 2025):
    ┌─────────────────────────────────────────────────────────────┐
    │                    CCIterateMaster                          │
    │                    (self-contained)                         │
    ├─────────────────────────────────────────────────────────────┤
    │  run():                                                     │
    │    1. Read input from upstream (candidate pairs/messages)   │
    │    2. Process messages, compute labels, output with edges   │
    │    3. Aggregate changes via @master_callable                │
    │    4. If changed and iteration < max:                       │
    │       - Reset iteration counters                            │
    │       - Trigger re-computation from edges in payload        │
    │    5. Output final labels to downstream                     │
    └─────────────────────────────────────────────────────────────┘

Key design points:
- Iteration happens INSIDE the stage, not in the runner
- Edges flow through payload (Arrow tables) for scale
- Labels tracked via iteration change counters (@master_callable)
- No local SlateDB state store needed
- Future: labels via WorkQueue state API (state_get/state_put)
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List

import ray

from solstice.core.stage_master import StageMaster

if TYPE_CHECKING:
    from solstice.core.stage import Stage, StageRuntime
    from solstice.core.split_payload_store import SplitPayloadStore


@dataclass
class IterationStats:
    """Statistics for one iteration."""

    iteration: int
    changes: int = 0
    duration: float = 0.0


class CCIterateMaster(StageMaster):
    """Self-contained iterative stage master for Connected Components.

    Handles iteration internally:
    1. Run base stage logic to process input
    2. Aggregate changes via @master_callable
    3. If not converged, reset and continue
    4. When converged, output final results

    No special handling needed in RayJobRunner.

    Configuration is read from stage.operator_config (CCIterateConfig):
    - max_iterations: Maximum iterations before forced stop
    - convergence_threshold: Number of changes below which to stop
    """

    def __init__(
        self,
        job_id: str,
        stage: "Stage",
        payload_store: "SplitPayloadStore",
        runtime: "StageRuntime",
    ):
        super().__init__(job_id, stage, payload_store, runtime)

        # Read iteration config from operator config
        op_config = stage.operator_config
        self._max_iterations = getattr(op_config, "max_iterations", 100)
        self._convergence_threshold = getattr(op_config, "convergence_threshold", 0)
        self._iteration_stats: List[IterationStats] = []
        self._num_partitions: int = getattr(op_config, "num_partitions", 1)

        # Iteration state
        self._current_iteration = 0
        self._converged = False

    async def run(self) -> bool:
        """Run the stage with internal iteration loop.

        Iteration Algorithm:
        1. First pass: Process initial input (candidate pairs -> messages)
        2. Aggregate changes from workers via @master_callable
        3. If not converged, reset iteration and continue
        4. Output final labels

        Convergence Conditions:
        - Total changes across all workers < convergence_threshold
        - Or max_iterations reached
        """
        self.logger.info(
            f"CCIterateMaster running (max_iterations={self._max_iterations}, "
            f"convergence_threshold={self._convergence_threshold})"
        )

        start_time = time.time()

        try:
            # Run first iteration using base StageMaster logic
            self._current_iteration = 1
            first_pass_result = await super().run()

            if not first_pass_result:
                self.logger.error("First pass failed")
                return False

            # Aggregate changes from workers via @master_callable
            total_changes = await self._aggregate_worker_changes()
            iteration_duration = time.time() - start_time

            self._iteration_stats.append(
                IterationStats(
                    iteration=1,
                    changes=total_changes,
                    duration=iteration_duration,
                )
            )

            self.logger.info(
                f"Iteration 1 completed: {total_changes} changes, "
                f"duration={iteration_duration:.2f}s"
            )

            # Check convergence after first iteration
            if self._check_convergence(total_changes):
                self.logger.info("Converged after first iteration")
                self._converged = True
                return True

            # Continue iteration loop until convergence or max iterations
            while self._current_iteration < self._max_iterations:
                self._current_iteration += 1
                iteration_start = time.time()

                # Reset iteration state in workers
                await self._reset_worker_iterations()

                # Trigger re-computation (workers process data from output queue)
                total_changes = await self._recompute_worker_iterations()
                iteration_duration = time.time() - iteration_start

                self._iteration_stats.append(
                    IterationStats(
                        iteration=self._current_iteration,
                        changes=total_changes,
                        duration=iteration_duration,
                    )
                )

                self.logger.info(
                    f"Iteration {self._current_iteration} completed: {total_changes} changes, "
                    f"duration={iteration_duration:.2f}s"
                )

                # Check convergence
                if self._check_convergence(total_changes):
                    self.logger.info(f"Converged after {self._current_iteration} iterations")
                    self._converged = True
                    break

            total_duration = time.time() - start_time
            self.logger.info(
                f"CC iteration complete: {self._current_iteration} iterations, "
                f"converged={self._converged}, total_duration={total_duration:.2f}s"
            )

            return True

        except Exception as e:
            self.logger.error(f"CCIterateMaster run failed: {e}")
            raise

    def _check_convergence(self, total_changes: int) -> bool:
        """Check if iteration has converged.

        Args:
            total_changes: Total label changes in this iteration

        Returns:
            True if converged (changes <= threshold)
        """
        return total_changes <= self._convergence_threshold

    async def _aggregate_worker_changes(self) -> int:
        """Aggregate changes from all workers via @master_callable.

        Calls get_iteration_changes() on each worker and sums the results.

        Returns:
            Total number of changes across all workers
        """
        if not self._worker_manager:
            return 0

        total_changes = 0
        futures = []

        for worker in self._worker_manager.workers.values():
            try:
                futures.append(worker.invoke_operator.remote("get_iteration_changes"))
            except Exception as e:
                self.logger.warning(f"Failed to get worker changes: {e}")

        if futures:
            try:
                results = ray.get(futures, timeout=30.0)
                total_changes = sum(r for r in results if r is not None)
            except Exception as e:
                self.logger.warning(f"Failed to aggregate worker changes: {e}")

        return total_changes

    async def _reset_worker_iterations(self) -> None:
        """Reset iteration state in all workers via invoke_operator."""
        if not self._worker_manager:
            return

        futures = []
        for worker in self._worker_manager.workers.values():
            try:
                futures.append(worker.invoke_operator.remote("reset_iteration"))
            except Exception as e:
                self.logger.warning(f"Failed to reset worker iteration: {e}")

        # Wait for all resets to complete
        if futures:
            try:
                ray.get(futures, timeout=30.0)
            except Exception as e:
                self.logger.warning(f"Failed waiting for iteration reset: {e}")

    async def _recompute_worker_iterations(self) -> int:
        """Trigger recomputation in all workers.

        In the payload-based model, workers recompute labels from edges
        stored in the payload. The master coordinates by:
        1. Resetting iteration counters
        2. Triggering recompute (workers read from output queue)
        3. Aggregating change counts

        Note: For iteration 2+, data flows through the queue again.
        The output queue from iteration N becomes input for iteration N+1.

        Returns:
            Total number of changes across all workers
        """
        if not self._worker_manager:
            return 0

        # In payload-based iteration, workers process data from queue
        # For now, we call recompute_labels with empty data as a signal
        # TODO: Implement proper queue loopback for iteration 2+
        futures = []

        for worker in self._worker_manager.workers.values():
            try:
                # Workers will read from output queue and recompute
                futures.append(worker.invoke_operator.remote("recompute_labels", []))
            except Exception as e:
                self.logger.warning(f"Failed to trigger worker recompute: {e}")

        if futures:
            try:
                results = ray.get(futures, timeout=60.0)
                return sum(r for r in results if r is not None)
            except Exception as e:
                self.logger.warning(f"Failed to recompute worker iterations: {e}")

        return 0

    def get_iteration_summary(self) -> Dict[str, Any]:
        """Get summary of iteration execution."""
        return {
            "converged": self._converged,
            "total_iterations": self._current_iteration,
            "max_iterations": self._max_iterations,
            "iteration_stats": [
                {
                    "iteration": s.iteration,
                    "changes": s.changes,
                    "duration": s.duration,
                }
                for s in self._iteration_stats
            ],
        }
