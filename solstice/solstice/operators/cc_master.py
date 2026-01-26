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

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                    CCIterateMaster                          │
    │                    (self-contained)                         │
    ├─────────────────────────────────────────────────────────────┤
    │  run():                                                     │
    │    1. Read input from upstream (candidate pairs/messages)   │
    │    2. Process and update labels in state store              │
    │    3. Poll workers for changes (operator tracks internally) │
    │    4. If changed and iteration < max:                       │
    │       - Reset iteration counters                            │
    │       - Loop back to step 2                                 │
    │    5. Output final labels to downstream                     │
    └─────────────────────────────────────────────────────────────┘

Key design points:
- Iteration happens INSIDE the stage, not in the runner
- State (labels) is stored in SlateDB per partition
- Each worker processes its assigned partitions
- Master polls workers for changes (no callbacks)
- Iteration state lives in operator, not worker
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray

from solstice.core.stage_master import StageMaster
from solstice.state.slatedb_store import SlateDBPartitionStateStore

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
    2. Poll workers for iteration changes (operator tracks them)
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

        # State store config for reading changes
        self._state_store_path: Optional[str] = getattr(op_config, "state_store_path", None)
        self._num_partitions: int = getattr(op_config, "num_partitions", 1)

        # Iteration state
        self._current_iteration = 0
        self._converged = False

    async def run(self) -> bool:
        """Run the stage with internal iteration loop.

        Iteration Algorithm:
        1. First pass: Process initial input (candidate pairs -> messages)
        2. Poll workers for changes (operator tracks internally)
        3. If not converged, reset iteration and continue
        4. Output final labels

        Convergence Conditions:
        - Total changes across all partitions < convergence_threshold
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

            # Poll workers for changes from first iteration
            total_changes = await self._poll_worker_changes()
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

                # Trigger re-computation from stored state
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

    async def _poll_worker_changes(self) -> int:
        """Read total changes from state store.

        Workers store their change counts in state store with key `__changes__`.
        We read from each partition and sum them up.

        Returns:
            Total number of changes across all partitions
        """
        if not self._state_store_path:
            self.logger.warning("No state_store_path configured, cannot poll changes")
            return 0

        total_changes = 0

        # Create a state store instance to read from
        state_store = SlateDBPartitionStateStore(
            base_path=self._state_store_path,
            job_id=self.job_id,
            stage_id=self.stage_id,
        )

        try:
            for partition_id in range(self._num_partitions):
                try:
                    # Acquire partition for reading
                    state_store.acquire_partition(partition_id)
                    # Read changes count
                    changes_bytes = state_store.get(partition_id, b"__changes__")
                    if changes_bytes:
                        partition_changes = int(changes_bytes.decode())
                        total_changes += partition_changes
                        self.logger.debug(f"Partition {partition_id}: {partition_changes} changes")
                except Exception as e:
                    self.logger.debug(f"Failed to read changes from partition {partition_id}: {e}")
                finally:
                    state_store.release_partition(partition_id)
        finally:
            state_store.close()

        return total_changes

    async def _reset_worker_iterations(self) -> None:
        """Reset iteration state in all workers via invoke_operator."""
        if not self._worker_manager:
            return

        for worker in self._worker_manager.workers.values():
            try:
                worker.invoke_operator.remote("reset_iteration")
            except Exception as e:
                self.logger.warning(f"Failed to reset worker iteration: {e}")

    async def _recompute_worker_iterations(self) -> int:
        """Trigger recomputation from stored state in all workers.

        Uses invoke_operator for generic dispatch to operator methods.

        Returns:
            Total number of changes across all workers
        """
        if not self._worker_manager:
            return 0

        total_changes = 0
        futures = []

        for worker_id, worker in self._worker_manager.workers.items():
            # Get partition assignment for this worker
            assigned_partitions = self._partition_manager.get_assignment(worker_id)
            if not assigned_partitions:
                self.logger.warning(f"No partitions assigned to worker {worker_id}")
                continue
            futures.append(
                worker.invoke_operator.remote("recompute_from_state", assigned_partitions)
            )

        if futures:
            try:
                results = ray.get(futures, timeout=60.0)
                total_changes = sum(r for r in results if r is not None)
            except Exception as e:
                self.logger.warning(f"Failed to recompute worker iterations: {e}")

        return total_changes

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
