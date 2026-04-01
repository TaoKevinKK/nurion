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

"""Stage definition and runtime configuration.

This module contains:
- Stage: User-defined stage configuration (immutable after creation)
- StageRuntime: System-assigned runtime parameters (frozen dataclass)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

from _internal.core.operator import OperatorConfig

if TYPE_CHECKING:
    from _internal.core.models import QueueEndpoint
    from _internal.runtime.queue_stats import QueueRef


# =============================================================================
# Stage Runtime - System-assigned parameters (immutable after creation)
# =============================================================================


@dataclass(frozen=True)
class StageRuntime:
    """Runtime parameters assigned by the runner.

    These are determined when the job starts and remain constant throughout
    the stage's lifecycle. Immutable (frozen) for distributed safety.

    Upstream data:
    - upstream: QueueRef identifying the upstream queue.
      Source stages: QueueRef.queue(planner_queue_name) — set by StageMaster
      after SplitPlanner creates its queue. None initially.
      Non-source stages: QueueRef.group(group_name) — set by runner.
    - upstream_num_partitions: partition count for round-robin assignment
      (0 for source stages / non-shuffle).
    """

    broker_endpoint: Optional["QueueEndpoint"] = None
    upstream: Optional["QueueRef"] = None
    claim_timeout_secs: float = 60.0
    upstream_num_partitions: int = 0
    max_pending_total: int = (
        0  # 0 = unlimited. Total budget for output queue (divided by partitions at creation).
    )


# =============================================================================
# Stage - User-defined configuration
# =============================================================================


class Stage:
    """Represents a stage in the processing pipeline.

    User-defined configuration that specifies how a stage should behave.
    Includes operator config, parallelism settings, and worker resources.
    """

    def __init__(
        self,
        stage_id: str,
        operator_config: OperatorConfig,
        parallelism: Union[int, Tuple[int, int]] = 1,
        worker_resources: Optional[Dict[str, float]] = None,
        # Processing configuration
        batch_size: int = 100,
        # Backpressure thresholds
        backpressure_threshold_lag: int = 5000,
        backpressure_threshold_queue_size: int = 1000,
        # Worker lifecycle
        worker_ready_timeout_seconds: float = 30.0,
        worker_spawn_retry_delay_seconds: float = 2.0,
    ):
        """Initialize a stage.

        Args:
            stage_id: Unique identifier for the stage
            operator_config: Configuration for the operator (OperatorConfig subclass).
                For source stages, the config should implement create_source()
                to provide a SplitPlanner or DirectProducer.
            parallelism: Number of workers. Can be:
                - int: Fixed number of workers (no auto-scaling)
                - Tuple[int, int]: (min_workers, max_workers) for auto-scaling
            worker_resources: Resource requirements per worker (num_cpus, num_gpus, memory)
            batch_size: Number of messages to claim per batch
            backpressure_threshold_lag: Lag threshold for backpressure activation
            backpressure_threshold_queue_size: Queue size threshold for backpressure
            worker_ready_timeout_seconds: Max time to wait for worker to be ready
            worker_spawn_retry_delay_seconds: Delay between spawn retries

        Examples:
            >>> # Fixed 4 workers, no scaling
            >>> Stage('process', MyOperatorConfig(param=value), parallelism=4)

            >>> # Auto-scaling between 2 and 10 workers
            >>> Stage('process', MyOperatorConfig(param=value), parallelism=(2, 10))
        """
        self.stage_id = stage_id
        self.operator_config = operator_config

        # Parse parallelism parameter
        if isinstance(parallelism, int):
            self.min_parallelism = parallelism
            self.max_parallelism = parallelism
        elif isinstance(parallelism, tuple) and len(parallelism) == 2:
            min_p, max_p = parallelism
            if min_p > max_p:
                raise ValueError(
                    f"min_parallelism ({min_p}) cannot be greater than max_parallelism ({max_p})"
                )
            self.min_parallelism = min_p
            self.max_parallelism = max_p
        else:
            raise ValueError(f"parallelism must be int or Tuple[int, int], got {type(parallelism)}")

        # Default worker resources
        self.worker_resources = worker_resources or {
            "num_cpus": 0.5,
            "num_gpus": 0,
            "memory": 500 * 1024**2,  # 500MB
        }

        # Processing configuration
        self.batch_size = batch_size

        # Backpressure thresholds
        self.backpressure_threshold_lag = backpressure_threshold_lag
        self.backpressure_threshold_queue_size = backpressure_threshold_queue_size

        # Worker lifecycle
        self.worker_ready_timeout_seconds = worker_ready_timeout_seconds
        self.worker_spawn_retry_delay_seconds = worker_spawn_retry_delay_seconds

    @property
    def parallelism(self) -> Tuple[int, int]:
        """Get parallelism configuration as (min, max)."""
        return (self.min_parallelism, self.max_parallelism)

    @property
    def num_cpus(self) -> float:
        """CPU resources per worker."""
        return self.worker_resources.get("num_cpus", 0.5)

    @property
    def num_gpus(self) -> float:
        """GPU resources per worker."""
        return self.worker_resources.get("num_gpus", 0.0)

    @property
    def memory_mb(self) -> int:
        """Memory (MB) per worker."""
        return int(self.worker_resources.get("memory", 0) / (1024**2))

    def to_dict(self) -> Dict[str, Any]:
        """Convert stage to dictionary representation."""
        return {
            "stage_id": self.stage_id,
            "operator_config": self.operator_config.to_dict(),
            "max_parallelism": self.max_parallelism,
            "min_parallelism": self.min_parallelism,
            "worker_resources": self.worker_resources,
            "batch_size": self.batch_size,
        }
