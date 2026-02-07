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

"""Source strategy protocols for StageMaster composition.

Two protocols replace the SourceMaster inheritance tree:

- SplitPlanner: Plans splits for workers to consume via a planner queue.
  Used by regular sources (Lance, Spark V1) where workers read actual data.

- DirectProducer: Produces data directly to the output queue, bypassing workers.
  Used by external systems (Spark V2 JVM) that write data directly.

StageMaster accepts an optional SourceStrategy (union of the two) and
handles both paths internally, keeping StageMaster as a final class.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterator, Union, runtime_checkable

from typing_extensions import Protocol

from solstice.core.models import Split

if TYPE_CHECKING:
    from solstice.core.models import QueueEndpoint
    from solstice.queue import WorkQueueQueueClient


@runtime_checkable
class SplitPlanner(Protocol):
    """Plans splits for workers to consume via planner queue.

    Pure data transformation: takes config, yields Splits.
    No queue management, no worker management, no state.

    Implementations are lightweight objects created by OperatorConfig.create_source().
    """

    def plan_splits(self, stage_id: str) -> Iterator[Split]: ...

    def cleanup(self) -> None:
        """Release resources after all workers finish (e.g., stop Spark session).

        Default is a no-op. Override in implementations that hold resources
        beyond plan_splits() (e.g., SparkSplitPlanner keeps Spark alive
        so workers can read object refs).
        """
        ...


@dataclass(frozen=True)
class DirectProduceContext:
    """Context passed to DirectProducer during produce().

    Provides access to queue infrastructure so the producer can
    write data directly to the output queue.
    """

    queue_client: WorkQueueQueueClient
    output_queue_name: str
    broker_endpoint: QueueEndpoint
    stage_id: str
    job_id: str


@runtime_checkable
class DirectProducer(Protocol):
    """Produces data directly to the output queue, bypassing workers.

    Used when an external system (e.g., Spark JVM) writes data directly
    to the output queue. No planner queue, no StageWorkers needed.
    """

    async def produce(self, ctx: DirectProduceContext) -> int:
        """Produce data to output queue.

        Returns:
            Number of items produced.
        """
        ...

    def cleanup(self) -> None:
        """Called during stop() to release resources (e.g., Spark session)."""
        ...


# Union type for OperatorConfig.create_source()
SourceStrategy = Union[SplitPlanner, DirectProducer]
