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

"""Base operator interface with config/runtime separation.

Design Principles:
- OperatorConfig: User-defined configuration, immutable after creation
- OperatorRuntime: System-assigned runtime parameters, immutable after creation
- Operator: Processor with state managed via Anvil Server

State Management (Anvil model):
- State is integrated into Anvil Server (single-writer, no partition conflicts)
- Workers access state via Anvil Client: state_get(), state_put()
- Atomic operations: ack + state update in single transaction
- No local SlateDB state store needed in operators
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, fields
from typing import (
    Any,
    AsyncIterator,
    Callable,
    ClassVar,
    Coroutine,
    Dict,
    Iterator,
    Optional,
    Type,
    TypeVar,
    Union,
    TYPE_CHECKING,
)
import asyncio
import logging

import pyarrow as pa

from _internal.core.models import RawOutputBytes, SplitPayload, Split

# All supported return types for process_split
PayloadResult = Union[
    None,  # drop (filter)
    SplitPayload,  # single output (map)
    RawOutputBytes,  # raw bytes to forward to output queue (sink commit)
    Iterator[SplitPayload],  # multiple outputs (explode)
    AsyncIterator[SplitPayload],  # async multiple outputs
    Coroutine[Any, Any, Optional[SplitPayload]],  # async single
    Coroutine[Any, Any, Iterator[SplitPayload]],  # async multiple
]

if TYPE_CHECKING:
    from _internal.core.models import QueueEndpoint
    from _internal.core.source import SourceStrategy
    from _internal.core.sink import SinkCommitter
    from _internal.core.split_payload_store import SplitPayloadStore


T = TypeVar("T", bound="Operator")
C = TypeVar("C", bound="OperatorConfig")
F = TypeVar("F", bound=Callable[..., Any])


# =============================================================================
# Operator Runtime - System-assigned parameters (immutable after creation)
# =============================================================================


@dataclass(frozen=True)
class OperatorRuntime:
    """Runtime parameters assigned by the system.

    These are determined at worker startup and remain constant throughout
    the operator's lifecycle. Immutable (frozen) for distributed safety.

    Attributes:
        job_id: Job identifier
        stage_id: Stage identifier
        worker_id: Worker identifier
        broker_endpoint: Optional queue broker endpoint for operators that
            need direct queue access (e.g., sink operators pushing commit metadata)
    """

    job_id: str
    stage_id: str
    worker_id: str
    broker_endpoint: Optional["QueueEndpoint"] = None
    payload_store: Optional["SplitPayloadStore"] = None


# =============================================================================
# Operator Decorator - Auto-bind Config to Operator
# =============================================================================


def operator(config_class: Type[C]) -> Callable[[Type[T]], Type[T]]:
    """Decorator to bind an Operator class to its Config class.

    This establishes the bidirectional relationship between Config and Operator,
    eliminating the need for manual `Config.operator_class = Operator` assignment.

    Example:
        @dataclass
        class MyOperatorConfig(OperatorConfig):
            param: str

        @operator(MyOperatorConfig)
        class MyOperator(Operator):
            def __init__(self, config: MyOperatorConfig, runtime: OperatorRuntime):
                super().__init__(config, runtime)

            def process_split(self, split, payload):
                ...

        # Now MyOperatorConfig.operator_class == MyOperator
        # And MyOperator.config_class == MyOperatorConfig
    """

    def decorator(op_class: Type[T]) -> Type[T]:
        config_class.operator_class = op_class  # type: ignore[attr-defined]
        op_class.config_class = config_class  # type: ignore[attr-defined]
        return op_class

    return decorator


# =============================================================================
# Master-Callable Decorator
# =============================================================================
#
# Marks operator methods that can be invoked remotely by the master via
# worker.invoke_operator(). This provides a secure, extensible mechanism
# for master-worker communication without modifying StageWorker for each
# new operator feature.


def master_callable(func: F) -> F:
    """Decorator to mark operator methods as callable by master.

    Methods decorated with @master_callable can be invoked remotely via
    worker.invoke_operator("method_name", *args, **kwargs).

    This enables extensible master-worker communication:
    - Add new operator methods without modifying StageWorker
    - Explicit marking ensures only intended methods are exposed
    - Type safety preserved in the operator class

    Example:
        class MyOperator(Operator):
            @master_callable
            def get_stats(self) -> Dict[str, int]:
                return {"processed": self._count}

            @master_callable
            def reset_state(self, iteration: int) -> None:
                self._iteration = iteration

        # From master:
        stats = ray.get(worker.invoke_operator.remote("get_stats"))
        ray.get(worker.invoke_operator.remote("reset_state", iteration=2))
    """
    func._master_callable = True  # type: ignore[attr-defined]
    return func


def is_master_callable(method: Any) -> bool:
    """Check if a method is marked with @master_callable."""
    return getattr(method, "_master_callable", False)


@dataclass
class OperatorConfig(ABC):
    """Base configuration class for operators.

    User-defined configuration that is immutable after creation.
    Runtime parameters are passed separately via OperatorRuntime.

    Subclasses should define their configuration fields as dataclass fields.
    Use the @operator decorator to bind Config to Operator class.

    Example:
        @dataclass
        class MyOperatorConfig(OperatorConfig):
            param1: str
            param2: int = 10

        @operator(MyOperatorConfig)
        class MyOperator(Operator):
            def __init__(self, config: MyOperatorConfig, runtime: OperatorRuntime):
                super().__init__(config, runtime)

        # Usage:
        config = MyOperatorConfig(param1="value")
        runtime = OperatorRuntime(
            job_id="job_123",
            stage_id="stage_0",
            worker_id="worker_0",
        )
        operator = config.setup(runtime)

    Class Variables:
        operator_class: The operator class to instantiate (set by @operator decorator)
        master_class: Optional override for stage orchestration (e.g., CCIterateMaster).
            Most operators should use create_source() / create_sink_committer() instead.
    """

    operator_class: ClassVar[Optional[Type["Operator"]]] = None

    def get_merge_upstream(self) -> int:
        """Number of upstream messages to merge into one process_split() call.

        When > 1, StageWorker claims multiple messages, merges their
        SplitPayloads (Arrow table concatenation), and calls process_split()
        once with the merged data. All upstream messages are acked atomically
        after processing via ack_and_scatter.

        Override in configs that benefit from larger batches (e.g., Lance sink
        wants larger fragments rather than one per upstream message).

        Default: 1 (no merge, process each message individually).
        """
        return 1

    def get_output_partition_count(self) -> int:
        """Number of output partitions. 0 = no partitioning (default).

        Override in shuffle/repartition configs to declare how many
        partition queues the worker should route output to.
        """
        return 0

    def get_partition_column(self) -> str:
        """Column name used for partition routing.

        Only meaningful when get_output_partition_count() > 0.
        The worker splits output rows by this integer column and
        routes each partition to its corresponding queue.
        """
        return "__partition"

    def get_source_schema(self) -> Optional[pa.Schema]:
        """Return the Arrow schema of data this source produces.

        Override in source configs to enable schema validation for Union and
        Anti-Join operations. Reads only metadata (no data scan).

        Returns:
            The output schema, or None if unknown / not applicable.
        """
        return None

    def create_source(self) -> Optional["SourceStrategy"]:
        """Create a source strategy for this operator.

        Override in source operator configs. Returns:
        - SplitPlanner for regular sources (workers consume splits)
        - DirectProducer for direct-write sources (no workers)
        - None for non-source operators (default)
        """
        return None

    def create_sink_committer(self) -> Optional["SinkCommitter"]:
        """Create a sink committer for batched commit coordination.

        Override in sink operator configs that need batched commits
        (e.g., Lance sink with fragment write + queue-based commit).
        Returns None for operators that don't need commit coordination.
        """
        return None

    def prepare(self, payload_store: "SplitPayloadStore") -> None:
        """Pre-flight hook called by StageMaster before workers spawn.

        Override for one-time setup that needs payload store access
        (e.g., anti-join builds exclude key table and stores it).

        Default: no-op.
        """
        pass

    def setup(self, runtime: OperatorRuntime) -> "Operator":
        """Create and return an operator instance with this configuration.

        Args:
            runtime: Runtime parameters (job_id, stage_id, worker_id)

        Returns:
            Configured operator instance

        Raises:
            TypeError: If operator_class is not set (e.g., DirectProducer configs
                that have no associated operator).
        """
        if self.operator_class is None:
            raise TypeError(
                f"{type(self).__name__} has no operator_class. "
                f"DirectProducer sources do not have operators."
            )
        return self.operator_class(config=self, runtime=runtime)

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary representation."""
        result = {}
        for f in fields(self):
            value = getattr(self, f.name)
            # Handle nested configs
            if isinstance(value, OperatorConfig):
                result[f.name] = value.to_dict()
            else:
                result[f.name] = value
        return result


class Operator(ABC):
    """Base class for all operators.

    Design Principle: Operators receive immutable config and runtime parameters.
    State is managed via Anvil Server (not local state store).

    State Management (Anvil model):
    - State operations go through Anvil Client
    - Atomic ack + state update supported via ack_and_scatter()
    - No local SlateDB needed in operators

    Usage:
        runtime = OperatorRuntime(
            job_id="job_123",
            stage_id="stage_0",
            worker_id="worker_0",
        )
        op = config.setup(runtime)
        # ... process messages ...
    """

    # Class variable set by @operator decorator
    config_class: ClassVar[Type[OperatorConfig]]

    def __init__(self, config: OperatorConfig, runtime: OperatorRuntime):
        self._config = config
        self._runtime = runtime
        self.logger = logging.getLogger(self.__class__.__name__)

        self.task: Optional[asyncio.Task[None]] = None

    @property
    def config(self) -> OperatorConfig:
        """User-defined configuration (immutable)."""
        return self._config

    @property
    def runtime(self) -> OperatorRuntime:
        """System-assigned runtime parameters (immutable)."""
        return self._runtime

    @property
    def worker_id(self) -> str:
        """Worker ID from _internal."""
        return self._runtime.worker_id

    @property
    def job_id(self) -> str:
        """Job ID from _internal."""
        return self._runtime.job_id

    @property
    def stage_id(self) -> str:
        """Stage ID from _internal."""
        return self._runtime.stage_id

    # =========================================================================
    # Abstract Methods
    # =========================================================================

    @abstractmethod
    def process_split(self, split: Split, payload: Optional[SplitPayload] = None) -> PayloadResult:
        """Process a split. Can be sync or async, single or multi-output.

        Return types:
            None                        → drop (filter)
            SplitPayload                → single output (map, 1:1)
            Iterator[SplitPayload]      → multiple outputs (explode, 1:N)

        All of the above also work as async:
            async def process_split(self, split, payload):
                ...
                return payload           # single
                yield payload1           # async generator (1:N)
        """
        pass

    def close(self) -> None:
        """Clean up operator resources."""
        if self.task and not self.task.done():
            self.task.cancel()
