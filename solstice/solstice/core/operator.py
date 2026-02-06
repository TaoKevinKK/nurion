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
- Operator: Processor with state managed via WorkQueue Server

State Management (WorkQueue model):
- State is integrated into WorkQueue Server (single-writer, no partition conflicts)
- Workers access state via WorkQueue Client: state_get(), state_put()
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

from solstice.core.models import SplitPayload, Split

# All supported return types for process_split
PayloadResult = Union[
    None,  # drop (filter)
    SplitPayload,  # single output (map)
    Iterator[SplitPayload],  # multiple outputs (explode)
    AsyncIterator[SplitPayload],  # async multiple outputs
    Coroutine[Any, Any, Optional[SplitPayload]],  # async single
    Coroutine[Any, Any, Iterator[SplitPayload]],  # async multiple
]

if TYPE_CHECKING:
    from solstice.core.stage_master import StageMaster


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
    """

    job_id: str
    stage_id: str
    worker_id: str


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
        master_class: The master class to use (None = use default StageMaster)
    """

    operator_class: ClassVar[Type["Operator"]]
    master_class: ClassVar[Optional[Type["StageMaster"]]] = None  # Default: use StageMaster

    def setup(self, runtime: OperatorRuntime) -> "Operator":
        """Create and return an operator instance with this configuration.

        Args:
            runtime: Runtime parameters (job_id, stage_id, worker_id)

        Returns:
            Configured operator instance
        """
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
    State is managed via WorkQueue Server (not local state store).

    State Management (WorkQueue model):
    - State operations go through WorkQueue Client
    - Atomic ack + state update supported via ack_and_forward()
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
        """Worker ID from runtime."""
        return self._runtime.worker_id

    @property
    def job_id(self) -> str:
        """Job ID from runtime."""
        return self._runtime.job_id

    @property
    def stage_id(self) -> str:
        """Stage ID from runtime."""
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
