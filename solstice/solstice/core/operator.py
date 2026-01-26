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
- Operator: Stateless processor with optional state store for exactly-once semantics

Operators support two semantic guarantees (configured at job level):
- AT_LEAST_ONCE (default): No dedup overhead, messages may be processed multiple times
- EXACTLY_ONCE: Dedup via offset tracking (offset <= last_offset means duplicate)

For sequential partition consumption, offset-based dedup is sufficient.
No need for separate split_id tracking since split_id is derived from offset.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, fields
from enum import Enum
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    TYPE_CHECKING,
)
import asyncio
import logging

from solstice.core.models import SplitPayload, Split

if TYPE_CHECKING:
    from solstice.core.stage_master import StageMaster
    from solstice.state.protocols import PartitionStateStore


T = TypeVar("T", bound="Operator")
C = TypeVar("C", bound="OperatorConfig")
F = TypeVar("F", bound=Callable[..., Any])


# =============================================================================
# Semantic Guarantee
# =============================================================================


class SemanticGuarantee(Enum):
    """Processing semantics for the job.

    AT_LEAST_ONCE: Messages may be processed multiple times on failure.
                   No dedup overhead, highest throughput.
    EXACTLY_ONCE:  Messages processed exactly once via offset-based dedup.
                   Offset + state saved atomically to ensure consistency.
    """

    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


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
        worker_id: Worker identifier (includes partition suffix)
        partition_id: Partition this operator handles
        semantic_guarantee: AT_LEAST_ONCE or EXACTLY_ONCE
    """

    job_id: str
    stage_id: str
    worker_id: str
    partition_id: int
    semantic_guarantee: SemanticGuarantee = SemanticGuarantee.AT_LEAST_ONCE


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
# State Store Keys
# =============================================================================

OFFSET_KEY = b"_solstice_offset"


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
            partition_id=0,
        )
        operator = config.setup(runtime)

    Class Variables:
        operator_class: The operator class to instantiate (set by @operator decorator)
        master_class: The master class to use (None = use default StageMaster)
    """

    operator_class: ClassVar[Type["Operator"]]
    master_class: ClassVar[Optional[Type["StageMaster"]]] = None  # Default: use StageMaster

    # State store configuration (optional, for stateful operators)
    # Using kw_only=True to allow child classes to have positional required fields
    state_store_path: Optional[str] = field(default=None, kw_only=True)

    def setup(self, runtime: OperatorRuntime) -> "Operator":
        """Create and return an operator instance with this configuration.

        Args:
            runtime: Runtime parameters (job_id, stage_id, worker_id, partition_id)

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
    """Base class for all operators with partition-aware state management.

    Design Principle: Operators receive immutable config and runtime parameters.
    Optional state store for exactly-once semantics.

    Partition-per-Operator Model:
    - Each partition gets its own Operator instance
    - partition_id is in runtime, accessible via self.partition_id
    - State store is used for offset + business state persistence

    Offset-based Dedup (for EXACTLY_ONCE):
    - last_offset: Last processed offset
    - is_duplicate(offset): Returns True if offset <= last_offset
    - For sequential partition consumption, this is sufficient
    - No need for separate split_id tracking

    Usage:
        # Worker creates operator per partition
        runtime = OperatorRuntime(
            job_id="job_123",
            stage_id="stage_0",
            worker_id="worker_0_p0",
            partition_id=0,
            semantic_guarantee=SemanticGuarantee.EXACTLY_ONCE,
        )
        op = config.setup(runtime)
        op.init_from_state_store()  # Recover last_offset
        # ... process messages ...
        op.mark_processed(offset)
    """

    # Class variable set by @operator decorator
    config_class: ClassVar[Type[OperatorConfig]]

    def __init__(self, config: OperatorConfig, runtime: OperatorRuntime):
        self._config = config
        self._runtime = runtime
        self.logger = logging.getLogger(self.__class__.__name__)

        # State store (created lazily if state_store_path is set)
        self._state_store: Optional["PartitionStateStore"] = None
        self._acquired_partitions: set[int] = set()  # Track acquired partition IDs

        # Offset tracking for dedup
        self.last_offset: int = -1  # -1 = no offset recovered
        self.task: Optional[asyncio.Task[None]] = None

        # Metrics
        self.processed_count: int = 0
        self.error_count: int = 0
        self.total_input_records: int = 0
        self.total_output_records: int = 0
        self.total_processing_time: float = 0.0

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

    @property
    def partition_id(self) -> int:
        """Partition ID from runtime (for partition-per-operator model)."""
        return self._runtime.partition_id

    @property
    def semantic_guarantee(self) -> SemanticGuarantee:
        """Semantic guarantee from runtime."""
        return self._runtime.semantic_guarantee

    @property
    def is_exactly_once(self) -> bool:
        """Check if exactly-once semantics are enabled."""
        return self.semantic_guarantee == SemanticGuarantee.EXACTLY_ONCE

    @property
    def state_store(self) -> Optional["PartitionStateStore"]:
        """Lazily create state store from config.

        Returns None if state_store_path is not configured.
        """
        if self._state_store is None:
            path = self._config.state_store_path
            if path:
                from solstice.state import SlateDBPartitionStateStore

                self._state_store = SlateDBPartitionStateStore(
                    base_path=path,
                    job_id=self.job_id,
                    stage_id=self.stage_id,
                )
        return self._state_store

    def _ensure_partition_acquired(self, partition_id: Optional[int] = None) -> None:
        """Ensure partition is acquired in state store.

        Args:
            partition_id: Partition ID to acquire. If None, uses self.partition_id.
                         Multi-partition operators should pass explicit partition_id.
        """
        pid = partition_id if partition_id is not None else self.partition_id
        if pid in self._acquired_partitions:
            return
        store = self.state_store
        if store is None:
            return
        store.acquire_partition(pid)
        self._acquired_partitions.add(pid)

    # =========================================================================
    # Recovery Methods
    # =========================================================================

    def init_from_state_store(self) -> None:
        """Initialize last_offset from state store (for recovery)."""
        store = self.state_store
        if store is None:
            return

        self._ensure_partition_acquired()

        try:
            # Recover last_offset
            offset_bytes = store.get(self.partition_id, OFFSET_KEY)
            if offset_bytes is not None:
                self.last_offset = int.from_bytes(offset_bytes, "big", signed=True)
                self.logger.info(f"Recovered last_offset={self.last_offset}")
        except Exception as e:
            self.logger.warning(f"Failed to recover from state store: {e}")

    # =========================================================================
    # Dedup Methods
    # =========================================================================

    def is_duplicate(self, offset: int) -> bool:
        """Check if offset was already processed.

        For sequential partition consumption, offset <= last_offset means
        the message was already processed.

        Args:
            offset: The message offset to check

        Returns:
            True if this offset was already processed
        """
        if self.last_offset < 0:
            return False
        return offset <= self.last_offset

    # =========================================================================
    # State Persistence
    # =========================================================================

    def save_state(
        self,
        offset: int,
        state_updates: Optional[List[Tuple[bytes, bytes]]] = None,
    ) -> None:
        """Atomically save offset and optional business state.

        Args:
            offset: The offset to save
            state_updates: Optional additional state updates
        """
        # Update in-memory state
        self.last_offset = offset

        store = self.state_store
        if store is None:
            return

        self._ensure_partition_acquired()

        # Build batch writes
        writes: List[Tuple[int, bytes, bytes]] = []

        # Add operator's state updates
        if state_updates:
            for key, value in state_updates:
                writes.append((self.partition_id, key, value))

        # Add offset
        offset_bytes = offset.to_bytes(8, "big", signed=True)
        writes.append((self.partition_id, OFFSET_KEY, offset_bytes))

        # Atomic write
        store.put_batch(writes)

    def mark_processed(
        self,
        offset: int,
        state_updates: Optional[List[Tuple[bytes, bytes]]] = None,
    ) -> None:
        """Mark offset as processed.

        Atomically saves to state store if available.
        """
        self.save_state(offset, state_updates)
        self.processed_count += 1

    # =========================================================================
    # Metrics
    # =========================================================================

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        return {
            "partition_id": self.partition_id,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "total_input_records": self.total_input_records,
            "total_output_records": self.total_output_records,
            "total_processing_time": self.total_processing_time,
            "last_offset": self.last_offset,
        }

    # =========================================================================
    # Abstract Methods
    # =========================================================================

    @abstractmethod
    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        pass

    def close(self) -> None:
        """Clean up operator resources."""
        if self.task and not self.task.done():
            self.task.cancel()

        # Release all acquired partitions from state store
        if self._state_store is not None:
            for pid in self._acquired_partitions:
                try:
                    self._state_store.release_partition(pid)
                except Exception as e:
                    self.logger.warning(f"Error releasing partition {pid}: {e}")
            self._acquired_partitions.clear()
            self._state_store.close()
            self._state_store = None
