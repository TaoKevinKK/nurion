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

"""Base operator interface with EasyConfig pattern and partition-aware state management.

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

    Subclasses should define their configuration fields as dataclass fields,
    and set the `operator_class` class variable to the corresponding operator class.

    Example:
        @dataclass
        class MyOperatorConfig(OperatorConfig):
            operator_class = MyOperator

            param1: str
            param2: int = 10

        # Usage:
        config = MyOperatorConfig(param1="value")
        config.job_id = "job_123"
        config.stage_id = "stage_0"
        config.worker_id = "worker_0"
        config.partition_id = 0
        operator = config.setup()

    Class Variables:
        operator_class: The operator class to instantiate
        master_class: The master class to use (None = use default StageMaster)

    Runtime Context (set by runner/worker before setup()):
        job_id: Job identifier
        stage_id: Stage identifier
        worker_id: Worker identifier
        partition_id: Partition this operator handles
        semantic_guarantee: AT_LEAST_ONCE or EXACTLY_ONCE
    """

    operator_class: ClassVar[Type["Operator"]]
    master_class: ClassVar[Optional[Type["StageMaster"]]] = None  # Default: use StageMaster

    # State store configuration (optional, for stateful operators)
    # Using kw_only=True to allow child classes to have positional required fields
    state_store_path: Optional[str] = field(default=None, kw_only=True)

    # Runtime context - set by runner/worker before setup()
    # These are NOT constructor args, set via attribute assignment after init
    # Using init=False to avoid dataclass inheritance ordering issues
    job_id: Optional[str] = field(default=None, init=False, repr=False)
    stage_id: Optional[str] = field(default=None, init=False, repr=False)
    worker_id: Optional[str] = field(default=None, init=False, repr=False)
    partition_id: Optional[int] = field(default=None, init=False, repr=False)
    semantic_guarantee: SemanticGuarantee = field(
        default=SemanticGuarantee.AT_LEAST_ONCE, init=False, repr=False
    )

    def setup(self) -> "Operator":
        """Create and return an operator instance with this configuration.

        Note: job_id, stage_id, worker_id, partition_id should be set on the config
        before calling setup(). The operator accesses these via config.

        Returns:
            Configured operator instance
        """
        return self.operator_class(config=self)

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary representation."""
        result = {}
        for f in fields(self):
            # Skip runtime context fields
            if f.name in (
                "job_id",
                "stage_id",
                "worker_id",
                "partition_id",
                "semantic_guarantee",
            ):
                continue
            value = getattr(self, f.name)
            # Handle nested configs
            if isinstance(value, OperatorConfig):
                result[f.name] = value.to_dict()
            else:
                result[f.name] = value
        return result


class Operator(ABC):
    """Base class for all operators with partition-aware state management.

    Design Principle: Operators should be stateless configuration containers
    with optional state store for exactly-once semantics.

    Partition-per-Operator Model:
    - Each partition gets its own Operator instance
    - partition_id is in config, accessible via self.partition_id
    - State store is used for offset + business state persistence

    Offset-based Dedup (for EXACTLY_ONCE):
    - last_offset: Last processed offset
    - is_duplicate(offset): Returns True if offset <= last_offset
    - For sequential partition consumption, this is sufficient
    - No need for separate split_id tracking

    Usage:
        # Worker creates operator per partition
        config.partition_id = 0
        config.semantic_guarantee = SemanticGuarantee.EXACTLY_ONCE
        op = config.setup()
        op.init_from_state_store()  # Recover last_offset
        # ... process messages ...
        op.mark_processed(offset)
    """

    def __init__(self, config: OperatorConfig):
        self.config = config
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
    def worker_id(self) -> Optional[str]:
        """Worker ID from config (for backward compatibility)."""
        return self.config.worker_id

    @property
    def job_id(self) -> Optional[str]:
        """Job ID from config."""
        return self.config.job_id

    @property
    def stage_id(self) -> Optional[str]:
        """Stage ID from config."""
        return self.config.stage_id

    @property
    def partition_id(self) -> Optional[int]:
        """Partition ID from config (for partition-per-operator model)."""
        return self.config.partition_id

    @property
    def semantic_guarantee(self) -> SemanticGuarantee:
        """Semantic guarantee from config."""
        return self.config.semantic_guarantee

    @property
    def is_exactly_once(self) -> bool:
        """Check if exactly-once semantics are enabled."""
        return self.semantic_guarantee == SemanticGuarantee.EXACTLY_ONCE

    @property
    def state_store(self) -> Optional["PartitionStateStore"]:
        """Lazily create state store from config.

        Returns None if state_store_path is not configured or runtime context
        (job_id, stage_id) is not set.
        """
        if self._state_store is None:
            path = self.config.state_store_path
            if path and self.job_id and self.stage_id:
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
        if pid is None:
            return
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
        """Initialize last_offset from state store (for recovery).

        Call this after setting partition_id in config.
        """
        store = self.state_store
        if store is None or self.partition_id is None:
            return

        self._ensure_partition_acquired()
        partition_id = self.partition_id

        try:
            # Recover last_offset
            offset_bytes = store.get(partition_id, OFFSET_KEY)
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
        if store is None or self.partition_id is None:
            return

        self._ensure_partition_acquired()
        partition_id = self.partition_id

        # Build batch writes
        writes: List[Tuple[int, bytes, bytes]] = []

        # Add operator's state updates
        if state_updates:
            for key, value in state_updates:
                writes.append((partition_id, key, value))

        # Add offset
        offset_bytes = offset.to_bytes(8, "big", signed=True)
        writes.append((partition_id, OFFSET_KEY, offset_bytes))

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
