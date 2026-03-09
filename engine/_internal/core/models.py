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

"""Core data models for the streaming framework.

This module contains shared data classes:
- Split/SplitPayload: Data processing units
- Record: Single record flowing through pipeline
- FailurePolicy/FailureTracker: Worker fault tolerance
- SourceQueueMessage/DataQueueMessage/MessageType: Inter-stage message format
- StageStatus: Stage runtime status
- QueueEndpoint: Queue connection info
"""

import json
import time
import warnings
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Union

import pyarrow as pa


@dataclass
class Split:
    """Represents a logical split of data for processing.

    Each split tracks the scheduling metadata for a *single* data batch. The actual
    payload lives separately in :class:`SplitPayload` instances; the runtime associates
    splits with batches via identifiers/object references.
    """

    split_id: str
    stage_id: str
    data_range: Dict[str, Any]  # offset, file path, key range, etc.
    parent_split_ids: List[str] = field(default_factory=list)

    def lineage(self) -> Dict[str, Any]:
        """Return lineage metadata for downstream operators."""
        return {
            "split_id": self.split_id,
            "stage_id": self.stage_id,
            "parents": list(self.parent_split_ids),
        }

    def derive_output_split(
        self,
        target_split_id: str,
        target_stage_id: Optional[str] = None,
        data_range: Optional[Dict[str, Any]] = None,
    ) -> "Split":
        """Produce a new split metadata object for downstream consumption."""
        derived_stage_id = target_stage_id or self.stage_id
        derived_split_id = target_split_id or self.split_id

        return Split(
            split_id=derived_split_id,
            stage_id=derived_stage_id,
            data_range=data_range or {},
            parent_split_ids=[self.split_id],
        )


@dataclass
class BackpressureSignal:
    """Signal for backpressure propagation"""

    from_stage: str
    to_stage: str
    slow_down_factor: float  # 0.0 to 1.0, where 0.0 means pause
    reason: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class Record:
    """A single record flowing through the pipeline"""

    key: str = field(default="")
    value: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp,
        }


@dataclass
class RawOutputBytes:
    """Raw bytes to forward to the commit queue via ack_and_forward.

    Returned by sink operators that need to push raw data (e.g., fragment metadata)
    to a commit queue. StageWorker pushes these directly without going through
    payload_store or DataQueueMessage wrapping.
    """

    payloads: List[bytes]
    """Raw byte payloads to push to the output queue."""


@dataclass
class SplitPayload:
    """Arrow-backed payload of records tied to a split.

    The authoritative payload is stored as a :class:`pyarrow.Table` to enable
    zero-copy operations and efficient integration with the Arrow ecosystem.
    """

    data: pa.Table
    split_id: str
    timestamp: float = field(default_factory=time.time)

    NURION_KEY_COLUMN = "__solstice_key"
    NURION_TS_COLUMN = "__solstice_timestamp"

    def __len__(self) -> int:
        return int(self.data.num_rows)

    @property
    def schema(self) -> pa.Schema:
        return self.data.schema

    @property
    def column_names(self) -> List[str]:
        return list(self.data.column_names)

    @property
    def records(self) -> List[Record]:
        """Materialize Python ``Record`` objects from the Arrow payload.

        Accessing this property incurs a copy; callers that can operate on Arrow
        data should prefer :meth:`to_table`, :meth:`column` or other zero-copy APIs.
        """
        warnings.warn(
            "SplitPayload.records materializes Python objects and defeats zero-copy benefits. "
            "Prefer operating on Arrow tables directly.",
            DeprecationWarning,
            stacklevel=2,
        )
        return list(self.to_records())

    def to_table(self) -> pa.Table:
        return self.data

    def to_pylist(self) -> List[Dict[str, Any]]:
        """Return the payload as a list of Python dictionaries."""
        return list(self.data.to_pylist())

    def to_records(self) -> List[Record]:
        rows: List[Record] = []
        key_col_present = self.NURION_KEY_COLUMN in self.data.column_names
        ts_col_present = self.NURION_TS_COLUMN in self.data.column_names
        for row in self.data.to_pylist():
            key = row.pop(self.NURION_KEY_COLUMN, None) if key_col_present else None
            timestamp = row.pop(self.NURION_TS_COLUMN, None) if ts_col_present else self.timestamp
            rows.append(
                Record(
                    key=key or "",
                    value=row,
                    timestamp=timestamp if timestamp is not None else time.time(),
                )
            )
        return rows

    def with_new_data(
        self,
        data: Union[pa.Table, pa.RecordBatch, Sequence[Record]],
        split_id: Optional[str] = None,
    ) -> "SplitPayload":
        """Return a new batch with the provided Arrow payload and optional overrides."""
        if isinstance(data, pa.Table):
            table = data
        elif isinstance(data, pa.RecordBatch):
            table = pa.Table.from_batches([data])
        elif isinstance(data, Sequence):
            if not all(isinstance(item, Record) for item in data):
                raise TypeError("Expected an iterable of Record instances")
            table = pa.Table.from_pylist(self._rows_from_records(data))
        else:
            raise TypeError(
                f"data must be a pyarrow.Table or pyarrow.RecordBatch, got {type(data)}"
            )
        return SplitPayload(
            data=table,
            split_id=split_id or self.split_id,
        )

    def column(self, name: str) -> pa.ChunkedArray:
        return self.data.column(name)

    def is_empty(self) -> bool:
        return len(self) == 0

    @classmethod
    def from_arrow(
        cls,
        data: Union[pa.Table, pa.RecordBatch],
        split_id: str,
    ) -> "SplitPayload":
        """Construct a batch from Arrow data."""
        if isinstance(data, pa.Table):
            table = data
        elif isinstance(data, pa.RecordBatch):
            table = pa.Table.from_batches([data])
        else:
            raise TypeError(
                f"data must be a pyarrow.Table or pyarrow.RecordBatch, got {type(data)}"
            )
        return cls(data=table, split_id=split_id)

    @classmethod
    def from_records(
        cls,
        records: Sequence[Union[Record, Dict[str, Any]]],
        split_id: str,
        schema: Optional[pa.Schema] = None,
    ) -> "SplitPayload":
        """Materialize an Arrow batch from Python ``Record`` objects or dictionaries."""
        rows: List[Dict[str, Any]] = []
        for record in records:
            if isinstance(record, Record):
                rows.append(cls._record_to_row(record))
            else:
                rows.append(dict(record))

        return cls(data=pa.Table.from_pylist(rows, schema=schema), split_id=split_id)

    @classmethod
    def empty(
        cls,
        split_id: str,
        schema: Optional[pa.Schema] = None,
    ) -> "SplitPayload":
        """Create an empty batch with an optional schema."""
        if schema:
            arrays = [pa.array([], type=field.type) for field in schema]
            table = pa.Table.from_arrays(arrays, schema=schema)
        else:
            table = pa.table({})
        return cls(data=table, split_id=split_id)

    @classmethod
    def _record_to_row(cls, record: Record) -> Dict[str, Any]:
        row: Dict[str, Any] = {}
        if isinstance(record.value, dict):
            row.update(record.value)
        else:
            row["value"] = record.value
        row[cls.NURION_KEY_COLUMN] = record.key
        row[cls.NURION_TS_COLUMN] = record.timestamp
        return row

    @classmethod
    def _rows_from_records(cls, records: Sequence[Record]) -> List[Dict[str, Any]]:
        return [cls._record_to_row(record) for record in records]


# =============================================================================
# Failure Handling
# =============================================================================


@dataclass
class FailurePolicy:
    """Worker failure handling policy.

    Based on the "Circuit Breaker with Sliding Window" pattern:
    - Track failures within a time window (not cumulative)
    - Use failure rate relative to worker count
    - Apply exponential backoff for recovery attempts

    Theory:
    - Transient failures (network blips, GC pauses) should be tolerated
    - Sustained failures indicate systemic issues and should fail-fast
    - The sliding window prevents old failures from affecting current decisions
    """

    # Time window for failure rate calculation (seconds)
    # Failures older than this are forgotten
    window_seconds: float = 60.0

    # Maximum allowed failures per worker within the window
    # e.g., 2.0 means each worker can fail twice per minute on average
    max_failures_per_worker: float = 2.0

    # Minimum absolute failures before applying rate limit
    # Prevents failing too early when there are few workers
    min_failures_before_limit: int = 3

    # Base delay between recovery attempts (seconds)
    base_recovery_delay: float = 0.5

    # Maximum delay (exponential backoff cap)
    max_recovery_delay: float = 5.0


class FailureTracker:
    """Tracks worker failures and decides when to give up.

    Uses a sliding window approach to distinguish between:
    - Transient failures: Occasional failures that should be recovered
    - Sustained failures: High failure rate indicating systemic issues
    """

    def __init__(self, policy: FailurePolicy, logger: Any) -> None:
        self.policy = policy
        self.logger = logger
        self._failure_timestamps: List[float] = []
        self._recovery_attempt: int = 0
        self._peak_workers: int = 1  # Track highest worker count seen

    def record_failures(self, count: int, current_worker_count: int) -> None:
        """Record worker failures and prune old entries."""
        now = time.time()

        # Add new failures
        self._failure_timestamps.extend([now] * count)

        # Prune failures outside the window
        cutoff = now - self.policy.window_seconds
        self._failure_timestamps = [t for t in self._failure_timestamps if t > cutoff]

        self.logger.debug(
            f"Recorded {count} failures, {len(self._failure_timestamps)} in window, "
            f"{current_worker_count} workers active"
        )

    def record_success(self) -> None:
        """Record successful completion, reset backoff."""
        self._recovery_attempt = 0

    def should_give_up(self, current_worker_count: int) -> tuple[bool, str]:
        """Decide if we should stop trying to recover.

        Uses the higher of current workers or peak workers seen to avoid
        failing too early when many workers fail simultaneously.

        Returns:
            (should_give_up, reason)
        """
        failure_count = len(self._failure_timestamps)

        # Always allow some minimum failures before applying rate limit
        if failure_count < self.policy.min_failures_before_limit:
            return False, ""

        # Track peak worker count to handle simultaneous failures fairly
        # When all workers fail at once, we should still allow recovery attempts
        self._peak_workers = max(
            self._peak_workers,
            current_worker_count,
            failure_count,  # At least as many workers as failures seen
        )

        # Use peak workers for rate calculation
        effective_workers = max(1, self._peak_workers)
        max_allowed = self.policy.max_failures_per_worker * effective_workers

        if failure_count >= max_allowed:
            rate = failure_count / effective_workers
            return True, (
                f"Failure rate too high: {failure_count} failures / {effective_workers} workers "
                f"= {rate:.1f} per worker (limit: {self.policy.max_failures_per_worker})"
            )

        return False, ""

    def get_recovery_delay(self) -> float:
        """Get delay before next recovery attempt (exponential backoff)."""
        delay = self.policy.base_recovery_delay * (2**self._recovery_attempt)
        delay = float(min(delay, self.policy.max_recovery_delay))
        self._recovery_attempt += 1
        return delay

    def reset(self) -> None:
        """Reset tracker state."""
        self._failure_timestamps.clear()
        self._recovery_attempt = 0


# =============================================================================
# Queue Messages
# =============================================================================


class MessageType:
    """Message types for inter-stage communication."""

    SOURCE = "source"  # Source message: no payload, carries split routing metadata
    DATA = "data"  # Transform output: references a SplitPayload in the store
    EOF = "eof"  # End-of-stream marker - no more messages after this


@dataclass
class SourceQueueMessage:
    """Message pushed by SourceManager into the first-stage queue.

    Carries no payload — the worker will build the payload from storage
    using ``data_range``.  ``payload_key`` is intentionally absent so that
    ``isinstance`` checks replace fragile ``payload_key`` truthiness tests.
    """

    message_id: str
    split_id: str
    data_range: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    message_type: str = MessageType.SOURCE

    def to_bytes(self) -> bytes:
        return json.dumps(
            {
                "message_id": self.message_id,
                "split_id": self.split_id,
                "data_range": self.data_range,
                "metadata": self.metadata,
                "timestamp": self.timestamp,
                "message_type": self.message_type,
            }
        ).encode()

    def is_eof(self) -> bool:
        return False


@dataclass
class DataQueueMessage:
    """Message pushed by StageWorker between stages.

    References a ``SplitPayload`` stored in ``SplitPayloadStore`` via
    ``payload_key``.  Also used for the EOF sentinel (``message_type="eof"``).
    """

    message_id: str
    split_id: str
    payload_key: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    message_type: str = MessageType.DATA

    def to_bytes(self) -> bytes:
        return json.dumps(
            {
                "message_id": self.message_id,
                "split_id": self.split_id,
                "payload_key": self.payload_key,
                "metadata": self.metadata,
                "timestamp": self.timestamp,
                "message_type": self.message_type,
            }
        ).encode()

    def is_eof(self) -> bool:
        """Check if this is an end-of-stream marker."""
        return self.message_type == MessageType.EOF

    @classmethod
    def create_eof(cls) -> "DataQueueMessage":
        """Create an EOF marker message."""
        return cls(
            message_id="eof",
            split_id="",
            payload_key="",
            message_type=MessageType.EOF,
            metadata={},
        )


# Union type for type annotations that accept either message kind.
AnyQueueMessage = Union[SourceQueueMessage, DataQueueMessage]


def queue_message_from_bytes(data: bytes) -> AnyQueueMessage:
    """Deserialize a queue message, dispatching to the correct concrete type.

    Handles three wire formats:
    - New ``SourceQueueMessage`` (``message_type="source"``)
    - New ``DataQueueMessage`` (``message_type="data"`` or ``"eof"``)
    - Legacy format (no ``message_type``): discriminated by empty ``payload_key``
    """
    d = json.loads(data.decode())
    msg_type = d.get("message_type", "")

    if msg_type == MessageType.SOURCE:
        return SourceQueueMessage(
            message_id=d["message_id"],
            split_id=d["split_id"],
            data_range=d.get("data_range", {}),
            metadata=d.get("metadata", {}),
            timestamp=d.get("timestamp", time.time()),
        )

    if msg_type in (MessageType.DATA, MessageType.EOF):
        return DataQueueMessage(
            message_id=d["message_id"],
            split_id=d["split_id"],
            payload_key=d.get("payload_key", ""),
            metadata=d.get("metadata", {}),
            timestamp=d.get("timestamp", time.time()),
            message_type=msg_type,
        )

    # Legacy wire format: no message_type field.
    # Source messages had payload_key="" and stored data_range in metadata.
    meta = dict(d.get("metadata", {}))
    if not d.get("payload_key"):
        data_range = meta.pop("data_range", {})
        return SourceQueueMessage(
            message_id=d["message_id"],
            split_id=d["split_id"],
            data_range=data_range,
            metadata=meta,
            timestamp=d.get("timestamp", time.time()),
        )
    return DataQueueMessage(
        message_id=d["message_id"],
        split_id=d["split_id"],
        payload_key=d["payload_key"],
        metadata=meta,
        timestamp=d.get("timestamp", time.time()),
    )


# =============================================================================
# Stage Status
# =============================================================================


@dataclass
class StageStatus:
    """Status of a stage."""

    stage_id: str
    worker_count: int
    output_queue_size: int  # Real-time progress indicator (records in output queue)
    is_running: bool
    is_finished: bool
    failed: bool = False
    failure_message: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    backpressure_active: bool = False  # Backpressure status


# =============================================================================
# Queue Stats
# =============================================================================


@dataclass(frozen=True)
class QueueStats:
    """WorkQueue stats snapshot for a single queue."""

    pending_count: int = 0
    claimed_count: int = 0
    total_pushed: int = 0
    total_acked: int = 0


# =============================================================================
# Queue Endpoint
# =============================================================================


@dataclass
class QueueEndpoint:
    """Queue connection info that can be serialized to workers."""

    host: str = "localhost"
    port: int = 50051
    storage_url: str = "file:///tmp/workqueue"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "storage_url": self.storage_url,
        }


# =============================================================================
# Utility Functions
# =============================================================================


def make_split_id(job_id: str, stage_id: str, msg_id: str) -> str:
    """Generate a deterministic split ID from the message ID.

    This ID is derived from the upstream message ID, enabling deduplication
    for exactly-once semantics.

    Args:
        job_id: The job identifier
        stage_id: The stage identifier
        msg_id: The upstream message ID

    Returns:
        A deterministic split ID in the format "job:stage:msg_id"
    """
    return f"{job_id}:{stage_id}:{msg_id}"
