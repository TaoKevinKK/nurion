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

"""State message definitions for push-based metrics.

Architecture:
- WORKER_STATE: Real-time worker lifecycle and status (immediate produce/consume)
- SPLIT_METRICS_BATCH: Atomic per-split processing metrics (batch produce/consume)

Key design principles:
1. Split metrics are atomic - no aggregation, just raw data
2. Split metrics bind to partition (strong), worker (weak)
3. Worker state is real-time for lifecycle management
4. All rate calculations done at query time (Prometheus-style)
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class StateMessageType(str, Enum):
    """Types of state messages."""

    # Job lifecycle
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"

    # Stage lifecycle
    STAGE_STARTED = "stage_started"
    STAGE_COMPLETED = "stage_completed"

    # Worker lifecycle
    WORKER_STARTED = "worker_started"
    WORKER_STOPPED = "worker_stopped"

    # Worker state (immediate) - lightweight status update
    WORKER_STATE = "worker_state"

    # Split metrics (batch) - atomic per-split data
    SPLIT_METRICS_BATCH = "split_metrics_batch"

    # Events
    EXCEPTION = "exception"
    BACKPRESSURE = "backpressure"


@dataclass
class StateMessage:
    """Unified message for job state and metrics."""

    message_type: StateMessageType
    job_id: str
    source_id: str
    timestamp: float = field(default_factory=time.time)
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_bytes(self) -> bytes:
        """Serialize to bytes for Tansu produce."""
        return json.dumps(
            {
                "message_type": self.message_type.value,
                "job_id": self.job_id,
                "source_id": self.source_id,
                "timestamp": self.timestamp,
                "payload": self.payload,
            }
        ).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> StateMessage:
        """Deserialize from Tansu consume."""
        d = json.loads(data.decode("utf-8"))
        return cls(
            message_type=StateMessageType(d["message_type"]),
            job_id=d["job_id"],
            source_id=d["source_id"],
            timestamp=d["timestamp"],
            payload=d.get("payload", {}),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "message_type": self.message_type.value,
            "job_id": self.job_id,
            "source_id": self.source_id,
            "timestamp": self.timestamp,
            "payload": self.payload,
        }


# =============================================================================
# Split Metrics - Atomic per-split data (batch produce/consume)
# =============================================================================


@dataclass
class SplitMetric:
    """Atomic metrics for a single split.

    Labels (dimensions):
        - stage_id: Which stage processed this
        - partition_id: Which partition this split came from (strong binding)
        - offset: Message offset in the partition
        - worker_id: Which worker processed (weak binding, for debugging)

    Metrics:
        - process_time_ms: Time to process this split
        - input_records: Records in input
        - output_records: Records in output
    """

    stage_id: str
    partition_id: int
    offset: int
    worker_id: str
    process_time_ms: float
    input_records: int = 0
    output_records: int = 0
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage_id": self.stage_id,
            "partition_id": self.partition_id,
            "offset": self.offset,
            "worker_id": self.worker_id,
            "process_time_ms": self.process_time_ms,
            "input_records": self.input_records,
            "output_records": self.output_records,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SplitMetric:
        return cls(
            stage_id=d["stage_id"],
            partition_id=d["partition_id"],
            offset=d["offset"],
            worker_id=d["worker_id"],
            process_time_ms=d["process_time_ms"],
            input_records=d.get("input_records", 0),
            output_records=d.get("output_records", 0),
            timestamp=d.get("timestamp", time.time()),
        )


def split_metrics_batch_message(
    job_id: str,
    stage_id: str,
    worker_id: str,
    metrics: List[SplitMetric],
) -> StateMessage:
    """Create a SPLIT_METRICS_BATCH message."""
    return StateMessage(
        message_type=StateMessageType.SPLIT_METRICS_BATCH,
        job_id=job_id,
        source_id=worker_id,
        payload={
            "stage_id": stage_id,
            "metrics": [m.to_dict() for m in metrics],
        },
    )


# =============================================================================
# Worker State - Real-time status (immediate produce/consume)
# =============================================================================


def worker_state_message(
    job_id: str,
    stage_id: str,
    worker_id: str,
    status: str,  # "RUNNING", "IDLE", "STOPPED"
    assigned_partitions: List[int],
    partition_offsets: Optional[Dict[int, int]] = None,
) -> StateMessage:
    """Create a WORKER_STATE message."""
    return StateMessage(
        message_type=StateMessageType.WORKER_STATE,
        job_id=job_id,
        source_id=worker_id,
        payload={
            "stage_id": stage_id,
            "status": status,
            "assigned_partitions": assigned_partitions,
            "partition_offsets": partition_offsets or {},
        },
    )


# =============================================================================
# Job/Stage Lifecycle Messages
# =============================================================================


def job_started_message(
    job_id: str,
    dag_edges: Dict[str, list],
    stages: list,
    config: Optional[Dict[str, Any]] = None,
) -> StateMessage:
    """Create a JOB_STARTED message."""
    return StateMessage(
        message_type=StateMessageType.JOB_STARTED,
        job_id=job_id,
        source_id=job_id,
        payload={
            "dag_edges": dag_edges,
            "stages": stages,
            "config": config or {},
        },
    )


def job_completed_message(
    job_id: str,
    status: str = "COMPLETED",
    duration_ms: Optional[int] = None,
) -> StateMessage:
    """Create a JOB_COMPLETED or JOB_FAILED message."""
    msg_type = (
        StateMessageType.JOB_COMPLETED if status == "COMPLETED" else StateMessageType.JOB_FAILED
    )
    return StateMessage(
        message_type=msg_type,
        job_id=job_id,
        source_id=job_id,
        payload={
            "status": status,
            "duration_ms": duration_ms,
        },
    )


def stage_started_message(
    job_id: str,
    stage_id: str,
    operator_type: str,
    min_parallelism: int,
    max_parallelism: int,
) -> StateMessage:
    """Create a STAGE_STARTED message."""
    return StateMessage(
        message_type=StateMessageType.STAGE_STARTED,
        job_id=job_id,
        source_id=stage_id,
        payload={
            "operator_type": operator_type,
            "min_parallelism": min_parallelism,
            "max_parallelism": max_parallelism,
        },
    )


def stage_completed_message(
    job_id: str,
    stage_id: str,
) -> StateMessage:
    """Create a STAGE_COMPLETED message."""
    return StateMessage(
        message_type=StateMessageType.STAGE_COMPLETED,
        job_id=job_id,
        source_id=stage_id,
        payload={},
    )


# =============================================================================
# Worker Lifecycle Messages
# =============================================================================


def worker_started_message(
    job_id: str,
    stage_id: str,
    worker_id: str,
    assigned_partitions: Optional[list] = None,
) -> StateMessage:
    """Create a WORKER_STARTED message."""
    return StateMessage(
        message_type=StateMessageType.WORKER_STARTED,
        job_id=job_id,
        source_id=worker_id,
        payload={
            "stage_id": stage_id,
            "assigned_partitions": assigned_partitions or [],
        },
    )


def worker_stopped_message(
    job_id: str,
    stage_id: str,
    worker_id: str,
    reason: str = "completed",
) -> StateMessage:
    """Create a WORKER_STOPPED message."""
    return StateMessage(
        message_type=StateMessageType.WORKER_STOPPED,
        job_id=job_id,
        source_id=worker_id,
        payload={
            "stage_id": stage_id,
            "reason": reason,
        },
    )


# =============================================================================
# Event Messages
# =============================================================================


def exception_message(
    job_id: str,
    stage_id: str,
    worker_id: Optional[str],
    exception_type: str,
    message: str,
    stacktrace: str,
    split_id: Optional[str] = None,
) -> StateMessage:
    """Create an EXCEPTION message."""
    return StateMessage(
        message_type=StateMessageType.EXCEPTION,
        job_id=job_id,
        source_id=worker_id or stage_id,
        payload={
            "stage_id": stage_id,
            "worker_id": worker_id,
            "exception_type": exception_type,
            "message": message,
            "stacktrace": stacktrace,
            "split_id": split_id,
        },
    )


def backpressure_message(
    job_id: str,
    stage_id: str,
    active: bool,
    queue_lag: int = 0,
) -> StateMessage:
    """Create a BACKPRESSURE message."""
    return StateMessage(
        message_type=StateMessageType.BACKPRESSURE,
        job_id=job_id,
        source_id=stage_id,
        payload={
            "active": active,
            "queue_lag": queue_lag,
        },
    )
