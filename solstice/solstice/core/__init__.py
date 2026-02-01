"""Core components of the streaming framework"""

from solstice.core.job import Job, JobConfig
from solstice.core.operator import (
    Operator,
    OperatorConfig,
    OperatorRuntime,
    operator,
    master_callable,
    is_master_callable,
)
from solstice.core.source_operator import SourceOperator
from solstice.core.sink_operator import SinkOperator
from solstice.core.stage import Stage, StageRuntime
from solstice.core.models import (
    FailurePolicy,
    FailureTracker,
    QueueEndpoint,
    QueueMessage,
    StageStatus,
    MessageType,
    make_split_id,
)
from solstice.core.stage_master import StageMaster
from solstice.core.stage_worker import StageWorker, WorkerRuntime

__all__ = [
    # Job
    "Job",
    "JobConfig",
    # Stage
    "Stage",
    "StageRuntime",
    "StageMaster",
    "StageWorker",
    "WorkerRuntime",
    # Operator
    "Operator",
    "OperatorConfig",
    "OperatorRuntime",
    "SourceOperator",
    "SinkOperator",
    "operator",
    "master_callable",
    "is_master_callable",
    # Queue
    "QueueEndpoint",
    "QueueMessage",
    "MessageType",
    "make_split_id",
    # Status
    "StageStatus",
    # Failure handling
    "FailurePolicy",
    "FailureTracker",
]
