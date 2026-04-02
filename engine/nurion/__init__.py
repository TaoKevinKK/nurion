"""Nurion public API.

This module re-exports engine symbols from the internal implementation so
users can depend on a single "nurion" name.
"""

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

from _internal import __version__ as __version__
from _internal.config import EngineConfig, configure, get_config
from _internal.core.job import Job, JobConfig, WebUIConfig
from _internal.core.models import Split, SplitPayload
from _internal.core.operator import Operator, OperatorConfig, OperatorRuntime, operator
from _internal.core.source_operator import SourceOperator
from _internal.core.stage import Stage
from _internal.operators.filter import FilterOperatorConfig
from _internal.operators.map import (
    FlatMapOperatorConfig,
    MapBatchesOperatorConfig,
    MapOperatorConfig,
)
from _internal.operators.sinks import (
    FileSinkConfig,
    LanceCommitPolicy,
    LanceSinkCommitter,
    LanceSinkConfig,
    PrintSinkConfig,
)
from _internal.operators.sources import (
    AntiJoinSourceConfig,
    FileSourceConfig,
    IcebergSourceConfig,
    LanceTableSourceConfig,
    SparkSourceConfig,
    SparkSourceV2Config,
    UnionSourceConfig,
)

# SparkSourceConfig / SparkSourceV2Config are None when [spark] extra is not installed
from _internal.operators.llm import (
    EmbeddedLLMOperator,
    EmbeddedLLMOperatorConfig,
    ExternalLLMOperator,
    ExternalLLMOperatorConfig,
)
from _internal.core.nvme_payload_store import NvmeSplitPayloadStore, WritePolicy
from _internal.serve import ModelConfig, ModelServiceManager, create_manager
from _internal.serve.client import ModelClient
from _internal.utils.arrow_tensor import arrow_to_ndarray_list, ndarray_list_to_arrow

__all__ = [
    "__version__",
    "EngineConfig",
    "configure",
    "get_config",
    "Job",
    "JobConfig",
    "WebUIConfig",
    "Stage",
    "SourceOperator",
    "Operator",
    "OperatorConfig",
    "OperatorRuntime",
    "operator",
    "Split",
    "SplitPayload",
    "AntiJoinSourceConfig",
    "FileSourceConfig",
    "IcebergSourceConfig",
    "LanceTableSourceConfig",
    "SparkSourceConfig",
    "SparkSourceV2Config",
    "UnionSourceConfig",
    "FileSinkConfig",
    "LanceCommitPolicy",
    "LanceSinkCommitter",
    "LanceSinkConfig",
    "PrintSinkConfig",
    "MapOperatorConfig",
    "MapBatchesOperatorConfig",
    "FlatMapOperatorConfig",
    "FilterOperatorConfig",
    "EmbeddedLLMOperator",
    "EmbeddedLLMOperatorConfig",
    "ExternalLLMOperator",
    "ExternalLLMOperatorConfig",
    "ModelConfig",
    "ModelServiceManager",
    "create_manager",
    "ModelClient",
    "NvmeSplitPayloadStore",
    "WritePolicy",
    "ndarray_list_to_arrow",
    "arrow_to_ndarray_list",
]
