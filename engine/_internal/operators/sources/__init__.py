"""Built-in source operators."""

from _internal.operators.sources.file import FileSource, FileSourceConfig
from _internal.operators.sources.iceberg import IcebergSource, IcebergSourceConfig
from _internal.operators.sources.lance import (
    LanceTableSource,
    LanceTableSourceConfig,
    LanceSplitPlanner,
)
from _internal.core.source import SplitPlanner
from _internal.operators.sources.spark import (
    SparkSource,
    SparkSourceConfig,
    SparkSplitPlanner,
)
from _internal.operators.sources.sparkv2 import (
    SparkSourceV2Config,
    SparkDirectProducer,
)

__all__ = [
    # File source
    "FileSource",
    "FileSourceConfig",
    # Iceberg source
    "IcebergSource",
    "IcebergSourceConfig",
    # Lance source
    "LanceTableSource",
    "LanceTableSourceConfig",
    "LanceSplitPlanner",
    # Source protocol
    "SplitPlanner",
    # Spark source V1
    "SparkSource",
    "SparkSourceConfig",
    "SparkSplitPlanner",
    # Spark source V2 (DirectProducer - no operator needed)
    "SparkSourceV2Config",
    "SparkDirectProducer",
]
