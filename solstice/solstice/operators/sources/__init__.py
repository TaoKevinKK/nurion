"""Built-in source operators."""

from solstice.operators.sources.file import FileSource, FileSourceConfig
from solstice.operators.sources.iceberg import IcebergSource, IcebergSourceConfig
from solstice.operators.sources.lance import (
    LanceTableSource,
    LanceTableSourceConfig,
    LanceSplitPlanner,
)
from solstice.core.source import SplitPlanner
from solstice.operators.sources.spark import (
    SparkSource,
    SparkSourceConfig,
    SparkSplitPlanner,
)
from solstice.operators.sources.sparkv2 import (
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
