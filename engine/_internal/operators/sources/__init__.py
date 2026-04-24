"""Built-in source operators."""

from _internal.operators.sources.anti_join import (
    AntiJoinSourceConfig,
    AntiJoinSplitPlanner,
    AntiJoinSourceOperator,
)
from _internal.operators.sources.file import FileSource, FileSourceConfig
from _internal.core.source import SplitPlanner
from _internal.operators.sources.union import UnionSourceConfig, UnionSplitPlanner
from _internal.utils.optional import optional_dependency_placeholder

# Iceberg source requires optional [iceberg] extra (pyiceberg + sqlalchemy)
try:
    from _internal.operators.sources.iceberg import IcebergSource, IcebergSourceConfig
except ImportError:
    IcebergSource = optional_dependency_placeholder("IcebergSource", "iceberg")  # type: ignore[assignment,misc]
    IcebergSourceConfig = optional_dependency_placeholder("IcebergSourceConfig", "iceberg")  # type: ignore[assignment,misc]

# Lance source requires optional [lance] extra (pylance)
try:
    from _internal.operators.sources.lance import (
        LanceTableSource,
        LanceTableSourceConfig,
        LanceSplitPlanner,
    )
except ImportError:
    LanceTableSource = optional_dependency_placeholder("LanceTableSource", "lance")  # type: ignore[assignment,misc]
    LanceTableSourceConfig = optional_dependency_placeholder("LanceTableSourceConfig", "lance")  # type: ignore[assignment,misc]
    LanceSplitPlanner = optional_dependency_placeholder("LanceSplitPlanner", "lance")  # type: ignore[assignment,misc]

# Spark sources require optional [spark] extra (pyspark + nurion-raydp-spark4)
try:
    from _internal.operators.sources.spark import (
        SparkSource,
        SparkSourceConfig,
        SparkSplitPlanner,
    )
    from _internal.operators.sources.sparkv2 import (
        SparkSourceV2Config,
        SparkDirectProducer,
    )
except ImportError:
    SparkSource = optional_dependency_placeholder("SparkSource", "spark")  # type: ignore[assignment,misc]
    SparkSourceConfig = optional_dependency_placeholder("SparkSourceConfig", "spark")  # type: ignore[assignment,misc]
    SparkSplitPlanner = optional_dependency_placeholder("SparkSplitPlanner", "spark")  # type: ignore[assignment,misc]
    SparkSourceV2Config = optional_dependency_placeholder("SparkSourceV2Config", "spark")  # type: ignore[assignment,misc]
    SparkDirectProducer = optional_dependency_placeholder("SparkDirectProducer", "spark")  # type: ignore[assignment,misc]

__all__ = [
    # Anti-join source
    "AntiJoinSourceConfig",
    "AntiJoinSplitPlanner",
    "AntiJoinSourceOperator",
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
    # Union source
    "UnionSourceConfig",
    "UnionSplitPlanner",
]
