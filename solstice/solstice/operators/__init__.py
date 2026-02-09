"""Built-in operators"""

from solstice.operators.sources import (
    FileSource,
    FileSourceConfig,
    IcebergSource,
    IcebergSourceConfig,
    LanceTableSource,
    LanceTableSourceConfig,
)
from solstice.operators.map import (
    MapOperator,
    MapOperatorConfig,
    FlatMapOperator,
    FlatMapOperatorConfig,
    MapBatchesOperator,
    MapBatchesOperatorConfig,
)
from solstice.operators.filter import FilterOperator, FilterOperatorConfig
from solstice.operators.sinks import (
    FileSink,
    FileSinkConfig,
    LanceSink,
    LanceSinkConfig,
    PrintSink,
    PrintSinkConfig,
)
from solstice.operators.video import (
    FFmpegSceneDetectOperator,
    FFmpegSceneDetectConfig,
    FFmpegSliceOperator,
    FFmpegSliceConfig,
)
from solstice.operators.shuffle import (
    ShuffleOperator,
    ShuffleOperatorConfig,
    RepartitionOperator,
    RepartitionConfig,
    split_by_partition,
    is_shuffle_operator,
)
from solstice.operators.dedupe import (
    HashDedupeOperator,
    HashDedupeConfig,
)

# New dedup operators (Union-Find Service architecture)
from solstice.operators.dedup import (
    MinHashEncoderConfig,
    BucketUnionOperatorConfig,
    DedupFilterOperatorConfig,
)

# HTTP operators
from solstice.operators.http import (
    HttpOperator,
    HttpOperatorConfig,
    CircuitBreaker,
    CircuitBreakerConfig,
    GlobalRateLimiter,
)

# LLM operators
from solstice.operators.llm import (
    EmbeddedLLMOperator,
    EmbeddedLLMOperatorConfig,
    ExternalLLMOperator,
    ExternalLLMOperatorConfig,
)

__all__ = [
    # Source operators and configs
    "LanceTableSource",
    "LanceTableSourceConfig",
    "IcebergSource",
    "IcebergSourceConfig",
    "FileSource",
    "FileSourceConfig",
    # Map operators and configs
    "MapOperator",
    "MapOperatorConfig",
    "FlatMapOperator",
    "FlatMapOperatorConfig",
    "MapBatchesOperator",
    "MapBatchesOperatorConfig",
    # Filter operator and config
    "FilterOperator",
    "FilterOperatorConfig",
    # Sink operators and configs
    "FileSink",
    "FileSinkConfig",
    "LanceSink",
    "LanceSinkConfig",
    "PrintSink",
    "PrintSinkConfig",
    # Video operators and configs
    "FFmpegSceneDetectOperator",
    "FFmpegSceneDetectConfig",
    "FFmpegSliceOperator",
    "FFmpegSliceConfig",
    # Shuffle operators and configs
    "ShuffleOperator",
    "ShuffleOperatorConfig",
    "RepartitionOperator",
    "RepartitionConfig",
    "split_by_partition",
    "is_shuffle_operator",
    # Dedupe operators and configs
    "HashDedupeOperator",
    "HashDedupeConfig",
    # Dedup operators (Union-Find Service architecture)
    "MinHashEncoderConfig",
    "BucketUnionOperatorConfig",
    "DedupFilterOperatorConfig",
    # HTTP operators
    "HttpOperator",
    "HttpOperatorConfig",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "GlobalRateLimiter",
    # LLM operators (embedded mode - recommended for batch)
    "EmbeddedLLMOperator",
    "EmbeddedLLMOperatorConfig",
    # LLM operators (external mode - for external services)
    "ExternalLLMOperator",
    "ExternalLLMOperatorConfig",
]
