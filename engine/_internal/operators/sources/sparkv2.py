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

"""Spark Source V2: Direct Queue Integration.

This module provides SparkSourceV2, an optimized Spark source that has JVM-side
executors write directly to Ray Object Store and output_queue.

Key improvements over V1:
- Eliminates Python-side plan_splits() iteration
- Eliminates source_queue and operator read step
- JVM writes directly to output_queue with managed ObjectRef lifetime
- Single serialization path (Spark -> Arrow -> Object Store -> output_queue)

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                    SparkDirectProducer                        │
    │  (Python - control plane, via StageMaster)                   │
    │                                                              │
    │  1. Create output_queue (StageMaster handles this)           │
    │  2. Call JVM with (storeActorName, queueEndpoint)           │
    │  3. Wait for JVM to complete                                 │
    │  4. Return count, StageMaster marks complete                 │
    └──────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                    JVM (Spark Executor)                      │
    │                                                              │
    │  1. Ray.put(arrowBytes, owner=storeActor)  <- managed       │
    │  2. Produce to output_queue                <- direct write   │
    │     payload_key = "_v2ref:{object_id_b64}"                   │
    └─────────────────────────────────────────────────────────────┘
                               │
                               ▼
    ┌─────────────────────────────────────────────────────────────┐
    │               Downstream Stage Workers                       │
    │                                                              │
    │  1. Consume from output_queue                                │
    │  2. payload_store.get(payload_key)                          │
    │     -> detects _v2ref: prefix                                │
    │     -> reconstructs ObjectRef from ID                        │
    │     -> ray.get() -> auto-convert Arrow to SplitPayload       │
    └─────────────────────────────────────────────────────────────┘

Usage:
    from _internal.operators.sources.sparkv2 import SparkSourceV2Config

    config = SparkSourceV2Config(
        dataframe_fn=lambda spark: spark.read.parquet("/data"),
        num_executors=2,
    )
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

from _internal.core.operator import OperatorConfig
from _internal.core.source import DirectProduceContext

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame


# Type alias for the DataFrame factory function
DataFrameFactory = Callable[["SparkSession"], "DataFrame"]


@dataclass
class SparkSourceV2Config(OperatorConfig):
    """Configuration for Spark Source V2.

    V2 bypasses source_queue and operator by having JVM write directly
    to output_queue with ObjectRef ID embedded in payload_key.

    Attributes:
        app_name: Spark application name
        num_executors: Number of Spark executors
        executor_cores: Number of cores per executor
        executor_memory: Memory per executor (e.g., "1g", "2g")
        spark_configs: Additional Spark configurations
        dataframe_fn: Function that takes SparkSession and returns DataFrame
        parallelism: Number of partitions for the output data
    """

    # Spark configuration
    app_name: str = "nurion-spark-v2"
    num_executors: int = 1
    executor_cores: int = 2
    executor_memory: str = "1g"
    spark_configs: Dict[str, str] = field(default_factory=dict)

    # DataFrame factory function: (SparkSession) -> DataFrame
    dataframe_fn: Optional[DataFrameFactory] = None

    # Output configuration
    parallelism: Optional[int] = None

    def create_source(self) -> "SparkDirectProducer":
        """Create a direct producer for this Spark V2 source."""
        return SparkDirectProducer(self)


class SparkDirectProducer:
    """Produces data directly via Spark JVM to the output queue.

    Implements the DirectProducer protocol. JVM-side executors write
    Arrow data directly to the output queue, bypassing workers entirely.
    """

    def __init__(self, config: SparkSourceV2Config):
        self._config = config
        self._spark: Any = None
        self._spark_initialized = False
        self._logger = logging.getLogger("SparkDirectProducer")

    async def produce(self, ctx: DirectProduceContext) -> int:
        """Execute Spark write via JVM.

        JVM writes directly to output_queue:
        1. Ray.put(arrowBytes, owner=storeActor) - managed lifetime
        2. Produce to output_queue with payload_key = "_v2ref:{id}"

        Returns:
            Number of splits written
        """
        import raydp  # type: ignore[import-not-found]

        # Initialize Spark
        spark_configs = {
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            **self._config.spark_configs,
        }

        self._spark = raydp.init_spark(
            app_name=self._config.app_name,
            num_executors=self._config.num_executors,
            executor_cores=self._config.executor_cores,
            executor_memory=self._config.executor_memory,
            configs=spark_configs,
        )
        self._spark_initialized = True
        self._logger.info(f"Initialized Spark session: {self._config.app_name}")

        # Get DataFrame
        if self._config.dataframe_fn is None:
            raise ValueError(
                "dataframe_fn must be provided in SparkSourceV2Config. "
                "Example: dataframe_fn=lambda spark: spark.read.json('/path/to/data')"
            )

        df = self._config.dataframe_fn(self._spark)

        # Repartition if parallelism is specified
        if self._config.parallelism is not None:
            num_partitions = df.rdd.getNumPartitions()
            if num_partitions != self._config.parallelism:
                df = df.repartition(self._config.parallelism)

        # Output queue connection info
        queue_bootstrap = f"{ctx.broker_endpoint.host}:{ctx.broker_endpoint.port}"
        queue_topic = ctx.output_queue_name

        self._logger.info(f"JVM writing directly to output_queue: {queue_bootstrap}/{queue_topic}")

        # Call JVM method to write Arrow data directly to output_queue
        jvm: Any = df.sql_ctx.sparkSession.sparkContext._jvm
        writer = jvm.org.apache.spark.sql.raydp.ObjectStoreWriter(df._jdf)

        count = writer.saveToStoreAndQueue(
            False,  # useBatch
            queue_bootstrap,
            queue_topic,
            ctx.stage_id,
        )

        self._logger.info(f"JVM write completed: {count} splits to output_queue")
        return count

    def cleanup(self) -> None:
        """Stop Spark session."""
        if self._spark_initialized:
            import raydp  # type: ignore[import-not-found]

            raydp.stop_spark()
            self._spark = None
            self._spark_initialized = False
            self._logger.info("Stopped Spark session")
