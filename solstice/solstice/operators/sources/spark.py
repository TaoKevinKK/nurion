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

"""Spark source operator and split planner for reading data via raydp."""

from __future__ import annotations

import base64
import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterator, Optional, TYPE_CHECKING

import pyarrow as pa
import ray

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import OperatorConfig, OperatorRuntime, operator
from solstice.core.source_operator import SourceOperator

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame


# Type alias for the DataFrame factory function
DataFrameFactory = Callable[["SparkSession"], "DataFrame"]


@dataclass
class SparkSourceConfig(OperatorConfig):
    """Unified configuration for Spark source (both operator and planner).

    Contains raydp init_spark parameters and a DataFrame factory function.
    The operator reads Arrow data from Ray object store (ObjectRefs),
    while the planner uses the Spark config to initialize Spark and create splits.

    Attributes:
        app_name: Spark application name
        num_executors: Number of Spark executors
        executor_cores: Number of cores per executor
        executor_memory: Memory per executor (e.g., "1g", "2g")
        spark_configs: Additional Spark configurations
        dataframe_fn: Function that takes SparkSession and returns DataFrame.
                      This is the main way to define your data source.
        parallelism: Number of partitions for the output data

    Example:
        >>> config = SparkSourceConfig(
        ...     app_name="my-app",
        ...     num_executors=2,
        ...     dataframe_fn=lambda spark: spark.read.json("/data/events.json"),
        ... )
    """

    # raydp init_spark parameters
    app_name: str = "solstice-spark-source"
    num_executors: int = 1
    executor_cores: int = 2
    executor_memory: str = "1g"
    spark_configs: Dict[str, str] = field(default_factory=dict)

    # DataFrame factory function: (SparkSession) -> DataFrame
    dataframe_fn: Optional[DataFrameFactory] = None

    # Output configuration
    parallelism: Optional[int] = None

    # SourceConfig fields for master
    workqueue_db_path: str = "memory://"
    """WorkQueue storage path (memory://, file://)."""

    def create_source(self) -> "SparkSplitPlanner":
        """Create a split planner for this Spark source."""
        return SparkSplitPlanner(self)


@operator(SparkSourceConfig)
class SparkSource(SourceOperator):
    """Source operator for reading Arrow data from Ray object store.

    This operator reads Arrow data from ObjectRefs that were persisted
    by SparkSplitPlanner using raydp.
    """

    def __init__(self, config: SparkSourceConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)

    def read(self, split: Split) -> Optional[SplitPayload]:
        """Read Arrow data from Ray object store.

        The split contains:
            - object_ref: Base64-encoded cloudpickle of ObjectRef
            - block_size: Number of records in this block
        """
        object_ref = split.data_range.get("object_ref")
        if object_ref is None:
            raise ValueError("Split missing 'object_ref' for SparkSource")

        # Handle both raw ObjectRef (tests) and serialized string (production)
        if isinstance(object_ref, str):
            object_ref = ray.cloudpickle.loads(base64.b64decode(object_ref))

        # Get Arrow data from object store
        arrow_data = ray.get(object_ref)

        if arrow_data is None:
            return None

        # Handle different data types from object store
        if isinstance(arrow_data, pa.Table):
            arrow_table = arrow_data
        elif isinstance(arrow_data, pa.RecordBatch):
            arrow_table = pa.Table.from_batches([arrow_data])
        elif isinstance(arrow_data, bytes):
            # Arrow IPC format (from raydp) - deserialize using IPC reader
            import io

            import pyarrow.ipc as ipc

            reader = ipc.open_stream(io.BytesIO(arrow_data))
            arrow_table = reader.read_all()
        else:
            raise ValueError(f"Unsupported data type from object store: {type(arrow_data)}")

        if arrow_table.num_rows == 0:
            return None

        return SplitPayload.from_arrow(
            arrow_table,
            split_id=split.split_id,
        )

    def close(self) -> None:
        """Clean up resources."""
        pass


class SparkSplitPlanner:
    """Plans splits by initializing Spark, loading data, and persisting to object store.

    Implements the SplitPlanner protocol. Created by SparkSourceConfig.create_source().

    Uses raydp to efficiently transfer Spark data to Ray object store as Arrow blocks,
    then yields splits containing serialized ObjectRefs.
    """

    def __init__(self, config: SparkSourceConfig):
        self._config = config
        self._spark = None
        self._spark_initialized = False
        self._logger = logging.getLogger("SparkSplitPlanner")

    def plan_splits(self, stage_id: str) -> Iterator[Split]:
        """Initialize Spark, load data, persist to object store, and yield splits."""
        from raydp.spark.dataset import (  # type: ignore[import-not-found]
            _save_spark_df_to_object_store,
            get_raydp_master_owner,
        )

        # Initialize Spark
        self._init_spark()

        # Get DataFrame
        df = self._get_dataframe()

        # Repartition if parallelism is specified
        if self._config.parallelism is not None:
            num_partitions = df.rdd.getNumPartitions()
            if num_partitions != self._config.parallelism:
                df = df.repartition(self._config.parallelism)

        # Get the owner for object lifetime management
        owner = get_raydp_master_owner(self._spark)

        # Save DataFrame to object store
        blocks, block_sizes = _save_spark_df_to_object_store(
            df,
            use_batch=False,
            owner=owner,
        )

        self._logger.info(
            f"Persisted Spark DataFrame to object store: "
            f"{len(blocks)} blocks, {sum(block_sizes)} total records"
        )

        # Yield splits containing ObjectRef serialized via cloudpickle
        for idx, (block_ref, block_size) in enumerate(zip(blocks, block_sizes)):
            object_ref_b64 = base64.b64encode(ray.cloudpickle.dumps(block_ref)).decode("ascii")
            yield Split(
                split_id=f"split_{idx}",
                stage_id=stage_id,
                data_range={
                    "object_ref": object_ref_b64,
                    "block_size": block_size,
                    "block_index": idx,
                },
            )

        # NOTE: Do NOT stop Spark here. The object refs in splits are owned by
        # the raydp master; stopping Spark kills the owner and invalidates the
        # refs. Cleanup happens via cleanup() after workers finish.

    def _init_spark(self) -> None:
        """Initialize Spark session via raydp."""
        if self._spark_initialized:
            return

        import raydp  # type: ignore[import-not-found]

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

    def _get_dataframe(self):
        """Get DataFrame by calling the dataframe_fn with SparkSession."""
        if self._config.dataframe_fn is None:
            raise ValueError(
                "dataframe_fn must be provided in SparkSourceConfig. "
                "Example: dataframe_fn=lambda spark: spark.read.json('/path/to/data')"
            )

        self._logger.info("Calling dataframe_fn to load data")
        return self._config.dataframe_fn(self._spark)

    def cleanup(self) -> None:
        """Clean up resources (stop Spark session).

        Called by SourceManager when the stage finishes, after all workers
        have read the object refs from the store.
        """
        self._stop_spark()

    def _stop_spark(self) -> None:
        """Stop Spark session."""
        if self._spark_initialized:
            import raydp  # type: ignore[import-not-found]

            raydp.stop_spark()
            self._spark = None
            self._spark_initialized = False
            self._logger.info("Stopped Spark session")
