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

"""Tests for SparkSource operator using raydp."""

from __future__ import annotations

from pathlib import Path

import pytest
import pyarrow as pa
import ray

from tests.conftest import make_operator_runtime
from _internal.core.models import Split
from _internal.core.stage import Stage
from _internal.operators.filter import FilterOperatorConfig
from _internal.operators.map import MapOperatorConfig
from _internal.operators.sources.spark import (
    SparkSourceConfig,
    SparkSplitPlanner,
)


# Test data path
TESTDATA_DIR = Path(__file__).parent / "testdata" / "resources" / "spark"
TEST_DATA_1000 = TESTDATA_DIR / "test_data_1000.parquet"
TEST_DATA_100 = TESTDATA_DIR / "test_data_100.parquet"


class TestSparkSourceOperator:
    """Tests for SparkSource operator reading from ObjectRefs."""

    def test_spark_source_read_arrow_table(self, ray_cluster):
        """Test reading Arrow table from object store."""
        # Create test data and put in object store
        test_data = pa.Table.from_pylist(
            [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
                {"id": 3, "name": "Charlie", "age": 35},
            ]
        )

        object_ref = ray.put(test_data)

        # Create source and read
        config = SparkSourceConfig()
        source = config.setup(make_operator_runtime())

        split = Split(
            split_id="test_split_0",
            stage_id="spark_source",
            data_range={
                "object_ref": object_ref,
                "block_size": 3,
            },
        )

        payload = source.read(split)

        assert payload is not None
        assert len(payload) == 3
        assert "id" in payload.column_names
        assert "name" in payload.column_names
        assert "age" in payload.column_names

        records = payload.to_pylist()
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"
        assert records[2]["name"] == "Charlie"

    def test_spark_source_read_record_batch(self, ray_cluster):
        """Test reading Arrow RecordBatch from object store."""
        # Create test data as RecordBatch
        test_batch = pa.RecordBatch.from_pydict(
            {
                "value": [1, 2, 3, 4, 5],
                "label": ["a", "b", "c", "d", "e"],
            }
        )

        object_ref = ray.put(test_batch)

        config = SparkSourceConfig()
        source = config.setup(make_operator_runtime())

        split = Split(
            split_id="test_split_batch",
            stage_id="spark_source",
            data_range={
                "object_ref": object_ref,
                "block_size": 5,
            },
        )

        payload = source.read(split)

        assert payload is not None
        assert len(payload) == 5
        assert "value" in payload.column_names
        assert "label" in payload.column_names

    def test_spark_source_empty_table(self, ray_cluster):
        """Test reading empty Arrow table returns None."""
        empty_table = pa.Table.from_pylist([])

        object_ref = ray.put(empty_table)

        config = SparkSourceConfig()
        source = config.setup(make_operator_runtime())

        split = Split(
            split_id="test_split_empty",
            stage_id="spark_source",
            data_range={
                "object_ref": object_ref,
                "block_size": 0,
            },
        )

        payload = source.read(split)
        assert payload is None

    def test_spark_source_missing_object_ref(self):
        """Test error when object_ref is missing."""
        config = SparkSourceConfig()
        source = config.setup(make_operator_runtime())

        split = Split(
            split_id="test_split_no_ref",
            stage_id="spark_source",
            data_range={},  # Missing object_ref
        )

        with pytest.raises(ValueError, match="missing 'object_ref'"):
            source.read(split)


class TestSparkSourcePipeline:
    """Test SparkSource with operators directly (without LocalJobRunner)."""

    def test_spark_source_to_filter(self, ray_cluster):
        """Test reading from ObjectRefs and filtering."""
        # Create test data
        test_data = pa.Table.from_pylist(
            [
                {"id": i, "department": "engineering" if i % 3 == 0 else "sales", "score": i * 10}
                for i in range(100)
            ]
        )
        object_ref = ray.put(test_data)

        # Create source operator and read
        source_config = SparkSourceConfig()
        source = source_config.setup(make_operator_runtime())

        split = Split(
            split_id="split_0",
            stage_id="spark_source",
            data_range={
                "object_ref": object_ref,
                "block_size": 100,
            },
        )

        payload = source.read(split)
        assert payload is not None
        assert len(payload) == 100

        # Apply filter operator
        filter_config = FilterOperatorConfig(
            filter_fn=lambda row: row.get("department") == "engineering",
        )
        filter_op = filter_config.setup(make_operator_runtime())

        filtered = filter_op.process_split(split, payload)
        assert filtered is not None

        # Verify filter results
        total_engineering = len(filtered)
        expected_count = len([i for i in range(100) if i % 3 == 0])
        assert total_engineering == expected_count

        for record in filtered.to_pylist():
            assert record["department"] == "engineering"

    def test_spark_source_to_map(self, ray_cluster):
        """Test reading from ObjectRefs and transforming."""
        test_data = pa.Table.from_pylist([{"id": i, "value": i * 2} for i in range(50)])
        object_ref = ray.put(test_data)

        # Create source and read
        source = SparkSourceConfig().setup(make_operator_runtime())
        split = Split(
            split_id="split_0",
            stage_id="spark_source",
            data_range={
                "object_ref": object_ref,
                "block_size": 50,
            },
        )

        payload = source.read(split)
        assert payload is not None

        # Apply map operator
        map_config = MapOperatorConfig(
            map_fn=lambda row: {
                **row,
                "doubled": row["value"] * 2,
            },
        )
        map_op = map_config.setup(make_operator_runtime())

        mapped = map_op.process_split(split, payload)
        assert mapped is not None
        assert len(mapped) == 50

        for record in mapped.to_pylist():
            assert "doubled" in record
            assert record["doubled"] == record["value"] * 2

    def test_multiple_blocks(self, ray_cluster):
        """Test processing multiple blocks."""
        # Create multiple blocks
        blocks = []
        for block_idx in range(5):
            block_data = pa.Table.from_pylist(
                [{"block": block_idx, "id": i, "value": block_idx * 100 + i} for i in range(20)]
            )
            blocks.append(ray.put(block_data))

        source = SparkSourceConfig().setup(make_operator_runtime())
        map_config = MapOperatorConfig(
            map_fn=lambda row: {**row, "processed": True},
        )
        map_op = map_config.setup(make_operator_runtime())

        total_records = 0
        for idx, block_ref in enumerate(blocks):
            split = Split(
                split_id=f"split_{idx}",
                stage_id="spark_source",
                data_range={
                    "object_ref": block_ref,
                    "block_size": 20,
                },
            )

            payload = source.read(split)
            assert payload is not None

            mapped = map_op.process_split(split, payload)
            assert mapped is not None

            for record in mapped.to_pylist():
                assert record["processed"] is True
                total_records += 1

        assert total_records == 100  # 5 blocks * 20 records


@pytest.mark.integration
class TestSparkSplitPlanner:
    """Integration tests for SparkSplitPlanner using raydp.

    These tests verify that SparkSplitPlanner correctly:
    1. Initializes Spark via raydp.init_spark() using config parameters
    2. Calls dataframe_fn to load data
    3. Persists data to Ray object store using raydp
    4. Returns splits with ObjectRefs

    Note: These tests require:
    - Java 11+ runtime for Spark
    - raydp JAR files (built from raydp java sources)
    """

    def test_stage_master_plan_splits_with_parquet(self, ray_cluster):
        """Test SparkSplitPlanner.plan_splits() with parquet file.

        Verifies the full StageMaster flow:
        - StageMaster initializes Spark via raydp.init_spark() using config
        - dataframe_fn is called to read parquet
        - Data is persisted to object store via raydp
        - Splits contain valid ObjectRefs
        """
        test_path = str(TEST_DATA_100)

        source_stage = Stage(
            stage_id="spark_source",
            operator_config=SparkSourceConfig(
                app_name="test-fetch-splits",
                num_executors=1,
                executor_cores=1,
                executor_memory="512m",
                dataframe_fn=lambda spark: spark.read.parquet(test_path),
            ),
        )

        # Create SparkSplitPlanner directly from operator config
        planner = SparkSplitPlanner(source_stage.operator_config)

        # Fetch splits using the planner
        splits = list(planner.plan_splits("spark_source"))

        assert len(splits) > 0
        total_records = sum(s.data_range["block_size"] for s in splits)
        assert total_records == 100

        # Verify splits have correct structure
        for split in splits:
            assert "object_ref" in split.data_range
            assert "block_size" in split.data_range
            assert split.stage_id == "spark_source"

        # Use SparkSource operator to read the splits
        source = SparkSourceConfig().setup(make_operator_runtime())
        all_records = []
        for split in splits:
            payload = source.read(split)
            if payload:
                all_records.extend(payload.to_pylist())

        assert len(all_records) == 100

        # Cleanup
        planner.cleanup()

    def test_stage_master_with_sql_query(self, ray_cluster):
        """Test SparkSplitPlanner with SQL query in dataframe_fn.

        The dataframe_fn can use any Spark operations including SQL.
        This test creates a temp view and queries it within the dataframe_fn.
        """
        test_path = str(TEST_DATA_100)

        def sql_dataframe_fn(spark):
            """Load data, create temp view, then query with SQL."""
            df = spark.read.parquet(test_path)
            df.createOrReplaceTempView("employees")
            return spark.sql("SELECT id, name, department, salary FROM employees WHERE age > 40")

        source_stage = Stage(
            stage_id="spark_source",
            operator_config=SparkSourceConfig(
                app_name="test-sql-query",
                num_executors=1,
                executor_cores=1,
                executor_memory="512m",
                dataframe_fn=sql_dataframe_fn,
            ),
        )

        # Create SparkSplitPlanner directly from operator config
        planner = SparkSplitPlanner(source_stage.operator_config)

        # Fetch splits - this triggers Spark init via raydp.init_spark()
        splits = list(planner.plan_splits("spark_source"))

        assert len(splits) > 0
        total_records = sum(s.data_range["block_size"] for s in splits)
        print(f"SQL query returned {total_records} records")

        # Read and verify
        source = SparkSourceConfig().setup(make_operator_runtime())
        all_records = []
        for split in splits:
            payload = source.read(split)
            if payload:
                all_records.extend(payload.to_pylist())

        assert len(all_records) == total_records
        assert len(all_records) > 0

        # Cleanup Spark
        planner.cleanup()

    def test_stage_master_1000_records_full_pipeline(self, ray_cluster):
        """Test SparkSplitPlanner with 1000 records - verify split generation and data integrity.

        This test verifies:
        1. StageMaster initializes Spark via config and fetches splits
        2. All splits can be read and contain valid data
        3. Data can be processed through operators
        """
        test_path = str(TEST_DATA_1000)

        # Create stage with config
        source_stage = Stage(
            stage_id="spark_source",
            operator_config=SparkSourceConfig(
                app_name="test-1000-records",
                num_executors=1,
                executor_cores=1,
                executor_memory="512m",
                dataframe_fn=lambda spark: spark.read.parquet(test_path),
            ),
        )

        # Create SparkSplitPlanner directly from operator config
        planner = SparkSplitPlanner(source_stage.operator_config)

        splits = list(planner.plan_splits("spark_source"))
        total_records = sum(s.data_range["block_size"] for s in splits)
        assert total_records == 1000
        print(f"Fetched {len(splits)} splits with {total_records} total records")

        # Read all splits and verify data
        source = SparkSourceConfig().setup(make_operator_runtime())
        all_records = []
        for split in splits:
            payload = source.read(split)
            if payload:
                all_records.extend(payload.to_pylist())

        assert len(all_records) == 1000

        # Apply filter logic manually to verify data quality
        def is_high_performer(row):
            return row.get("status") == "active" and row.get("performance_score", 0) >= 4.0

        high_performers = [r for r in all_records if is_high_performer(r)]

        assert len(high_performers) > 0
        print(f"Found {len(high_performers)} high performers out of 1000 records")

        planner.cleanup()

    def test_stage_master_with_parallelism(self, ray_cluster):
        """Test SparkSplitPlanner with custom parallelism setting.

        The parallelism config controls how many partitions/splits are created.
        """
        test_path = str(TEST_DATA_100)

        # Create stage with parallelism=4
        source_stage = Stage(
            stage_id="spark_source",
            operator_config=SparkSourceConfig(
                app_name="test-parallelism",
                num_executors=1,
                executor_cores=1,
                executor_memory="512m",
                dataframe_fn=lambda spark: spark.read.parquet(test_path),
                parallelism=4,  # Force 4 partitions
            ),
        )

        planner = SparkSplitPlanner(source_stage.operator_config)

        splits = list(planner.plan_splits("spark_source"))

        # Should have 4 splits due to parallelism setting
        assert len(splits) == 4

        total_records = sum(s.data_range["block_size"] for s in splits)
        assert total_records == 100

        planner.cleanup()

    def test_stage_master_complex_dataframe_fn(self, ray_cluster):
        """Test SparkSplitPlanner with complex dataframe_fn logic.

        The dataframe_fn can contain arbitrary Spark transformations.
        """
        test_path = str(TEST_DATA_100)

        def complex_load(spark):
            """Complex data loading with transformations."""
            df = spark.read.parquet(test_path)
            # Apply some Spark transformations
            return (
                df.filter(df.age > 30)
                .select("id", "name", "department", "salary", "age")
                .orderBy("salary", ascending=False)
                .limit(50)
            )

        source_stage = Stage(
            stage_id="spark_source",
            operator_config=SparkSourceConfig(
                app_name="test-complex",
                num_executors=1,
                executor_cores=1,
                executor_memory="512m",
                dataframe_fn=complex_load,
            ),
        )

        planner = SparkSplitPlanner(source_stage.operator_config)

        splits = list(planner.plan_splits("spark_source"))
        total_records = sum(s.data_range["block_size"] for s in splits)

        # Should have at most 50 records (limit in dataframe_fn)
        assert total_records <= 50

        # Verify all records have age > 30
        source = SparkSourceConfig().setup(make_operator_runtime())
        for split in splits:
            payload = source.read(split)
            if payload:
                for record in payload.to_pylist():
                    assert record["age"] > 30

        planner.cleanup()

    @pytest.mark.asyncio
    async def test_full_pipeline_with_queue(self, ray_cluster, anvil_backend):
        """Test complete SparkSource pipeline with Anvil queue.

        This test verifies the full flow:
        1. SparkSplitPlanner starts and creates source queue
        2. Splits are written to source queue
        3. Workers consume splits and produce to output queue
        4. All data is processed through the pipeline
        """
        test_path = str(TEST_DATA_100)

        source_stage = Stage(
            stage_id="spark_source",
            operator_config=SparkSourceConfig(
                app_name="test-full-pipeline",
                num_executors=1,
                executor_cores=1,
                executor_memory="512m",
                dataframe_fn=lambda spark: spark.read.parquet(test_path),
            ),
        )

        from _internal.core.split_payload_store import RaySplitPayloadStore
        from _internal.core.stage import StageRuntime
        from _internal.core.stage_master import QueueEndpoint, StageMaster

        payload_store = RaySplitPayloadStore(name="test-full-pipeline_store")
        runtime = StageRuntime(
            broker_endpoint=QueueEndpoint(
                host="localhost",
                port=anvil_backend.port,
                storage_url="memory://",
            ),
        )
        master = StageMaster(
            job_id="test-full-pipeline",
            stage=source_stage,
            payload_store=payload_store,
            runtime=runtime,
        )

        # Start the full pipeline (creates queues, spawns workers)
        await master.start()

        # Verify queue client was created
        output_queue = master.get_queue_client()
        assert output_queue is not None

        # Wait for workers to produce output (with timeout)
        import asyncio

        max_wait = 30  # seconds
        start_time = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start_time < max_wait:
            status = master.get_status()
            # Check if output queue has messages (workers processed data)
            if status.output_queue_size > 0:
                break
            await asyncio.sleep(0.5)

        # Verify workers produced output
        final_status = master.get_status()
        assert final_status.output_queue_size > 0, "Workers should have produced output"

        # Cleanup
        await master.stop()
        print(f"Pipeline completed: {final_status.output_queue_size} messages in output queue")
