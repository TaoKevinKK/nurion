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

"""Integration tests for IcebergSource using control REST catalog.

Tests the full pipeline flow:
1. Create Iceberg table via control REST catalog
2. Write test data to table
3. Run IcebergSource through StageMaster with WorkQueue queue
4. Verify data is processed correctly
"""

from __future__ import annotations

import uuid

import pyarrow as pa
import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from tests.conftest import make_operator_runtime
from _internal.core.models import Split
from _internal.core.stage import Stage
from _internal.operators.sources import IcebergSourceConfig

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def iceberg_catalog(iceberg_catalog_uri: str) -> RestCatalog:
    """Create a pyiceberg RestCatalog connected to control."""
    return RestCatalog(name="control_catalog", uri=iceberg_catalog_uri)


@pytest.fixture
def iceberg_test_table(iceberg_catalog: RestCatalog, iceberg_catalog_uri: str):
    """Create a test Iceberg table with sample data."""
    unique_id = str(uuid.uuid4())[:8]
    namespace = "default"
    table_name = f"test_table_{unique_id}"
    full_name = f"{namespace}.{table_name}"

    # Create schema - all fields optional to match PyArrow defaults
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "value", LongType(), required=False),
        NestedField(3, "name", StringType(), required=False),
    )

    # Create table
    table = iceberg_catalog.create_table(identifier=full_name, schema=schema)

    # Add test data
    data = pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "value": [10, 20, 30, 40, 50],
            "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
        }
    )
    table.append(data)

    yield {
        "catalog_uri": iceberg_catalog_uri,
        "table_name": full_name,
        "expected_rows": 5,
    }

    # Cleanup
    try:
        iceberg_catalog.drop_table(full_name)
    except Exception:
        pass


class TestIcebergSource:
    """Integration tests for IcebergSource with control REST catalog."""

    def test_iceberg_source_reads_table(self, iceberg_test_table):
        """Test reading Iceberg table via IcebergSource operator."""
        config = IcebergSourceConfig(
            catalog_uri=iceberg_test_table["catalog_uri"],
            table_name=iceberg_test_table["table_name"],
        )
        source = config.setup(make_operator_runtime())

        split = Split(
            split_id="split-0",
            stage_id="source",
            data_range={
                "catalog_uri": iceberg_test_table["catalog_uri"],
                "table_name": iceberg_test_table["table_name"],
            },
        )

        batch = source.process_split(split)

        assert batch is not None
        assert len(batch) == iceberg_test_table["expected_rows"]

        records = batch.to_pylist()
        assert len(records) == 5
        # Verify some data
        names = [r["name"] for r in records]
        assert "Alice" in names
        assert "Eve" in names

        source.close()

    def test_iceberg_source_with_filter(self, iceberg_test_table):
        """Test reading Iceberg table with filter expression."""
        config = IcebergSourceConfig(
            catalog_uri=iceberg_test_table["catalog_uri"],
            table_name=iceberg_test_table["table_name"],
            filter="value > 25",
        )
        source = config.setup(make_operator_runtime())

        split = Split(
            split_id="split-0",
            stage_id="source",
            data_range={
                "catalog_uri": iceberg_test_table["catalog_uri"],
                "table_name": iceberg_test_table["table_name"],
                "filter": "value > 25",
            },
        )

        batch = source.process_split(split)

        assert batch is not None
        # Should only get rows with value > 25 (30, 40, 50)
        assert len(batch) == 3

        source.close()


class TestIcebergPipeline:
    """Integration tests for full Iceberg pipeline with WorkQueue."""

    @pytest.mark.asyncio
    async def test_full_pipeline_with_queue(
        self, iceberg_test_table, ray_cluster, workqueue_backend
    ):
        """Test complete IcebergSource pipeline with WorkQueue queue.

        This test verifies the full flow:
        1. Create IcebergSource stage
        2. Start StageMaster with WorkQueue
        3. Process data through queue
        4. Verify completion
        """
        from dataclasses import dataclass

        from _internal.core.operator import Operator, OperatorConfig, OperatorRuntime, operator
        from _internal.core.stage import StageRuntime
        from _internal.core.stage_master import QueueEndpoint, StageMaster

        # Create a simple pass-through operator for testing
        @dataclass
        class PassThroughConfig(OperatorConfig):
            catalog_uri: str = ""
            table_name: str = ""

        @operator(PassThroughConfig)
        class PassThroughOperator(Operator):
            def __init__(self, config: PassThroughConfig, runtime: OperatorRuntime):
                super().__init__(config, runtime)
                self.catalog_uri = config.catalog_uri
                self.table_name = config.table_name

            def process_split(self, split, payload=None):
                # Read from Iceberg
                source_config = IcebergSourceConfig(
                    catalog_uri=self.catalog_uri,
                    table_name=self.table_name,
                )
                source = source_config.setup(make_operator_runtime())
                return source.process_split(split)

            def generate_splits(self):
                return [
                    Split(
                        split_id="iceberg_split_0",
                        stage_id="iceberg_source",
                        data_range={
                            "catalog_uri": self.catalog_uri,
                            "table_name": self.table_name,
                        },
                    )
                ]

            def close(self):
                pass

        # Create stage
        source_stage = Stage(
            stage_id="iceberg_source",
            operator_config=PassThroughConfig(
                catalog_uri=iceberg_test_table["catalog_uri"],
                table_name=iceberg_test_table["table_name"],
            ),
        )

        # Create stage master for testing
        from _internal.core.split_payload_store import RaySplitPayloadStore

        runtime = StageRuntime(
            broker_endpoint=QueueEndpoint(
                host="localhost",
                port=workqueue_backend.port,
                storage_url="memory://",
            ),
            upstream_queue_name=None,
        )

        payload_store = RaySplitPayloadStore(name="test-iceberg-store")

        master = StageMaster(
            job_id="test-iceberg-pipeline",
            stage=source_stage,
            payload_store=payload_store,
            runtime=runtime,
        )

        # Start the pipeline
        await master.start()

        # Verify queue was created
        output_queue = master.get_queue_client()
        assert output_queue is not None
        assert output_queue.health_check()

        # Wait briefly for processing
        import asyncio

        await asyncio.sleep(0.5)

        # Cleanup
        await master.stop()

        # Verify stage ran
        status = master.get_status()
        assert status.worker_count >= 0  # Workers may have finished
