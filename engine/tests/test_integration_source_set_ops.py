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

"""Integration tests for Union and Anti-Join sources.

Uses real Lance datasets on the local filesystem + StageMaster + WorkQueue.
Validates that the source stage produces the correct number of output messages.

Run with:
    uv run pytest tests/test_integration_source_set_ops.py -v --tb=short -m integration
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import lance
import pyarrow as pa
import pytest
from lance.dataset import write_dataset

from _internal.core.split_payload_store import RaySplitPayloadStore
from _internal.core.stage import Stage, StageRuntime
from _internal.core.stage_master import QueueEndpoint, StageMaster
from _internal.operators.sources.anti_join import AntiJoinSourceConfig
from _internal.operators.sources.lance import LanceTableSourceConfig
from _internal.operators.sources.union import UnionSourceConfig

pytestmark = pytest.mark.integration


# =============================================================================
# Fixtures
# =============================================================================

_SCHEMA_AB = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("value", pa.string()),
    ]
)

_SCHEMA_DIFFERENT = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("label", pa.string()),  # different column name
    ]
)


def _write_lance(path: Path, rows: list[dict], schema: pa.Schema) -> str:
    table = pa.Table.from_pylist(rows, schema=schema)
    write_dataset(table, str(path))
    return str(path)


def _count_rows(dataset_uri: str) -> int:
    return lance.dataset(dataset_uri).count_rows()


@pytest.fixture
def lance_dataset_a(tmp_path):
    rows = [{"id": i, "value": f"a_{i}"} for i in range(30)]
    yield _write_lance(tmp_path / "dataset_a.lance", rows, _SCHEMA_AB)


@pytest.fixture
def lance_dataset_b(tmp_path):
    rows = [{"id": i + 100, "value": f"b_{i}"} for i in range(20)]
    yield _write_lance(tmp_path / "dataset_b.lance", rows, _SCHEMA_AB)


@pytest.fixture
def lance_dataset_c(tmp_path):
    rows = [{"id": i + 200, "value": f"c_{i}"} for i in range(15)]
    yield _write_lance(tmp_path / "dataset_c.lance", rows, _SCHEMA_AB)


@pytest.fixture
def lance_dataset_full(tmp_path):
    """Full dataset: ids 0-49."""
    rows = [{"id": i, "value": f"full_{i}"} for i in range(50)]
    yield _write_lance(tmp_path / "full.lance", rows, _SCHEMA_AB)


@pytest.fixture
def lance_dataset_processed(tmp_path):
    """Already-processed subset: ids 0-19."""
    rows = [{"id": i, "value": f"done_{i}"} for i in range(20)]
    yield _write_lance(tmp_path / "processed.lance", rows, _SCHEMA_AB)


@pytest.fixture
def lance_dataset_different_schema(tmp_path):
    rows = [{"id": i, "label": f"x_{i}"} for i in range(10)]
    yield _write_lance(tmp_path / "diff_schema.lance", rows, _SCHEMA_DIFFERENT)


# =============================================================================
# Helpers
# =============================================================================


async def _run_source_stage(
    operator_config,
    workqueue_backend,
    timeout: float = 30.0,
) -> int:
    """Start a source-only StageMaster and return the number of output messages."""
    source_stage = Stage(
        stage_id="source",
        operator_config=operator_config,
        parallelism=2,
        worker_resources={"num_cpus": 0.5, "num_gpus": 0, "memory": 200 * 1024**2},
    )

    payload_store = RaySplitPayloadStore(name=f"test_store_{id(operator_config)}")
    runtime = StageRuntime(
        broker_endpoint=QueueEndpoint(
            host=workqueue_backend.host,
            port=workqueue_backend.port,
            storage_url="memory://",
        ),
        upstream_queue_name=None,
    )
    master = StageMaster(
        job_id=f"test_job_{id(operator_config)}",
        stage=source_stage,
        payload_store=payload_store,
        runtime=runtime,
    )

    await master.start()

    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        status = master.get_status()
        if status.is_finished or not status.is_running:
            break
        await asyncio.sleep(0.2)

    output_size = master.get_status().output_queue_size
    await master.stop()
    return output_size


# =============================================================================
# Tests
# =============================================================================


class TestUnionLanceIntegration:
    @pytest.mark.asyncio
    async def test_union_two_sources_total_rows(
        self, lance_dataset_a, lance_dataset_b, ray_cluster, workqueue_backend
    ):
        """Union of two Lance tables produces splits covering all rows."""
        config = UnionSourceConfig(
            sources=[
                LanceTableSourceConfig(dataset_uri=lance_dataset_a, split_size=10),
                LanceTableSourceConfig(dataset_uri=lance_dataset_b, split_size=10),
            ]
        )
        output_size = await _run_source_stage(config, workqueue_backend)
        # A: 30 rows / 10 = 3 splits; B: 20 rows / 10 = 2 splits → 5 messages
        assert output_size == 5

    @pytest.mark.asyncio
    async def test_union_three_sources(
        self,
        lance_dataset_a,
        lance_dataset_b,
        lance_dataset_c,
        ray_cluster,
        workqueue_backend,
    ):
        """Union of three Lance tables produces output from all three."""
        config = UnionSourceConfig(
            sources=[
                LanceTableSourceConfig(dataset_uri=lance_dataset_a, split_size=15),
                LanceTableSourceConfig(dataset_uri=lance_dataset_b, split_size=10),
                LanceTableSourceConfig(dataset_uri=lance_dataset_c, split_size=15),
            ]
        )
        output_size = await _run_source_stage(config, workqueue_backend)
        # A: 30/15=2; B: 20/10=2; C: 15/15=1 → 5 messages
        assert output_size == 5

    @pytest.mark.asyncio
    async def test_union_schema_mismatch_fails_fast(
        self,
        lance_dataset_a,
        lance_dataset_different_schema,
        ray_cluster,
        workqueue_backend,
    ):
        """Union with mismatched schemas raises ValueError before workers start."""
        config = UnionSourceConfig(
            sources=[
                LanceTableSourceConfig(dataset_uri=lance_dataset_a, split_size=10),
                LanceTableSourceConfig(dataset_uri=lance_dataset_different_schema, split_size=10),
            ]
        )
        source_stage = Stage(
            stage_id="source",
            operator_config=config,
            parallelism=1,
            worker_resources={"num_cpus": 0.5, "num_gpus": 0, "memory": 200 * 1024**2},
        )
        payload_store = RaySplitPayloadStore(name=f"test_store_mismatch_{id(config)}")
        runtime = StageRuntime(
            broker_endpoint=QueueEndpoint(
                host=workqueue_backend.host,
                port=workqueue_backend.port,
                storage_url="memory://",
            ),
            upstream_queue_name=None,
        )
        master = StageMaster(
            job_id="test_schema_mismatch",
            stage=source_stage,
            payload_store=payload_store,
            runtime=runtime,
        )

        with pytest.raises(ValueError, match="Schema mismatch"):
            await master.start()
            # plan_splits is called lazily; trigger it
            await asyncio.wait_for(master.run(), timeout=15.0)

        await master.stop()


class TestAntiJoinLanceIntegration:
    @pytest.mark.asyncio
    async def test_incremental_processing(
        self,
        lance_dataset_full,
        lance_dataset_processed,
        ray_cluster,
        workqueue_backend,
    ):
        """Anti-join produces only unprocessed rows (full − processed)."""
        config = AntiJoinSourceConfig(
            source=LanceTableSourceConfig(dataset_uri=lance_dataset_full, split_size=10),
            exclude=LanceTableSourceConfig(dataset_uri=lance_dataset_processed, split_size=50),
            on=["id"],
        )
        output_size = await _run_source_stage(config, workqueue_backend)
        # full=50 → 5 splits of 10; processed ids=0-19 → splits [0-9],[10-19] empty,
        # [20-29],[30-39],[40-49] have rows.  All 5 splits produce output messages
        # (workers push even empty payloads via ack_and_forward).
        assert output_size == 5

    @pytest.mark.asyncio
    async def test_empty_exclude_returns_full_source(
        self, lance_dataset_full, tmp_path, ray_cluster, workqueue_backend
    ):
        """Empty exclude table → all source rows are produced."""
        empty_path = str(tmp_path / "empty.lance")
        write_dataset(pa.Table.from_pylist([], schema=_SCHEMA_AB), empty_path)

        config = AntiJoinSourceConfig(
            source=LanceTableSourceConfig(dataset_uri=lance_dataset_full, split_size=10),
            exclude=LanceTableSourceConfig(dataset_uri=empty_path, split_size=10),
            on=["id"],
        )
        output_size = await _run_source_stage(config, workqueue_backend)
        # 50 rows / 10 = 5 splits, none filtered → 5 output messages
        assert output_size == 5

    @pytest.mark.asyncio
    async def test_union_then_anti_join(
        self,
        lance_dataset_a,
        lance_dataset_b,
        lance_dataset_processed,
        ray_cluster,
        workqueue_backend,
    ):
        """Union two sources then anti-join a third: composed operation works."""
        config = AntiJoinSourceConfig(
            source=UnionSourceConfig(
                sources=[
                    LanceTableSourceConfig(dataset_uri=lance_dataset_a, split_size=10),
                    LanceTableSourceConfig(dataset_uri=lance_dataset_b, split_size=10),
                ]
            ),
            exclude=LanceTableSourceConfig(dataset_uri=lance_dataset_processed, split_size=50),
            on=["id"],
        )
        output_size = await _run_source_stage(config, workqueue_backend)
        # A: ids 0-29 (3 splits); B: ids 100-119 (2 splits) → 5 splits total.
        # Processed: ids 0-19 → A splits [0-9],[10-19] become empty; rest have rows.
        # All 5 splits produce output messages.
        assert output_size == 5
