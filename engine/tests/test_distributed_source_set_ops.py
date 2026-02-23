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

"""Distributed end-to-end tests for Union and Anti-Join sources.

Full pipeline: source → transform → collecting sink.
Multiple parallel workers.  Uses RecordCollector for result validation.

Run with:
    uv run pytest tests/test_distributed_source_set_ops.py -v --tb=short -m distributed
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from typing import Iterator, List, Optional

import pyarrow as pa
import pytest
import ray

from _internal.core.job import Job, JobConfig
from _internal.core.models import Split, SplitPayload
from _internal.core.operator import OperatorConfig, OperatorRuntime
from _internal.core.source_operator import SourceOperator
from _internal.core.stage import Stage
from _internal.operators.sources.anti_join import AntiJoinSourceConfig
from _internal.operators.sources.union import UnionSourceConfig
from _internal.runtime.ray_runner import RayJobRunner
from tests.utils import (
    CollectingSinkConfig,
    DataValidator,
    PassthroughConfig,
    create_collector,
    get_sink_records,
)

pytestmark = pytest.mark.distributed

# ---------------------------------------------------------------------------
# In-memory test source (no Lance / no disk)
# ---------------------------------------------------------------------------

_TEST_RESOURCES = {"num_cpus": 0.1, "num_gpus": 0, "memory": 100 * 1024**2}


@dataclass
class _MemSourceConfig(OperatorConfig):
    """Generates rows with sequential ids in [id_start, id_start + num_records)."""

    num_records: int = 100
    batch_size: int = 50
    id_start: int = 0

    def create_source(self) -> "_MemSplitPlanner":
        return _MemSplitPlanner(self)

    def get_source_schema(self):
        return pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])


_MemSourceConfig.operator_class = None  # set below


class _MemSourceOperator(SourceOperator):
    """Operator that generates rows from data_range metadata."""

    def __init__(self, config: _MemSourceConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)

    def read(self, split: Split) -> Optional[SplitPayload]:
        cfg: _MemSourceConfig = self._config  # type: ignore[assignment]
        start = split.data_range["start"]
        end = split.data_range["end"]
        rows = [
            {"id": cfg.id_start + i, "value": f"v{cfg.id_start + i}"} for i in range(start, end)
        ]
        if not rows:
            return SplitPayload.empty(split_id=split.split_id)
        table = pa.Table.from_pylist(rows)
        return SplitPayload.from_arrow(table, split_id=split.split_id)


_MemSourceConfig.operator_class = _MemSourceOperator


class _MemSplitPlanner:
    def __init__(self, config: _MemSourceConfig):
        self._config = config

    def plan_splits(self, stage_id: str) -> Iterator[Split]:
        n = self._config.num_records
        bs = self._config.batch_size
        for i, start in enumerate(range(0, n, bs)):
            end = min(start + bs, n)
            yield Split(
                split_id=f"mem_split_{i}",
                stage_id=stage_id,
                data_range={"start": start, "end": end},
            )

    def cleanup(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Pipeline factory helpers
# ---------------------------------------------------------------------------


def _make_job(source_config: OperatorConfig, collector_name: str) -> Job:
    job = Job(
        job_id=f"test_{uuid.uuid4().hex[:8]}",
        config=JobConfig(
            workqueue_db_path="memory://",
            claim_timeout_secs=2.0,
            recovery_interval_secs=0.5,
        ),
    )
    job.add_stage(
        Stage(
            stage_id="source",
            operator_config=source_config,
            parallelism=(1, 2),
            worker_resources=_TEST_RESOURCES,
        )
    )
    job.add_stage(
        Stage(
            stage_id="transform",
            operator_config=PassthroughConfig(),
            parallelism=(2, 4),
            worker_resources=_TEST_RESOURCES,
        ),
        upstream_stages=["source"],
    )
    job.add_stage(
        Stage(
            stage_id="sink",
            operator_config=CollectingSinkConfig(collector_name=collector_name),
            parallelism=(1, 2),
            worker_resources=_TEST_RESOURCES,
        ),
        upstream_stages=["transform"],
    )
    return job


async def _run(job: Job, timeout: float = 90.0) -> None:
    runner = RayJobRunner(job)
    try:
        await runner.initialize()
        await asyncio.wait_for(runner.run(), timeout=timeout)
    finally:
        await runner.stop()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
async def collector(ray_cluster, request):
    """Create a unique RecordCollector for each test and expose its name."""
    name = f"test_collector_{uuid.uuid4().hex}"
    create_collector(name)
    request.instance.collector_name = name
    yield name
    try:
        ray.kill(ray.get_actor(name))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Union tests
# ---------------------------------------------------------------------------


class TestUnionDistributed:
    @pytest.mark.asyncio
    async def test_union_two_sources_no_data_loss(self, ray_cluster):
        """Union of two in-memory sources: no data loss, no duplicates."""
        NUM_A, NUM_B = 500, 700
        config = UnionSourceConfig(
            sources=[
                _MemSourceConfig(num_records=NUM_A, batch_size=100, id_start=0),
                _MemSourceConfig(num_records=NUM_B, batch_size=100, id_start=10000),
            ]
        )
        await _run(_make_job(config, self.collector_name))

        records = get_sink_records(self.collector_name)
        assert DataValidator.verify_count(records, NUM_A + NUM_B), (
            f"Expected {NUM_A + NUM_B} records, got {len(records)}"
        )
        assert DataValidator.verify_no_duplicates(records), (
            f"Duplicates: {DataValidator.get_duplicate_ids(records)}"
        )

    @pytest.mark.asyncio
    async def test_union_three_sources(self, ray_cluster):
        """Union of three in-memory sources: correct total count."""
        NUM_A, NUM_B, NUM_C = 300, 400, 200
        config = UnionSourceConfig(
            sources=[
                _MemSourceConfig(num_records=NUM_A, batch_size=100, id_start=0),
                _MemSourceConfig(num_records=NUM_B, batch_size=100, id_start=10000),
                _MemSourceConfig(num_records=NUM_C, batch_size=100, id_start=20000),
            ]
        )
        await _run(_make_job(config, self.collector_name))

        records = get_sink_records(self.collector_name)
        assert DataValidator.verify_count(records, NUM_A + NUM_B + NUM_C)
        assert DataValidator.verify_no_duplicates(records)

    @pytest.mark.asyncio
    async def test_union_large_volume(self, ray_cluster):
        """Union of two large sources: no data loss at scale."""
        NUM_A, NUM_B = 5000, 6000
        config = UnionSourceConfig(
            sources=[
                _MemSourceConfig(num_records=NUM_A, batch_size=500, id_start=0),
                _MemSourceConfig(num_records=NUM_B, batch_size=500, id_start=100000),
            ]
        )
        await _run(_make_job(config, self.collector_name), timeout=120.0)

        records = get_sink_records(self.collector_name)
        assert DataValidator.verify_count(records, NUM_A + NUM_B)
        assert DataValidator.verify_no_duplicates(records)

    @pytest.mark.asyncio
    async def test_union_single_source_passthrough(self, ray_cluster):
        """Union of a single source behaves identically to that source alone."""
        NUM = 400
        config = UnionSourceConfig(
            sources=[
                _MemSourceConfig(num_records=NUM, batch_size=100, id_start=0),
            ]
        )
        await _run(_make_job(config, self.collector_name))

        records = get_sink_records(self.collector_name)
        assert DataValidator.verify_count(records, NUM)


# ---------------------------------------------------------------------------
# Anti-Join tests
# ---------------------------------------------------------------------------


@dataclass
class _ExcludeSourceConfig(OperatorConfig):
    """Source that produces only the 'id' column for use as an exclude set."""

    exclude_ids: List[int] = field(default_factory=list)
    batch_size: int = 50

    def create_source(self) -> "_ExcludeSplitPlanner":
        return _ExcludeSplitPlanner(self)

    def get_source_schema(self):
        return pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])


_ExcludeSourceConfig.operator_class = None  # set below


class _ExcludeOperator(SourceOperator):
    def __init__(self, config: _ExcludeSourceConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)

    def read(self, split: Split) -> Optional[SplitPayload]:
        cfg: _ExcludeSourceConfig = self._config  # type: ignore[assignment]
        start = split.data_range["start"]
        end = split.data_range["end"]
        ids = cfg.exclude_ids[start:end]
        if not ids:
            return SplitPayload.empty(split_id=split.split_id)
        rows = [{"id": i, "value": f"exc_{i}"} for i in ids]
        table = pa.Table.from_pylist(rows)
        return SplitPayload.from_arrow(table, split_id=split.split_id)


_ExcludeSourceConfig.operator_class = _ExcludeOperator


class _ExcludeSplitPlanner:
    def __init__(self, config: _ExcludeSourceConfig):
        self._config = config

    def plan_splits(self, stage_id: str) -> Iterator[Split]:
        ids = self._config.exclude_ids
        bs = self._config.batch_size
        for i, start in enumerate(range(0, len(ids), bs)):
            end = min(start + bs, len(ids))
            yield Split(
                split_id=f"exc_split_{i}",
                stage_id=stage_id,
                data_range={"start": start, "end": end},
            )

    def cleanup(self) -> None:
        pass


class TestAntiJoinDistributed:
    @pytest.mark.asyncio
    async def test_anti_join_removes_excluded_rows(self, ray_cluster):
        """Anti-join: rows with excluded ids are not present in sink output."""
        NUM_SOURCE = 600
        EXCLUDE_IDS = list(range(0, 200))  # exclude first 200

        source_cfg = _MemSourceConfig(num_records=NUM_SOURCE, batch_size=100, id_start=0)
        exclude_cfg = _ExcludeSourceConfig(exclude_ids=EXCLUDE_IDS, batch_size=100)

        config = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])
        await _run(_make_job(config, self.collector_name))

        records = get_sink_records(self.collector_name)
        expected = NUM_SOURCE - len(EXCLUDE_IDS)
        assert DataValidator.verify_count(records, expected), (
            f"Expected {expected} records, got {len(records)}"
        )
        result_ids = {r["id"] for r in records}
        assert not result_ids.intersection(set(EXCLUDE_IDS)), "Excluded ids found in output"

    @pytest.mark.asyncio
    async def test_anti_join_empty_exclude_returns_all(self, ray_cluster):
        """Empty exclude set → all source rows reach the sink."""
        NUM_SOURCE = 400
        source_cfg = _MemSourceConfig(num_records=NUM_SOURCE, batch_size=100, id_start=0)
        exclude_cfg = _ExcludeSourceConfig(exclude_ids=[], batch_size=50)

        config = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])
        await _run(_make_job(config, self.collector_name))

        records = get_sink_records(self.collector_name)
        assert DataValidator.verify_count(records, NUM_SOURCE)

    @pytest.mark.asyncio
    async def test_anti_join_full_overlap_produces_zero_rows(self, ray_cluster):
        """All source ids excluded → sink receives zero rows."""
        NUM_SOURCE = 100
        source_cfg = _MemSourceConfig(num_records=NUM_SOURCE, batch_size=50, id_start=0)
        exclude_cfg = _ExcludeSourceConfig(exclude_ids=list(range(NUM_SOURCE)), batch_size=50)

        config = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])
        await _run(_make_job(config, self.collector_name))

        records = get_sink_records(self.collector_name)
        assert len(records) == 0, f"Expected 0 records, got {len(records)}"

    @pytest.mark.asyncio
    async def test_anti_join_large_exclude_set(self, ray_cluster):
        """Large exclude set (5 000 keys) is handled correctly."""
        NUM_SOURCE = 10000
        EXCLUDE_IDS = list(range(0, 5000))

        source_cfg = _MemSourceConfig(num_records=NUM_SOURCE, batch_size=500, id_start=0)
        exclude_cfg = _ExcludeSourceConfig(exclude_ids=EXCLUDE_IDS, batch_size=500)

        config = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])
        await _run(_make_job(config, self.collector_name), timeout=120.0)

        records = get_sink_records(self.collector_name)
        expected = NUM_SOURCE - len(EXCLUDE_IDS)
        assert DataValidator.verify_count(records, expected)
        result_ids = {r["id"] for r in records}
        assert not result_ids.intersection(set(EXCLUDE_IDS))


# ---------------------------------------------------------------------------
# Composed: Union + Anti-Join
# ---------------------------------------------------------------------------


class TestUnionAntiJoinComposed:
    @pytest.mark.asyncio
    async def test_union_then_anti_join(self, ray_cluster):
        """Union two sources then anti-join: correct combined result."""
        NUM_A, NUM_B = 300, 400
        EXCLUDE_IDS = list(range(0, 100))  # first 100 from source A

        union_cfg = UnionSourceConfig(
            sources=[
                _MemSourceConfig(num_records=NUM_A, batch_size=100, id_start=0),
                _MemSourceConfig(num_records=NUM_B, batch_size=100, id_start=10000),
            ]
        )
        exclude_cfg = _ExcludeSourceConfig(exclude_ids=EXCLUDE_IDS, batch_size=100)

        config = AntiJoinSourceConfig(source=union_cfg, exclude=exclude_cfg, on=["id"])
        await _run(_make_job(config, self.collector_name))

        records = get_sink_records(self.collector_name)
        expected = (NUM_A - len(EXCLUDE_IDS)) + NUM_B
        assert DataValidator.verify_count(records, expected), (
            f"Expected {expected}, got {len(records)}"
        )
        result_ids = {r["id"] for r in records}
        assert not result_ids.intersection(set(EXCLUDE_IDS))
        assert DataValidator.verify_no_duplicates(records)
