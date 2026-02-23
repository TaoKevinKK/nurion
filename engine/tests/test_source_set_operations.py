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

"""Unit tests for Union and Anti-Join source operations.

Pure logic tests — no Ray, no WorkQueue, no Lance on disk.
All sources use in-memory stubs.
"""

from __future__ import annotations

import unittest.mock as mock
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional

import pyarrow as pa
import pytest

from _internal.core.models import Split, SplitPayload
from _internal.core.operator import OperatorConfig, OperatorRuntime
from _internal.core.source_operator import SourceOperator
from _internal.operators.sources.anti_join import (
    AntiJoinSourceConfig,
    AntiJoinSourceOperator,
    AntiJoinSplitPlanner,
    _ANTI_JOIN_PAYLOAD_KEY,
)
from _internal.operators.sources.union import (
    UnionSourceConfig,
    UnionSourceOperator,
    UnionSplitPlanner,
    _parse_union_source_idx,
)


# =============================================================================
# Stubs
# =============================================================================


def _make_runtime(payload_store=None) -> OperatorRuntime:
    return OperatorRuntime(
        job_id="test", stage_id="source", worker_id="w0", payload_store=payload_store
    )


@dataclass
class _StubSourceConfig(OperatorConfig):
    """In-memory source config for testing.  Produces rows with 'id' and 'value'."""

    rows: List[dict] = field(default_factory=list)
    schema: Optional[pa.Schema] = field(default=None)
    batch_size: int = 10

    def get_source_schema(self) -> Optional[pa.Schema]:
        return self.schema

    def create_source(self) -> "_StubSplitPlanner":
        return _StubSplitPlanner(self)


_StubSourceConfig.operator_class = None  # set below


class _StubSourceOperator(SourceOperator):
    def __init__(self, config: _StubSourceConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)

    def read(self, split: Split) -> Optional[SplitPayload]:
        cfg: _StubSourceConfig = self._config  # type: ignore[assignment]
        start = split.data_range["start"]
        end = split.data_range["end"]
        batch = cfg.rows[start:end]
        if not batch:
            return SplitPayload.empty(split_id=split.split_id)
        table = pa.Table.from_pylist(batch)
        return SplitPayload.from_arrow(table, split_id=split.split_id)


_StubSourceConfig.operator_class = _StubSourceOperator


class _StubSplitPlanner:
    def __init__(self, config: _StubSourceConfig):
        self._config = config
        self.cleaned_up = False

    def plan_splits(self, stage_id: str) -> Iterator[Split]:
        rows = self._config.rows
        bs = self._config.batch_size
        for i, start in enumerate(range(0, len(rows), bs)):
            end = min(start + bs, len(rows))
            yield Split(
                split_id=f"stub_split_{i}",
                stage_id=stage_id,
                data_range={"start": start, "end": end},
            )

    def cleanup(self) -> None:
        self.cleaned_up = True


def _stub_source(
    n: int,
    schema: Optional[pa.Schema] = None,
    batch_size: int = 10,
    start_id: int = 0,
) -> _StubSourceConfig:
    rows = [{"id": start_id + i, "value": f"v{start_id + i}"} for i in range(n)]
    if schema is None:
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
    return _StubSourceConfig(rows=rows, schema=schema, batch_size=batch_size)


def _collect_splits(planner: UnionSplitPlanner | AntiJoinSplitPlanner) -> List[Split]:
    return list(planner.plan_splits("test_stage"))


class _FakePayloadStore:
    """Minimal in-memory payload store for testing."""

    def __init__(self) -> None:
        self._data: Dict[str, SplitPayload] = {}

    def store(self, key: str, payload: SplitPayload) -> str:
        self._data[key] = payload
        return key

    def get(self, key: str) -> Optional[SplitPayload]:
        return self._data.get(key)

    def delete(self, key: str) -> bool:
        return self._data.pop(key, None) is not None

    def clear(self) -> int:
        count = len(self._data)
        self._data.clear()
        return count


# =============================================================================
# get_source_schema
# =============================================================================


class TestGetSourceSchema:
    def test_base_config_returns_none(self):
        """Default OperatorConfig.get_source_schema() returns None."""

        @dataclass
        class _Bare(OperatorConfig):
            pass

        assert _Bare().get_source_schema() is None

    def test_stub_config_returns_schema(self):
        schema = pa.schema([pa.field("x", pa.int32())])
        cfg = _StubSourceConfig(rows=[], schema=schema)
        assert cfg.get_source_schema().equals(schema)

    def test_union_config_returns_first_schema(self):
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        cfg = UnionSourceConfig(sources=[_stub_source(5, schema), _stub_source(3, schema)])
        assert cfg.get_source_schema().equals(schema)

    def test_anti_join_config_delegates_to_source(self):
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        cfg = AntiJoinSourceConfig(
            source=_stub_source(5, schema),
            exclude=_stub_source(2, schema),
            on=["id"],
        )
        assert cfg.get_source_schema().equals(schema)


# =============================================================================
# UnionSourceConfig / UnionSplitPlanner
# =============================================================================


class TestUnionSplitPlanner:
    def test_concatenates_two_sources(self):
        """Splits from two sources are concatenated."""
        cfg = UnionSourceConfig(sources=[_stub_source(20), _stub_source(15)])
        splits = _collect_splits(cfg.create_source())
        assert len(splits) == 2 + 2  # 20/10 + 15/10 = 2+2

    def test_three_sources(self):
        cfg = UnionSourceConfig(sources=[_stub_source(10), _stub_source(20), _stub_source(30)])
        splits = _collect_splits(cfg.create_source())
        assert len(splits) == 1 + 2 + 3

    def test_split_ids_globally_unique(self):
        """No two splits share the same split_id."""
        cfg = UnionSourceConfig(sources=[_stub_source(25), _stub_source(25)])
        splits = _collect_splits(cfg.create_source())
        ids = [s.split_id for s in splits]
        assert len(ids) == len(set(ids))

    def test_split_ids_encode_source_index(self):
        """Split IDs contain the sub-source index."""
        cfg = UnionSourceConfig(sources=[_stub_source(10), _stub_source(10)])
        splits = _collect_splits(cfg.create_source())
        assert any(s.split_id.startswith("union_0_") for s in splits)
        assert any(s.split_id.startswith("union_1_") for s in splits)

    def test_data_range_preserved(self):
        """data_range from inner planner is preserved unchanged."""
        cfg = UnionSourceConfig(sources=[_stub_source(10)])
        splits = _collect_splits(cfg.create_source())
        assert splits[0].data_range == {"start": 0, "end": 10}

    def test_single_source(self):
        """Union of a single source behaves like that source directly."""
        cfg = UnionSourceConfig(sources=[_stub_source(15)])
        splits = _collect_splits(cfg.create_source())
        assert len(splits) == 2  # ceil(15/10)

    def test_empty_sub_source_skipped(self):
        """A sub-source with no rows contributes zero splits."""
        cfg = UnionSourceConfig(sources=[_stub_source(0), _stub_source(10)])
        splits = _collect_splits(cfg.create_source())
        assert len(splits) == 1

    def test_schema_validation_pass(self):
        """Identical schemas pass validation without error."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        cfg = UnionSourceConfig(sources=[_stub_source(5, schema), _stub_source(5, schema)])
        splits = _collect_splits(cfg.create_source())
        assert len(splits) == 1 + 1

    def test_schema_validation_fail_column_name(self):
        """Different column names raise ValueError."""
        schema_a = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        schema_b = pa.schema([pa.field("id", pa.int64()), pa.field("label", pa.string())])
        cfg = UnionSourceConfig(sources=[_stub_source(5, schema_a), _stub_source(5, schema_b)])
        with pytest.raises(ValueError, match="Schema mismatch"):
            _collect_splits(cfg.create_source())

    def test_schema_validation_fail_column_type(self):
        """Same column names but different types raise ValueError."""
        schema_a = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        schema_b = pa.schema([pa.field("id", pa.int32()), pa.field("value", pa.string())])
        cfg = UnionSourceConfig(sources=[_stub_source(5, schema_a), _stub_source(5, schema_b)])
        with pytest.raises(ValueError, match="Schema mismatch"):
            _collect_splits(cfg.create_source())

    def test_schema_validation_skipped_when_none(self):
        """If no sub-source exposes a schema, validation is skipped."""
        cfg_a = _StubSourceConfig(rows=[{"id": i} for i in range(5)], schema=None, batch_size=10)
        cfg_b = _StubSourceConfig(rows=[{"id": i} for i in range(3)], schema=None, batch_size=10)
        cfg = UnionSourceConfig(sources=[cfg_a, cfg_b])
        splits = _collect_splits(cfg.create_source())
        assert len(splits) == 1 + 1

    def test_cleanup_propagates_to_all_planners(self):
        """cleanup() calls cleanup() on every inner planner."""
        cfg = UnionSourceConfig(sources=[_stub_source(5), _stub_source(5)])
        planner = cfg.create_source()
        # Exhaust splits so planners are created.
        _collect_splits(planner)
        planner.cleanup()
        for _, inner in planner._planners:
            assert inner.cleaned_up

    def test_rejects_direct_producer(self):
        """A DirectProducer source raises TypeError."""
        from _internal.core.source import DirectProduceContext

        class _FakeDirectConfig(OperatorConfig):
            def create_source(self):
                class _FakeProducer:
                    async def produce(self, ctx: DirectProduceContext) -> int:
                        return 0

                    def cleanup(self) -> None:
                        pass

                return _FakeProducer()

        cfg = UnionSourceConfig(sources=[_stub_source(5), _FakeDirectConfig()])
        with pytest.raises(TypeError, match="SplitPlanner"):
            cfg.create_source()

    def test_requires_at_least_one_source(self):
        with pytest.raises(ValueError):
            UnionSourceConfig(sources=[])


# =============================================================================
# AntiJoinSourceConfig / AntiJoinSourceOperator
# =============================================================================


def _keys_to_table(
    exclude_keys: set, join_keys: List[str], key_type: pa.DataType = pa.int64()
) -> pa.Table:
    """Convert a set of key tuples to an Arrow table for injection into tests."""
    if not exclude_keys:
        return pa.table({k: pa.array([], type=key_type) for k in join_keys})
    rows = [dict(zip(join_keys, tup)) for tup in exclude_keys]
    return pa.Table.from_pylist(rows)


def _make_anti_join_operator(
    source_rows: List[dict],
    exclude_keys: set,
    join_keys: List[str] = None,
) -> AntiJoinSourceOperator:
    """Build an AntiJoinSourceOperator with a pre-loaded exclude key table."""
    if join_keys is None:
        join_keys = ["id"]
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
    source_cfg = _stub_source(len(source_rows), schema)
    source_cfg.rows = source_rows

    exclude_cfg = _stub_source(0, schema)
    cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=join_keys)

    runtime = _make_runtime()
    inner_op = source_cfg.setup(runtime)
    op = AntiJoinSourceOperator(config=cfg, runtime=runtime, inner_operator=inner_op)
    op._exclude_table = _keys_to_table(exclude_keys, join_keys)  # inject directly
    return op


def _read_payload(op: AntiJoinSourceOperator, rows: List[dict]) -> Optional[SplitPayload]:
    split = Split(split_id="test", stage_id="source", data_range={"start": 0, "end": len(rows)})
    op._inner = _DirectReadOperator(rows, _make_runtime())
    return op.read(split)


class _DirectReadOperator(SourceOperator):
    """Helper that returns a fixed table regardless of split."""

    def __init__(self, rows: List[dict], runtime: OperatorRuntime):
        cfg = _StubSourceConfig(rows=rows)
        super().__init__(cfg, runtime)
        self._rows = rows

    def read(self, split: Split) -> Optional[SplitPayload]:
        if not self._rows:
            return SplitPayload.empty(split_id=split.split_id)
        table = pa.Table.from_pylist(self._rows)
        return SplitPayload.from_arrow(table, split_id=split.split_id)


class TestAntiJoinSourceOperator:
    def _op(self, source_rows, exclude_ids, join_keys=None):
        if join_keys is None:
            join_keys = ["id"]
        exclude_keys = {(int(i),) for i in exclude_ids}
        return _make_anti_join_operator(source_rows, exclude_keys, join_keys)

    def _read(self, op, source_rows):
        return _read_payload(op, source_rows)

    def test_filters_matching_rows(self):
        """Rows whose key is in exclude_keys are removed."""
        rows = [{"id": i, "value": f"v{i}"} for i in range(100)]
        op = self._op(rows, exclude_ids=range(30))
        result = self._read(op, rows)
        assert result is not None
        assert len(result) == 70
        ids = set(result.data.column("id").to_pylist())
        assert ids == set(range(30, 100))

    def test_no_overlap_returns_all_rows(self):
        """No overlap between source and exclude → all rows retained."""
        rows = [{"id": i, "value": f"v{i}"} for i in range(50)]
        op = self._op(rows, exclude_ids=range(100, 200))
        result = self._read(op, rows)
        assert len(result) == 50

    def test_full_overlap_returns_empty(self):
        """All source keys in exclude → empty payload."""
        rows = [{"id": i, "value": f"v{i}"} for i in range(10)]
        op = self._op(rows, exclude_ids=range(10))
        result = self._read(op, rows)
        assert result is not None
        assert result.is_empty()

    def test_empty_exclude_returns_all_rows(self):
        """Empty exclude set → all rows retained."""
        rows = [{"id": i, "value": f"v{i}"} for i in range(20)]
        op = self._op(rows, exclude_ids=[])
        result = self._read(op, rows)
        assert len(result) == 20

    def test_multi_column_key(self):
        """Composite key (col_a, col_b) is filtered correctly."""
        rows = [{"col_a": i % 5, "col_b": i % 3, "val": i} for i in range(30)]
        exclude_keys = {(0, 0), (1, 1), (2, 2)}

        schema = pa.schema(
            [
                pa.field("col_a", pa.int64()),
                pa.field("col_b", pa.int64()),
                pa.field("val", pa.int64()),
            ]
        )
        source_cfg = _StubSourceConfig(rows=rows, schema=schema)
        exclude_cfg = _StubSourceConfig(rows=[], schema=schema)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["col_a", "col_b"])
        runtime = _make_runtime()
        inner_op = _DirectReadOperator(rows, runtime)
        op = AntiJoinSourceOperator(config=cfg, runtime=runtime, inner_operator=inner_op)
        op._exclude_table = _keys_to_table(exclude_keys, ["col_a", "col_b"])

        split = Split(split_id="t", stage_id="s", data_range={"start": 0, "end": len(rows)})
        result = op.read(split)
        assert result is not None
        for row in result.data.to_pylist():
            assert (row["col_a"], row["col_b"]) not in exclude_keys

    def test_preserves_non_key_columns(self):
        """Non-key column values are unchanged after filtering."""
        rows = [{"id": i, "value": f"v{i}", "extra": i * 10} for i in range(20)]
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("value", pa.string()),
                pa.field("extra", pa.int64()),
            ]
        )
        source_cfg = _StubSourceConfig(rows=rows, schema=schema)
        exclude_cfg = _StubSourceConfig(rows=[], schema=schema)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])
        runtime = _make_runtime()
        inner_op = _DirectReadOperator(rows, runtime)
        op = AntiJoinSourceOperator(config=cfg, runtime=runtime, inner_operator=inner_op)
        op._exclude_table = _keys_to_table({(i,) for i in range(10)}, ["id"])

        split = Split(split_id="t", stage_id="s", data_range={"start": 0, "end": len(rows)})
        result = op.read(split)
        assert result is not None
        for row in result.data.to_pylist():
            assert row["extra"] == row["id"] * 10

    def test_none_payload_passthrough(self):
        """None payload from inner operator is returned as-is."""
        rows: List[dict] = []
        op = self._op(rows, exclude_ids=[])
        split = Split(split_id="t", stage_id="s", data_range={"start": 0, "end": 0})
        op._inner = _DirectReadOperator([], _make_runtime())
        result = op.read(split)
        # empty table → empty SplitPayload, not None
        assert result is not None
        assert result.is_empty()

    def test_null_keys_are_retained(self):
        """Rows with null key values are never in the exclude set → retained."""
        rows = [
            {"id": None, "value": "null_row"},
            {"id": 1, "value": "one"},
            {"id": 2, "value": "two"},
        ]
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        source_cfg = _StubSourceConfig(rows=rows, schema=schema)
        exclude_cfg = _StubSourceConfig(rows=[], schema=schema)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])
        runtime = _make_runtime()
        inner_op = _DirectReadOperator(rows, runtime)
        op = AntiJoinSourceOperator(config=cfg, runtime=runtime, inner_operator=inner_op)
        op._exclude_table = _keys_to_table({(1,), (2,)}, ["id"])

        split = Split(split_id="t", stage_id="s", data_range={"start": 0, "end": len(rows)})
        result = op.read(split)
        assert result is not None
        assert len(result) == 1
        assert result.data.column("value").to_pylist() == ["null_row"]

    def test_loads_exclude_from_payload_store(self):
        """_get_exclude_table fetches from payload_store when key is in split."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        # Exclude has ids 1, 2, 3.
        exclude_rows = [{"id": i, "value": f"excl_{i}"} for i in range(1, 4)]
        source_cfg = _stub_source(5, schema)
        exclude_cfg = _StubSourceConfig(rows=exclude_rows, schema=schema, batch_size=10)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])

        # Use prepare() so the payload key is set correctly (includes _instance_id).
        store = _FakePayloadStore()
        cfg.prepare(store)
        payload_key = cfg._exclude_payload_key

        runtime = _make_runtime(payload_store=store)
        rows = [{"id": i, "value": f"v{i}"} for i in range(5)]
        inner_op = _DirectReadOperator(rows, runtime)
        op = AntiJoinSourceOperator(config=cfg, runtime=runtime, inner_operator=inner_op)

        split = Split(
            split_id="t",
            stage_id="s",
            data_range={"start": 0, "end": 5, _ANTI_JOIN_PAYLOAD_KEY: payload_key},
        )
        result = op.read(split)
        assert result is not None
        # ids 1,2,3 excluded → ids 0,4 remain
        assert len(result) == 2
        assert set(result.data.column("id").to_pylist()) == {0, 4}


class TestAntiJoinSplitPlanner:
    def test_plan_splits_delegates_to_inner(self):
        """plan_splits() yields the same number of splits as the inner planner."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        source_cfg = _stub_source(25, schema)
        exclude_cfg = _stub_source(5, schema)

        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])

        store = _FakePayloadStore()
        cfg.prepare(store)

        planner = cfg.create_source()
        splits = list(planner.plan_splits("test_stage"))

        # Inner planner (25 rows, batch 10) → 3 splits
        assert len(splits) == 3
        # Each split must carry the payload key so workers can fetch it.
        # The key is unique per instance, so use cfg._exclude_payload_key.
        for split in splits:
            assert _ANTI_JOIN_PAYLOAD_KEY in split.data_range
            assert split.data_range[_ANTI_JOIN_PAYLOAD_KEY] == cfg._exclude_payload_key

    def test_key_column_missing_in_source_raises(self):
        """Missing join key in source schema raises ValueError."""
        schema_src = pa.schema([pa.field("id", pa.int64())])
        schema_exc = pa.schema([pa.field("id", pa.int64()), pa.field("key2", pa.string())])
        source_cfg = _stub_source(5, schema_src)
        exclude_cfg = _StubSourceConfig(rows=[], schema=schema_exc)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["key2"])

        planner = cfg.create_source()
        with pytest.raises(ValueError, match="not found in source schema"):
            list(planner.plan_splits("test_stage"))

    def test_key_type_mismatch_raises(self):
        """Type mismatch on join key raises ValueError."""
        schema_src = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        schema_exc = pa.schema([pa.field("id", pa.int32()), pa.field("value", pa.string())])
        source_cfg = _stub_source(5, schema_src)
        exclude_cfg = _StubSourceConfig(rows=[], schema=schema_exc)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])

        planner = cfg.create_source()
        with pytest.raises(ValueError, match="type mismatch"):
            list(planner.plan_splits("test_stage"))

    def test_requires_at_least_one_key(self):
        schema = pa.schema([pa.field("id", pa.int64())])
        with pytest.raises(ValueError):
            AntiJoinSourceConfig(
                source=_stub_source(5, schema),
                exclude=_stub_source(2, schema),
                on=[],
            )

    def test_plan_splits_without_prepare_raises(self):
        """plan_splits() raises RuntimeError if prepare() was not called first."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        source_cfg = _stub_source(10, schema)
        exclude_cfg = _stub_source(0, schema)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])

        planner = cfg.create_source()
        with pytest.raises(RuntimeError, match="prepare"):
            list(planner.plan_splits("test_stage"))


class TestAntiJoinPrepare:
    def test_prepare_stores_exclude_keys(self):
        """prepare() scans exclude, deduplicates, and stores in payload store."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        # Exclude has rows with ids 0-4, some duplicated.
        exclude_rows = [{"id": i % 3, "value": f"v{i}"} for i in range(9)]
        source_cfg = _stub_source(20, schema)
        exclude_cfg = _StubSourceConfig(rows=exclude_rows, schema=schema, batch_size=5)

        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])
        store = _FakePayloadStore()
        cfg.prepare(store)

        # Key is unique per instance (contains _instance_id suffix).
        assert cfg._exclude_payload_key is not None
        assert cfg._exclude_payload_key.startswith("__anti_join_exclude_keys_")
        payload = store.get(cfg._exclude_payload_key)
        assert payload is not None
        # 9 rows with id % 3 → 3 distinct keys: 0, 1, 2
        assert payload.data.num_rows == 3
        assert set(payload.data.column("id").to_pylist()) == {0, 1, 2}
        # Only key columns stored, not 'value'.
        assert payload.data.column_names == ["id"]

    def test_prepare_unique_keys_per_instance(self):
        """Two distinct configs store under different payload keys."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        cfg_a = AntiJoinSourceConfig(
            source=_stub_source(5, schema), exclude=_stub_source(0, schema), on=["id"]
        )
        cfg_b = AntiJoinSourceConfig(
            source=_stub_source(5, schema), exclude=_stub_source(0, schema), on=["id"]
        )
        store = _FakePayloadStore()
        cfg_a.prepare(store)
        cfg_b.prepare(store)

        assert cfg_a._exclude_payload_key != cfg_b._exclude_payload_key
        # Both payloads are independently accessible.
        assert store.get(cfg_a._exclude_payload_key) is not None
        assert store.get(cfg_b._exclude_payload_key) is not None

    def test_prepare_empty_exclude(self):
        """prepare() with empty exclude stores an empty table."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        source_cfg = _stub_source(10, schema)
        exclude_cfg = _stub_source(0, schema)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])

        store = _FakePayloadStore()
        cfg.prepare(store)

        payload = store.get(cfg._exclude_payload_key)
        assert payload is not None
        assert payload.data.num_rows == 0

    def test_prepare_propagates_to_source_and_exclude(self):
        """prepare() calls prepare() on both source and exclude configs."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        source_cfg = _stub_source(10, schema)
        exclude_cfg = _stub_source(0, schema)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])

        store = _FakePayloadStore()
        with (
            mock.patch.object(source_cfg, "prepare") as mock_source,
            mock.patch.object(exclude_cfg, "prepare") as mock_exclude,
        ):
            cfg.prepare(store)
            mock_source.assert_called_once_with(store)
            mock_exclude.assert_called_once_with(store)


# =============================================================================
# UnionSourceOperator
# =============================================================================


class TestUnionSourceOperator:
    def test_setup_returns_union_source_operator(self):
        """UnionSourceConfig.setup() returns a UnionSourceOperator instance."""
        cfg = UnionSourceConfig(sources=[_stub_source(10), _stub_source(5)])
        op = cfg.setup(_make_runtime())
        assert isinstance(op, UnionSourceOperator)

    def test_dispatches_to_correct_inner_operator(self):
        """split for source 0 is read by source 0's operator; same for source 1."""
        src_a = _stub_source(10, start_id=0)  # ids 0-9
        src_b = _stub_source(10, start_id=100)  # ids 100-109
        cfg = UnionSourceConfig(sources=[src_a, src_b])
        op = cfg.setup(_make_runtime())

        # A split from source 0 should return ids in [0, 10).
        split_0 = Split(
            split_id="union_0_split_0",
            stage_id="source",
            data_range={"start": 0, "end": 10},
        )
        result_0 = op.read(split_0)
        assert result_0 is not None
        assert set(result_0.data.column("id").to_pylist()).issubset(set(range(10)))

        # A split from source 1 should return ids in [100, 110).
        split_1 = Split(
            split_id="union_1_split_1",
            stage_id="source",
            data_range={"start": 0, "end": 10},
        )
        result_1 = op.read(split_1)
        assert result_1 is not None
        assert set(result_1.data.column("id").to_pylist()).issubset(set(range(100, 110)))

    def test_parse_union_source_idx(self):
        """_parse_union_source_idx parses single and multi-digit indices."""
        assert _parse_union_source_idx("union_0_split_0") == 0
        assert _parse_union_source_idx("union_3_split_99") == 3
        assert _parse_union_source_idx("union_12_split_5") == 12

    def test_parse_invalid_split_id_raises(self):
        """Malformed split_id raises ValueError."""
        with pytest.raises(ValueError):
            _parse_union_source_idx("not_a_union_split")

    def test_out_of_range_source_idx_raises(self):
        """source_idx beyond the number of sources raises ValueError."""
        cfg = UnionSourceConfig(sources=[_stub_source(5)])
        op = cfg.setup(_make_runtime())
        bad_split = Split(
            split_id="union_5_split_0",
            stage_id="source",
            data_range={"start": 0, "end": 5},
        )
        with pytest.raises(ValueError, match="out of range"):
            op.read(bad_split)

    def test_close_propagates_to_inner_operators(self):
        """close() is forwarded to all inner operators."""
        cfg = UnionSourceConfig(sources=[_stub_source(5), _stub_source(5)])
        op = cfg.setup(_make_runtime())
        for inner in op._inner_operators:
            inner.close = mock.MagicMock()
        op.close()
        for inner in op._inner_operators:
            inner.close.assert_called_once()


# =============================================================================
# Anti-join payload key in split data_range
# =============================================================================


class TestAntiJoinRefInSplit:
    def test_payload_key_stripped_before_inner_read(self):
        """_ANTI_JOIN_PAYLOAD_KEY is removed from split.data_range before inner.read()."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        source_cfg = _stub_source(5, schema)
        exclude_cfg = _stub_source(0, schema)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])
        runtime = _make_runtime()

        received_splits = []

        class _RecordingSplitOp(_DirectReadOperator):
            def read(self, split):
                received_splits.append(split)
                return super().read(split)

        op = AntiJoinSourceOperator(
            config=cfg,
            runtime=runtime,
            inner_operator=_RecordingSplitOp(
                [{"id": i, "value": f"v{i}"} for i in range(5)], runtime
            ),
        )
        op._exclude_table = pa.table({"id": pa.array([], type=pa.int64())})

        split = Split(
            split_id="test",
            stage_id="source",
            data_range={"start": 0, "end": 5, _ANTI_JOIN_PAYLOAD_KEY: "some_key"},
        )
        op.read(split)

        assert len(received_splits) == 1
        assert _ANTI_JOIN_PAYLOAD_KEY not in received_splits[0].data_range

    def test_missing_payload_key_raises(self):
        """If _ANTI_JOIN_PAYLOAD_KEY is absent from split.data_range, RuntimeError is raised."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        rows = [{"id": i, "value": f"v{i}"} for i in range(5)]
        source_cfg = _stub_source(5, schema)
        exclude_cfg = _stub_source(0, schema)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])
        runtime = _make_runtime()
        op = AntiJoinSourceOperator(
            config=cfg,
            runtime=runtime,
            inner_operator=_DirectReadOperator(rows, runtime),
        )
        # Missing payload key is a programming error (prepare() not called).
        split = Split(split_id="t", stage_id="s", data_range={"start": 0, "end": 5})
        with pytest.raises(RuntimeError, match="payload_key"):
            op.read(split)

    def test_missing_payload_store_raises(self):
        """If runtime.payload_store is None, RuntimeError is raised."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        rows = [{"id": i, "value": f"v{i}"} for i in range(5)]
        source_cfg = _stub_source(5, schema)
        exclude_cfg = _stub_source(0, schema)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])
        runtime = _make_runtime()  # payload_store=None
        op = AntiJoinSourceOperator(
            config=cfg,
            runtime=runtime,
            inner_operator=_DirectReadOperator(rows, runtime),
        )
        split = Split(
            split_id="t",
            stage_id="s",
            data_range={"start": 0, "end": 5, _ANTI_JOIN_PAYLOAD_KEY: "some_key"},
        )
        with pytest.raises(RuntimeError, match="payload_store"):
            op.read(split)


# =============================================================================
# Union prepare propagation
# =============================================================================


class TestUnionPrepare:
    def test_prepare_propagates_to_all_sources(self):
        """UnionSourceConfig.prepare() calls prepare() on each sub-source."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        src_a = _stub_source(5, schema)
        src_b = _stub_source(5, schema)
        cfg = UnionSourceConfig(sources=[src_a, src_b])

        store = _FakePayloadStore()
        with (
            mock.patch.object(src_a, "prepare") as mock_a,
            mock.patch.object(src_b, "prepare") as mock_b,
        ):
            cfg.prepare(store)
            mock_a.assert_called_once_with(store)
            mock_b.assert_called_once_with(store)
