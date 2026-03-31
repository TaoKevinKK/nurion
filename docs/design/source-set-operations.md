# Source Set Operations: Union & Anti-Join

## Summary

Support **Union** (merge multiple sources) and **Anti-Join** (set difference) operations at the source stage. Union concatenates splits from multiple data sources into a single planner queue; Anti-Join filters the main source's rows at read time using a key set built from an exclude source.

## Motivation

Common use cases:
1. **Union**: Multiple Lance tables with identical schemas need to be processed together (e.g., combining data batches for captioning).
2. **Anti-Join**: Incremental processing — full table minus already-processed table = pending records.

Both operations happen at the **split planning layer of the source stage** and require no changes to worker processing logic.

## Design Principle

**Compose at the SplitPlanner level; do not introduce new Stage types.** The existing architecture has `OperatorConfig.create_source()` return a `SplitPlanner`. We create composable SplitPlanners that implement Union and Anti-Join, completely transparent to StageMaster and StageWorker.

## User API

### Union

```python
from nurion import Job, Stage, LanceTableSourceConfig, UnionSourceConfig

job = Job(job_id="multi_source")

job.add_stage(Stage(
    stage_id="source",
    operator_config=UnionSourceConfig(
        sources=[
            LanceTableSourceConfig(dataset_uri="/data/batch_001"),
            LanceTableSourceConfig(dataset_uri="/data/batch_002"),
            LanceTableSourceConfig(dataset_uri="/data/batch_003"),
        ],
    ),
    parallelism=4,
))
```

### Anti-Join

```python
from nurion import Job, Stage, LanceTableSourceConfig, AntiJoinSourceConfig

job = Job(job_id="incremental")

job.add_stage(Stage(
    stage_id="source",
    operator_config=AntiJoinSourceConfig(
        source=LanceTableSourceConfig(dataset_uri="/data/full_table"),
        exclude=LanceTableSourceConfig(dataset_uri="/data/processed_table"),
        on=["file_id"],  # join key columns
    ),
    parallelism=4,
))
```

### Composed

```python
job.add_stage(Stage(
    stage_id="source",
    operator_config=AntiJoinSourceConfig(
        source=UnionSourceConfig(
            sources=[
                LanceTableSourceConfig(dataset_uri="/data/batch_001"),
                LanceTableSourceConfig(dataset_uri="/data/batch_002"),
            ],
        ),
        exclude=LanceTableSourceConfig(dataset_uri="/data/already_done"),
        on=["file_id"],
    ),
    parallelism=4,
))
```

## Design

### 1. UnionSourceConfig

Union is implemented at the SplitPlanner level: call each sub-source's `plan_splits()` in sequence and concatenate the output.

**Schema validation**: At the start of `plan_splits()`, read schemas from all sub-sources and verify consistency. Fail immediately on mismatch to avoid runtime data incompatibility.

```python
@dataclass
class UnionSourceConfig(OperatorConfig):
    sources: list[OperatorConfig]

    def create_source(self) -> "UnionSplitPlanner":
        planners = []
        for src in self.sources:
            planner = src.create_source()
            if not isinstance(planner, SplitPlanner):
                raise TypeError("Union only supports SplitPlanner sources")
            planners.append((src, planner))
        return UnionSplitPlanner(planners)
```

**UnionSplitPlanner**:

```python
class UnionSplitPlanner:
    def __init__(self, planners: list[tuple[OperatorConfig, SplitPlanner]]):
        self._planners = planners

    def plan_splits(self, stage_id: str) -> Iterator[Split]:
        # Phase 1: Schema validation
        schemas = []
        for config, _ in self._planners:
            schema = config.get_source_schema()
            schemas.append(schema)

        ref_schema = schemas[0]
        for i, schema in enumerate(schemas[1:], 1):
            if not ref_schema.equals(schema):
                raise ValueError(
                    f"Schema mismatch in union source {i}: "
                    f"expected {ref_schema}, got {schema}"
                )

        # Phase 2: Concatenate splits with globally unique IDs
        global_idx = 0
        for source_idx, (_, planner) in enumerate(self._planners):
            for split in planner.plan_splits(stage_id):
                yield Split(
                    split_id=f"union_{source_idx}_split_{global_idx}",
                    stage_id=stage_id,
                    data_range=split.data_range,
                    parent_split_ids=split.parent_split_ids,
                )
                global_idx += 1

    def cleanup(self) -> None:
        for _, planner in self._planners:
            planner.cleanup()
```

**Schema reading**: Each source config provides schema access via an optional method on `OperatorConfig`:

```python
class OperatorConfig(ABC):
    def get_source_schema(self) -> Optional[pa.Schema]:
        """Return the schema of data this source produces.

        Override in source configs to enable schema validation
        for Union/AntiJoin operations. Returns None by default.
        """
        return None
```

Lance implementation:

```python
@dataclass
class LanceTableSourceConfig(OperatorConfig):
    def get_source_schema(self) -> pa.Schema:
        dataset = lance.dataset(self.dataset_uri, ...)
        schema = dataset.schema
        if self.columns:
            schema = pa.schema([schema.field(c) for c in self.columns])
        return schema
```

### 2. AntiJoinSourceConfig

Anti-Join is implemented at the SplitPlanner level: build an in-memory key set from the exclude source, then filter rows at worker read time.

**Key design decision**: filtering granularity.

- **Option A: Split-level filtering** (filter entire splits in the planner) — too coarse; a split may contain both rows to keep and rows to exclude.
- **Option B: Row-level filtering in worker** (filter after worker reads data) — precise, but requires modifying worker logic. ✗
- **Option C: Wrap SourceOperator** (filter in `read()`) — precise, no worker changes needed. ✓

**Chosen: Option C.** Create a wrapping SourceOperator that filters rows in `read()` after the inner operator reads data.

```python
@dataclass
class AntiJoinSourceConfig(OperatorConfig):
    source: OperatorConfig
    exclude: OperatorConfig
    on: list[str]  # join key columns

    def prepare(self, payload_store: SplitPayloadStore) -> None:
        # Called by StageMaster before workers spawn — scans exclude once.
        self.source.prepare(payload_store)
        self.exclude.prepare(payload_store)
        exclude_table = self._scan_exclude_keys()      # key columns only, deduplicated
        payload_key = f"__anti_join_exclude_keys_{self._instance_id}"
        payload_store.store(payload_key, SplitPayload.from_arrow(exclude_table))
        self._exclude_payload_key = payload_key

    def create_source(self) -> "AntiJoinSplitPlanner":
        # Planner does no I/O; injects payload_key into each split's data_range.
        inner_source = self.source.create_source()
        return AntiJoinSplitPlanner(inner_planner=inner_source, config=self)

    def setup(self, runtime: OperatorRuntime) -> "AntiJoinSourceOperator":
        inner_op = self.source.setup(runtime)
        return AntiJoinSourceOperator(config=self, runtime=runtime, inner_operator=inner_op)
```

**AntiJoinSplitPlanner** — no I/O, just injects payload key:

```python
class AntiJoinSplitPlanner:
    def plan_splits(self, stage_id: str) -> Iterator[Split]:
        payload_key = self._config._exclude_payload_key
        if payload_key is None:
            raise RuntimeError("prepare() must be called before plan_splits()")
        for split in self._inner.plan_splits(stage_id):
            augmented = dict(split.data_range)
            augmented[_ANTI_JOIN_PAYLOAD_KEY] = payload_key
            yield Split(split_id=split.split_id, ..., data_range=augmented)
```

**AntiJoinSourceOperator** — lazy fetch from payload store + DuckDB ANTI JOIN:

```python
class AntiJoinSourceOperator(SourceOperator):
    def read(self, split: Split) -> Optional[SplitPayload]:
        # Strip our synthetic key so inner operators don't receive it.
        inner_split = _strip_payload_key(split)
        payload = self._inner.read(inner_split)
        if payload is None or payload.is_empty():
            return payload
        exclude_table = self._get_exclude_table(split)  # cached after first call
        if exclude_table.num_rows == 0:
            return payload
        return payload.with_new_data(self._apply_anti_join(payload.data, exclude_table))

    def _apply_anti_join(self, table, exclude_table):
        # DuckDB ANTI JOIN — identical semantics for 1 and N key columns,
        # including null handling (null key rows are always retained).
        conn = self._duckdb_conn   # reused across splits
        conn.register("source_tbl", table)
        conn.register("exclude_tbl", exclude_table)
        join_cond = " AND ".join(
            f'source_tbl."{k}" = exclude_tbl."{k}"' for k in self._join_keys
        )
        result = conn.execute(
            f"SELECT source_tbl.* FROM source_tbl ANTI JOIN exclude_tbl ON {join_cond}"
        ).fetch_arrow_table()
        conn.unregister("source_tbl")
        conn.unregister("exclude_tbl")
        return result
```

### Exclude Key Table Distribution

The exclude key table is built in `prepare()` (StageMaster process) and must be available in each worker process. The chosen approach:

**`prepare()` hook + `SplitPayloadStore`**
- `StageMaster.start()` calls `config.prepare(payload_store)` once, before workers spawn.
- `AntiJoinSourceConfig.prepare()` scans the exclude source (key columns only via column projection where possible), deduplicates with `group_by`, and calls `payload_store.store(unique_key, payload)`.
- A unique key per instance (`_instance_id = uuid4().hex[:16]`) prevents collisions when multiple `AntiJoinSourceConfig` objects share the same store.
- The key is injected into each split's `data_range` by `AntiJoinSplitPlanner`.
- Each worker lazily fetches the table once via `payload_store.get(key)` and caches it.

**Why not `ray.put()` directly?**  The previous design used `ray.put()` + base64-encoded `ObjectRef` in split metadata.  This was replaced because:
1. It bypassed the `SplitPayloadStore` abstraction (no `FsspecSplitPayloadStore` support).
2. Planner had to do I/O, violating the principle that planners only produce metadata.
3. `ray.ObjectRef` serialised as base64 is fragile and undocumented.

### 3. Schema Validation Strategy

| Operation | When | What is validated |
|-----------|------|-------------------|
| Union | Start of `plan_splits()` | All sub-source schemas are identical (column names + types) |
| Anti-Join | Start of `plan_splits()` | Join key columns exist in both source and exclude, with matching types |

Schema validation uses `OperatorConfig.get_source_schema()`, which reads only metadata (no data scan).

## Data Model Changes

### New: `OperatorConfig.get_source_schema()`

```python
class OperatorConfig(ABC):
    def get_source_schema(self) -> Optional[pa.Schema]:
        """Return the output schema for schema validation. None = unknown."""
        return None
```

### New: `UnionSourceConfig`

```python
@dataclass
class UnionSourceConfig(OperatorConfig):
    sources: list[OperatorConfig]
    operator_class = None  # Delegates to inner source's operator_class
```

### New: `AntiJoinSourceConfig`

```python
@dataclass
class AntiJoinSourceConfig(OperatorConfig):
    source: OperatorConfig
    exclude: OperatorConfig
    on: list[str]
```

## Files Changed

| File | Change |
|------|--------|
| `engine/_internal/core/operator.py` | Add `get_source_schema()` to `OperatorConfig` |
| `engine/_internal/operators/sources/lance.py` | Implement `get_source_schema()` |
| `engine/_internal/operators/sources/union.py` (new) | `UnionSourceConfig`, `UnionSplitPlanner` |
| `engine/_internal/operators/sources/anti_join.py` (new) | `AntiJoinSourceConfig`, `AntiJoinSplitPlanner`, `AntiJoinSourceOperator` |
| `engine/_internal/operators/sources/__init__.py` | Export new configs |
| `engine/nurion/__init__.py` | Export `UnionSourceConfig`, `AntiJoinSourceConfig` |
| `engine/tests/test_source_set_operations.py` (new) | Unit tests |
| `engine/tests/test_integration_source_set_ops.py` (new) | Integration tests |
| `engine/tests/test_distributed_source_set_ops.py` (new) | Distributed tests |

## Execution Flow

### Union

```
UnionSourceConfig.create_source()
  → UnionSplitPlanner(planners=[LanceSplitPlanner, LanceSplitPlanner, ...])

StageMaster.start()
  → SourceManager._produce_splits()
    → UnionSplitPlanner.plan_splits()
      → validate schemas (all equal)
      → for each inner planner:
          → yield splits with unique IDs
    → push splits to planner queue

Workers claim splits; each split's data_range points to its specific source
  → LanceTableSource.read(split)  (unchanged)
```

### Anti-Join

```
AntiJoinSourceConfig.create_source()
  → AntiJoinSplitPlanner(inner=LanceSplitPlanner, config=self)
     (no I/O — planner only holds a reference to config)

StageMaster.start()
  1. payload_store.wait_ready()                     ← ensure Ray actor is ready
  2. config.prepare(payload_store)
     ├── source.prepare(payload_store)              ← propagate to nested configs
     ├── exclude.prepare(payload_store)
     ├── scan exclude (key cols only via projection)
     ├── group_by → deduplicated Arrow table
     └── payload_store.store(unique_key, table)     ← unique_key contains _instance_id
  3. SourceManager._produce_splits()
     → AntiJoinSplitPlanner.plan_splits()
       → raise RuntimeError if prepare() not called
       → for each inner split: inject _ANTI_JOIN_PAYLOAD_KEY into data_range
       → push augmented splits to planner queue

Workers claim splits
  → AntiJoinSourceOperator.read(split)
    → strip _ANTI_JOIN_PAYLOAD_KEY from split before forwarding to inner
    → inner_operator.read(inner_split) → full payload
    → _get_exclude_table(split):
        → payload_store.get(key)  [cached after first call]
        → raises RuntimeError if key missing / store None / payload not found
    → DuckDB ANTI JOIN → filtered payload
```

## Edge Cases

1. **Empty exclude source**: exclude key set is empty; all rows are retained (equivalent to no anti-join).
2. **Empty union sub-source**: a sub-source with no data produces no splits; handled naturally.
3. **Large exclude set**: millions of keys ≈ ~100 MB; shared via Ray Object Store so each worker reads it once.
4. **Schema mismatch**: error raised during `plan_splits()`, before any workers start.

## Alternatives Considered

### 1. New Stage types (UnionStage / AntiJoinStage)
- Requires changes to DAG topology, RayJobRunner, and StageMaster.
- Over-engineered: these operations are fundamentally data selection at the source layer.

### 2. Merge multiple sources inside StageMaster
- StageMaster would need to manage multiple SourceManagers.
- Increases StageMaster complexity; violates single responsibility.

### 3. Anti-Join filtering at the planner level (split granularity)
- Too coarse: a single split may contain both rows to keep and rows to exclude.
- Only accurate when split granularity equals row granularity (impractical).

**The chosen approach (SplitPlanner composition) minimizes the change surface and is completely transparent to the existing architecture.**

## Test Plan

Three layers: Unit Tests (pure logic, no Ray), Integration Tests (Lance datasets + StageMaster), Distributed Tests (full pipeline + Ray cluster).

### Layer 1: Unit Tests — `tests/test_source_set_operations.py`

Pure logic tests with no Ray/Anvil dependency. Validate planner and operator correctness.

#### Union Tests

| Test | Description | Assertions |
|------|-------------|------------|
| `test_union_plan_splits_concatenates` | Union of two TestSourceConfigs | Total splits = sum of both; all split_ids unique |
| `test_union_plan_splits_three_sources` | Union of three sources | Total splits = sum of all three |
| `test_union_schema_validation_pass` | Sources with identical schemas | No exception; splits yielded normally |
| `test_union_schema_validation_fail_column_name` | Sources with different column names | Raises `ValueError` with schema mismatch message |
| `test_union_schema_validation_fail_column_type` | Same column names, different types | Raises `ValueError` |
| `test_union_empty_source` | One sub-source has no data | Total splits = splits from non-empty sources |
| `test_union_single_source` | Union of a single source | Equivalent to using that source directly |
| `test_union_split_ids_globally_unique` | Split IDs across sub-sources | No duplicate split_ids in the full set |
| `test_union_cleanup_calls_all_planners` | `cleanup()` propagation | All inner planner `cleanup()` methods called (mock) |
| `test_union_rejects_direct_producer` | Union includes a DirectProducer source | Raises `TypeError` |

#### Anti-Join Tests

| Test | Description | Assertions |
|------|-------------|------------|
| `test_anti_join_filters_matching_rows` | Source 100 rows, exclude 30 keys | Output 70 rows |
| `test_anti_join_no_overlap` | Source and exclude have no common keys | Output = full source |
| `test_anti_join_full_overlap` | All source keys are in exclude | Output 0 rows (empty payload) |
| `test_anti_join_empty_exclude` | Exclude set is empty | Output = full source |
| `test_anti_join_multi_column_key` | `on=["col_a", "col_b"]` composite key | Correctly filters on composite key |
| `test_anti_join_key_column_missing` | Source missing a join key column | Raises `ValueError` |
| `test_anti_join_key_type_mismatch` | Key column types differ between source and exclude | Raises `ValueError` |
| `test_anti_join_preserves_non_key_columns` | Non-key columns after filtering | All non-key column values intact |
| `test_anti_join_plan_splits_delegates` | `plan_splits()` delegates to inner planner | Split count and content unchanged |
| `test_anti_join_with_null_keys` | Key column contains null values | Null-key rows are retained (null ≠ any value) |

#### get_source_schema Tests

| Test | Description | Assertions |
|------|-------------|------------|
| `test_base_config_returns_none` | Default `OperatorConfig` | Returns `None` |
| `test_lance_config_returns_schema` | `LanceTableSourceConfig` reads schema | Returns correct `pa.Schema` |
| `test_union_config_returns_first_schema` | `UnionSourceConfig.get_source_schema()` | Returns first sub-source schema |

```python
# Example: Anti-Join unit test
class TestAntiJoinOperator:
    def test_anti_join_filters_matching_rows(self):
        """Anti-join correctly filters rows matching exclude keys."""
        source_table = pa.table({"id": list(range(100)), "value": [f"v{i}" for i in range(100)]})
        exclude_keys = {(i,) for i in range(30)}

        operator = _make_anti_join_operator(join_keys=["id"])
        operator._exclude_keys = exclude_keys

        payload = SplitPayload(data=source_table, split_id="test")
        split = Split(split_id="test", stage_id="source", data_range={})
        result = operator.read(split)

        assert len(result) == 70
        result_ids = set(result.data.column("id").to_pylist())
        assert result_ids == set(range(30, 100))
```

### Layer 2: Integration Tests — `tests/test_integration_source_set_ops.py`

Real Lance datasets + StageMaster + Anvil. Validates the end-to-end source stage.

**Marker**: `pytestmark = pytest.mark.integration`

**Fixtures**:
- `lance_dataset_a` / `lance_dataset_b` / `lance_dataset_c`: local Lance datasets with identical schemas
- `lance_dataset_different_schema`: Lance dataset with a different schema
- `anvil_backend`, `ray_cluster`: from `conftest.py`

| Test | Description | Assertions |
|------|-------------|------------|
| `test_union_lance_sources_full_pipeline` | Union two Lance tables → StageMaster → output queue | Output rows = sum of both tables |
| `test_union_lance_schema_mismatch_fails_fast` | Union Lance tables with different schemas | Error raised during `StageMaster.start()` |
| `test_anti_join_lance_incremental` | Full table anti-join processed table | Output rows = full − processed |
| `test_anti_join_lance_empty_exclude` | Exclude table is empty | Output = full table |
| `test_union_then_anti_join_lance` | Union two tables then anti-join a third | Correct combined result |

```python
# Example: Lance Union integration test
class TestLanceUnionIntegration:
    @pytest.mark.asyncio
    async def test_union_lance_sources_full_pipeline(
        self, lance_dataset_a, lance_dataset_b, ray_cluster, anvil_backend
    ):
        """Union of two Lance tables produces correct total rows."""
        source_stage = Stage(
            stage_id="source",
            operator_config=UnionSourceConfig(
                sources=[
                    LanceTableSourceConfig(dataset_uri=lance_dataset_a, split_size=5),
                    LanceTableSourceConfig(dataset_uri=lance_dataset_b, split_size=5),
                ],
            ),
        )
        # ... create StageMaster, start, wait for output ...
        expected_rows = count_rows(lance_dataset_a) + count_rows(lance_dataset_b)
        assert output_rows == expected_rows
```

### Layer 3: Distributed Tests — `tests/test_distributed_source_set_ops.py`

Full pipeline (source → transform → sink), multiple parallel workers, using `RecordCollector` for result validation.

**Marker**: `pytestmark = pytest.mark.distributed`

**Dependencies**: `ray_cluster`, `record_collector` fixtures

| Test | Description | Workers | Assertions |
|------|-------------|---------|------------|
| `test_union_e2e_no_data_loss` | Two TestSources union → passthrough → collecting sink | 4 transform | Total rows = A + B; no duplicates |
| `test_union_e2e_three_sources` | Three TestSources union | 4 workers | Total rows = A + B + C |
| `test_anti_join_e2e_no_data_loss` | TestSource anti-join → passthrough → collecting sink | 4 workers | Rows = source − overlap |
| `test_anti_join_e2e_full_exclude` | All source keys in exclude | 2 workers | Sink receives 0 rows |
| `test_union_anti_join_composed_e2e` | Union two sources then anti-join | 4 workers | Correct row count |
| `test_union_e2e_large_volume` | 10 000+ rows union | 8 workers | No data loss; no duplicates |
| `test_anti_join_e2e_large_exclude` | Large exclude set (5 000 keys) | 4 workers | Correct filtering |

```python
# Example: Distributed Union E2E test
class TestUnionDistributed:
    @pytest.fixture(autouse=True)
    async def setup_collector(self, ray_cluster, request):
        self.collector_name = f"test_collector_{uuid.uuid4().hex}"
        create_collector(self.collector_name)
        yield
        try:
            ray.kill(ray.get_actor(self.collector_name))
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_union_e2e_no_data_loss(self, ray_cluster):
        """Union of two sources has no data loss through the full pipeline."""
        NUM_A, NUM_B = 500, 700
        test_resources = {"num_cpus": 0.1, "num_gpus": 0, "memory": 100 * 1024**2}

        job = Job(
            job_id=f"test_union_{uuid.uuid4().hex[:8]}",
            config=JobConfig(anvil_db_path="memory://"),
        )
        job.add_stage(Stage(
            stage_id="source",
            operator_config=UnionSourceConfig(sources=[
                TestSourceConfig(num_records=NUM_A, batch_size=100),
                TestSourceConfig(num_records=NUM_B, batch_size=100),
            ]),
            parallelism=(1, 2),
            worker_resources=test_resources,
        ))
        job.add_stage(Stage(
            stage_id="transform",
            operator_config=PassthroughConfig(),
            parallelism=(2, 4),
            worker_resources=test_resources,
        ), upstream_stages=["source"])
        job.add_stage(Stage(
            stage_id="sink",
            operator_config=CollectingSinkConfig(collector_name=self.collector_name),
            parallelism=(1, 2),
            worker_resources=test_resources,
        ), upstream_stages=["transform"])

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=60)
        finally:
            await runner.stop()

        records = get_sink_records(self.collector_name)
        assert DataValidator.verify_count(records, NUM_A + NUM_B)
        assert DataValidator.verify_no_duplicates(records)
```

### Test File Summary

| File | Layer | Marker | Dependencies | Tests |
|------|-------|--------|--------------|-------|
| `tests/test_source_set_operations.py` | Unit | (none) | pyarrow only | ~20 |
| `tests/test_integration_source_set_ops.py` | Integration | `integration` | Lance + Anvil + Ray | ~5 |
| `tests/test_distributed_source_set_ops.py` | Distributed | `distributed` | Full pipeline + Ray cluster | ~7 |

### Run Commands

```bash
cd engine

# Unit tests only (fast, no external deps)
uv run pytest tests/test_source_set_operations.py -v --tb=short

# Integration tests (requires Lance)
uv run pytest tests/test_integration_source_set_ops.py -v --tb=short -m integration

# Distributed tests (requires Ray cluster)
uv run pytest tests/test_distributed_source_set_ops.py -v --tb=short -m distributed

# All set operation tests
uv run pytest tests/test_source_set_operations.py \
    tests/test_integration_source_set_ops.py \
    tests/test_distributed_source_set_ops.py -v --tb=short
```
