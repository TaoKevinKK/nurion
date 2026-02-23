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

"""Anti-join source: subtract an exclude dataset from a main source.

AntiJoinSourceConfig wraps a main source and an exclude source.  During the
``prepare()`` hook (called by StageMaster before workers spawn), the exclude
dataset is scanned to build an in-memory Arrow table containing only the key
columns.  This table is stored in the ``SplitPayloadStore`` so all workers can
retrieve it by key — the exclude source is scanned exactly once regardless of
worker count.

Each worker retrieves the exclude key table from the payload store on first
use, then applies vectorised filtering via DuckDB ``ANTI JOIN`` (vectorised
execution, zero-copy Arrow input/output via ``register()`` /
``fetch_arrow_table()``).  This applies consistently for both single-column and
multi-column join keys, ensuring identical null handling in all cases.

Usage::

    job.add_stage(Stage(
        stage_id="source",
        operator_config=AntiJoinSourceConfig(
            source=LanceTableSourceConfig(dataset_uri="/data/full"),
            exclude=LanceTableSourceConfig(dataset_uri="/data/done"),
            on=["file_id"],
        ),
        parallelism=4,
    ))
"""

from __future__ import annotations

import dataclasses
import logging
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterator, List, Optional

import pyarrow as pa

from _internal.core.models import Split, SplitPayload
from _internal.core.operator import OperatorConfig, OperatorRuntime
from _internal.core.source import SplitPlanner
from _internal.core.source_operator import SourceOperator

if TYPE_CHECKING:
    from _internal.core.split_payload_store import SplitPayloadStore

# Key injected into each split's data_range so workers can retrieve the
# exclude key table from the SplitPayloadStore.
_ANTI_JOIN_PAYLOAD_KEY = "__anti_join_exclude_payload_key"


@dataclass
class AntiJoinSourceConfig(OperatorConfig):
    """Configuration for an anti-join source (set difference).

    Produces rows from ``source`` whose key columns do NOT appear in
    ``exclude``.  Equivalent to SQL ``source EXCEPT (SELECT on FROM exclude)``.

    The exclude key set is built once during ``prepare()`` and shared with
    workers via the ``SplitPayloadStore``.

    Attributes:
        source: Main data source config (must return a ``SplitPlanner``).
        exclude: Exclude data source config (scanned once to build key set).
        on: Column name(s) used as the join key.
    """

    source: OperatorConfig = field(default_factory=lambda: _missing_config("source"))
    exclude: OperatorConfig = field(default_factory=lambda: _missing_config("exclude"))
    on: List[str] = field(default_factory=list)

    # Unique instance ID — prevents key collisions when multiple AntiJoinSourceConfigs
    # share the same SplitPayloadStore (e.g. two anti-join stages in one job).
    _instance_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16], init=False, repr=False)
    # Set by prepare(); not user-facing.
    _exclude_payload_key: Optional[str] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.on:
            raise ValueError("AntiJoinSourceConfig requires at least one key column in 'on'")

    def get_source_schema(self) -> Optional[pa.Schema]:
        """Delegate to the main source's schema."""
        return self.source.get_source_schema()

    def create_source(self) -> "AntiJoinSplitPlanner":
        """Build an AntiJoinSplitPlanner from the source config."""
        inner_planner = self.source.create_source()
        if not isinstance(inner_planner, SplitPlanner):
            raise TypeError(
                f"AntiJoinSourceConfig.source must return a SplitPlanner, "
                f"but {type(self.source).__name__}.create_source() returned "
                f"{type(inner_planner).__name__}."
            )
        return AntiJoinSplitPlanner(inner_planner=inner_planner, config=self)

    def prepare(self, payload_store: "SplitPayloadStore") -> None:
        """Build the exclude key table and store it in the payload store.

        Called by StageMaster before workers spawn.  Scans the exclude source
        (key columns only where possible), deduplicates, and stores the
        resulting Arrow table in ``payload_store``.

        The payload key is unique per instance (``_instance_id`` suffix) to
        prevent collisions when multiple ``AntiJoinSourceConfig`` objects share
        the same ``SplitPayloadStore``.

        Also propagates ``prepare()`` to both nested ``source`` and ``exclude``
        configs.
        """
        self.source.prepare(payload_store)
        self.exclude.prepare(payload_store)

        logger = logging.getLogger("AntiJoinSourceConfig")
        exclude_table = self._scan_exclude_keys()
        logger.info(
            f"AntiJoinSourceConfig.prepare: built exclude table with "
            f"{exclude_table.num_rows} distinct keys on columns {self.on}"
        )

        payload_key = f"__anti_join_exclude_keys_{self._instance_id}"
        payload = SplitPayload.from_arrow(exclude_table, split_id=payload_key)
        payload_store.store(payload_key, payload)
        # Use object.__setattr__ because dataclass may be frozen in subclass.
        object.__setattr__(self, "_exclude_payload_key", payload_key)

    def setup(self, runtime: OperatorRuntime) -> "AntiJoinSourceOperator":
        """Create the worker-side operator that applies row-level filtering."""
        inner_op = self.source.setup(runtime)
        assert isinstance(inner_op, SourceOperator), (
            f"AntiJoinSourceConfig.source must produce a SourceOperator, "
            f"but {type(self.source).__name__}.setup() returned {type(inner_op).__name__}."
        )
        return AntiJoinSourceOperator(
            config=self,
            runtime=runtime,
            inner_operator=inner_op,
        )

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _scan_exclude_keys(self) -> pa.Table:
        """Scan the exclude source and return a deduplicated Arrow table of key columns.

        Tries column projection via ``dataclasses.replace(exclude, columns=on)``
        first.  Falls back to reading all columns and projecting afterward.
        """
        projected_config = self._project_exclude_config()

        exclude_planner = projected_config.create_source()
        if not isinstance(exclude_planner, SplitPlanner):
            raise TypeError(
                f"AntiJoinSourceConfig.exclude must return a SplitPlanner, "
                f"but {type(projected_config).__name__}.create_source() returned "
                f"{type(exclude_planner).__name__}."
            )

        runtime = OperatorRuntime(
            job_id="anti_join_build",
            stage_id="anti_join_exclude",
            worker_id="anti_join_planner",
        )
        exclude_op = projected_config.setup(runtime)

        chunks: List[pa.Table] = []
        for split in exclude_planner.plan_splits("anti_join_exclude"):
            payload = exclude_op.process_split(split, None)
            if not isinstance(payload, SplitPayload) or payload.is_empty():
                continue
            # Ensure only key columns are kept (in case projection didn't work).
            chunks.append(payload.data.select(self.on))

        exclude_op.close()
        exclude_planner.cleanup()

        if not chunks:
            return self._empty_key_table()

        combined = pa.concat_tables(chunks)
        # Deduplicate via group_by (pure Arrow C++, no Python loops).
        return combined.group_by(self.on).aggregate([])

    def _project_exclude_config(self) -> OperatorConfig:
        """Try to create a projected exclude config that reads only key columns.

        Uses ``dataclasses.replace(exclude, columns=on)`` for configs that
        support a ``columns`` field (e.g. Lance).  Falls back to the original
        config if the field doesn't exist or the replacement fails.
        """
        try:
            return dataclasses.replace(self.exclude, columns=self.on)  # type: ignore[call-arg]
        except TypeError:
            return self.exclude

    def _empty_key_table(self) -> pa.Table:
        """Return an empty table with the correct key column types."""
        schema_for_types = self.exclude.get_source_schema()
        if schema_for_types is None:
            schema_for_types = self.source.get_source_schema()
        if schema_for_types is not None:
            return pa.table({k: pa.array([], type=schema_for_types.field(k).type) for k in self.on})
        return pa.table({k: pa.array([], type=pa.null()) for k in self.on})


class AntiJoinSplitPlanner:
    """Plans splits from the main source, injecting the exclude payload key.

    Implements the ``SplitPlanner`` protocol.  Created by
    ``AntiJoinSourceConfig.create_source()``.

    The planner does no I/O itself — the exclude key table is built in
    ``AntiJoinSourceConfig.prepare()`` and stored in the payload store.
    This planner only reads the resulting payload key from the config and
    injects it into each split's ``data_range``.
    """

    def __init__(
        self,
        inner_planner: SplitPlanner,
        config: AntiJoinSourceConfig,
    ) -> None:
        self._inner = inner_planner
        self._config = config
        self._logger = logging.getLogger("AntiJoinSplitPlanner")

    def plan_splits(self, stage_id: str) -> Iterator[Split]:
        """Validate schemas, then yield splits with the exclude payload key injected."""
        self._validate_key_columns()

        payload_key = self._config._exclude_payload_key
        if payload_key is None:
            raise RuntimeError(
                "AntiJoinSplitPlanner.plan_splits() called before prepare(). "
                "StageMaster must call config.prepare(payload_store) before "
                "starting split production."
            )

        for split in self._inner.plan_splits(stage_id):
            augmented = dict(split.data_range)
            augmented[_ANTI_JOIN_PAYLOAD_KEY] = payload_key
            yield Split(
                split_id=split.split_id,
                stage_id=split.stage_id,
                data_range=augmented,
                parent_split_ids=split.parent_split_ids,
            )

    def cleanup(self) -> None:
        self._inner.cleanup()

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _validate_key_columns(self) -> None:
        """Validate that join key columns exist in both source and exclude schemas."""
        source_schema = self._config.source.get_source_schema()
        exclude_schema = self._config.exclude.get_source_schema()

        if source_schema is None or exclude_schema is None:
            self._logger.debug(
                "AntiJoinSplitPlanner: one or both schemas are None; skipping key column validation"
            )
            return

        source_names = set(source_schema.names)
        exclude_names = set(exclude_schema.names)

        for key in self._config.on:
            if key not in source_names:
                raise ValueError(
                    f"Join key column '{key}' not found in source schema. "
                    f"Available columns: {sorted(source_names)}"
                )
            if key not in exclude_names:
                raise ValueError(
                    f"Join key column '{key}' not found in exclude schema. "
                    f"Available columns: {sorted(exclude_names)}"
                )
            src_type = source_schema.field(key).type
            exc_type = exclude_schema.field(key).type
            if src_type != exc_type:
                raise ValueError(
                    f"Join key column '{key}' type mismatch: "
                    f"source has {src_type}, exclude has {exc_type}"
                )


class AntiJoinSourceOperator(SourceOperator):
    """Worker-side operator that wraps a source and filters rows by exclude keys.

    Retrieves the exclude key table from the ``SplitPayloadStore`` on first
    use (key comes from ``split.data_range[_ANTI_JOIN_PAYLOAD_KEY]``), then
    applies vectorised filtering via DuckDB ``ANTI JOIN`` (zero-copy Arrow I/O)
    for both single-column and multi-column join keys.

    A single DuckDB connection is created once and reused across splits to
    avoid per-split connection overhead.
    """

    def __init__(
        self,
        config: AntiJoinSourceConfig,
        runtime: OperatorRuntime,
        inner_operator: SourceOperator,
    ) -> None:
        super().__init__(config, runtime)
        self._inner = inner_operator
        self._join_keys: List[str] = config.on
        self._exclude_table: Optional[pa.Table] = None
        import duckdb

        self._duckdb_conn = duckdb.connect()

    def read(self, split: Split) -> Optional[SplitPayload]:
        """Read from the inner source and filter out rows present in the exclude table.

        The split's ``data_range`` may contain ``_ANTI_JOIN_PAYLOAD_KEY``
        injected by ``AntiJoinSplitPlanner``.  That key is stripped before the
        split is forwarded to the inner operator so that the inner operator
        does not receive an unexpected keyword argument.
        """
        # Strip our synthetic key so inner operators (e.g. LanceTableSource)
        # don't receive an unexpected data_range kwarg.
        if _ANTI_JOIN_PAYLOAD_KEY in split.data_range:
            clean_data_range = {
                k: v for k, v in split.data_range.items() if k != _ANTI_JOIN_PAYLOAD_KEY
            }
            inner_split = Split(
                split_id=split.split_id,
                stage_id=split.stage_id,
                data_range=clean_data_range,
                parent_split_ids=split.parent_split_ids,
            )
        else:
            inner_split = split

        payload = self._inner.read(inner_split)
        if payload is None or payload.is_empty():
            return payload

        exclude_table = self._get_exclude_table(split)
        if exclude_table.num_rows == 0:
            return payload

        table = payload.data
        filtered = self._apply_anti_join(table, exclude_table)

        if filtered.num_rows == 0:
            return SplitPayload.empty(split_id=payload.split_id, schema=table.schema)
        return payload.with_new_data(filtered)

    def close(self) -> None:
        self._inner.close()
        self._duckdb_conn.close()

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _get_exclude_table(self, split: Split) -> pa.Table:
        """Lazily fetch the exclude key table from the SplitPayloadStore.

        The payload key is retrieved from
        ``split.data_range[_ANTI_JOIN_PAYLOAD_KEY]``.  The decoded table is
        cached so the payload store is queried at most once per worker.

        Raises:
            RuntimeError: If the payload key is missing from the split, the
                payload store is None, or the payload cannot be found.  These
                conditions indicate a programming error (e.g. ``prepare()`` was
                not called before split production).
        """
        if self._exclude_table is not None:
            return self._exclude_table

        payload_key = split.data_range.get(_ANTI_JOIN_PAYLOAD_KEY)
        if payload_key is None:
            raise RuntimeError(
                f"AntiJoinSourceOperator: split.data_range is missing "
                f"'{_ANTI_JOIN_PAYLOAD_KEY}'.  Ensure that "
                f"AntiJoinSourceConfig.prepare() was called before split "
                f"production (StageMaster does this automatically)."
            )

        payload_store = self._runtime.payload_store
        if payload_store is None:
            raise RuntimeError(
                "AntiJoinSourceOperator: runtime.payload_store is None.  "
                "StageWorker must pass payload_store to OperatorRuntime."
            )

        payload = payload_store.get(payload_key)
        if payload is None:
            raise RuntimeError(
                f"AntiJoinSourceOperator: exclude key table not found in "
                f"payload store for key '{payload_key}'.  "
                f"AntiJoinSourceConfig.prepare() may not have completed "
                f"successfully."
            )

        self._exclude_table = payload.data
        self.logger.debug(
            "AntiJoinSourceOperator: loaded exclude table with %d rows",
            self._exclude_table.num_rows,
        )
        return self._exclude_table

    def _apply_anti_join(self, table: pa.Table, exclude_table: pa.Table) -> pa.Table:
        """Filter *table* to rows whose join key does NOT appear in *exclude_table*.

        Uses DuckDB ANTI JOIN for both single- and multi-column keys to ensure
        consistent behaviour (null handling, type coercion, etc.) regardless of
        key count.  Zero-copy Arrow I/O via ``register()`` / ``fetch_arrow_table()``.

        Column names are double-quoted to handle special characters (spaces,
        reserved words, etc.).  The shared DuckDB connection is reused across
        splits; tables are unregistered after each query to release Arrow refs.
        """
        conn = self._duckdb_conn
        conn.register("source_tbl", table)
        conn.register("exclude_tbl", exclude_table)
        try:
            join_cond = " AND ".join(
                f'source_tbl."{k}" = exclude_tbl."{k}"' for k in self._join_keys
            )
            sql = f"SELECT source_tbl.* FROM source_tbl ANTI JOIN exclude_tbl ON {join_cond}"
            result = conn.execute(sql).fetch_arrow_table()
        finally:
            conn.unregister("source_tbl")
            conn.unregister("exclude_tbl")
        return result


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _missing_config(field_name: str) -> OperatorConfig:
    raise TypeError(f"AntiJoinSourceConfig.{field_name} is required but was not provided.")
