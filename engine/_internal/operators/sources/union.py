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

"""Union source: combine multiple SplitPlanner sources into one.

UnionSourceConfig wraps N source configs and produces a single SplitPlanner
that concatenates their splits.  Schema consistency is validated at the start
of plan_splits() — before any worker is spawned — so mismatches fail fast.

Usage::

    job.add_stage(Stage(
        stage_id="source",
        operator_config=UnionSourceConfig(
            sources=[
                LanceTableSourceConfig(dataset_uri="/data/batch_001"),
                LanceTableSourceConfig(dataset_uri="/data/batch_002"),
            ],
        ),
        parallelism=4,
    ))
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterator, List, Optional

import pyarrow as pa

from _internal.core.models import Split, SplitPayload
from _internal.core.operator import OperatorConfig, OperatorRuntime
from _internal.core.source import SplitPlanner
from _internal.core.source_operator import SourceOperator

if TYPE_CHECKING:
    from _internal.core.split_payload_store import SplitPayloadStore


@dataclass
class UnionSourceConfig(OperatorConfig):
    """Configuration for a union of multiple SplitPlanner sources.

    All sub-sources must expose the same Arrow schema via
    ``get_source_schema()``.  Schema validation happens at the start of
    ``plan_splits()`` so failures surface before workers are spawned.

    Attributes:
        sources: Two or more source ``OperatorConfig`` instances.  Each must
            return a ``SplitPlanner`` from ``create_source()``.
    """

    sources: List[OperatorConfig] = field(default_factory=list)

    def __post_init__(self) -> None:
        if len(self.sources) < 1:
            raise ValueError("UnionSourceConfig requires at least one source")

    def get_source_schema(self) -> Optional[pa.Schema]:
        """Return the schema of the first sub-source (all must be identical)."""
        return self.sources[0].get_source_schema()

    def create_source(self) -> "UnionSplitPlanner":
        """Build a UnionSplitPlanner from all sub-source configs."""
        planners: list[tuple[OperatorConfig, SplitPlanner]] = []
        for src in self.sources:
            planner = src.create_source()
            if not isinstance(planner, SplitPlanner):
                raise TypeError(
                    f"UnionSourceConfig only supports SplitPlanner sources, "
                    f"but {type(src).__name__}.create_source() returned "
                    f"{type(planner).__name__}. DirectProducer sources are not supported."
                )
            planners.append((src, planner))
        return UnionSplitPlanner(planners)

    def prepare(self, payload_store: "SplitPayloadStore") -> None:
        """Propagate prepare() to all nested source configs."""
        for src in self.sources:
            src.prepare(payload_store)

    def setup(self, runtime: OperatorRuntime) -> "UnionSourceOperator":
        """Create the worker-side operator that dispatches reads to the right sub-source."""
        return UnionSourceOperator(config=self, runtime=runtime)


class UnionSplitPlanner:
    """Concatenates splits from multiple SplitPlanners with schema validation.

    Implements the ``SplitPlanner`` protocol.  Created by
    ``UnionSourceConfig.create_source()``.

    Split IDs are made globally unique by prefixing with the sub-source index
    (``union_{source_idx}_split_{global_idx}``), preventing collisions when
    multiple sub-sources produce splits with the same local IDs.
    """

    def __init__(self, planners: list[tuple[OperatorConfig, SplitPlanner]]) -> None:
        self._planners = planners
        self._logger = logging.getLogger("UnionSplitPlanner")

    def plan_splits(self, stage_id: str) -> Iterator[Split]:
        """Yield splits from all sub-sources after validating schema consistency.

        Raises:
            ValueError: If any sub-source schema differs from the first.
        """
        self._validate_schemas()

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

        self._logger.info(f"UnionSplitPlanner yielded {global_idx} splits total")

    def cleanup(self) -> None:
        """Propagate cleanup to all inner planners."""
        for _, planner in self._planners:
            planner.cleanup()

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _validate_schemas(self) -> None:
        """Raise ValueError if sub-source schemas are not all identical."""
        schemas: list[Optional[pa.Schema]] = []
        for config, _ in self._planners:
            schemas.append(config.get_source_schema())

        # If no sub-source exposes a schema we cannot validate — skip silently.
        if all(s is None for s in schemas):
            self._logger.debug(
                "UnionSplitPlanner: no sub-source exposes get_source_schema(); "
                "skipping schema validation"
            )
            return

        ref_schema = next(s for s in schemas if s is not None)
        for i, schema in enumerate(schemas):
            if schema is None:
                continue
            if not ref_schema.equals(schema):
                raise ValueError(
                    f"Schema mismatch in union source at index {i}.\n"
                    f"  Expected: {ref_schema}\n"
                    f"  Got:      {schema}"
                )

        self._logger.debug(
            f"UnionSplitPlanner: schema validation passed for {len(self._planners)} sources"
        )


def _parse_union_source_idx(split_id: str) -> int:
    """Parse the source index from a union split ID.

    Union split IDs have the format ``union_{source_idx}_split_{global_idx}``.
    For example ``union_2_split_15`` → 2.

    Raises:
        ValueError: If the split_id does not match the expected format.
    """
    try:
        after_prefix = split_id[len("union_") :]  # "2_split_15"
        source_idx_str = after_prefix.split("_split_")[0]  # "2"
        return int(source_idx_str)
    except (IndexError, ValueError) as exc:
        raise ValueError(
            f"Cannot parse source index from union split_id '{split_id}'. "
            f"Expected format: 'union_{{source_idx}}_split_{{global_idx}}'."
        ) from exc


class UnionSourceOperator(SourceOperator):
    """Worker-side operator that dispatches reads to the correct sub-source operator.

    Each split produced by ``UnionSplitPlanner`` encodes the originating
    sub-source index in its split_id (``union_{source_idx}_split_{global_idx}``).
    This operator parses that index and forwards the read to the corresponding
    inner operator, so that each sub-source's serialization logic (e.g. Lance
    fragment reading) is used transparently.
    """

    def __init__(self, config: UnionSourceConfig, runtime: OperatorRuntime) -> None:
        super().__init__(config, runtime)
        inner_ops = [src.setup(runtime) for src in config.sources]
        assert all(isinstance(op, SourceOperator) for op in inner_ops), (
            "All sub-sources in UnionSourceConfig must produce SourceOperator instances"
        )
        self._inner_operators: List[SourceOperator] = inner_ops  # type: ignore[assignment]

    def read(self, split: Split) -> Optional[SplitPayload]:
        """Dispatch the read to the sub-source operator identified by the split ID."""
        source_idx = _parse_union_source_idx(split.split_id)
        if source_idx >= len(self._inner_operators):
            raise ValueError(
                f"source_idx {source_idx} is out of range; "
                f"UnionSourceConfig has {len(self._inner_operators)} sources."
            )
        return self._inner_operators[source_idx].read(split)

    def close(self) -> None:
        for op in self._inner_operators:
            op.close()
