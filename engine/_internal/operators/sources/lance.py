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

"""Lance table source operator and split planner."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Iterator, Optional

import lance

from _internal.core.models import Split, SplitPayload
from _internal.core.operator import OperatorConfig, OperatorRuntime, operator
from _internal.core.source_operator import SourceOperator

if TYPE_CHECKING:
    pass


@dataclass
class LanceTableSourceConfig(OperatorConfig):
    """Configuration for LanceTableSource operator and LanceSplitPlanner.

    This unified config is used by both the operator (for reading splits)
    and the planner (for planning splits via create_source()).

    Note: queue_type and workqueue_db_path are configured via JobConfig,
    not here. The runner passes these to the master via StageRuntime.
    """

    dataset_uri: str
    """URI of the Lance dataset."""

    filter: Optional[str] = None
    """Filter expression to apply when reading."""

    columns: Optional[Iterable[str]] = None
    """Columns to read from the dataset."""

    split_size: int = 1024
    """Number of rows per split."""

    max_rows: Optional[int] = None
    """Maximum total rows to read. None = no limit (read all rows)."""

    def create_source(self) -> "LanceSplitPlanner":
        """Create a split planner for this Lance source."""
        return LanceSplitPlanner(self)


def _get_lance_storage_options(uri: str) -> Optional[dict]:
    """Get storage options for S3 URIs."""
    if uri.startswith("s3://"):
        from _internal.utils.remote import get_lance_storage_options

        bucket = uri[5:].split("/")[0]
        return get_lance_storage_options(bucket)
    return None


@operator(LanceTableSourceConfig)
class LanceTableSource(SourceOperator):
    """Source operator for reading from Lance tables."""

    def __init__(self, config: LanceTableSourceConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        if not config.dataset_uri:
            raise ValueError("dataset_uri is required for LanceTableSource")
        self.dataset_uri: str = config.dataset_uri
        self.storage_options = _get_lance_storage_options(self.dataset_uri)

    def read(self, split: Split) -> Optional[SplitPayload]:
        """Read data for a split from the Lance dataset."""
        dataset = lance.dataset(self.dataset_uri, storage_options=self.storage_options)

        # Get split metadata from data_range
        data_range = dict(split.data_range)  # Make a copy to avoid modifying original
        fragment_id = data_range.pop("fragment_id")

        fragment = dataset.get_fragment(fragment_id)
        if fragment is None:
            raise ValueError(f"Fragment {fragment_id} not found in dataset")
        fragment_scanner = fragment.scanner(
            **data_range,
            with_row_id=True,
        )
        table = fragment_scanner.to_table()
        if table.num_rows == 0:
            return SplitPayload.empty(split_id=f"{split.split_id}:{self.worker_id}")

        return SplitPayload.from_arrow(
            table,
            split_id=f"{split.split_id}:{self.worker_id}",
        )

    def close(self) -> None:
        self.dataset_uri = None  # type: ignore[assignment]


class LanceSplitPlanner:
    """Plans splits from Lance dataset fragments.

    Implements the SplitPlanner protocol. Created by
    LanceTableSourceConfig.create_source().
    """

    def __init__(self, config: LanceTableSourceConfig):
        self._config = config
        self._logger = logging.getLogger("LanceSplitPlanner")

    def plan_splits(self, stage_id: str) -> Iterator[Split]:
        """Plan splits based on Lance dataset fragments.

        Generates one split per (fragment, offset) pair, ensuring
        deterministic split ordering based on fragment_id.

        If max_rows is set, stops generating splits once the limit is reached.
        """
        storage_options = _get_lance_storage_options(self._config.dataset_uri)
        dataset = lance.dataset(self._config.dataset_uri, storage_options=storage_options)

        sorted_fragments = sorted(dataset.get_fragments(), key=lambda x: x.fragment_id)

        split_idx = 0
        total_rows_planned = 0

        for frag in sorted_fragments:
            row_count = frag.count_rows()
            for offset in range(0, row_count, self._config.split_size):
                rows_in_split = min(self._config.split_size, row_count - offset)

                if self._config.max_rows is not None:
                    remaining = self._config.max_rows - total_rows_planned
                    if remaining <= 0:
                        self._logger.info(
                            f"Planned {split_idx} splits ({total_rows_planned} rows, "
                            f"limited by max_rows={self._config.max_rows}) "
                            f"from {len(sorted_fragments)} fragments"
                        )
                        return
                    rows_in_split = min(rows_in_split, remaining)

                yield Split(
                    split_id=f"split_{split_idx}",
                    stage_id=stage_id,
                    data_range={
                        "filter": self._config.filter,
                        "columns": list(self._config.columns) if self._config.columns else None,
                        "fragment_id": frag.fragment_id,
                        "offset": offset,
                        "limit": rows_in_split,
                    },
                )
                split_idx += 1
                total_rows_planned += rows_in_split

                if (
                    self._config.max_rows is not None
                    and total_rows_planned >= self._config.max_rows
                ):
                    self._logger.info(
                        f"Planned {split_idx} splits ({total_rows_planned} rows, "
                        f"limited by max_rows={self._config.max_rows}) "
                        f"from {len(sorted_fragments)} fragments"
                    )
                    return

        self._logger.info(f"Planned {split_idx} splits from {len(sorted_fragments)} fragments")

    def cleanup(self) -> None:
        pass
