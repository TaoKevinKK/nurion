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

"""Generic Arrow table partitioning utilities.

This module provides partition-aware table splitting used by StageWorker
to route output rows to per-partition queues. It has no dependency on any
specific operator — the partition column name is supplied by the caller.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc


def split_table_by_column(table: pa.Table, column: str) -> dict[int, pa.Table]:
    """Split an Arrow table by an integer column, dropping that column from output.

    Args:
        table: Input table containing *column* with integer partition IDs.
        column: Name of the integer column to split on.

    Returns:
        Mapping from partition ID to the subset table (without *column*).

    Raises:
        ValueError: If *column* is not present in *table*.
    """
    if column not in table.column_names:
        raise ValueError(f"Table missing {column} column")

    partition_col = table.column(column)
    unique_ids = pc.unique(partition_col).to_pylist()

    result: dict[int, pa.Table] = {}
    for pid in unique_ids:
        mask = pc.equal(partition_col, pid)
        result[pid] = table.filter(mask).drop([column])

    return result
