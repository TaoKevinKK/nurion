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

"""Deduplication operators for removing duplicate records.

This module provides operators for deduplicating data:

1. **HashDedupeOperator**: Exact deduplication by key columns
   - Shuffles data by dedup key
   - Deduplicates within batch using DuckDB
   - Future: Cross-batch dedup via WorkQueue state API

Architecture for HashDedupe (WorkQueue model, Jan 2025):
    Input -> Shuffle by dedup_keys -> HashDedupeOperator -> Deduplicated Output

Design rationale for 10B+ scale:
- Batch-level dedup via DuckDB (efficient, in-memory)
- Shuffle ensures same keys go to same partition
- Cross-batch dedup via WorkQueue state API (future)
- No local SlateDB state store needed

For exact cross-batch deduplication at scale, use MinHash + CC flow
which handles 10B+ records via payload-based iteration.
"""

from dataclasses import dataclass, field
from typing import List, Optional

import pyarrow as pa

from solstice.core.operator import OperatorRuntime, operator
from solstice.operators.shuffle import ShuffleOperator, ShuffleOperatorConfig


@dataclass
class HashDedupeConfig(ShuffleOperatorConfig):
    """Configuration for exact hash-based deduplication.

    Deduplicates records by computing a hash of the specified key columns.
    Records with the same key hash are considered duplicates.

    Attributes:
        dedup_keys: Columns that define uniqueness (same as partition_keys)
        keep: Which duplicate to keep ("first" or "last")
    """

    dedup_keys: List[str] = field(default_factory=list)
    keep: str = "first"  # "first" or "last"

    def __post_init__(self):
        # dedup_keys are also partition_keys for shuffle
        if self.dedup_keys and not self.partition_keys:
            self.partition_keys = self.dedup_keys


@operator(HashDedupeConfig)
class HashDedupeOperator(ShuffleOperator):
    """Operator for exact hash-based deduplication.

    This operator:
    1. Shuffles data by dedup keys (handled by ShuffleOperator base)
    2. Uses DuckDB for efficient batch-level deduplication
    3. Outputs deduplicated records

    WorkQueue-based design (no local state store):
    - Batch-level dedup via DuckDB (efficient, handles most cases)
    - Shuffle ensures same keys go to same partition
    - For exact cross-batch dedup at 10B+ scale, use MinHash + CC flow

    Future: Cross-batch dedup via WorkQueue state API:
    - state_get(key_hash) to check if seen
    - atomic ack + state_put(key_hash) to mark as seen

    Example:
        config = HashDedupeConfig(dedup_keys=["user_id", "event_id"])
        stage = Stage("dedupe", config, parallelism=8)
    """

    def __init__(self, config: HashDedupeConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self.dedupe_config = config

    @property
    def dedup_keys(self) -> List[str]:
        """Get the deduplication key columns."""
        return self.dedupe_config.dedup_keys

    def process_data(self, table: pa.Table) -> Optional[pa.Table]:
        """Deduplicate the input data within the batch.

        Uses DuckDB for efficient batch-level deduplication.
        Since data is shuffled by dedup keys, same keys end up in the
        same partition, making batch-level dedup effective.

        For exact cross-batch deduplication at 10B+ scale,
        use the MinHash + CC flow instead.
        """
        if not self.dedup_keys:
            # No dedup keys specified, pass through
            return table

        if table.num_rows == 0:
            return None

        # Use DuckDB for efficient deduplication within the batch
        deduped_table = self.engine.dedupe(
            table,
            key_columns=self.dedup_keys,
            keep=self.dedupe_config.keep,
        )

        if deduped_table.num_rows == 0:
            return None

        return deduped_table
