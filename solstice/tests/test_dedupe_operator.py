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

"""Tests for deduplication operators."""

import pyarrow as pa
import pytest

from tests.conftest import make_operator_runtime
from solstice.core.models import Split, SplitPayload
from solstice.operators.dedupe import (
    HashDedupeConfig,
)
from solstice.operators.shuffle import ShuffleOperator


class TestHashDedupeOperator:
    """Tests for HashDedupeOperator.

    The operator performs batch-level deduplication using DuckDB.
    Since data is shuffled by dedup keys, same keys end up in the same
    partition, making batch-level dedup effective for most cases.

    For exact cross-batch deduplication at 10B+ scale,
    use the MinHash + CC flow instead.
    """

    @pytest.fixture
    def sample_table_with_dupes(self):
        """Create a sample table with duplicates."""
        return pa.table(
            {
                "user_id": [1, 2, 1, 3, 2, 1],
                "event_id": ["a", "b", "a", "c", "b", "d"],
                "value": [10, 20, 30, 40, 50, 60],
            }
        )

    @pytest.fixture
    def sample_payload(self, sample_table_with_dupes):
        """Create a sample payload."""
        return SplitPayload(data=sample_table_with_dupes, split_id="test")

    @pytest.fixture
    def sample_split(self):
        """Create a sample split."""
        return Split(split_id="test", stage_id="dedupe", data_range={})

    def test_dedupe_single_key(self, sample_split, sample_payload):
        """Test deduplication by single key."""
        config = HashDedupeConfig(dedup_keys=["user_id"], num_partitions=4)
        operator = config.setup(make_operator_runtime())

        result = operator.process_split(sample_split, sample_payload)

        assert result is not None
        table = result.to_table()

        # Remove partition column for checking
        if ShuffleOperator.PARTITION_COLUMN in table.column_names:
            table = table.drop([ShuffleOperator.PARTITION_COLUMN])

        # Should have 3 unique user_ids
        assert table.num_rows == 3
        user_ids = set(table.column("user_id").to_pylist())
        assert user_ids == {1, 2, 3}

        operator.close()

    def test_dedupe_multiple_keys(self, sample_split, sample_payload):
        """Test deduplication by multiple keys."""
        config = HashDedupeConfig(dedup_keys=["user_id", "event_id"], num_partitions=4)
        operator = config.setup(make_operator_runtime())

        result = operator.process_split(sample_split, sample_payload)

        assert result is not None
        table = result.to_table()

        if ShuffleOperator.PARTITION_COLUMN in table.column_names:
            table = table.drop([ShuffleOperator.PARTITION_COLUMN])

        # Should have 5 unique (user_id, event_id) combinations
        # (1, a), (2, b), (3, c), (1, d) = 4 unique, but (1, a) appears twice
        # and (2, b) appears twice
        assert table.num_rows == 4

        operator.close()

    def test_dedupe_no_duplicates(self, sample_split):
        """Test with data that has no duplicates."""
        table = pa.table(
            {
                "user_id": [1, 2, 3, 4],
                "value": [10, 20, 30, 40],
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = HashDedupeConfig(dedup_keys=["user_id"], num_partitions=4)
        operator = config.setup(make_operator_runtime())

        result = operator.process_split(sample_split, payload)

        assert result is not None
        result_table = result.to_table()

        if ShuffleOperator.PARTITION_COLUMN in result_table.column_names:
            result_table = result_table.drop([ShuffleOperator.PARTITION_COLUMN])

        assert result_table.num_rows == 4

        operator.close()

    def test_dedupe_all_duplicates(self, sample_split):
        """Test with data where all rows are duplicates."""
        table = pa.table(
            {
                "user_id": [1, 1, 1, 1],
                "value": [10, 20, 30, 40],
            }
        )
        payload = SplitPayload(data=table, split_id="test")

        config = HashDedupeConfig(dedup_keys=["user_id"], num_partitions=4)
        operator = config.setup(make_operator_runtime())

        result = operator.process_split(sample_split, payload)

        assert result is not None
        result_table = result.to_table()

        if ShuffleOperator.PARTITION_COLUMN in result_table.column_names:
            result_table = result_table.drop([ShuffleOperator.PARTITION_COLUMN])

        assert result_table.num_rows == 1

        operator.close()

    def test_dedupe_empty_payload(self, sample_split):
        """Test with empty payload."""
        config = HashDedupeConfig(dedup_keys=["user_id"], num_partitions=4)
        operator = config.setup(make_operator_runtime())

        result = operator.process_split(sample_split, None)
        assert result is None

        operator.close()

    def test_dedupe_batch_level_only(self, sample_split):
        """Test that deduplication is batch-level only.

        Each batch is deduplicated independently. Cross-batch deduplication
        requires using MinHash + CC flow for 10B+ scale scenarios.
        """
        config = HashDedupeConfig(dedup_keys=["user_id"], num_partitions=4)
        operator = config.setup(make_operator_runtime())

        # First batch
        table1 = pa.table(
            {
                "user_id": [1, 2],
                "value": [10, 20],
            }
        )
        payload1 = SplitPayload(data=table1, split_id="test1")
        result1 = operator.process_split(sample_split, payload1)

        # Second batch with overlapping keys
        table2 = pa.table(
            {
                "user_id": [2, 3],  # user_id=2 would be duplicate in full dataset
                "value": [30, 40],
            }
        )
        payload2 = SplitPayload(data=table2, split_id="test2")
        result2 = operator.process_split(sample_split, payload2)

        # First batch should have both rows
        assert result1 is not None
        r1_table = result1.to_table()
        if ShuffleOperator.PARTITION_COLUMN in r1_table.column_names:
            r1_table = r1_table.drop([ShuffleOperator.PARTITION_COLUMN])
        assert r1_table.num_rows == 2

        # Second batch - batch-level dedup only, so both rows pass
        assert result2 is not None
        r2_table = result2.to_table()
        if ShuffleOperator.PARTITION_COLUMN in r2_table.column_names:
            r2_table = r2_table.drop([ShuffleOperator.PARTITION_COLUMN])
        assert r2_table.num_rows == 2

        operator.close()

    def test_dedupe_partition_keys_set(self):
        """Test that partition_keys are set from dedup_keys."""
        config = HashDedupeConfig(dedup_keys=["user_id", "event_id"])
        assert config.partition_keys == ["user_id", "event_id"]

    def test_dedupe_is_shuffle_operator(self):
        """Test that HashDedupeOperator is a ShuffleOperator."""
        config = HashDedupeConfig(dedup_keys=["user_id"])
        operator = config.setup(make_operator_runtime())
        assert isinstance(operator, ShuffleOperator)
        operator.close()
