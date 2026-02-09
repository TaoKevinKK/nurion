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

"""Tests for LanceSinkCommitter, LanceCommitPolicy, and LanceSplitPlanner."""

import json
import tempfile
import time
from unittest.mock import MagicMock, Mock

import lance
import pyarrow as pa

from _internal.core.source import SplitPlanner
from _internal.operators.sinks.lance_commit import LanceCommitPolicy, LanceSinkCommitter
from _internal.operators.sources.lance import LanceSplitPlanner, LanceTableSourceConfig

# Mock queue client for _do_commit tests (no pending acks to ack)
_MOCK_QUEUE_CLIENT = Mock()
_MOCK_COMMIT_QUEUE = "test_commits"


def _fake_fragment(physical_rows: int = 100):
    """Create a mock fragment with physical_rows attribute."""
    frag = MagicMock()
    frag.physical_rows = physical_rows
    return frag


# ============================================================================
# LanceCommitPolicy Tests
# ============================================================================


class TestLanceCommitPolicy:
    """Tests for the commit policy threshold logic."""

    def test_default_values(self):
        policy = LanceCommitPolicy()
        assert policy.interval_s == 30.0
        assert policy.fragment_threshold == 10
        assert policy.row_threshold == 100_000

    def test_custom_values(self):
        policy = LanceCommitPolicy(
            interval_s=5.0,
            fragment_threshold=3,
            row_threshold=50_000,
        )
        assert policy.interval_s == 5.0
        assert policy.fragment_threshold == 3
        assert policy.row_threshold == 50_000


class TestLanceSinkCommitter:
    """Tests for the LanceSinkCommitter."""

    def test_should_commit_empty(self):
        """No commit when no fragments pending."""
        committer = LanceSinkCommitter(table_path="/tmp/test.lance")
        assert not committer._should_commit()

    def test_should_commit_fragment_threshold(self):
        """Commit when fragment count reaches threshold."""
        policy = LanceCommitPolicy(
            fragment_threshold=2,
            interval_s=9999,
            row_threshold=0,
        )
        committer = LanceSinkCommitter(table_path="/tmp/test.lance", policy=policy)

        # Add fake fragments
        committer._pending_fragments = [_fake_fragment(), _fake_fragment()]
        assert committer._should_commit()

    def test_should_commit_row_threshold(self):
        """Commit when row count reaches threshold."""
        policy = LanceCommitPolicy(
            fragment_threshold=9999,
            interval_s=9999,
            row_threshold=100,
        )
        committer = LanceSinkCommitter(table_path="/tmp/test.lance", policy=policy)

        committer._pending_fragments = [_fake_fragment(physical_rows=100)]
        assert committer._should_commit()

    def test_should_commit_time_threshold(self):
        """Commit when time interval elapsed."""
        policy = LanceCommitPolicy(
            fragment_threshold=9999,
            interval_s=0.01,
            row_threshold=0,
        )
        committer = LanceSinkCommitter(table_path="/tmp/test.lance", policy=policy)

        committer._pending_fragments = [_fake_fragment()]
        committer._last_commit_time = time.time() - 1.0
        assert committer._should_commit()

    def test_should_not_commit_below_all_thresholds(self):
        """No commit when all thresholds are unmet."""
        policy = LanceCommitPolicy(
            fragment_threshold=10,
            interval_s=9999,
            row_threshold=100_000,
        )
        committer = LanceSinkCommitter(table_path="/tmp/test.lance", policy=policy)

        committer._pending_fragments = [_fake_fragment(physical_rows=50)]
        committer._last_commit_time = time.time()
        assert not committer._should_commit()

    def test_parse_fragment(self):
        """Test parsing fragment metadata from queue records."""
        committer = LanceSinkCommitter(table_path="/tmp/test.lance")

        with tempfile.TemporaryDirectory() as tmpdir:
            table_path = f"{tmpdir}/test.lance"
            table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            from lance.fragment import write_fragments

            fragments = write_fragments(table, table_path, schema=table.schema)

            for frag in fragments:
                payload = json.dumps(frag.to_json()).encode()
                parsed = committer._parse_fragment(payload)
                assert parsed is not None
                committer._pending_fragments.append(parsed)

            assert len(committer._pending_fragments) == len(fragments)

    def test_do_commit_creates_dataset(self):
        """Test that _do_commit creates a Lance dataset with accumulated fragments."""
        with tempfile.TemporaryDirectory() as tmpdir:
            table_path = f"{tmpdir}/test.lance"

            # Write fragments without committing
            table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            from lance.fragment import write_fragments

            fragments = write_fragments(table, table_path, schema=table.schema)

            # Set up committer
            committer = LanceSinkCommitter(table_path=table_path, mode="create")
            committer._pending_fragments = list(fragments)
            committer._schema = table.schema

            # Commit (pass mock queue_client since no pending acks)
            committer._do_commit(_MOCK_QUEUE_CLIENT, _MOCK_COMMIT_QUEUE)

            # Verify dataset was created
            ds = lance.dataset(table_path)
            assert ds.count_rows() == 3
            assert committer._read_version == ds.version

    def test_do_commit_append_mode(self):
        """Test multiple commits in append mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            table_path = f"{tmpdir}/test.lance"

            # First batch
            table1 = pa.table({"x": [1, 2, 3]})
            from lance.fragment import write_fragments

            frags1 = write_fragments(table1, table_path, schema=table1.schema)

            committer = LanceSinkCommitter(table_path=table_path, mode="create")
            committer._pending_fragments = list(frags1)
            committer._schema = table1.schema
            committer._do_commit(_MOCK_QUEUE_CLIENT, _MOCK_COMMIT_QUEUE)

            assert committer._read_version is not None

            # Second batch (append)
            frags2 = write_fragments(pa.table({"x": [4, 5, 6]}), table_path, schema=table1.schema)
            committer._pending_fragments = list(frags2)
            committer._do_commit(_MOCK_QUEUE_CLIENT, _MOCK_COMMIT_QUEUE)

            # Verify all data
            ds = lance.dataset(table_path)
            assert ds.count_rows() == 6


# ============================================================================
# LanceSplitPlanner Tests
# ============================================================================


class TestLanceSplitPlanner:
    """Tests for the LanceSplitPlanner."""

    def test_implements_protocol(self):
        """LanceSplitPlanner satisfies the SplitPlanner protocol."""
        config = LanceTableSourceConfig(dataset_uri="/tmp/nonexistent.lance")
        planner = LanceSplitPlanner(config)
        assert isinstance(planner, SplitPlanner)

    def test_plan_splits_basic(self):
        """Test basic split planning from a Lance dataset."""
        with tempfile.TemporaryDirectory() as tmpdir:
            table_path = f"{tmpdir}/test.lance"

            # Create a test dataset with known data
            table = pa.table({"id": list(range(100)), "value": [f"v{i}" for i in range(100)]})
            lance.write_dataset(table, table_path)

            config = LanceTableSourceConfig(
                dataset_uri=table_path,
                split_size=30,
            )
            planner = LanceSplitPlanner(config)
            splits = list(planner.plan_splits("test_source"))

            # Should have enough splits to cover 100 rows with split_size=30
            assert len(splits) >= 1
            total_limit = sum(s.data_range.get("limit", 0) for s in splits)
            assert total_limit == 100

    def test_plan_splits_with_max_rows(self):
        """Test split planning respects max_rows limit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            table_path = f"{tmpdir}/test.lance"

            table = pa.table({"id": list(range(100))})
            lance.write_dataset(table, table_path)

            config = LanceTableSourceConfig(
                dataset_uri=table_path,
                split_size=30,
                max_rows=50,
            )
            planner = LanceSplitPlanner(config)
            splits = list(planner.plan_splits("test_source"))

            total_limit = sum(s.data_range.get("limit", 0) for s in splits)
            assert total_limit == 50

    def test_create_source_returns_planner(self):
        """Test that LanceTableSourceConfig.create_source() returns a LanceSplitPlanner."""
        config = LanceTableSourceConfig(dataset_uri="/tmp/test.lance")
        source = config.create_source()
        assert isinstance(source, LanceSplitPlanner)
        assert isinstance(source, SplitPlanner)
