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

"""Unit tests for FsspecSplitPayloadStore."""

from __future__ import annotations

import pyarrow as pa

from _internal.core.models import SplitPayload
from _internal.core.split_payload_store import (
    FsspecSplitPayloadStore,
    _sanitize_key,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_payload(split_id: str = "test_split", num_rows: int = 5) -> SplitPayload:
    """Create a simple SplitPayload for testing."""
    table = pa.table(
        {
            "id": list(range(num_rows)),
            "name": [f"row_{i}" for i in range(num_rows)],
            "value": [float(i) * 1.5 for i in range(num_rows)],
        }
    )
    return SplitPayload(data=table, split_id=split_id)


def _make_store(tmp_path) -> FsspecSplitPayloadStore:
    """Create a FsspecSplitPayloadStore backed by a local tmpdir."""
    return FsspecSplitPayloadStore(
        base_uri=f"file://{tmp_path}",
        job_id="test_job",
    )


class TestSanitizeKey:
    """Tests for _sanitize_key."""

    def test_colons_replaced(self):
        assert _sanitize_key("job1:stage1:split_0") == "job1_stage1_split_0"

    def test_slashes_replaced(self):
        assert _sanitize_key("a/b/c") == "a_b_c"

    def test_mixed(self):
        assert _sanitize_key("job:stage/split:0") == "job_stage_split_0"

    def test_plain_key(self):
        assert _sanitize_key("simple_key_123") == "simple_key_123"


# ---------------------------------------------------------------------------
# FsspecSplitPayloadStore
# ---------------------------------------------------------------------------


class TestFsspecSplitPayloadStore:
    """Tests for FsspecSplitPayloadStore using file:// backend."""

    def test_store_and_get(self, tmp_path):
        store = _make_store(tmp_path)
        payload = _make_payload(split_id="k1", num_rows=3)

        result_key = store.store("k1", payload)
        assert result_key == "k1"

        retrieved = store.get("k1")
        assert retrieved is not None
        assert retrieved.split_id == "k1"
        assert retrieved.data.num_rows == 3
        assert retrieved.data.equals(payload.data)

    def test_get_missing_key(self, tmp_path):
        store = _make_store(tmp_path)
        assert store.get("nonexistent") is None

    def test_delete_existing(self, tmp_path):
        store = _make_store(tmp_path)
        store.store("k1", _make_payload())

        assert store.delete("k1") is True
        assert store.get("k1") is None

    def test_delete_missing(self, tmp_path):
        store = _make_store(tmp_path)
        assert store.delete("nonexistent") is False

    def test_clear(self, tmp_path):
        store = _make_store(tmp_path)
        store.store("k1", _make_payload(split_id="k1"))
        store.store("k2", _make_payload(split_id="k2"))
        store.store("k3", _make_payload(split_id="k3"))

        count = store.clear()
        assert count == 3

        assert store.get("k1") is None
        assert store.get("k2") is None
        assert store.get("k3") is None

    def test_clear_empty(self, tmp_path):
        store = _make_store(tmp_path)
        assert store.clear() == 0

    def test_key_sanitization(self, tmp_path):
        """Keys with colons and slashes should be stored and retrieved correctly."""
        store = _make_store(tmp_path)
        key = "job1:stage1:split_0"
        payload = _make_payload(split_id=key)

        store.store(key, payload)
        retrieved = store.get(key)
        assert retrieved is not None
        assert retrieved.split_id == key
        assert retrieved.data.equals(payload.data)

    def test_overwrite(self, tmp_path):
        """Storing the same key twice should overwrite the previous value."""
        store = _make_store(tmp_path)

        payload_v1 = _make_payload(split_id="k1", num_rows=2)
        payload_v2 = _make_payload(split_id="k1", num_rows=7)

        store.store("k1", payload_v1)
        store.store("k1", payload_v2)

        retrieved = store.get("k1")
        assert retrieved is not None
        assert retrieved.data.num_rows == 7

    def test_multiple_keys(self, tmp_path):
        store = _make_store(tmp_path)
        for i in range(10):
            store.store(f"key_{i}", _make_payload(split_id=f"key_{i}", num_rows=i + 1))

        for i in range(10):
            retrieved = store.get(f"key_{i}")
            assert retrieved is not None
            assert retrieved.data.num_rows == i + 1

    def test_get_metrics(self, tmp_path):
        store = _make_store(tmp_path)
        store.store("k1", _make_payload())
        store.store("k2", _make_payload())
        store.delete("k1")

        metrics = store.get_metrics()
        assert metrics["total_stored"] == 2
        assert metrics["total_deleted"] == 1
        assert "fs_type" in metrics

    def test_store_with_storage_options(self, tmp_path):
        """Verify storage_options is accepted (even if empty for file://)."""
        store = FsspecSplitPayloadStore(
            base_uri=f"file://{tmp_path}",
            job_id="test_job",
            storage_options={"auto_mkdir": True},
        )
        store.store("k1", _make_payload())
        assert store.get("k1") is not None

    def test_large_payload(self, tmp_path):
        """Test with a larger payload to verify no size-related issues."""
        store = _make_store(tmp_path)
        table = pa.table(
            {
                "id": list(range(100_000)),
                "text": [f"row_data_{i}" * 10 for i in range(100_000)],
            }
        )
        payload = SplitPayload(data=table, split_id="large")

        store.store("large", payload)
        retrieved = store.get("large")
        assert retrieved is not None
        assert retrieved.data.num_rows == 100_000
        assert retrieved.data.equals(payload.data)
