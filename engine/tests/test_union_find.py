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

"""Tests for Union-Find data structure."""

from _internal.utils.union_find import UnionFind


class TestUnionFindBasic:
    """Basic Union-Find operations."""

    def test_empty(self):
        uf = UnionFind()
        assert len(uf) == 0
        assert uf.num_components == 0

    def test_single_element(self):
        uf = UnionFind()
        assert uf.find("a") == "a"
        assert len(uf) == 1
        assert uf.num_components == 1

    def test_union_two(self):
        uf = UnionFind()
        assert uf.union("a", "b") is True
        assert len(uf) == 2
        assert uf.num_components == 1
        assert uf.find("a") == uf.find("b")

    def test_union_idempotent(self):
        uf = UnionFind()
        assert uf.union("a", "b") is True
        assert uf.union("a", "b") is False  # Already same component
        assert uf.num_components == 1

    def test_union_chain(self):
        """Test A-B, B-C, C-D -> all in same component."""
        uf = UnionFind()
        uf.union("a", "b")
        uf.union("b", "c")
        uf.union("c", "d")
        assert uf.num_components == 1
        assert uf.find("a") == uf.find("d")

    def test_two_components(self):
        uf = UnionFind()
        uf.union("a", "b")
        uf.union("c", "d")
        assert uf.num_components == 2
        assert uf.find("a") == uf.find("b")
        assert uf.find("c") == uf.find("d")
        assert uf.find("a") != uf.find("c")

    def test_merge_components(self):
        uf = UnionFind()
        uf.union("a", "b")
        uf.union("c", "d")
        assert uf.num_components == 2

        uf.union("b", "c")  # Merge the two components
        assert uf.num_components == 1
        assert uf.find("a") == uf.find("d")

    def test_connected(self):
        uf = UnionFind()
        uf.union("a", "b")
        assert uf.connected("a", "b") is True
        assert uf.connected("a", "c") is False
        assert uf.connected("x", "y") is False  # Non-existent keys

    def test_batch_union(self):
        uf = UnionFind()
        count = uf.batch_union([("a", "b"), ("c", "d"), ("a", "c")])
        assert count == 3
        assert uf.num_components == 1

    def test_batch_find(self):
        uf = UnionFind()
        uf.union("a", "b")
        uf.union("c", "d")
        results = uf.batch_find(["a", "b", "c", "d"])
        assert results[0] == results[1]  # a and b same root
        assert results[2] == results[3]  # c and d same root

    def test_many_elements(self):
        """Test with a larger number of elements."""
        uf = UnionFind()
        # Chain 1000 elements
        for i in range(999):
            uf.union(f"doc_{i}", f"doc_{i + 1}")
        assert len(uf) == 1000
        assert uf.num_components == 1
        # All should have same root
        root = uf.find("doc_0")
        assert uf.find("doc_999") == root


class TestUnionFindSerialization:
    """Checkpoint/restore via Arrow tables."""

    def test_to_arrow_empty(self):
        uf = UnionFind()
        table = uf.to_arrow()
        assert table.num_rows == 0

    def test_roundtrip_simple(self):
        uf = UnionFind()
        uf.union("a", "b")
        uf.union("c", "d")

        # Serialize
        table = uf.to_arrow()
        assert table.num_rows == 4

        # Restore
        uf2 = UnionFind.from_arrow(table)
        assert len(uf2) == 4
        assert uf2.num_components == 2
        assert uf2.connected("a", "b")
        assert uf2.connected("c", "d")
        assert not uf2.connected("a", "c")

    def test_roundtrip_chain(self):
        uf = UnionFind()
        uf.union("a", "b")
        uf.union("b", "c")
        uf.union("c", "d")

        table = uf.to_arrow()
        uf2 = UnionFind.from_arrow(table)

        assert uf2.num_components == 1
        assert uf2.find("a") == uf2.find("d")

    def test_roundtrip_preserves_structure(self):
        uf = UnionFind()
        for i in range(100):
            uf.union(f"doc_{i}", f"doc_{i + 1}")

        table = uf.to_arrow()
        uf2 = UnionFind.from_arrow(table)

        assert len(uf2) == 101
        assert uf2.num_components == 1
        assert uf2.find("doc_0") == uf2.find("doc_100")


class TestUnionFindExport:
    """Export cluster mappings."""

    def test_export_empty(self):
        uf = UnionFind()
        table = uf.export_clusters()
        assert table.num_rows == 0

    def test_export_single_component(self):
        uf = UnionFind()
        uf.union("a", "b")
        uf.union("b", "c")

        table = uf.export_clusters()
        assert table.num_rows == 3

        doc_ids = table.column("doc_id").to_pylist()
        cluster_ids = table.column("cluster_id").to_pylist()

        # All should have the same cluster_id
        assert len(set(cluster_ids)) == 1

        # All doc_ids should be present
        assert set(doc_ids) == {"a", "b", "c"}

    def test_export_two_components(self):
        uf = UnionFind()
        uf.union("a", "b")
        uf.union("c", "d")

        table = uf.export_clusters()
        clusters = dict(
            zip(
                table.column("doc_id").to_pylist(),
                table.column("cluster_id").to_pylist(),
            )
        )

        assert clusters["a"] == clusters["b"]
        assert clusters["c"] == clusters["d"]
        assert clusters["a"] != clusters["c"]


class TestUnionFindMerge:
    """Merge two Union-Find instances."""

    def test_merge_disjoint(self):
        uf1 = UnionFind()
        uf1.union("a", "b")

        uf2 = UnionFind()
        uf2.union("c", "d")

        count = uf1.merge(uf2)
        assert count == 1  # One new union: c -> d
        assert uf1.connected("c", "d")
        assert not uf1.connected("a", "c")

    def test_merge_overlapping(self):
        uf1 = UnionFind()
        uf1.union("a", "b")

        uf2 = UnionFind()
        uf2.union("b", "c")

        count = uf1.merge(uf2)
        assert count == 1  # b -> c creates new union
        assert uf1.connected("a", "c")
        assert uf1.num_components == 1

    def test_merge_already_connected(self):
        uf1 = UnionFind()
        uf1.union("a", "b")
        uf1.union("b", "c")

        uf2 = UnionFind()
        uf2.union("a", "c")  # Already connected in uf1

        count = uf1.merge(uf2)
        assert count == 0  # No new unions
