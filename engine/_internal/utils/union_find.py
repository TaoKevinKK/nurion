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

"""Efficient Union-Find (Disjoint Set Union) data structure.

Features:
- Union by rank + path compression -> O(alpha(n)) amortized per operation
- Checkpoint/restore via Arrow Table + SplitPayloadStore
- String doc_id support via internal integer mapping
- Batch operations for high-throughput distributed usage

This is the core building block for the Union-Find Service architecture,
used by UFShard actors to manage cluster membership.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import pyarrow as pa

if TYPE_CHECKING:
    from _internal.core.split_payload_store import SplitPayloadStore


class UnionFind:
    """Union-Find with union-by-rank and path compression.

    Supports string keys via an internal string -> int mapping.
    All operations are O(alpha(n)) amortized.

    Usage:
        uf = UnionFind()
        uf.union("doc_a", "doc_b")
        uf.union("doc_b", "doc_c")
        assert uf.find("doc_a") == uf.find("doc_c")
        assert uf.num_components() == 1
    """

    def __init__(self) -> None:
        # Internal integer-indexed arrays for performance
        self._parent: list[int] = []
        self._rank: list[int] = []

        # String key <-> int index mapping
        self._key_to_idx: dict[str, int] = {}
        self._idx_to_key: list[str] = []

        # Track number of distinct components
        self._num_components: int = 0

    def __len__(self) -> int:
        """Return total number of elements."""
        return len(self._parent)

    @property
    def num_components(self) -> int:
        """Return the number of distinct components."""
        return self._num_components

    def _ensure_key(self, key: str) -> int:
        """Get or create an integer index for a string key."""
        idx = self._key_to_idx.get(key)
        if idx is not None:
            return idx
        idx = len(self._parent)
        self._key_to_idx[key] = idx
        self._idx_to_key.append(key)
        self._parent.append(idx)
        self._rank.append(0)
        self._num_components += 1
        return idx

    def _find_idx(self, idx: int) -> int:
        """Find root with path compression (iterative)."""
        root = idx
        while self._parent[root] != root:
            root = self._parent[root]
        # Path compression
        while self._parent[idx] != root:
            next_idx = self._parent[idx]
            self._parent[idx] = root
            idx = next_idx
        return root

    def find(self, key: str) -> str:
        """Find the root representative for a key.

        Creates the element if it doesn't exist yet.

        Args:
            key: Element key

        Returns:
            Root representative key for the component
        """
        idx = self._ensure_key(key)
        root_idx = self._find_idx(idx)
        return self._idx_to_key[root_idx]

    def union(self, key_a: str, key_b: str) -> bool:
        """Union two elements.

        Args:
            key_a: First element key
            key_b: Second element key

        Returns:
            True if a new union was performed (elements were in different components),
            False if they were already in the same component.
        """
        idx_a = self._ensure_key(key_a)
        idx_b = self._ensure_key(key_b)

        root_a = self._find_idx(idx_a)
        root_b = self._find_idx(idx_b)

        if root_a == root_b:
            return False

        # Union by rank, with deterministic tie-breaking by key lexicographic order
        rank_a = self._rank[root_a]
        rank_b = self._rank[root_b]
        
        if rank_a < rank_b:
            self._parent[root_a] = root_b
        elif rank_a > rank_b:
            self._parent[root_b] = root_a
        else:
            # Ranks equal: use lexicographic order for deterministic root selection
            # Always make the smaller key the root
            key_root_a = self._idx_to_key[root_a]
            key_root_b = self._idx_to_key[root_b]
            if key_root_a < key_root_b:
                self._parent[root_b] = root_a
                self._rank[root_a] += 1
            else:
                self._parent[root_a] = root_b
                self._rank[root_b] += 1

        self._num_components -= 1
        return True

    def batch_union(self, pairs: list[tuple[str, str]]) -> int:
        """Union multiple pairs at once.

        Args:
            pairs: List of (key_a, key_b) pairs to union

        Returns:
            Number of new unions performed
        """
        count = 0
        for a, b in pairs:
            if self.union(a, b):
                count += 1
        return count

    def batch_find(self, keys: list[str]) -> list[str]:
        """Find root representatives for multiple keys.

        Args:
            keys: List of keys to find

        Returns:
            List of root representative keys (same order as input)
        """
        return [self.find(k) for k in keys]

    def connected(self, key_a: str, key_b: str) -> bool:
        """Check if two elements are in the same component.

        Args:
            key_a: First element key
            key_b: Second element key

        Returns:
            True if both elements exist and are in the same component
        """
        if key_a not in self._key_to_idx or key_b not in self._key_to_idx:
            return False
        root_a = self._find_idx(self._key_to_idx[key_a])
        root_b = self._find_idx(self._key_to_idx[key_b])
        return root_a == root_b

    def export_clusters(self) -> pa.Table:
        """Export all (key, cluster_id) mappings as an Arrow Table.

        Performs full path compression first to ensure all cluster_ids
        are canonical roots.

        Returns:
            Arrow Table with columns: (doc_id: string, cluster_id: string)
        """
        keys: list[str] = []
        cluster_ids: list[str] = []

        for idx in range(len(self._parent)):
            root_idx = self._find_idx(idx)
            keys.append(self._idx_to_key[idx])
            cluster_ids.append(self._idx_to_key[root_idx])

        return pa.table({"doc_id": keys, "cluster_id": cluster_ids})

    def to_arrow(self) -> pa.Table:
        """Serialize the full Union-Find state to an Arrow Table.

        This captures the internal structure for checkpoint/restore,
        including parent pointers and ranks.

        Returns:
            Arrow Table with columns: (key: string, parent_key: string, rank: int32)
        """
        keys: list[str] = []
        parent_keys: list[str] = []
        ranks: list[int] = []

        for idx in range(len(self._parent)):
            keys.append(self._idx_to_key[idx])
            parent_keys.append(self._idx_to_key[self._parent[idx]])
            ranks.append(self._rank[idx])

        return pa.table(
            {
                "key": keys,
                "parent_key": parent_keys,
                "rank": pa.array(ranks, type=pa.int32()),
            }
        )

    @classmethod
    def from_arrow(cls, table: pa.Table) -> "UnionFind":
        """Restore Union-Find state from an Arrow Table checkpoint.

        Args:
            table: Arrow Table with columns (key, parent_key, rank)

        Returns:
            Restored UnionFind instance
        """
        uf = cls()

        keys = table.column("key").to_pylist()
        parent_keys = table.column("parent_key").to_pylist()
        ranks = table.column("rank").to_pylist()

        # First pass: register all keys to build the index mapping
        for key in keys:
            uf._ensure_key(key)

        # Second pass: restore parent pointers and ranks
        for key, parent_key, rank in zip(keys, parent_keys, ranks):
            idx = uf._key_to_idx[key]
            parent_idx = uf._key_to_idx[parent_key]
            uf._parent[idx] = parent_idx
            uf._rank[idx] = rank

        # Recompute num_components by counting unique roots
        roots = set()
        for idx in range(len(uf._parent)):
            roots.add(uf._find_idx(idx))
        uf._num_components = len(roots)

        return uf

    def checkpoint(self, store: "SplitPayloadStore", key: str) -> str:
        """Checkpoint Union-Find state to a PayloadStore.

        Args:
            store: PayloadStore instance (Ray or S3 backed)
            key: Storage key for the checkpoint

        Returns:
            The storage key
        """
        from _internal.core.models import SplitPayload

        table = self.to_arrow()
        payload = SplitPayload(data=table, split_id=key)
        return store.store(key, payload)

    @classmethod
    def restore(cls, store: "SplitPayloadStore", key: str) -> Optional["UnionFind"]:
        """Restore Union-Find state from a PayloadStore checkpoint.

        Args:
            store: PayloadStore instance
            key: Storage key for the checkpoint

        Returns:
            Restored UnionFind instance, or None if checkpoint not found
        """
        payload = store.get(key)
        if payload is None:
            return None
        return cls.from_arrow(payload.to_table())

    def merge(self, other: "UnionFind") -> int:
        """Merge another UnionFind into this one.

        For each connected pair in `other`, performs union in `self`.
        This is used for cross-shard merging.

        Args:
            other: Another UnionFind to merge from

        Returns:
            Number of new unions performed
        """
        count = 0
        # Export clusters from other and union them here
        for idx in range(len(other._parent)):
            key = other._idx_to_key[idx]
            root_idx = other._find_idx(idx)
            root_key = other._idx_to_key[root_idx]
            if key != root_key:
                if self.union(key, root_key):
                    count += 1
        return count
