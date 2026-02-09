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

"""UFShard - Ray actor holding a partition of Union-Find state.

Checkpoint design:
    The shard receives a PayloadStore handle at init (optional). When
    checkpoint_interval > 0 and a store is provided, the shard auto-
    checkpoints after every N operations by writing directly to the
    PayloadStore from within its own process. No data round-trips
    through the manager.

    On startup, the shard checks the PayloadStore for an existing
    checkpoint and restores if found.

    This follows the same pattern as StageWorker: it receives a
    PayloadStore handle and stores payloads directly.
"""

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING, Any, Optional

import pyarrow as pa

from _internal.utils.logging import create_ray_logger
from _internal.utils.union_find import UnionFind

if TYPE_CHECKING:
    from _internal.core.split_payload_store import SplitPayloadStore

logger = logging.getLogger(__name__)


def _deterministic_hash(key: str) -> int:
    """Compute deterministic hash using SHA-256.

    Must match the implementation in client.py and manager.py for consistent routing.
    """
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:16], 16)


def _ckpt_key(cluster_id: str, shard_id: int, part: str) -> str:
    """Deterministic PayloadStore key for a shard checkpoint part."""
    return f"uf_ckpt:{cluster_id}:{shard_id}:{part}"


_CKPT_PARTS = ("uf", "band_index", "cross_edges")


class UFShard:
    """One shard of a distributed Union-Find cluster.

    Deployed as a Ray actor. Optionally receives a PayloadStore for
    self-managed checkpoint/restore (no manager round-trip).
    """

    def __init__(
        self,
        shard_id: int,
        num_shards: int,
        cluster_id: str = "dedup",
        checkpoint_interval: int = 0,
        payload_store: Optional["SplitPayloadStore"] = None,
    ) -> None:
        self._shard_id = shard_id
        self._num_shards = num_shards
        self._cluster_id = cluster_id
        self._checkpoint_interval = checkpoint_interval
        self._payload_store = payload_store

        self._uf = UnionFind()
        self._cross_shard_edges: list[tuple[str, str]] = []
        self._band_hash_index: dict[int, str] = {}

        # Metrics
        self._total_unions: int = 0
        self._total_finds: int = 0
        self._total_cross_shard: int = 0
        self._total_band_matches: int = 0
        self._ops_since_checkpoint: int = 0
        self._checkpoint_count: int = 0

        self._logger = create_ray_logger(f"UFShard-{shard_id}")
        self._logger.info(
            f"UFShard-{shard_id} initialized (num_shards={num_shards}, "
            f"cluster_id={cluster_id}, "
            f"checkpoint={'every ' + str(checkpoint_interval) + ' ops' if checkpoint_interval > 0 and payload_store else 'disabled'})"
        )

        # Auto-restore from checkpoint on startup
        if payload_store:
            self._try_restore()

    # =========================================================================
    # Health
    # =========================================================================

    def ping(self) -> bool:
        return True

    # =========================================================================
    # Core operations
    # =========================================================================

    def _owns_key(self, key: str) -> bool:
        """Check if this shard owns the given key using deterministic hash."""
        return _deterministic_hash(key) % self._num_shards == self._shard_id

    def batch_union(self, pairs: list[tuple[str, str]]) -> dict[str, int]:
        """Union multiple pairs of document IDs."""
        local_unions = 0
        cross_shard = 0

        for a, b in pairs:
            owns_a = self._owns_key(a)
            owns_b = self._owns_key(b)

            if owns_a and owns_b:
                if self._uf.union(a, b):
                    local_unions += 1
            elif owns_a or owns_b:
                local_key = a if owns_a else b
                self._uf.find(local_key)
                self._cross_shard_edges.append((a, b))
                cross_shard += 1
            else:
                self._cross_shard_edges.append((a, b))
                cross_shard += 1

        self._total_unions += local_unions
        self._total_cross_shard += cross_shard
        self._ops_since_checkpoint += local_unions + cross_shard
        self._maybe_checkpoint()
        return {"local_unions": local_unions, "cross_shard": cross_shard}

    def batch_match_and_union(self, entries: list[tuple[int, str]]) -> dict[str, int]:
        """Match documents by band_hash and union matches.

        Idempotent: re-sending the same (band_hash, doc_id) is safe.
        """
        new_hashes = 0
        matches = 0
        cross_shard = 0

        for band_hash, doc_id in entries:
            existing = self._band_hash_index.get(band_hash)
            if existing is None:
                self._band_hash_index[band_hash] = doc_id
                self._uf.find(doc_id)
                new_hashes += 1
            else:
                owns_existing = self._owns_key(existing)
                owns_new = self._owns_key(doc_id)

                if owns_existing and owns_new:
                    if self._uf.union(existing, doc_id):
                        matches += 1
                elif owns_existing or owns_new:
                    self._uf.find(doc_id)
                    self._cross_shard_edges.append((existing, doc_id))
                    cross_shard += 1
                else:
                    self._cross_shard_edges.append((existing, doc_id))
                    cross_shard += 1

        self._total_unions += matches
        self._total_band_matches += matches + cross_shard
        self._total_cross_shard += cross_shard
        self._ops_since_checkpoint += matches + cross_shard + new_hashes
        self._maybe_checkpoint()
        return {"new_hashes": new_hashes, "matches": matches, "cross_shard": cross_shard}

    def batch_find(self, keys: list[str]) -> list[str]:
        """Find cluster representatives for multiple keys."""
        self._total_finds += len(keys)
        return self._uf.batch_find(keys)

    def get_cross_shard_edges(self) -> list[tuple[str, str]]:
        return list(self._cross_shard_edges)

    def resolve_cross_shard(self, global_mappings: dict[str, str]) -> int:
        """Apply global cross-shard resolution mappings.

        Important: We apply all mappings regardless of ownership, because:
        1. A doc_id owned by this shard may need to point to a global_root in another shard
        2. The global_root will be created as a stub if it doesn't exist locally
        3. During export, we only export doc_ids we own, preserving the global_root reference
        """
        count = 0
        for doc_id, global_root in global_mappings.items():
            # Apply the mapping (don't check _owns_key here)
            if self._uf.union(doc_id, global_root):
                count += 1
        self._logger.info(f"Applied {count} cross-shard resolutions")
        return count

    def export_clusters(self) -> pa.Table:
        """Export (doc_id, cluster_id) mappings for keys owned by this shard.

        Only exports keys that this shard is responsible for (based on hash partitioning).
        This prevents duplicate entries when multiple shards have the same key due to
        cross-shard resolution.
        """
        full_table = self._uf.export_clusters()

        if full_table.num_rows == 0:
            return full_table

        # Filter to only keys owned by this shard
        doc_ids = full_table.column("doc_id").to_pylist()
        cluster_ids = full_table.column("cluster_id").to_pylist()

        owned_doc_ids = []
        owned_cluster_ids = []

        for doc_id, cluster_id in zip(doc_ids, cluster_ids):
            if self._owns_key(doc_id):
                owned_doc_ids.append(doc_id)
                owned_cluster_ids.append(cluster_id)

        if not owned_doc_ids:
            return pa.table(
                {
                    "doc_id": pa.array([], type=pa.string()),
                    "cluster_id": pa.array([], type=pa.string()),
                }
            )

        return pa.table(
            {
                "doc_id": owned_doc_ids,
                "cluster_id": owned_cluster_ids,
            }
        )

    # =========================================================================
    # Checkpoint / Restore
    # =========================================================================

    def _maybe_checkpoint(self) -> None:
        """Auto-checkpoint if interval reached and store available."""
        if (
            self._checkpoint_interval > 0
            and self._payload_store is not None
            and self._ops_since_checkpoint >= self._checkpoint_interval
        ):
            self.save_checkpoint()

    def _try_restore(self) -> None:
        """Restore from PayloadStore on startup if checkpoint exists."""
        if self._payload_store is None:
            return
        try:
            tables = self._load_checkpoint()
            if tables:
                self._restore_from_tables(tables)
        except Exception as e:
            self._logger.warning(f"Failed to restore from checkpoint: {e}")

    def _load_checkpoint(self) -> Optional[dict[str, pa.Table]]:
        """Load checkpoint tables from PayloadStore."""
        if self._payload_store is None:
            return None

        tables: dict[str, pa.Table] = {}
        for part in _CKPT_PARTS:
            key = _ckpt_key(self._cluster_id, self._shard_id, part)
            payload = self._payload_store.get(key)
            if payload is not None:
                tables[part] = payload.to_table()

        return tables if tables else None

    def save_checkpoint(self) -> bool:
        """Persist state directly to PayloadStore from within this shard.

        No data round-trip through the manager. The shard writes directly
        to the PayloadStore (which is a lightweight Ray actor client).

        Returns:
            True if successful, False if no store configured or error
        """
        if self._payload_store is None:
            return False

        try:
            from _internal.core.models import SplitPayload

            tables = self._serialize_state()
            for part, table in tables.items():
                key = _ckpt_key(self._cluster_id, self._shard_id, part)
                self._payload_store.store(key, SplitPayload(data=table, split_id=key))

            self._ops_since_checkpoint = 0
            self._checkpoint_count += 1
            self._logger.info(
                f"Checkpoint #{self._checkpoint_count}: "
                f"{len(self._uf)} elements, "
                f"{len(self._band_hash_index)} band hashes, "
                f"{len(self._cross_shard_edges)} cross edges"
            )
            return True
        except Exception as e:
            self._logger.warning(f"Checkpoint failed: {e}")
            return False

    def clear_checkpoint(self) -> bool:
        """Delete checkpoint from PayloadStore."""
        if self._payload_store is None:
            return False
        try:
            for part in _CKPT_PARTS:
                key = _ckpt_key(self._cluster_id, self._shard_id, part)
                self._payload_store.delete(key)
            return True
        except Exception as e:
            self._logger.warning(f"Failed to clear checkpoint: {e}")
            return False

    def _serialize_state(self) -> dict[str, pa.Table]:
        """Serialize full shard state as Arrow Tables."""
        result: dict[str, pa.Table] = {}

        result["uf"] = self._uf.to_arrow()

        if self._band_hash_index:
            result["band_index"] = pa.table(
                {
                    "band_hash": pa.array(list(self._band_hash_index.keys()), type=pa.int64()),
                    "doc_id": list(self._band_hash_index.values()),
                }
            )
        else:
            result["band_index"] = pa.table(
                {
                    "band_hash": pa.array([], type=pa.int64()),
                    "doc_id": pa.array([], type=pa.string()),
                }
            )

        if self._cross_shard_edges:
            a_list, b_list = zip(*self._cross_shard_edges)
            result["cross_edges"] = pa.table(
                {
                    "doc_id_a": list(a_list),
                    "doc_id_b": list(b_list),
                }
            )
        else:
            result["cross_edges"] = pa.table(
                {
                    "doc_id_a": pa.array([], type=pa.string()),
                    "doc_id_b": pa.array([], type=pa.string()),
                }
            )

        return result

    def _restore_from_tables(self, tables: dict[str, pa.Table]) -> None:
        """Restore state from checkpoint tables."""
        if "uf" in tables and tables["uf"].num_rows > 0:
            self._uf = UnionFind.from_arrow(tables["uf"])
        else:
            self._uf = UnionFind()

        if "band_index" in tables and tables["band_index"].num_rows > 0:
            idx = tables["band_index"]
            self._band_hash_index = dict(
                zip(idx.column("band_hash").to_pylist(), idx.column("doc_id").to_pylist())
            )
        else:
            self._band_hash_index = {}

        if "cross_edges" in tables and tables["cross_edges"].num_rows > 0:
            et = tables["cross_edges"]
            self._cross_shard_edges = list(
                zip(et.column("doc_id_a").to_pylist(), et.column("doc_id_b").to_pylist())
            )
        else:
            self._cross_shard_edges = []

        self._ops_since_checkpoint = 0
        self._logger.info(
            f"Restored: {len(self._uf)} elements, "
            f"{self._uf.num_components} components, "
            f"{len(self._band_hash_index)} band hashes, "
            f"{len(self._cross_shard_edges)} cross edges"
        )

    # Public API for tests and manager (non-PayloadStore path)

    def checkpoint(self) -> dict[str, pa.Table]:
        """Serialize state as dict of Arrow Tables (for tests / manager)."""
        self._ops_since_checkpoint = 0
        self._checkpoint_count += 1
        return self._serialize_state()

    def restore(self, tables: dict[str, pa.Table]) -> None:
        """Restore from dict of Arrow Tables (for tests / manager)."""
        self._restore_from_tables(tables)

    # =========================================================================
    # Status
    # =========================================================================

    def get_ops_since_checkpoint(self) -> int:
        return self._ops_since_checkpoint

    def get_status(self) -> dict[str, Any]:
        return {
            "shard_id": self._shard_id,
            "cluster_id": self._cluster_id,
            "num_elements": len(self._uf),
            "num_components": self._uf.num_components,
            "band_hash_index_size": len(self._band_hash_index),
            "total_unions": self._total_unions,
            "total_band_matches": self._total_band_matches,
            "total_finds": self._total_finds,
            "total_cross_shard": self._total_cross_shard,
            "pending_cross_shard_edges": len(self._cross_shard_edges),
            "ops_since_checkpoint": self._ops_since_checkpoint,
            "checkpoint_count": self._checkpoint_count,
            "has_payload_store": self._payload_store is not None,
        }

    def clear(self) -> None:
        """Clear all state."""
        self._uf = UnionFind()
        self._cross_shard_edges.clear()
        self._band_hash_index.clear()
        self._total_unions = 0
        self._total_finds = 0
        self._total_cross_shard = 0
        self._total_band_matches = 0
        self._ops_since_checkpoint = 0
        self._checkpoint_count = 0
        self._logger.info("State cleared")


def get_shard_actor_name(cluster_id: str, shard_id: int) -> str:
    return f"nurion_uf_shard_{cluster_id}_{shard_id}"
