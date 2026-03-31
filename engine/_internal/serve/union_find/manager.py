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

"""UnionFindServiceManager - Control plane for Union-Find cluster.

Checkpoint design:
    The manager passes a PayloadStore reference to each UFShard at deploy
    time. Shards write checkpoints directly to the PayloadStore from within
    their own process (no data round-trip through the manager). Shards also
    auto-restore from PayloadStore on startup.

    The manager's role is limited to:
    - Passing the PayloadStore to shards
    - Triggering force-checkpoint before critical operations
    - Cleaning up checkpoint data on shutdown
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import TYPE_CHECKING, Any, Optional

import pyarrow as pa
import ray

from _internal.serve.union_find.client import UFClient
from _internal.serve.union_find.config import UFClusterConfig
from _internal.serve.union_find.shard import UFShard, get_shard_actor_name
from _internal.utils.union_find import UnionFind

if TYPE_CHECKING:
    from _internal.core.split_payload_store import SplitPayloadStore

logger = logging.getLogger(__name__)


def _deterministic_hash(key: str) -> int:
    """Compute deterministic hash using SHA-256.

    Must match the implementation in client.py for consistent routing.
    """
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:16], 16)


class UnionFindServiceManager:
    """Control plane for a distributed Union-Find cluster.

    Usage:
        manager = UnionFindServiceManager()
        await manager.deploy(config, payload_store=store)

        client = manager.create_client()
        # ... pipeline operators call client.batch_match_and_union() ...

        await manager.resolve_cross_shard()
        clusters = await manager.export_clusters()
        await manager.shutdown()
    """

    def __init__(self) -> None:
        self._config: Optional[UFClusterConfig] = None
        self._shards: list[ray.actor.ActorHandle] = []
        self._deployed = False

    @property
    def is_deployed(self) -> bool:
        return self._deployed

    @property
    def config(self) -> Optional[UFClusterConfig]:
        return self._config

    async def deploy(
        self,
        config: UFClusterConfig,
        wait_ready: bool = True,
        timeout: float = 60.0,
        payload_store: Optional["SplitPayloadStore"] = None,
    ) -> dict[str, Any]:
        """Deploy a Union-Find cluster.

        Args:
            config: Cluster configuration
            wait_ready: Wait for all shards to respond to ping
            timeout: Timeout for waiting
            payload_store: Optional PayloadStore for shard checkpoints.
                Passed directly to each shard actor. Shards write/read
                checkpoints themselves (no manager round-trip).
        """
        if self._deployed:
            assert self._config is not None
            raise RuntimeError(
                f"Cluster '{self._config.cluster_id}' already deployed. Call shutdown() first."
            )

        start_time = time.time()
        self._config = config

        logger.info(
            f"Deploying Union-Find cluster '{config.cluster_id}' with {config.num_shards} shards"
        )

        # Create shard actors -- each gets the PayloadStore handle directly
        self._shards = []
        for shard_id in range(config.num_shards):
            actor_name = get_shard_actor_name(config.cluster_id, shard_id)
            shard = (
                ray.remote(UFShard)
                .options(
                    name=actor_name,
                    num_cpus=config.shard_num_cpus,
                    memory=config.shard_memory_mb * 1024 * 1024,
                )
                .remote(
                    shard_id=shard_id,
                    num_shards=config.num_shards,
                    cluster_id=config.cluster_id,
                    checkpoint_interval=config.checkpoint_interval,
                    payload_store=payload_store,
                )
            )
            self._shards.append(shard)  # type: ignore[arg-type]

        if wait_ready:
            futures = [shard.ping.remote() for shard in self._shards]
            ray.get(futures, timeout=timeout)

        self._deployed = True
        duration = time.time() - start_time

        logger.info(
            f"Union-Find cluster '{config.cluster_id}' deployed: "
            f"{config.num_shards} shards in {duration:.1f}s"
        )

        return {
            "cluster_id": config.cluster_id,
            "num_shards": config.num_shards,
            "status": "ready",
            "duration_s": duration,
        }

    def create_client(self) -> UFClient:
        """Create a UFClient for pipeline operators."""
        if not self._deployed or self._config is None:
            raise RuntimeError("Cluster not deployed. Call deploy() first.")
        return UFClient(
            shards=list(self._shards),
            num_shards=self._config.num_shards,
        )

    # =========================================================================
    # Checkpoint (manager triggers, shards execute locally)
    # =========================================================================

    async def force_checkpoint(self, timeout: float = 120.0) -> int:
        """Force all shards to checkpoint now.

        Each shard writes directly to its PayloadStore -- no data
        flows through the manager.

        Returns:
            Number of shards that successfully checkpointed
        """
        if not self._deployed:
            return 0
        futures = [shard.save_checkpoint.remote() for shard in self._shards]
        results = ray.get(futures, timeout=timeout)
        success = sum(1 for r in results if r)
        logger.info(f"Force checkpoint: {success}/{len(self._shards)} shards")
        return success

    # =========================================================================
    # Cross-shard resolution
    # =========================================================================

    async def resolve_cross_shard(self, timeout: float = 300.0) -> dict[str, int | float]:
        """Resolve cross-shard edges after all bucket processing."""
        if not self._deployed or self._config is None:
            raise RuntimeError("Cluster not deployed")

        logger.info("Starting cross-shard resolution...")
        start_time = time.time()

        edge_futures = [shard.get_cross_shard_edges.remote() for shard in self._shards]
        all_edges_per_shard = ray.get(edge_futures, timeout=timeout)

        all_cross_edges: list[tuple[str, str]] = []
        for edges in all_edges_per_shard:
            all_cross_edges.extend(edges)

        if not all_cross_edges:
            logger.info("No cross-shard edges to resolve")
            return {"total_cross_edges": 0, "resolutions": 0, "duration_s": 0.0}

        # Use set for dedup but convert to sorted list for deterministic iteration
        unique_edges_set: set[tuple[str, str]] = set()
        for a, b in all_cross_edges:
            unique_edges_set.add((min(a, b), max(a, b)))

        # Sort edges for deterministic processing order
        unique_edges = sorted(unique_edges_set)

        logger.info(f"Resolving {len(unique_edges)} unique cross-shard edges")

        # Collect all keys in deterministic order
        all_keys_set: set[str] = set()
        for a, b in unique_edges:
            all_keys_set.add(a)
            all_keys_set.add(b)

        # Sort keys for deterministic processing
        all_keys = sorted(all_keys_set)

        client = self.create_client()
        local_roots = client.batch_find(list(all_keys), timeout=timeout)
        key_to_local_root = dict(zip(all_keys, local_roots))

        global_uf = UnionFind()
        for a, b in unique_edges:
            root_a = key_to_local_root[a]
            root_b = key_to_local_root[b]
            global_uf.union(root_a, root_b)
            global_uf.union(a, root_a)
            global_uf.union(b, root_b)

        # Build shard mappings: send both key->global_root and ensure global_root is known
        shard_mappings: dict[int, dict[str, str]] = {i: {} for i in range(self._config.num_shards)}
        for key in all_keys:
            local_root = key_to_local_root[key]
            global_root = global_uf.find(key)
            if local_root != global_root:
                key_shard = _deterministic_hash(key) % self._config.num_shards
                root_shard = _deterministic_hash(global_root) % self._config.num_shards

                # Send mapping to key's shard
                shard_mappings[key_shard][key] = global_root

                # If global_root belongs to a different shard, ensure it knows about itself
                # This handles the case where key is in shard A, global_root is in shard B
                if key_shard != root_shard:
                    # Make sure root_shard has global_root pointing to itself
                    if global_root not in shard_mappings[root_shard]:
                        shard_mappings[root_shard][global_root] = global_root

        resolve_futures = []
        for shard_id, mappings in shard_mappings.items():
            if mappings:
                resolve_futures.append(self._shards[shard_id].resolve_cross_shard.remote(mappings))

        total_resolutions = 0
        if resolve_futures:
            counts = ray.get(resolve_futures, timeout=timeout)
            total_resolutions = sum(counts)

        duration = time.time() - start_time
        logger.info(
            f"Cross-shard resolution: {len(unique_edges)} edges, "
            f"{total_resolutions} resolutions, {duration:.1f}s"
        )

        return {
            "total_cross_edges": len(unique_edges),
            "resolutions": total_resolutions,
            "duration_s": duration,
        }

    # =========================================================================
    # Export
    # =========================================================================

    async def export_clusters(self, timeout: float = 300.0) -> pa.Table:
        """Export all cluster mappings from all shards."""
        if not self._deployed:
            raise RuntimeError("Cluster not deployed")

        futures = [shard.export_clusters.remote() for shard in self._shards]
        tables = ray.get(futures, timeout=timeout)

        non_empty = [t for t in tables if t.num_rows > 0]
        if not non_empty:
            return pa.table(
                {
                    "doc_id": pa.array([], type=pa.string()),
                    "cluster_id": pa.array([], type=pa.string()),
                }
            )
        return pa.concat_tables(non_empty)

    # =========================================================================
    # Status
    # =========================================================================

    async def get_status(self, timeout: float = 30.0) -> dict[str, Any]:
        if not self._deployed or self._config is None:
            return {"status": "not_deployed"}

        futures = [shard.get_status.remote() for shard in self._shards]
        shard_statuses = ray.get(futures, timeout=timeout)

        return {
            "cluster_id": self._config.cluster_id,
            "status": "deployed",
            "num_shards": self._config.num_shards,
            "total_elements": sum(s["num_elements"] for s in shard_statuses),
            "total_components": sum(s["num_components"] for s in shard_statuses),
            "total_unions": sum(s["total_unions"] for s in shard_statuses),
            "total_band_matches": sum(s.get("total_band_matches", 0) for s in shard_statuses),
            "band_hash_index_size": sum(s.get("band_hash_index_size", 0) for s in shard_statuses),
            "total_cross_shard": sum(s["total_cross_shard"] for s in shard_statuses),
            "pending_cross_shard_edges": sum(
                s["pending_cross_shard_edges"] for s in shard_statuses
            ),
            "shards": shard_statuses,
        }

    # =========================================================================
    # Shutdown
    # =========================================================================

    async def shutdown(self, clear_checkpoints: bool = True) -> None:
        """Shutdown the cluster.

        Args:
            clear_checkpoints: If True, each shard deletes its own checkpoint
                data from PayloadStore before being killed.
        """
        if not self._deployed:
            return

        assert self._config is not None
        logger.info(f"Shutting down Union-Find cluster '{self._config.cluster_id}'")

        if clear_checkpoints:
            futures = [shard.clear_checkpoint.remote() for shard in self._shards]
            try:
                ray.get(futures, timeout=30.0)
            except Exception as e:
                logger.warning(f"Error clearing checkpoints: {e}")

        for shard in self._shards:
            try:
                ray.kill(shard)
            except Exception as e:
                logger.warning(f"Error killing shard: {e}")

        self._shards.clear()
        self._deployed = False
        logger.info("Union-Find cluster shutdown complete")
