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

"""UFClient - Client for distributed Union-Find operations.

The client routes operations to the correct UFShard actor based on
doc_id hashing. It batches operations per-shard for efficiency and
sends them in parallel via Ray.

Usage:
    client = UFClient(shards=shard_handles, num_shards=64)

    # Union pairs (auto-routed to correct shards)
    result = client.batch_union([("doc_a", "doc_b"), ("doc_c", "doc_d")])

    # Find cluster IDs
    cluster_ids = client.batch_find(["doc_a", "doc_c"])
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from typing import Any

import ray

logger = logging.getLogger(__name__)


def _deterministic_hash(key: str) -> int:
    """Compute deterministic hash using SHA-256.
    
    Python's built-in hash() is non-deterministic across runs due to
    hash randomization. We need deterministic routing for Union-Find shards.
    """
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:16], 16)


class UFClient:
    """Client for distributed Union-Find Service.

    Routes union/find operations to the correct UFShard actors
    based on doc_id hash partitioning.

    Thread-safe for use in Ray actors / operator workers.
    """

    def __init__(
        self,
        shards: list[ray.actor.ActorHandle],
        num_shards: int,
    ) -> None:
        """Initialize the client.

        Args:
            shards: List of UFShard actor handles, indexed by shard_id
            num_shards: Total number of shards (must match len(shards))
        """
        if len(shards) != num_shards:
            raise ValueError(f"Expected {num_shards} shard handles, got {len(shards)}")
        self._shards = shards
        self._num_shards = num_shards

    def _route(self, key: str) -> int:
        """Route a doc_id to a shard index using deterministic hash."""
        return _deterministic_hash(key) % self._num_shards

    def batch_union(self, pairs: list[tuple[str, str]], timeout: float = 60.0) -> dict[str, int]:
        """Union multiple pairs, routing to correct shards.

        For each pair (A, B):
        - If both A and B hash to the same shard: route to that shard
        - If A and B hash to different shards: route to both shards.
          Each shard records the edge; cross-shard resolution happens later.

        Args:
            pairs: List of (doc_id_a, doc_id_b) pairs to union
            timeout: Timeout for Ray RPC calls (seconds)

        Returns:
            Aggregated results: {local_unions, cross_shard}
        """
        # Group pairs by target shard
        shard_pairs: dict[int, list[tuple[str, str]]] = defaultdict(list)

        for a, b in pairs:
            shard_a = self._route(a)
            shard_b = self._route(b)

            if shard_a == shard_b:
                # Same shard: route once
                shard_pairs[shard_a].append((a, b))
            else:
                # Cross-shard: send to both shards so each records the edge
                shard_pairs[shard_a].append((a, b))
                shard_pairs[shard_b].append((a, b))

        if not shard_pairs:
            return {"local_unions": 0, "cross_shard": 0}

        # Send to shards in parallel
        futures = []
        for shard_id, shard_pair_list in shard_pairs.items():
            futures.append(self._shards[shard_id].batch_union.remote(shard_pair_list))

        results = ray.get(futures, timeout=timeout)

        # Aggregate results
        total_local = sum(r.get("local_unions", 0) for r in results)
        total_cross = sum(r.get("cross_shard", 0) for r in results)

        return {"local_unions": total_local, "cross_shard": total_cross}

    def batch_match_and_union(
        self, entries: list[tuple[int, str]], timeout: float = 60.0
    ) -> dict[str, int]:
        """Match documents by band_hash and union matches via UFService.

        Routes each (band_hash, doc_id) entry to a shard based on band_hash.
        The shard maintains a band_hash -> doc_id index and unions docs with
        matching band_hashes, even across different process_split calls.

        This is the primary method for MinHash dedup: it replaces the old
        approach of grouping by band_hash within a single batch.

        Args:
            entries: List of (band_hash, doc_id) tuples
            timeout: Timeout for Ray RPC calls (seconds)

        Returns:
            Aggregated results: {new_hashes, matches, cross_shard}
        """
        # Route by band_hash (NOT by doc_id) so that all entries with the
        # same band_hash go to the same shard for matching
        shard_entries: dict[int, list[tuple[int, str]]] = defaultdict(list)
        for band_hash, doc_id in entries:
            shard_id = band_hash % self._num_shards
            shard_entries[shard_id].append((band_hash, doc_id))

        if not shard_entries:
            return {"new_hashes": 0, "matches": 0, "cross_shard": 0}

        # Send to shards in parallel
        futures = []
        for shard_id, shard_entry_list in shard_entries.items():
            futures.append(self._shards[shard_id].batch_match_and_union.remote(shard_entry_list))

        results = ray.get(futures, timeout=timeout)

        # Aggregate
        return {
            "new_hashes": sum(r.get("new_hashes", 0) for r in results),
            "matches": sum(r.get("matches", 0) for r in results),
            "cross_shard": sum(r.get("cross_shard", 0) for r in results),
        }

    def batch_find(self, keys: list[str], timeout: float = 60.0) -> list[str]:
        """Find cluster representatives for multiple keys.

        Routes each key to its owning shard and collects results.

        Args:
            keys: List of doc_ids to find
            timeout: Timeout for Ray RPC calls (seconds)

        Returns:
            List of cluster representative doc_ids (same order as input)
        """
        # Group keys by shard
        shard_keys: dict[int, list[tuple[int, str]]] = defaultdict(list)
        for original_idx, key in enumerate(keys):
            shard_id = self._route(key)
            shard_keys[shard_id].append((original_idx, key))

        # Send to shards in parallel
        shard_id_order: list[int] = []
        futures = []
        for shard_id, idx_key_list in shard_keys.items():
            shard_id_order.append(shard_id)
            just_keys = [k for _, k in idx_key_list]
            futures.append(self._shards[shard_id].batch_find.remote(just_keys))

        results = ray.get(futures, timeout=timeout)

        # Reassemble results in original order
        output: list[str] = [""] * len(keys)
        for shard_id, shard_result in zip(shard_id_order, results):
            idx_key_list = shard_keys[shard_id]
            for (original_idx, _key), cluster_id in zip(idx_key_list, shard_result):
                output[original_idx] = cluster_id

        return output

    def get_all_status(self, timeout: float = 30.0) -> list[dict[str, Any]]:
        """Get status from all shards.

        Returns:
            List of shard status dicts
        """
        futures = [shard.get_status.remote() for shard in self._shards]
        return ray.get(futures, timeout=timeout)
