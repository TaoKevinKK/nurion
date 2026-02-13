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

"""GPU Allocator — best-fit bin-packing for multi-model inference.

Advisory layer on top of Ray's scheduler. Provides best-fit node suggestions
passed as soft NodeAffinitySchedulingStrategy hints. If the suggestion is
stale, Ray handles the fallback.

Plain class owned by ModelServiceManager — not a Ray actor.
All state is co-located with the Manager for zero-overhead access.
"""

from __future__ import annotations

import logging
import random
from collections import defaultdict
from typing import Optional

import ray

logger = logging.getLogger(__name__)

_FLOAT_TOLERANCE = 1e-9


class GPUAllocator:
    """Cluster-aware GPU bin-packing allocator.

    Tracks per-node GPU state (as floats for fractional GPU support) and
    provides advisory placement decisions. All methods are synchronous
    (fast, in-memory dict lookups). No Ray actor overhead.
    """

    def __init__(self) -> None:
        self._node_total: dict[str, float] = {}  # node_id -> total GPUs
        self._placements: dict[str, tuple[str, float]] = {}  # worker_id -> (node_id, gpus)

    # --- State management ---

    def refresh_nodes(self) -> dict[str, float]:
        """Refresh node list from ray.nodes(). Returns {node_id: total_gpus}."""
        self._node_total = {}
        for node in ray.nodes():
            if node.get("Alive"):
                gpus = float(node.get("Resources", {}).get("GPU", 0))
                if gpus > 0:
                    self._node_total[node["NodeID"]] = gpus
        # Prune placements on dead nodes
        live = set(self._node_total)
        self._placements = {
            wid: (nid, g) for wid, (nid, g) in self._placements.items() if nid in live
        }
        logger.info(
            f"GPUAllocator refreshed: {len(self._node_total)} GPU node(s), "
            f"{len(self._placements)} active placement(s)"
        )
        return dict(self._node_total)

    def record_placement(self, worker_id: str, node_id: str, gpus: float) -> None:
        """Record that a worker was placed on a node using gpus GPUs."""
        self._placements[worker_id] = (node_id, gpus)

    def record_removal(self, worker_id: str) -> None:
        """Record that a worker has been removed."""
        self._placements.pop(worker_id, None)

    def get_node_free_gpus(self) -> dict[str, float]:
        """Free GPUs per node = total - sum(placements on that node)."""
        used: dict[str, float] = defaultdict(float)
        for _, (node_id, gpus) in self._placements.items():
            used[node_id] += gpus
        return {nid: total - used.get(nid, 0.0) for nid, total in self._node_total.items()}

    def reconcile(self, active_worker_ids: set[str]) -> int:
        """Remove placements for workers that no longer exist.

        Called by the Manager after worker crash recovery to clean up
        phantom placements. Returns number of stale entries removed.
        """
        stale = [wid for wid in self._placements if wid not in active_worker_ids]
        for wid in stale:
            del self._placements[wid]
        if stale:
            logger.info(f"GPUAllocator reconciled: removed {len(stale)} stale placement(s)")
        return len(stale)

    # --- Advisory placement ---

    def suggest_nodes(self, gpus_per_worker: float, count: int) -> list[Optional[str]]:
        """Suggest best-fit nodes for ``count`` workers needing ``gpus_per_worker`` each.

        Simulates sequential placement so concurrent spawns from the same
        scale_to() call don't all target the same node.

        Returns a list of node_ids (or None if no fit) for each worker.
        """
        free = self.get_node_free_gpus()
        results: list[Optional[str]] = []

        for _ in range(count):
            candidates = [
                (f, random.random(), nid)  # random tiebreaker for equal free GPUs
                for nid, f in free.items()
                if f >= gpus_per_worker - _FLOAT_TOLERANCE
            ]
            if not candidates:
                results.append(None)
                continue

            candidates.sort()  # ascending by free GPUs = best-fit
            best_node = candidates[0][2]
            results.append(best_node)
            free[best_node] -= gpus_per_worker  # tentative deduction

        return results

    def suggest_workers_to_stop(self, worker_ids: list[str], count: int) -> list[str]:
        """Pick which workers to stop to best consolidate free GPUs.

        Strategy: prefer workers on nodes with the most free GPUs (least
        packed). Removing them makes those nodes even emptier, consolidating
        free space into larger contiguous blocks.
        """
        node_workers: dict[str, list[str]] = defaultdict(list)
        for wid in worker_ids:
            if wid in self._placements:
                node_id, _ = self._placements[wid]
                node_workers[node_id].append(wid)

        free = self.get_node_free_gpus()
        sorted_nodes = sorted(
            node_workers.keys(),
            key=lambda nid: free.get(nid, 0),
            reverse=True,  # emptiest node first
        )

        to_stop: list[str] = []
        for nid in sorted_nodes:
            for wid in node_workers[nid]:
                if len(to_stop) >= count:
                    return to_stop
                to_stop.append(wid)
        return to_stop

    # --- Compaction ---

    def plan_compaction(self, gpus_needed: float) -> Optional[tuple[str, list[str]]]:
        """Find the cheapest eviction plan to free ``gpus_needed`` GPUs on one node.

        Returns ``(node_id, [worker_ids_to_evict])`` or ``None``.
        Cheapest = fewest workers to evict. If a node already has enough
        free GPUs, returns it with an empty eviction list.
        """
        free = self.get_node_free_gpus()
        node_workers: dict[str, list[tuple[str, float]]] = defaultdict(list)
        for wid, (nid, gpus) in self._placements.items():
            node_workers[nid].append((wid, gpus))

        best: Optional[tuple[int, str, list[str]]] = None

        for nid, total in self._node_total.items():
            if total < gpus_needed - _FLOAT_TOLERANCE:
                continue  # node too small even if completely empty

            node_free = free.get(nid, 0.0)
            if node_free >= gpus_needed - _FLOAT_TOLERANCE:
                return (nid, [])  # already enough free

            workers_on_node = node_workers.get(nid, [])
            # Evict largest workers first for fastest freeing
            workers_on_node.sort(key=lambda x: x[1], reverse=True)

            evict_wids: list[str] = []
            freed = 0.0
            for wid, gpus in workers_on_node:
                evict_wids.append(wid)
                freed += gpus
                if node_free + freed >= gpus_needed - _FLOAT_TOLERANCE:
                    break

            if node_free + freed >= gpus_needed - _FLOAT_TOLERANCE:
                if best is None or len(evict_wids) < best[0]:
                    best = (len(evict_wids), nid, evict_wids)

        if best is None:
            return None
        return (best[1], best[2])
