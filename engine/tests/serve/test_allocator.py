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

"""Tests for GPUAllocator — best-fit bin-packing logic.

Pure unit tests (no Ray needed). We manually set ``_node_total`` and
``_placements`` to exercise the allocation algorithms.
"""

from __future__ import annotations

import pytest

from _internal.serve.allocator import GPUAllocator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_allocator(nodes: dict[str, float]) -> GPUAllocator:
    """Create allocator with pre-set node topology (bypasses ray.nodes())."""
    alloc = GPUAllocator()
    alloc._node_total = dict(nodes)
    return alloc


# ---------------------------------------------------------------------------
# suggest_nodes
# ---------------------------------------------------------------------------


class TestSuggestNodes:
    def test_best_fit_picks_tightest_node(self) -> None:
        """Best-fit should prefer the node where the worker just barely fits."""
        alloc = _make_allocator({"A": 8, "B": 4, "C": 2})
        # Need 2 GPUs — node C (2 free) is the tightest fit
        results = alloc.suggest_nodes(2.0, 1)
        assert results == ["C"]

    def test_no_fit_returns_none(self) -> None:
        """If no node has enough free GPUs, return None."""
        alloc = _make_allocator({"A": 4, "B": 2})
        # All 4 GPUs on A are occupied
        alloc.record_placement("w1", "A", 4.0)
        alloc.record_placement("w2", "B", 2.0)
        results = alloc.suggest_nodes(2.0, 1)
        assert results == [None]

    def test_sequential_placement_avoids_same_node(self) -> None:
        """When requesting multiple workers, simulated deduction prevents
        all placements on the same node."""
        alloc = _make_allocator({"A": 4, "B": 4})
        # Request 2 workers each needing 4 GPUs — one per node
        results = alloc.suggest_nodes(4.0, 2)
        assert set(results) == {"A", "B"}

    def test_partial_fit_then_none(self) -> None:
        """First request fits, second doesn't after deduction."""
        alloc = _make_allocator({"A": 4})
        results = alloc.suggest_nodes(4.0, 2)
        assert results[0] == "A"
        assert results[1] is None


class TestSuggestNodesFractional:
    def test_fractional_half_gpu(self) -> None:
        """Fractional GPU (0.5) tracking works correctly."""
        alloc = _make_allocator({"A": 1})
        # Place one 0.5 GPU worker
        alloc.record_placement("w1", "A", 0.5)
        # 0.5 free — can fit another 0.5 GPU worker
        results = alloc.suggest_nodes(0.5, 1)
        assert results == ["A"]

    def test_fractional_no_fit(self) -> None:
        """No fit when fractional GPUs are exhausted."""
        alloc = _make_allocator({"A": 1})
        alloc.record_placement("w1", "A", 0.5)
        alloc.record_placement("w2", "A", 0.5)
        results = alloc.suggest_nodes(0.5, 1)
        assert results == [None]

    def test_fractional_best_fit(self) -> None:
        """Best-fit logic works with fractional GPUs."""
        alloc = _make_allocator({"A": 4, "B": 1})
        # A has 4 free, B has 1 free — 0.5 should prefer B (tighter)
        alloc.record_placement("w1", "B", 0.5)
        # B now has 0.5 free
        results = alloc.suggest_nodes(0.5, 1)
        assert results == ["B"]


# ---------------------------------------------------------------------------
# suggest_workers_to_stop
# ---------------------------------------------------------------------------


class TestSuggestWorkersToStop:
    def test_emptiest_node_first(self) -> None:
        """Workers on the emptiest node (most free GPUs) should be stopped first."""
        alloc = _make_allocator({"A": 8, "B": 8})
        # A has 2 workers (4 GPU each) = 0 free
        alloc.record_placement("w1", "A", 4.0)
        alloc.record_placement("w2", "A", 4.0)
        # B has 1 worker (4 GPU) = 4 free
        alloc.record_placement("w3", "B", 4.0)

        to_stop = alloc.suggest_workers_to_stop(["w1", "w2", "w3"], 1)
        # B is emptiest (4 free) → w3 should be stopped first
        assert to_stop == ["w3"]

    def test_stop_count_respected(self) -> None:
        """Stop exactly `count` workers."""
        alloc = _make_allocator({"A": 8})
        alloc.record_placement("w1", "A", 2.0)
        alloc.record_placement("w2", "A", 2.0)
        alloc.record_placement("w3", "A", 2.0)

        to_stop = alloc.suggest_workers_to_stop(["w1", "w2", "w3"], 2)
        assert len(to_stop) == 2

    def test_unknown_workers_ignored(self) -> None:
        """Workers not in placements are silently skipped."""
        alloc = _make_allocator({"A": 8})
        alloc.record_placement("w1", "A", 4.0)

        to_stop = alloc.suggest_workers_to_stop(["w1", "w_unknown"], 1)
        assert to_stop == ["w1"]


# ---------------------------------------------------------------------------
# plan_compaction
# ---------------------------------------------------------------------------


class TestPlanCompaction:
    def test_already_free(self) -> None:
        """If a node already has enough free GPUs, no eviction needed."""
        alloc = _make_allocator({"A": 8, "B": 8})
        alloc.record_placement("w1", "A", 4.0)
        # A has 4 free — enough for 4 GPUs
        plan = alloc.plan_compaction(4.0)
        assert plan is not None
        node_id, evict_wids = plan
        assert node_id == "A"
        assert evict_wids == []

    def test_cheapest_eviction(self) -> None:
        """Pick the node with fewest evictions needed."""
        alloc = _make_allocator({"A": 8, "B": 8})
        # A: two 2-GPU workers, 4 free. Need 8 → evict both (2 evictions)
        alloc.record_placement("w1", "A", 2.0)
        alloc.record_placement("w2", "A", 2.0)
        # B: one 4-GPU worker, 4 free. Need 8 → evict one (1 eviction)
        alloc.record_placement("w3", "B", 4.0)

        plan = alloc.plan_compaction(8.0)
        assert plan is not None
        node_id, evict_wids = plan
        assert node_id == "B"
        assert evict_wids == ["w3"]

    def test_no_plan_if_nodes_too_small(self) -> None:
        """Returns None if no single node is large enough."""
        alloc = _make_allocator({"A": 4, "B": 4})
        plan = alloc.plan_compaction(8.0)
        assert plan is None

    def test_evicts_largest_workers_first(self) -> None:
        """Within a node, largest workers are evicted first for fastest freeing."""
        alloc = _make_allocator({"A": 8})
        alloc.record_placement("w_small", "A", 1.0)
        alloc.record_placement("w_big", "A", 4.0)
        alloc.record_placement("w_med", "A", 2.0)
        # 1 free, need 5 → evict w_big (4 GPU) first, then 5 free → done
        plan = alloc.plan_compaction(5.0)
        assert plan is not None
        _, evict_wids = plan
        assert evict_wids == ["w_big"]


# ---------------------------------------------------------------------------
# reconcile
# ---------------------------------------------------------------------------


class TestReconcile:
    def test_removes_stale_placements(self) -> None:
        """Reconcile removes entries for workers that no longer exist."""
        alloc = _make_allocator({"A": 8})
        alloc.record_placement("w1", "A", 4.0)
        alloc.record_placement("w2", "A", 4.0)

        removed = alloc.reconcile({"w1"})
        assert removed == 1
        assert "w2" not in alloc._placements
        assert "w1" in alloc._placements

    def test_no_stale_returns_zero(self) -> None:
        """Reconcile is a no-op when all placements are active."""
        alloc = _make_allocator({"A": 8})
        alloc.record_placement("w1", "A", 4.0)

        removed = alloc.reconcile({"w1"})
        assert removed == 0


# ---------------------------------------------------------------------------
# record_placement / record_removal / get_node_free_gpus
# ---------------------------------------------------------------------------


class TestPlacementTracking:
    def test_placement_and_free_gpus(self) -> None:
        alloc = _make_allocator({"A": 8, "B": 4})
        alloc.record_placement("w1", "A", 4.0)

        free = alloc.get_node_free_gpus()
        assert free["A"] == pytest.approx(4.0)
        assert free["B"] == pytest.approx(4.0)

    def test_removal_restores_free_gpus(self) -> None:
        alloc = _make_allocator({"A": 8})
        alloc.record_placement("w1", "A", 4.0)
        alloc.record_removal("w1")

        free = alloc.get_node_free_gpus()
        assert free["A"] == pytest.approx(8.0)

    def test_removal_idempotent(self) -> None:
        alloc = _make_allocator({"A": 8})
        alloc.record_removal("nonexistent")  # should not raise
