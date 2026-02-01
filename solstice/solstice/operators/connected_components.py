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

"""Distributed Connected Components via iterative label propagation.

This module implements distributed Connected Components (CC) for clustering
similar documents in MinHash deduplication. The algorithm uses iterative
label propagation:

Algorithm (per iteration):
1. **Map**: For each edge (A, B), emit (A, label[B]) and (B, label[A])
2. **Shuffle**: Route by doc_id to correct partition
3. **Reduce**: new_label[X] = min(current_label[X], received_labels)
4. **Converge**: If no label changed across all partitions, done

Architecture (WorkQueue-based, Jan 2025):
- Labels are stored via WorkQueue state API (single-writer, no conflicts)
- Edges flow through payload (Arrow tables) - scales to 10B+ records
- Convergence is detected via @master_callable aggregation
- No local SlateDB state store needed

Design rationale for 10B+ scale dedup:
- Edges (candidate pairs) may be 10B-100B - MUST be in payload
- Labels are one per doc_id (~1-10B entries) - can use state API
- State API: state_get/state_put with atomic ack+update

Stages:
1. **CCInitOperator**: Initialize labels (label = doc_id) from candidate pairs
2. **CCIterateOperator**: One round of label propagation (reduce step)
3. **CCMessageOperator**: Generate messages for next iteration (map step)

The CCIterateMaster orchestrates iterations until convergence.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, Type

import pyarrow as pa

from solstice.core.operator import master_callable

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import Operator, OperatorConfig, OperatorRuntime, operator
from solstice.operators.shuffle import ShuffleOperator, ShuffleOperatorConfig

if TYPE_CHECKING:
    from solstice.operators.cc_master import CCIterateMaster


@dataclass
class CCInitConfig(OperatorConfig):
    """Configuration for CC initialization.

    Takes candidate pairs and initializes labels for all documents.

    Attributes:
        doc_id_1_column: Column for first document ID
        doc_id_2_column: Column for second document ID
    """

    doc_id_1_column: str = "doc_id_1"
    doc_id_2_column: str = "doc_id_2"


@operator(CCInitConfig)
class CCInitOperator(Operator):
    """Initialize labels and generate initial messages from candidate pairs.

    Input: Candidate pairs (doc_id_1, doc_id_2, similarity)
    Output: Initial messages (doc_id, neighbor_label) for label propagation

    Each document starts with its own ID as its label.

    This operator is STATELESS - it generates messages without storing state.
    """

    def __init__(self, config: CCInitConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self.init_config = config

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Initialize labels and generate messages."""
        if payload is None:
            return None

        table = payload.to_table()
        if table.num_rows == 0:
            return None

        config = self.init_config

        doc_ids_1 = table.column(config.doc_id_1_column).to_pylist()
        doc_ids_2 = table.column(config.doc_id_2_column).to_pylist()

        # Generate bidirectional messages
        # For edge (A, B): emit (A, B) and (B, A)
        # This means "A should consider B's label" and vice versa
        messages = []
        for doc1, doc2 in zip(doc_ids_1, doc_ids_2):
            # Message to doc1: consider doc2's label
            messages.append(
                {
                    "doc_id": doc1,
                    "neighbor_label": doc2,  # Initially, label = doc_id
                }
            )
            # Message to doc2: consider doc1's label
            messages.append(
                {
                    "doc_id": doc2,
                    "neighbor_label": doc1,
                }
            )

        if not messages:
            return None

        result = pa.table(
            {
                "doc_id": [m["doc_id"] for m in messages],
                "neighbor_label": [m["neighbor_label"] for m in messages],
            }
        )

        return SplitPayload(data=result, split_id=split.split_id)


@dataclass
class CCIterateConfig(ShuffleOperatorConfig):
    """Configuration for CC iteration (reduce step).

    Takes messages and updates labels. Uses CCIterateMaster for
    self-contained iteration - no special logic needed in RayJobRunner.

    WorkQueue-based design:
    - Edges flow through payload (Arrow tables) for scale
    - Labels are tracked via iteration change counters
    - Future: labels stored via WorkQueue state API (state_get/state_put)

    Attributes:
        doc_id_column: Column for document ID
        neighbor_label_column: Column for neighbor's label
        current_label_column: Column for current label (in input)
        edges_column: Column for edges (comma-separated neighbor IDs)
        max_iterations: Maximum iterations before forced stop
        convergence_threshold: Number of changes below which to stop (0 = require full convergence)
    """

    doc_id_column: str = "doc_id"
    neighbor_label_column: str = "neighbor_label"
    current_label_column: str = "current_label"
    edges_column: str = "edges"
    max_iterations: int = 100
    convergence_threshold: int = 0

    operator_class: ClassVar[Type["CCIterateOperator"]] = None  # type: ignore[assignment]  # Set below
    master_class: ClassVar[Optional[Type["CCIterateMaster"]]] = None  # Set below

    def __post_init__(self):
        # Partition by doc_id for label aggregation
        if not self.partition_keys:
            self.partition_keys = [self.doc_id_column]


@operator(CCIterateConfig)
class CCIterateOperator(ShuffleOperator):
    """Operator for iterative label propagation (reduce step).

    Input: Messages (doc_id, neighbor_label, current_label, edges?)
    Output: Updated labels with edges (doc_id, label, edges, changed)

    For each document, the new label is the minimum of:
    - Current label (from input table)
    - All received neighbor labels

    WorkQueue-based design (no local state store):
    - Edges flow through payload (Arrow table) for scale
    - Labels are tracked in payload, not external state
    - Future: labels via WorkQueue state API (state_get/state_put)

    Iteration Protocol:
    - process_data(): Process messages, compute new labels, output with edges
    - reset_iteration(): Clear change counter before new iteration
    - get_iteration_changes(): Get total changes for convergence check
    - recompute_labels(): Recompute from edges data (for iteration 2+)
    """

    def __init__(self, config: CCIterateConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self.iterate_config = config

        # Iteration tracking (in-memory, aggregated via @master_callable)
        self._iteration_changes: int = 0

    @master_callable
    def reset_iteration(self) -> None:
        """Reset change counter for a new iteration."""
        self._iteration_changes = 0

    @master_callable
    def get_iteration_changes(self) -> int:
        """Get total number of label changes in this iteration.

        Used by master to check convergence.
        """
        return self._iteration_changes

    def process_data(self, table: pa.Table) -> Optional[pa.Table]:
        """Process messages and compute labels.

        Input columns:
        - doc_id: Document ID
        - neighbor_label: Neighbor's current label (or doc_id in iteration 1)
        - current_label (optional): Current label from previous iteration
        - edges (optional): Existing edges from previous iteration

        Output columns:
        - doc_id: Document ID
        - label: New label (min of current and all neighbors)
        - edges: Comma-separated neighbor IDs (for next iteration)
        - changed: Whether label changed in this iteration

        Edges are carried forward in payload for subsequent iterations.
        """
        config = self.iterate_config

        doc_ids = table.column(config.doc_id_column).to_pylist()
        neighbor_labels = table.column(config.neighbor_label_column).to_pylist()

        # Get current labels from table if available
        current_labels_from_table: Dict[str, str] = {}
        if config.current_label_column in table.column_names:
            current_label_values = table.column(config.current_label_column).to_pylist()
            for doc_id, current_label in zip(doc_ids, current_label_values):
                if doc_id not in current_labels_from_table and current_label is not None:
                    current_labels_from_table[doc_id] = current_label

        # Get existing edges from table if available (for iteration 2+)
        existing_edges_from_table: Dict[str, set[str]] = {}
        if config.edges_column in table.column_names:
            edges_values = table.column(config.edges_column).to_pylist()
            for doc_id, edges_str in zip(doc_ids, edges_values):
                if doc_id not in existing_edges_from_table and edges_str:
                    existing_edges_from_table[doc_id] = set(edges_str.split(","))

        # Group messages by doc_id and collect edges
        messages_by_doc: Dict[str, List[str]] = {}
        new_edges_by_doc: Dict[str, set[str]] = {}
        for doc_id, neighbor_label in zip(doc_ids, neighbor_labels):
            if doc_id not in messages_by_doc:
                messages_by_doc[doc_id] = []
                new_edges_by_doc[doc_id] = set()
            messages_by_doc[doc_id].append(neighbor_label)
            new_edges_by_doc[doc_id].add(neighbor_label)

        # Process all docs in memory
        results = []
        changes = 0

        for doc_id, neighbor_labels_list in messages_by_doc.items():
            # Get current label: table > default (doc_id)
            current_label = current_labels_from_table.get(doc_id, doc_id)

            # New label is minimum of current and all neighbors
            all_labels = [current_label] + neighbor_labels_list
            new_label = min(all_labels, key=str)

            changed = new_label != current_label
            if changed:
                changes += 1

            # Merge edges: existing + new
            existing = existing_edges_from_table.get(doc_id, set())
            all_edges = existing | new_edges_by_doc[doc_id]

            results.append(
                {
                    "doc_id": doc_id,
                    "label": new_label,
                    "edges": ",".join(sorted(all_edges)),
                    "changed": changed,
                }
            )

        if not results:
            return None

        self._iteration_changes += changes
        self.logger.debug(
            f"CC iteration: {changes} label changes (total: {self._iteration_changes})"
        )

        return pa.table(
            {
                "doc_id": [r["doc_id"] for r in results],
                "label": [r["label"] for r in results],
                "edges": [r["edges"] for r in results],
                "changed": [r["changed"] for r in results],
            }
        )

    @master_callable
    def recompute_labels(self, edges_data: List[Dict[str, str]]) -> int:
        """Recompute labels from edges data (for iteration 2+).

        This is called by master with aggregated edges data from all workers.
        Each worker processes a subset of the data.

        Args:
            edges_data: List of dicts with {doc_id, label, edges}

        Returns:
            Number of label changes in this iteration
        """
        if not edges_data:
            return 0

        # Build label lookup from input
        labels: Dict[str, str] = {}
        edges: Dict[str, set[str]] = {}

        for item in edges_data:
            doc_id = item["doc_id"]
            labels[doc_id] = item["label"]
            edges_str = item.get("edges", "")
            if edges_str:
                edges[doc_id] = set(edges_str.split(","))

        # Compute new labels
        changes = 0
        for doc_id, doc_edges in edges.items():
            if not doc_edges:
                continue

            current_label = labels.get(doc_id, doc_id)
            neighbor_labels = [labels.get(n, n) for n in doc_edges]

            all_labels = [current_label] + neighbor_labels
            new_label = min(all_labels, key=str)

            if new_label != current_label:
                changes += 1
                labels[doc_id] = new_label

        self._iteration_changes += changes
        self.logger.debug(
            f"CC recompute: {changes} label changes (total: {self._iteration_changes})"
        )

        return changes


# Set master_class after imports to avoid circular imports
from solstice.operators.cc_master import CCIterateMaster  # noqa: E402

CCIterateConfig.master_class = CCIterateMaster


@dataclass
class CCMessageConfig(OperatorConfig):
    """Configuration for CC message generation (map step).

    Takes current labels and edges, generates messages for next iteration.

    Attributes:
        doc_id_column: Column for document ID
        label_column: Column for current label
        neighbor_column: Column for neighbor document ID (for edges)
    """

    doc_id_column: str = "doc_id"
    label_column: str = "label"
    neighbor_column: str = "neighbor_id"


@operator(CCMessageConfig)
class CCMessageOperator(Operator):
    """Stateless operator for generating messages (map step).

    Input: Current labels with edges (doc_id, label, neighbor_id)
    Output: Messages (doc_id, neighbor_label, current_label) for next round

    For each row with (doc_id, label, neighbor_id):
    - Emit message to neighbor with current label

    This operator is STATELESS - edges must come from the input data.
    The pipeline should include edge information in the data flow.
    """

    def __init__(self, config: CCMessageConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self.message_config = config

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Generate messages from current labels and edges."""
        if payload is None:
            return None

        table = payload.to_table()
        if table.num_rows == 0:
            return None

        result = self.process_data(table)
        if result is None:
            return None

        return SplitPayload(data=result, split_id=split.split_id)

    def process_data(self, table: pa.Table) -> Optional[pa.Table]:
        """Generate messages from labels and edges."""
        config = self.message_config

        doc_ids = table.column(config.doc_id_column).to_pylist()
        labels = table.column(config.label_column).to_pylist()
        neighbors = table.column(config.neighbor_column).to_pylist()

        # Build label lookup
        label_map: Dict[str, str] = {}
        for doc_id, label in zip(doc_ids, labels):
            label_map[doc_id] = label

        # Generate messages: for each (doc, neighbor), send doc's label to neighbor
        messages = []
        for doc_id, label, neighbor in zip(doc_ids, labels, neighbors):
            if neighbor is not None:
                messages.append(
                    {
                        "doc_id": neighbor,
                        "neighbor_label": label,
                        "current_label": label_map.get(neighbor, neighbor),
                    }
                )

        if not messages:
            return None

        return pa.table(
            {
                "doc_id": [m["doc_id"] for m in messages],
                "neighbor_label": [m["neighbor_label"] for m in messages],
                "current_label": [m["current_label"] for m in messages],
            }
        )


@dataclass
class DedupeByClusterConfig(ShuffleOperatorConfig):
    """Configuration for deduplication by cluster.

    Takes clustered documents and keeps one representative per cluster.

    Attributes:
        doc_id_column: Column for document ID
        cluster_id_column: Column for cluster ID (label)
    """

    doc_id_column: str = "doc_id"
    cluster_id_column: str = "label"

    def __post_init__(self):
        # Partition by cluster_id for grouping
        self.partition_keys = [self.cluster_id_column]


@operator(DedupeByClusterConfig)
class DedupeByClusterOperator(ShuffleOperator):
    """Stateless operator to keep one representative document per cluster.

    Input: Documents with cluster labels (doc_id, label, ...)
    Output: One document per cluster (the one with smallest doc_id)

    This operator is STATELESS - it deduplicates within the batch only.
    Since data is shuffled by cluster_id, all documents in a cluster
    end up in the same partition, enabling within-batch deduplication.

    This is the final stage of MinHash deduplication.
    """

    def __init__(self, config: DedupeByClusterConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self.cluster_config = config

    def process_data(self, table: pa.Table) -> Optional[pa.Table]:
        """Keep one document per cluster (within batch)."""
        config = self.cluster_config

        doc_ids = table.column(config.doc_id_column).to_pylist()
        cluster_ids = table.column(config.cluster_id_column).to_pylist()

        # Group by cluster
        clusters: Dict[str, List[int]] = {}
        for i, (doc_id, cluster_id) in enumerate(zip(doc_ids, cluster_ids)):
            if cluster_id not in clusters:
                clusters[cluster_id] = []
            clusters[cluster_id].append(i)

        # Keep first document per cluster (smallest doc_id)
        keep_rows = []
        for cluster_id, row_indices in clusters.items():
            # Find row with smallest doc_id
            min_idx = min(row_indices, key=lambda i: str(doc_ids[i]))
            keep_rows.append(min_idx)

        if not keep_rows:
            return None

        # Return selected rows (without the partition column)
        return table.take(keep_rows)
