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

This is a distributed version that works across partitions:
- Labels are stored in SlateDB (external state)
- Each partition maintains labels for its assigned documents
- Messages are shuffled between partitions
- Convergence is detected globally by the runner

ALL OPERATORS ARE STATELESS:
- No in-memory caches or state
- All state is managed via SlateDB
- Enables fault tolerance and elastic scaling

Stages:
1. **CCInitOperator**: Initialize labels (label = doc_id) from candidate pairs
2. **CCIterateOperator**: One round of label propagation (reduce step)
3. **CCMessageOperator**: Generate messages for next iteration (map step)

The runner orchestrates iterations until convergence.
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

    Attributes:
        doc_id_column: Column for document ID
        neighbor_label_column: Column for neighbor's label
        current_label_column: Column for current label (in input)
        state_store_path: Path for SlateDB state storage
        max_iterations: Maximum iterations before forced stop
        convergence_threshold: Number of changes below which to stop (0 = require full convergence)
    """

    doc_id_column: str = "doc_id"
    neighbor_label_column: str = "neighbor_label"
    current_label_column: str = "current_label"
    # state_store_path is inherited from ShuffleOperatorConfig
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

    Input: Messages (doc_id, neighbor_label) + current labels
    Output: Updated labels (doc_id, label, changed)

    For each document, the new label is the minimum of:
    - Current label (from input or SlateDB)
    - All received neighbor labels

    State stored in SlateDB per partition:
    - label:{doc_id} -> current label
    - edges:{doc_id} -> comma-separated neighbor doc_ids (for re-iteration)

    Iteration Protocol:
    - Iteration 1: `process_data()` - process messages, store edges + labels
    - Iteration 2+: `recompute_from_state()` - recompute labels from stored edges
    - `reset_iteration()` - Clear change counter before new iteration
    - `get_iteration_changes()` - Get total changes for convergence check
    """

    def __init__(self, config: CCIterateConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self.iterate_config = config

        # Iteration tracking (in-memory for current batch, persisted to state store)
        self._iteration_changes: int = 0

    def _get_partition_for_doc(self, doc_id: str) -> int:
        """Compute partition for a doc_id using consistent hashing."""
        import hashlib

        h = int(hashlib.sha256(doc_id.encode()).hexdigest(), 16)
        return h % self.num_partitions

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
        """Process messages in iteration 1: store edges + compute labels.

        In iteration 1, neighbor_label is actually the neighbor's doc_id
        (since initially label = doc_id). We store these as edges for
        subsequent iterations.

        Optimized for batch I/O:
        1. Pre-compute all partitions and acquire upfront
        2. Batch read all labels and edges needed
        3. Process all docs in memory
        4. Batch write all results at the end
        """
        config = self.iterate_config

        doc_ids = table.column(config.doc_id_column).to_pylist()
        neighbor_labels = table.column(config.neighbor_label_column).to_pylist()

        # Get current labels from table if available
        current_labels_from_table: Dict[str, str] = {}
        if config.current_label_column in table.column_names:
            current_label_values = table.column(config.current_label_column).to_pylist()
            for doc_id, current_label in zip(doc_ids, current_label_values):
                if doc_id not in current_labels_from_table:
                    current_labels_from_table[doc_id] = current_label

        # Group messages by doc_id and collect edges
        messages_by_doc: Dict[str, List[str]] = {}
        edges_by_doc: Dict[str, set[str]] = {}
        for doc_id, neighbor_label in zip(doc_ids, neighbor_labels):
            if doc_id not in messages_by_doc:
                messages_by_doc[doc_id] = []
                edges_by_doc[doc_id] = set()
            messages_by_doc[doc_id].append(neighbor_label)
            edges_by_doc[doc_id].add(neighbor_label)

        # === Phase 1: Pre-compute partitions and acquire all upfront ===
        docs_by_partition: Dict[int, set[str]] = {}
        doc_to_partition: Dict[str, int] = {}
        for doc_id in messages_by_doc.keys():
            partition_id = self._get_partition_for_doc(doc_id)
            doc_to_partition[doc_id] = partition_id
            if partition_id not in docs_by_partition:
                docs_by_partition[partition_id] = set()
            docs_by_partition[partition_id].add(doc_id)

        # Acquire all partitions upfront (one check per partition, not per doc)
        for partition_id in docs_by_partition.keys():
            self._ensure_partition_acquired(partition_id)

        # === Phase 2: Batch read from state store ===
        stored_labels: Dict[str, str] = {}
        stored_edges: Dict[str, set[str]] = {}

        if self.state_store is not None:
            # Build batch read requests
            read_requests: list[tuple[int, bytes]] = []
            for doc_id, partition_id in doc_to_partition.items():
                # Only read label if not in table
                if doc_id not in current_labels_from_table:
                    read_requests.append((partition_id, f"label:{doc_id}".encode()))
                # Always read edges to merge
                read_requests.append((partition_id, f"edges:{doc_id}".encode()))

            # Also read existing doc_ids for each partition
            for partition_id in docs_by_partition.keys():
                read_requests.append((partition_id, b"__doc_ids__"))

            # Batch read
            read_results = self.state_store.get_batch(read_requests)

            # Parse results
            for (partition_id, key), value in read_results.items():
                if value is None:
                    continue
                key_str = key.decode()
                if key_str.startswith("label:"):
                    doc_id = key_str[6:]
                    stored_labels[doc_id] = value.decode()
                elif key_str.startswith("edges:"):
                    doc_id = key_str[6:]
                    edges_str = value.decode()
                    if edges_str:
                        stored_edges[doc_id] = set(edges_str.split(","))

        # === Phase 3: Process all docs in memory ===
        results = []
        changes = 0
        writes: list[tuple[int, bytes, bytes]] = []  # Collect writes for batch

        for doc_id, neighbor_labels_list in messages_by_doc.items():
            partition_id = doc_to_partition[doc_id]

            # Get current label: table > state store > default
            current_label = (
                current_labels_from_table.get(doc_id) or stored_labels.get(doc_id) or doc_id
            )

            # New label is minimum of current and all neighbors
            all_labels = [current_label] + neighbor_labels_list
            new_label = min(all_labels, key=str)

            changed = new_label != current_label
            if changed:
                changes += 1

            # Collect writes (don't write yet)
            if self.state_store is not None:
                writes.append((partition_id, f"label:{doc_id}".encode(), new_label.encode()))
                # Merge edges
                existing_edges = stored_edges.get(doc_id, set())
                all_edges = existing_edges | edges_by_doc[doc_id]
                writes.append(
                    (partition_id, f"edges:{doc_id}".encode(), ",".join(sorted(all_edges)).encode())
                )

            results.append({"doc_id": doc_id, "label": new_label, "changed": changed})

        if not results:
            return None

        # === Phase 4: Batch write to state store ===
        self._iteration_changes += changes

        if self.state_store is not None:
            # Add doc_ids metadata for each partition
            for partition_id, doc_ids_set in docs_by_partition.items():
                # Get existing doc_ids from batch read results
                existing_key = (partition_id, b"__doc_ids__")
                existing_value = read_results.get(existing_key) if "read_results" in dir() else None
                existing_doc_ids = set()
                if existing_value:
                    existing_str = existing_value.decode()
                    if existing_str:
                        existing_doc_ids = set(existing_str.split(","))
                all_doc_ids = existing_doc_ids | doc_ids_set
                writes.append(
                    (partition_id, b"__doc_ids__", ",".join(sorted(all_doc_ids)).encode())
                )

            # Write __changes__ only to FIRST partition (avoid overcounting when master sums)
            # Master reads from all partitions, so writing to each would cause N*changes
            first_partition = min(docs_by_partition.keys())
            writes.append((first_partition, b"__changes__", str(self._iteration_changes).encode()))

            # Single batch write (one flush per partition)
            self.state_store.put_batch(writes)

        self.logger.debug(
            f"CC iteration 1: {changes} label changes (total: {self._iteration_changes})"
        )

        return pa.table(
            {
                "doc_id": [r["doc_id"] for r in results],
                "label": [r["label"] for r in results],
                "changed": [r["changed"] for r in results],
            }
        )

    def _get_edges_from_store(self, doc_id: str) -> set[str]:
        """Get stored edges for a doc from state store."""
        if self.state_store is None:
            return set()
        partition_id = self._get_partition_for_doc(doc_id)
        self._ensure_partition_acquired(partition_id)
        stored = self.state_store.get(partition_id, f"edges:{doc_id}".encode())
        if stored is None:
            return set()
        edges_str = stored.decode()
        if not edges_str:
            return set()
        return set(edges_str.split(","))

    def _get_label_from_store(self, doc_id: str) -> str:
        """Get stored label for a doc from state store."""
        if self.state_store is None:
            return doc_id
        partition_id = self._get_partition_for_doc(doc_id)
        self._ensure_partition_acquired(partition_id)
        stored = self.state_store.get(partition_id, f"label:{doc_id}".encode())
        if stored is None:
            return doc_id
        return stored.decode()

    def _get_doc_ids_from_store(self, partition_id: int) -> set[str]:
        """Get all doc_ids in a partition from state store."""
        if self.state_store is None:
            return set()
        self._ensure_partition_acquired(partition_id)
        stored = self.state_store.get(partition_id, b"__doc_ids__")
        if stored is None:
            return set()
        doc_ids_str = stored.decode()
        if not doc_ids_str:
            return set()
        return set(doc_ids_str.split(","))

    @master_callable
    def recompute_from_state(self, assigned_partitions: Optional[List[int]] = None) -> int:
        """Recompute labels from stored edges (for iteration 2+).

        Optimized for batch I/O:
        1. Batch read all doc_ids, labels, edges upfront
        2. Process all docs in memory
        3. Batch write all changed labels at the end

        Args:
            assigned_partitions: List of partitions this worker handles.
                If None, returns 0 (worker must provide partitions).

        Returns:
            Number of label changes in this iteration
        """
        if self.state_store is None:
            self.logger.warning("No state store configured, cannot recompute")
            return 0

        if not assigned_partitions:
            self.logger.warning("No partitions provided, cannot recompute")
            return 0

        # === Phase 1: Acquire partitions and batch read doc_ids ===
        for partition_id in assigned_partitions:
            self._ensure_partition_acquired(partition_id)

        # Read all doc_ids first
        doc_id_reads = [(p, b"__doc_ids__") for p in assigned_partitions]
        doc_id_results = self.state_store.get_batch(doc_id_reads)

        # Parse doc_ids per partition
        docs_by_partition: Dict[int, set[str]] = {}
        all_doc_ids: set[str] = set()
        for partition_id in assigned_partitions:
            value = doc_id_results.get((partition_id, b"__doc_ids__"))
            if value:
                doc_ids_str = value.decode()
                if doc_ids_str:
                    docs = set(doc_ids_str.split(","))
                    docs_by_partition[partition_id] = docs
                    all_doc_ids.update(docs)

        if not all_doc_ids:
            return 0

        # === Phase 2: Batch read all labels and edges ===
        read_requests: list[tuple[int, bytes]] = []
        doc_to_partition: Dict[str, int] = {}

        for partition_id, doc_ids in docs_by_partition.items():
            for doc_id in doc_ids:
                doc_to_partition[doc_id] = partition_id
                read_requests.append((partition_id, f"label:{doc_id}".encode()))
                read_requests.append((partition_id, f"edges:{doc_id}".encode()))

        read_results = self.state_store.get_batch(read_requests)

        # Parse into dicts
        labels: Dict[str, str] = {}
        edges: Dict[str, set[str]] = {}
        for (partition_id, key), value in read_results.items():
            if value is None:
                continue
            key_str = key.decode()
            if key_str.startswith("label:"):
                doc_id = key_str[6:]
                labels[doc_id] = value.decode()
            elif key_str.startswith("edges:"):
                doc_id = key_str[6:]
                edges_str = value.decode()
                if edges_str:
                    edges[doc_id] = set(edges_str.split(","))

        # === Phase 3: Process all docs in memory ===
        changes = 0
        writes: list[tuple[int, bytes, bytes]] = []

        for doc_id in all_doc_ids:
            partition_id = doc_to_partition[doc_id]
            current_label = labels.get(doc_id, doc_id)
            doc_edges = edges.get(doc_id, set())

            if not doc_edges:
                continue

            # Get neighbor labels (from our in-memory dict)
            neighbor_labels = [labels.get(n, n) for n in doc_edges]

            # Compute new label
            all_labels = [current_label] + neighbor_labels
            new_label = min(all_labels, key=str)

            if new_label != current_label:
                changes += 1
                writes.append((partition_id, f"label:{doc_id}".encode(), new_label.encode()))

        # === Phase 4: Batch write ===
        self._iteration_changes += changes

        # Write __changes__ only to FIRST partition (avoid overcounting when master sums)
        # Note: In iteration 2+, master uses return value directly, but we write for consistency
        if assigned_partitions:
            first_partition = min(assigned_partitions)
            writes.append((first_partition, b"__changes__", str(self._iteration_changes).encode()))

        if writes:
            self.state_store.put_batch(writes)

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
