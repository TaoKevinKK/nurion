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

"""Dedup filter operator - filters duplicates based on Union-Find clusters.

This operator receives the original documents and filters them based on
cluster membership from the Union-Find Service. For each cluster, only
the representative document (smallest doc_id) is kept.

Two modes of operation:
1. **Lookup mode**: Calls UFClient.batch_find() to get cluster_id per doc
2. **Preloaded mode**: Uses a pre-exported cluster table (Arrow Table)
   for environments where UFService may already be shut down

Pipeline position:
    BucketUnion -> [cross-shard resolution] -> DedupFilter -> Sink
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pyarrow as pa

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import Operator, OperatorConfig, OperatorRuntime, operator
from solstice.serve.union_find.client import UFClient


@dataclass
class DedupFilterOperatorConfig(OperatorConfig):
    """Configuration for dedup filter operator.

    Attributes:
        id_column: Column containing document ID
        uf_client: UFClient for looking up cluster membership.
            Either uf_client or cluster_table must be set.
        cluster_table: Pre-exported cluster table with (doc_id, cluster_id).
            Used when UFService is already shut down.
    """

    id_column: str = "id"
    uf_client: Optional[UFClient] = field(default=None, repr=False)
    cluster_table: Optional[pa.Table] = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.uf_client is None and self.cluster_table is None:
            raise ValueError("Either uf_client or cluster_table must be provided")


@operator(DedupFilterOperatorConfig)
class DedupFilterOperator(Operator):
    """Stateless operator that filters duplicate documents.

    For each batch of documents:
    1. Look up cluster_id for each doc_id (via UFClient or cluster_table)
    2. For docs where doc_id == cluster_id (they are the representative):
       keep the document
    3. For docs where doc_id != cluster_id: drop the document (it's a duplicate)

    This keeps exactly one document per cluster (the one whose doc_id
    equals the cluster representative, which is the smallest doc_id
    due to Union-Find's min-root convention).
    """

    def __init__(self, config: DedupFilterOperatorConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self.filter_config = config
        self._client = config.uf_client

        # Build lookup dict from pre-exported cluster table
        self._cluster_lookup: Optional[dict[str, str]] = None
        if config.cluster_table is not None:
            ct = config.cluster_table
            doc_ids = ct.column("doc_id").to_pylist()
            cluster_ids = ct.column("cluster_id").to_pylist()
            self._cluster_lookup = dict(zip(doc_ids, cluster_ids))

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Filter duplicates from a batch of documents."""
        if payload is None:
            return None

        table = payload.to_table()
        if table.num_rows == 0:
            return None

        config = self.filter_config

        if config.id_column not in table.column_names:
            raise ValueError(f"ID column '{config.id_column}' not found in table")

        doc_ids = table.column(config.id_column).to_pylist()
        str_doc_ids = [str(d) for d in doc_ids]

        # Look up cluster_ids
        if self._client is not None:
            cluster_ids = self._client.batch_find(str_doc_ids)
        elif self._cluster_lookup is not None:
            cluster_ids = [self._cluster_lookup.get(d, d) for d in str_doc_ids]
        else:
            # No dedup info: pass through all documents
            return payload

        # Keep only representative documents (doc_id == cluster_id)
        keep_indices: list[int] = []
        for i, (doc_id, cluster_id) in enumerate(zip(str_doc_ids, cluster_ids)):
            if doc_id == cluster_id:
                keep_indices.append(i)

        if not keep_indices:
            return None

        if len(keep_indices) == table.num_rows:
            # All rows kept (no duplicates in this batch)
            return payload

        filtered_table = table.take(keep_indices)

        self.logger.debug(
            f"Filtered {table.num_rows - len(keep_indices)} duplicates, "
            f"kept {len(keep_indices)} documents"
        )

        return SplitPayload(data=filtered_table, split_id=split.split_id)
