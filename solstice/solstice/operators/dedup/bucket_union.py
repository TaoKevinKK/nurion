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

"""Bucket Union operator - matches documents by band_hash via UFService.

This operator receives (doc_id, band_hash) rows from the MinHashEncoder
stage and sends them to the Union-Find Service for matching. The UFService
shards maintain a band_hash -> doc_id index: when two docs share the same
band_hash, they are union'd.

Key design: matching happens at the SHARD level, not within a single batch.
This means docs from different source splits with the same band_hash get
union'd correctly, even without shuffle partition routing.

This replaces the old CandidatePairOperator + Connected Components approach:
- O(n) per bucket (chain union) instead of O(n^2) pairwise comparison
- No multi-round iteration needed (Union-Find is one-pass)
- Cross-batch matching via shard-side band_hash index

Pipeline position:
    MinHashEncoder -> BucketUnionOperator -> DedupFilter
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import Operator, OperatorConfig, OperatorRuntime, operator
from solstice.serve.union_find.client import UFClient


@dataclass
class BucketUnionOperatorConfig(OperatorConfig):
    """Configuration for bucket union operator.

    Attributes:
        doc_id_column: Column containing document ID
        band_hash_column: Column containing the band hash (from MinHashEncoder)
        uf_client: UFClient instance for RPC to Union-Find Service.
            Injected at runtime by the workflow before job submission.
    """

    doc_id_column: str = "doc_id"
    band_hash_column: str = "band_hash"
    uf_client: Optional[UFClient] = field(default=None, repr=False)


@operator(BucketUnionOperatorConfig)
class BucketUnionOperator(Operator):
    """Stateless operator that sends (band_hash, doc_id) to UFService for matching.

    For each batch of rows:
    1. Extract (band_hash, doc_id) pairs
    2. Send to UFClient.batch_match_and_union()
    3. UFService shards maintain band_hash index and union matching docs
    4. Return None (no output payload -- side-effect only)

    Cross-batch matching: The UFService shards keep a persistent band_hash
    index. When doc A (batch 1) and doc B (batch 2) share a band_hash, the
    shard sees the match on batch 2 arrival and unions A with B. This works
    regardless of source split boundaries.
    """

    def __init__(self, config: BucketUnionOperatorConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self.union_config = config

        if config.uf_client is None:
            raise ValueError(
                "BucketUnionOperatorConfig.uf_client must be set. "
                "Inject UFClient from UnionFindServiceManager.create_client()."
            )
        self._client = config.uf_client

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Send (band_hash, doc_id) entries to UFService for matching."""
        if payload is None:
            return None

        table = payload.to_table()
        if table.num_rows == 0:
            return None

        config = self.union_config

        doc_ids = table.column(config.doc_id_column).to_pylist()
        band_hashes = table.column(config.band_hash_column).to_pylist()

        # Build entries: (band_hash, doc_id)
        entries: list[tuple[int, str]] = [
            (int(bh), str(did)) for bh, did in zip(band_hashes, doc_ids)
        ]

        # Send to UFService -- matching happens at the shard level
        result = self._client.batch_match_and_union(entries)

        self.logger.info(
            f"Sent {len(entries)} entries to UFService: "
            f"{result['matches']} matches, {result['new_hashes']} new hashes, "
            f"{result['cross_shard']} cross-shard"
        )

        # No downstream output needed
        return None
