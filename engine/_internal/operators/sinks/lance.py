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

"""Lance sink implementation with fragment-based writes and queue-based commits.

Each worker writes fragments independently using lance.fragment.write_fragments()
(no version/commit created). Fragment metadata is returned as RawOutputBytes,
which StageWorker pushes to the commit queue via atomic ack_and_forward.

This ensures:
- No ack-before-write: upstream is only acked when fragment is written AND
  metadata is pushed to the commit queue (atomic via ack_and_forward)
- No queue client in operator: StageWorker handles all queue communication
- Smart batched commits: LanceSinkCommitter in StageMaster accumulates
  fragments and commits on a time/size schedule
"""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Set

import pyarrow as pa
from lance.fragment import write_fragments

from _internal.core.models import RawOutputBytes, Split, SplitPayload
from _internal.core.operator import OperatorConfig, OperatorRuntime, PayloadResult, operator
from _internal.core.sink_operator import SinkOperator
from _internal.operators.sinks.lance_commit import LanceCommitPolicy, LanceSinkCommitter


@dataclass
class LanceSinkConfig(OperatorConfig):
    """Configuration for LanceSink operator."""

    table_path: str
    """Path to the Lance table (local or S3)."""

    mode: Literal["create", "append", "overwrite"] = "append"
    """Write mode for the table."""

    blob_columns: List[str] = field(default_factory=lambda: [])
    """Columns to store as Lance blobs (large binary with blob encoding)."""

    storage_options: Optional[Dict[str, str]] = None
    """Storage options for S3/cloud backends (e.g., aws_access_key_id, endpoint_url)."""

    # Merge upstream splits into larger fragments
    merge_batch_size: int = 10
    """Number of upstream messages to merge before writing a fragment.
    Larger values produce fewer, bigger fragments. Default 10."""

    # Commit policy
    commit_interval_s: float = 30.0
    """Minimum seconds between commits."""

    commit_fragment_threshold: int = 10
    """Commit when this many fragments accumulate."""

    commit_row_threshold: int = 100_000
    """Commit when this many rows accumulate (0 = disabled)."""

    def get_merge_upstream(self) -> int:
        return self.merge_batch_size

    def create_sink_committer(self) -> LanceSinkCommitter:
        """Create a sink committer for batched Lance commits."""
        storage_options = self.storage_options
        if storage_options is None and self.table_path.startswith("s3://"):
            from _internal.utils.remote import get_lance_storage_options

            bucket = self.table_path[5:].split("/")[0]
            storage_options = get_lance_storage_options(bucket)

        return LanceSinkCommitter(
            table_path=self.table_path,
            mode=self.mode,
            policy=LanceCommitPolicy(
                interval_s=self.commit_interval_s,
                fragment_threshold=self.commit_fragment_threshold,
                row_threshold=self.commit_row_threshold,
            ),
            storage_options=storage_options,
        )


@operator(LanceSinkConfig)
class LanceSink(SinkOperator):
    """Sink that writes records to a Lance table via fragment-based writes.

    Each process_split() call:
    1. Writes a fragment via lance.fragment.write_fragments() (no commit)
    2. Returns RawOutputBytes with fragment metadata JSON

    StageWorker handles the rest:
    - Pushes fragment metadata to the commit queue via ack_and_forward
    - The ack is atomic with the push, ensuring no data loss

    No internal buffering across splits -- each split becomes a fragment.
    The LanceSinkCommitter batches fragments into commits.
    """

    def __init__(self, config: LanceSinkConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        if not config.table_path:
            raise ValueError("table_path is required for LanceSink")

        self.table_path = config.table_path
        self.blob_columns: Set[str] = set(config.blob_columns)

        if config.storage_options:
            self.storage_options = config.storage_options
        elif self.table_path.startswith("s3://"):
            from _internal.utils.remote import get_lance_storage_options

            bucket = self.table_path[5:].split("/")[0]
            self.storage_options = get_lance_storage_options(bucket)
        else:
            self.storage_options = None  # type: ignore[assignment]

        self.logger = logging.getLogger(self.__class__.__name__)

    def process_split(self, split: Split, batch: Optional[SplitPayload] = None) -> PayloadResult:
        """Write a fragment and return metadata for the commit queue.

        Each call writes a fragment immediately via write_fragments().
        Returns RawOutputBytes containing the serialized FragmentMetadata,
        which StageWorker pushes to the commit queue atomically with the
        upstream ack.
        """
        if batch is None:
            raise ValueError("LanceSink requires a batch")

        table = self._build_table(batch)
        if table.num_rows == 0:
            return None

        # Write fragment (NO version/commit created)
        fragments = write_fragments(
            table,
            self.table_path,
            schema=table.schema,
            storage_options=self.storage_options,
        )

        # Return fragment metadata + schema as raw bytes for the commit queue.
        # Schema is needed by the committer for the first Overwrite commit
        # (dataset doesn't exist yet, so it can't read schema from disk).
        schema_b64 = base64.b64encode(table.schema.serialize().to_pybytes()).decode()
        payloads = [
            json.dumps({"fragment": frag.to_json(), "schema_b64": schema_b64}).encode()
            for frag in fragments
        ]
        return RawOutputBytes(payloads=payloads)

    def _build_table(self, batch: SplitPayload) -> pa.Table:
        """Build PyArrow table from batch, handling reserved columns and blob encoding.

        Operates directly on the Arrow table (zero-copy where possible) instead
        of round-tripping through Python dicts.
        """
        table = batch.data

        # Drop reserved Lance column names (columnar drop, no row iteration)
        reserved = [c for c in table.column_names if c in {"_rowid", "_rowaddr"}]
        if reserved:
            table = table.drop_columns(reserved)

        # Apply blob column encoding via schema metadata
        blob_cols = [c for c in table.column_names if c in self.blob_columns]
        if blob_cols:
            new_fields = []
            for f in table.schema:
                if f.name in self.blob_columns:
                    metadata = dict(f.metadata) if f.metadata else {}
                    metadata[b"lance-encoding:blob"] = b"true"
                    new_fields.append(pa.field(f.name, pa.large_binary(), metadata=metadata))
                else:
                    new_fields.append(f)

            new_schema = pa.schema(new_fields)
            new_columns = []
            for i, f in enumerate(table.schema):
                col = table.column(i)
                if f.name in self.blob_columns:
                    col = col.cast(pa.large_binary())
                new_columns.append(col)

            table = pa.table(dict(zip(table.column_names, new_columns)), schema=new_schema)

        return table

    def close(self) -> None:
        """No cleanup needed -- no buffer, no queue client."""
        pass
