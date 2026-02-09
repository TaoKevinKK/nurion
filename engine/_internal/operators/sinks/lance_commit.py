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

"""Lance sink committer: queue-based fragment accumulation and smart batched commits.

Workers write fragments via lance.fragment.write_fragments() (no version created),
then push serialized FragmentMetadata JSON to a commit queue (non-blocking). This
committer runs as a background task in StageMaster, consuming from the commit
queue and batching commits intelligently using a dual-threshold policy.

Important: Messages are only acked AFTER a successful commit, not after accumulate.
This ensures fragments are not lost if the committer crashes before committing.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import lance
import pyarrow as pa
from lance import FragmentMetadata, LanceOperation

from _internal.queue import WorkQueueQueueClient


@dataclass
class LanceCommitPolicy:
    """Smart commit scheduling policy.

    Dual-threshold approach (similar to Kafka's linger.ms + batch.size):
    commits when ANY threshold is met.
    """

    interval_s: float = 30.0
    """Minimum seconds between commits. Commits when elapsed AND fragments pending."""

    fragment_threshold: int = 10
    """Commit when this many fragments accumulate."""

    row_threshold: int = 100_000
    """Commit when this many rows accumulate (0 = disabled)."""


# (msg_id, claim_token) pair for deferred ack
_PendingAck = Tuple[str, str]


class LanceSinkCommitter:
    """Queue-based commit coordinator for Lance sink.

    Implements the SinkCommitter protocol. Consumes FragmentMetadata from
    a commit queue, accumulates fragments, and commits to the Lance dataset
    on a smart schedule.

    Messages are acked only after a successful commit to prevent data loss.

    Lifecycle (managed by StageMaster):
    1. run_commit_loop() - background task claiming and committing
    2. finalize() - drain remaining fragments, final commit
    """

    def __init__(
        self,
        table_path: str,
        mode: str = "append",
        policy: Optional[LanceCommitPolicy] = None,
        storage_options: Optional[Dict[str, str]] = None,
    ):
        self._table_path = table_path
        self._mode = mode
        self._policy = policy or LanceCommitPolicy()
        self._storage_options = storage_options
        self._logger = logging.getLogger("LanceSinkCommitter")

        # Accumulated state: fragments + their pending acks
        self._pending_fragments: List[FragmentMetadata] = []
        self._pending_acks: List[_PendingAck] = []
        self._last_commit_time = time.time()

        # Version tracking for optimistic concurrency
        self._read_version: Optional[int] = None
        self._schema: Optional[pa.Schema] = None
        self._first_commit = True

    async def run_commit_loop(
        self, queue_client: WorkQueueQueueClient, commit_queue_name: str
    ) -> None:
        """Background task: claim from commit queue, accumulate, commit on schedule."""
        self._logger.info(
            f"Starting commit loop for {self._table_path} "
            f"(interval={self._policy.interval_s}s, "
            f"fragment_threshold={self._policy.fragment_threshold}, "
            f"row_threshold={self._policy.row_threshold})"
        )

        try:
            while True:
                self._claim_and_accumulate(queue_client, commit_queue_name)
                if self._should_commit():
                    self._do_commit(queue_client, commit_queue_name)
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            self._logger.debug("Commit loop cancelled")
            raise

    async def finalize(self, queue_client: WorkQueueQueueClient, commit_queue_name: str) -> None:
        """Drain commit queue and do final commit."""
        self._logger.info("Finalizing: draining commit queue for final commit")

        while True:
            records = queue_client.claim(
                commit_queue_name,
                batch_size=100,
                timeout_ms=500,
            )
            if not records:
                break
            for record in records:
                fragment = self._parse_fragment(record.value)
                if fragment is not None:
                    self._pending_fragments.append(fragment)
                    if record.claim_token:
                        self._pending_acks.append((record.msg_id, record.claim_token))

        if self._pending_fragments:
            self._do_commit(queue_client, commit_queue_name)
            self._logger.info("Final commit complete")
        else:
            self._logger.info("No pending fragments for final commit")

    # =========================================================================
    # Internal Methods
    # =========================================================================

    def _claim_and_accumulate(
        self, queue_client: WorkQueueQueueClient, commit_queue_name: str
    ) -> None:
        """Claim messages from commit queue and accumulate fragment metadata.

        Messages are NOT acked here -- they are acked after a successful commit.
        """
        try:
            records = queue_client.claim(
                commit_queue_name,
                batch_size=50,
                timeout_ms=100,
            )
        except Exception as e:
            self._logger.warning(f"Error claiming from commit queue: {e}")
            return

        if not records:
            return

        for record in records:
            fragment = self._parse_fragment(record.value)
            if fragment is not None:
                self._pending_fragments.append(fragment)
                if record.claim_token:
                    self._pending_acks.append((record.msg_id, record.claim_token))

    def _parse_fragment(self, value: bytes) -> Optional[FragmentMetadata]:
        """Parse a single commit queue record into FragmentMetadata.

        Message format (from LanceSink):
            {"fragment": <fragment_json_dict>, "schema_b64": "<base64_arrow_schema>"}

        The schema is extracted from the first message and cached for use
        in the first Overwrite commit (when the dataset doesn't exist yet).
        """
        try:
            parsed = json.loads(value.decode())

            # New format: wrapper with fragment + schema
            if isinstance(parsed, dict) and "fragment" in parsed:
                frag_json = parsed["fragment"]
                # Extract schema from first message that carries it
                if self._schema is None and "schema_b64" in parsed:
                    schema_bytes = base64.b64decode(parsed["schema_b64"])
                    self._schema = pa.ipc.read_schema(pa.BufferReader(schema_bytes))
                return FragmentMetadata.from_json(json.dumps(frag_json))

            # Legacy format: raw fragment JSON
            return FragmentMetadata.from_json(
                json.dumps(parsed) if isinstance(parsed, dict) else value.decode()
            )
        except Exception as e:
            self._logger.error(f"Error parsing commit record: {e}")
            return None

    def _ack_pending(self, queue_client: WorkQueueQueueClient, commit_queue_name: str) -> None:
        """Ack all pending messages after a successful commit."""
        if not self._pending_acks:
            return
        msg_ids = [a[0] for a in self._pending_acks]
        claim_tokens = [a[1] for a in self._pending_acks]
        try:
            queue_client.ack(commit_queue_name, msg_ids, claim_tokens=claim_tokens)
        except Exception as e:
            self._logger.warning(f"Error acking {len(msg_ids)} commit messages: {e}")
        self._pending_acks.clear()

    def _should_commit(self) -> bool:
        """Check if accumulated fragments should be committed."""
        if not self._pending_fragments:
            return False

        if len(self._pending_fragments) >= self._policy.fragment_threshold:
            return True

        if self._policy.row_threshold > 0:
            total_rows = sum(f.physical_rows for f in self._pending_fragments)
            if total_rows >= self._policy.row_threshold:
                return True

        elapsed = time.time() - self._last_commit_time
        if elapsed >= self._policy.interval_s:
            return True

        return False

    def _do_commit(self, queue_client: WorkQueueQueueClient, commit_queue_name: str) -> None:
        """Execute LanceDataset.commit() with accumulated fragments, then ack."""
        if not self._pending_fragments:
            return

        num_fragments = len(self._pending_fragments)

        try:
            op: LanceOperation.Overwrite | LanceOperation.Append
            if self._first_commit and self._mode in ("create", "overwrite"):
                schema = self._get_schema()
                op = LanceOperation.Overwrite(schema, self._pending_fragments)
                read_version = 0
            else:
                op = LanceOperation.Append(self._pending_fragments)
                read_version = self._read_version or self._get_current_version()

            ds = lance.LanceDataset.commit(
                self._table_path,
                op,
                read_version=read_version,
                storage_options=self._storage_options,
            )

            self._read_version = ds.version
            self._first_commit = False
            self._pending_fragments.clear()
            self._last_commit_time = time.time()

            # Ack messages AFTER successful commit
            self._ack_pending(queue_client, commit_queue_name)

            self._logger.info(f"Committed {num_fragments} fragments -> version {ds.version}")

        except Exception as e:
            self._logger.error(f"Commit failed: {e}")
            if "conflict" in str(e).lower() or "version" in str(e).lower():
                self._logger.info("Retrying commit with updated version...")
                self._read_version = self._get_current_version()
                self._do_commit(queue_client, commit_queue_name)
            else:
                raise

    def _get_schema(self) -> pa.Schema:
        """Get the Arrow schema for the dataset.

        Schema sources (in priority order):
        1. Cached from commit queue messages (set by _parse_fragment)
        2. Read from existing dataset on disk (for append mode)
        """
        if self._schema is not None:
            return self._schema
        try:
            ds = lance.dataset(self._table_path, storage_options=self._storage_options)
            self._schema = ds.schema
            return self._schema
        except Exception:
            raise RuntimeError(
                "Cannot determine schema for first commit. "
                "Ensure the dataset already exists or use 'append' mode."
            )

    def _get_current_version(self) -> int:
        """Get the current dataset version for read_version."""
        try:
            ds = lance.dataset(self._table_path, storage_options=self._storage_options)
            return ds.version
        except Exception:
            return 0
