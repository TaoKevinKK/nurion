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

"""Sink operator base class for writing to external systems."""

from typing import Any, Dict, Optional

from solstice.core.operator import Operator, OperatorConfig, OperatorRuntime


class SinkOperator(Operator):
    """Base class for sink operators with exactly-once semantics support.

    Sink operators can implement two-phase commit for exactly-once guarantees:
    1. `process_split()` - Buffer/stage writes (pre-commit)
    2. `prepare_commit()` - Prepare for commit (optional)
    3. `commit()` - Finalize writes
    4. `rollback()` - Rollback uncommitted writes on failure

    For simpler at-least-once semantics, just implement `process_split()`.
    """

    def __init__(self, config: OperatorConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        # Track pending writes for exactly-once
        self._pending_commit_id: Optional[str] = None
        self._commit_offset: Dict[str, Any] = {}

    def prepare_commit(self, checkpoint_id: str) -> bool:
        """Prepare for commit (phase 1 of two-phase commit).

        Called before checkpoint finalization. Implementations should
        flush any buffered data and prepare for commit.

        Args:
            checkpoint_id: The checkpoint ID this commit is associated with

        Returns:
            True if prepare succeeded, False otherwise
        """
        self._pending_commit_id = checkpoint_id
        return True

    def commit(self, checkpoint_id: str) -> bool:
        """Commit pending writes (phase 2 of two-phase commit).

        Called after checkpoint is successfully finalized.
        Implementations should finalize any staged writes.

        Args:
            checkpoint_id: The checkpoint ID to commit

        Returns:
            True if commit succeeded, False otherwise
        """
        if self._pending_commit_id == checkpoint_id:
            self._pending_commit_id = None
            return True
        return False

    def rollback(self, checkpoint_id: str) -> bool:
        """Rollback uncommitted writes.

        Called when checkpoint fails or job restarts.
        Implementations should discard any uncommitted staged writes.

        Args:
            checkpoint_id: The checkpoint ID to rollback

        Returns:
            True if rollback succeeded, False otherwise
        """
        if self._pending_commit_id == checkpoint_id:
            self._pending_commit_id = None
        return True

    def get_commit_offset(self) -> Dict[str, Any]:
        """Get the current commit offset for checkpointing.

        Returns:
            Dictionary containing commit state information
        """
        return dict(self._commit_offset)

    def restore_commit_offset(self, offset: Dict[str, Any]) -> None:
        """Restore commit offset from a checkpoint.

        Called during job recovery.

        Args:
            offset: Dictionary containing commit offset from checkpoint
        """
        self._commit_offset = dict(offset)
        self.logger.info(f"Restored commit offset: {offset}")
