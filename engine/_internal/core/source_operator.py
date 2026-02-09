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

"""Source operator base class for reading from external systems."""

from abc import abstractmethod
from typing import Any, Dict, Optional

from _internal.core.models import Split, SplitPayload
from _internal.core.operator import Operator, OperatorConfig, OperatorRuntime


class SourceOperator(Operator):
    """Base class for source operators that read data from external systems.

    Source operators maintain offset tracking for checkpoint/resume capability.
    Subclasses should update the offset after reading data using `update_offset()`.
    """

    def __init__(self, config: OperatorConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        # Offset tracking for checkpoint/resume
        self._current_offset: Dict[str, Any] = {}

    @abstractmethod
    def read(self, split: Split) -> Optional[SplitPayload]:
        """Read data for a specific split.

        Args:
            split: Split object containing all metadata needed to read data
                  (data_range, metadata, etc.)

        Returns:
            SplitPayload containing the data, or None if no data available

        Note:
            Implementations should call `update_offset()` after successful reads
            to enable checkpoint/resume functionality.
        """
        pass

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Process a split for source operators.

        For source operators, payload is None and split contains all metadata.
        This method calls read() with the split.
        """
        if payload is not None:
            raise ValueError("Source operators should not receive payload, only split")

        return self.read(split)

    def update_offset(self, offset: Dict[str, Any]) -> None:
        """Update the current read offset.

        Called by subclasses after successfully reading data.
        The offset is persisted during checkpoints for resume capability.

        Args:
            offset: Dictionary containing offset information (e.g., file position,
                   partition offset, row number, etc.)
        """
        self._current_offset.update(offset)

    def get_offset(self) -> Dict[str, Any]:
        """Get the current read offset for checkpointing.

        Returns:
            Dictionary containing the current offset state
        """
        return dict(self._current_offset)

    def restore_offset(self, offset: Dict[str, Any]) -> None:
        """Restore offset from a checkpoint.

        Called during job recovery to resume from a previous position.

        Args:
            offset: Dictionary containing offset information from checkpoint
        """
        self._current_offset = dict(offset)
        self.logger.info(f"Restored offset: {offset}")
