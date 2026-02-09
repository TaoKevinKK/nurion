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

"""Stage Master component managers.

These managers handle specific concerns within a StageMaster:
- WorkerManager: Worker lifecycle (spawn, stop, status)
- RecoveryManager: Failure tracking and worker recovery
- SourceManager: SplitPlanner / DirectProducer lifecycle
- SinkManager: SinkCommitter background commit lifecycle
"""

from _internal.core.managers.recovery_manager import RecoveryManager
from _internal.core.managers.sink_manager import SinkManager
from _internal.core.managers.source_manager import SourceManager
from _internal.core.managers.worker_manager import WorkerManager

__all__ = [
    "WorkerManager",
    "RecoveryManager",
    "SourceManager",
    "SinkManager",
]
