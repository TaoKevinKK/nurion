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

from _internal.core.operator import Operator, OperatorConfig, OperatorRuntime


class SinkOperator(Operator):
    """Base class for sink operators.

    For simple sinks, just implement `process_split()`.
    For two-phase commit sinks, use `create_sink_committer()` on the config
    to provide a `SinkCommitter` that coordinates batched commits.
    """

    def __init__(self, config: OperatorConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
