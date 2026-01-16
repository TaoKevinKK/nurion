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

"""LLM/VLM inference operators for Solstice.

Provides:
- SGLangRouterActor: Manages SGLang router lifecycle
- SGLangWorkerActor: Manages SGLang worker with dynamic registration
- LLMStageMaster: Orchestrates router and workers
- LLMOperator: Unified LLM/VLM inference operator (text-only, single image, multi-image)
"""

from solstice.operators.llm.config import (
    RouterConfig,
    WorkerConfig,
)
from solstice.operators.llm.router_actor import SGLangRouterActor
from solstice.operators.llm.worker_actor import SGLangWorkerActor
from solstice.operators.llm.stage_master import LLMStageMaster
from solstice.operators.llm.operator import (
    LLMOperator,
    LLMOperatorConfig,
)

__all__ = [
    # Configs
    "RouterConfig",
    "WorkerConfig",
    "LLMOperatorConfig",
    # Actors
    "SGLangRouterActor",
    "SGLangWorkerActor",
    # Stage Master
    "LLMStageMaster",
    # Operators
    "LLMOperator",
]
