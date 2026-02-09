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

"""LLM/VLM inference operators for Nurion Runtime.

Provides two inference modes:

1. **Embedded Mode** (recommended for batch processing):
   - EmbeddedLLMOperator: Embeds vLLM/SGLang engine directly in workers
   - Zero HTTP overhead, maximum throughput
   - Supports vLLM and SGLang backends
   - KV Cache optimization

2. **External Mode** (for external services):
   - ExternalLLMOperator: Calls external LLM APIs via HTTP
   - Works with any OpenAI-compatible API
   - Includes rate limiting, circuit breaker, retries
"""

from _internal.operators.llm.embedded import (
    EmbeddedLLMOperator,
    EmbeddedLLMOperatorConfig,
)
from _internal.operators.llm.operator import (
    ExternalLLMOperator,
    ExternalLLMOperatorConfig,
)

__all__ = [
    # Embedded mode (recommended for batch processing)
    "EmbeddedLLMOperator",
    "EmbeddedLLMOperatorConfig",
    # External mode (for calling external services)
    "ExternalLLMOperator",
    "ExternalLLMOperatorConfig",
]
