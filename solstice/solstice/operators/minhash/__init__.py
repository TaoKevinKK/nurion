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

"""Legacy MinHash operators (deprecated).

This module contains the original MinHash signature computation operator.
For the current dedup implementation using Union-Find Service architecture,
see ``solstice.operators.dedup`` instead.
"""

from solstice.operators.minhash.compute import (
    MinHashComputeConfig,
    MinHashComputeOperator,
)

__all__ = [
    "MinHashComputeConfig",
    "MinHashComputeOperator",
]
