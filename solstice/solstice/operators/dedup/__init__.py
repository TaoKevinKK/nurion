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

"""Deduplication operators using Union-Find Service architecture.

Pipeline:
    Source -> MinHashEncoder -> BucketUnion(UFService) -> DedupFilter -> Sink

Components:
- MinHashEncoderConfig/Operator: Compute MinHash signatures, shuffle by bucket
- BucketUnionConfig/Operator: Union same-bucket docs via UFService RPC
- DedupFilterConfig/Operator: Filter duplicates using cluster results
"""

from solstice.operators.dedup.encoder import MinHashEncoderConfig
from solstice.operators.dedup.bucket_union import BucketUnionOperatorConfig
from solstice.operators.dedup.filter import DedupFilterOperatorConfig

__all__ = [
    "MinHashEncoderConfig",
    "BucketUnionOperatorConfig",
    "DedupFilterOperatorConfig",
]
