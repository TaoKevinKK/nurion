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

"""Configuration for Union-Find Service."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class UFClusterConfig:
    """Configuration for a Union-Find cluster deployment.

    Attributes:
        cluster_id: Unique identifier for this cluster instance
        num_shards: Number of UFShard actors to create. Each shard manages
            a portion of the band_hash space. More shards = more parallelism
            but more cross-shard edges to resolve.
        checkpoint_interval: Number of operations between auto-checkpoints
            per shard. Set to 0 to disable. Only effective if PayloadStore
            is provided at deploy time.
        shard_memory_mb: Memory limit per shard actor (MB). Used for Ray
            resource scheduling.
        shard_num_cpus: CPU allocation per shard actor.
    """

    cluster_id: str = "dedup"
    num_shards: int = 16
    checkpoint_interval: int = 100_000
    shard_memory_mb: int = 4096
    shard_num_cpus: float = 1.0

    def __post_init__(self) -> None:
        if self.num_shards < 1:
            raise ValueError("num_shards must be >= 1")
        if not self.cluster_id:
            raise ValueError("cluster_id must be non-empty")
