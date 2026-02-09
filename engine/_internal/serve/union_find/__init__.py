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

"""Union-Find Service for distributed deduplication.

This package provides a distributed Union-Find cluster that can be deployed
as a long-lived service (similar to the LLM serve pattern). Pipeline operators
call the service via RPC to perform union/find operations.

Architecture:
    UnionFindServiceManager  (control plane)
        └── UFCluster        (manages shard lifecycle)
            └── UFShard[]    (Ray actors holding Union-Find state)

    UFClient                 (data plane - used by pipeline operators)
        └── routes to UFShard actors via doc_id hash

Usage:
    # Deploy service (before running pipeline)
    manager = UnionFindServiceManager()
    await manager.deploy(UFClusterConfig(num_shards=64))

    # In pipeline operator
    client = manager.create_client()
    await client.batch_union([("doc_a", "doc_b"), ("doc_c", "doc_d")])

    # Export results
    clusters = await manager.export_clusters()

    # Shutdown
    await manager.shutdown()
"""

from _internal.serve.union_find.config import UFClusterConfig
from _internal.serve.union_find.client import UFClient
from _internal.serve.union_find.manager import UnionFindServiceManager

__all__ = [
    "UFClusterConfig",
    "UFClient",
    "UnionFindServiceManager",
]
