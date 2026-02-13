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

"""Multi-Model Inference Service Layer.

This module provides a multi-model inference service layer with:
- Multiple models co-existing with independent scaling
- GPU bin-packing allocator for anti-fragmentation scheduling
- HTTP-based inference using vLLM/SGLang servers
- Client-side load balancing
- Automatic scaling based on load
- Service discovery via Ray Named Actors

Example:
    ```python
    from _internal.serve import create_manager, ModelClient, ModelConfig

    async def main():
        # Create Manager actor
        manager = create_manager(detached=True)

        # Deploy models with anti-fragmentation ordering
        await manager.deploy_model.remote([
            ModelConfig(
                model_id="generation",
                model_source="Qwen/Qwen2.5-72B-Instruct",
                tensor_parallel_size=8,
                min_workers=1,
                max_workers=4,
            ),
            ModelConfig(
                model_id="decision",
                model_source="Qwen/Qwen2.5-7B-Instruct",
                min_workers=2,
                max_workers=8,
            ),
        ])

        # Discover endpoints
        registry = ray.get(manager.get_registry.remote())
        client = ModelClient(registry)
        endpoints = await client.get_endpoints("decision")
    ```
"""

from _internal.serve.allocator import GPUAllocator
from _internal.serve.client import ModelClient
from _internal.serve.config import AutoscaleConfig, ModelConfig, WorkerState
from _internal.serve.manager import MANAGER_ACTOR_NAME, ModelServiceManager, create_manager
from _internal.serve.pool import ModelPool
from _internal.serve.registry import ModelRegistry
from _internal.serve.worker import InferenceWorker

__all__ = [
    # Config
    "ModelConfig",
    "AutoscaleConfig",
    "WorkerState",
    # Control Plane
    "ModelServiceManager",
    "create_manager",
    "MANAGER_ACTOR_NAME",
    "ModelPool",
    "GPUAllocator",
    # Data Plane
    "ModelClient",
    # Infrastructure
    "ModelRegistry",
    "InferenceWorker",
]
