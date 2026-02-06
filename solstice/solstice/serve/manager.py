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

"""Model Service Manager - Control plane for multi-model inference service.

The ModelServiceManager is the main entry point for deploying and managing
multiple model inference services. It provides a Python-first, imperative API
for model lifecycle management.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

import ray

from solstice.serve.config import AutoscaleConfig, ModelConfig
from solstice.serve.pool import ModelPool, get_pool_actor_name
from solstice.serve.registry import REGISTRY_ACTOR_NAME, ModelRegistry

logger = logging.getLogger(__name__)


class ModelServiceManager:
    """Control plane for multi-model inference service.

    The manager provides a unified interface to:
    - Deploy and undeploy models
    - Scale models (manually or automatically)
    - Monitor model status
    - Freeze/unfreeze autoscaling

    All operations are imperative and return results synchronously
    (or via async/await), making it easy to integrate into Python code.

    Usage:
        manager = ModelServiceManager()

        # Deploy models
        await manager.deploy_model(ModelConfig(
            model_id="decision",
            model_source="Qwen/Qwen2.5-7B-Instruct",
            min_workers=2,
            max_workers=8,
        ))

        await manager.deploy_model(ModelConfig(
            model_id="generation",
            model_source="Qwen/Qwen2.5-72B-Instruct",
            tensor_parallel_size=8,
            min_workers=1,
            max_workers=4,
        ))

        # Get endpoints
        endpoints = manager.get_endpoints("decision")

        # Scale manually
        await manager.scale_model("decision", target=6)

        # Freeze autoscaling
        await manager.freeze_model("generation")

        # Get status
        status = await manager.get_model_status("decision")

        # Undeploy
        await manager.undeploy_model("decision")
    """

    def __init__(
        self,
        autoscale_config: Optional[AutoscaleConfig] = None,
    ) -> None:
        """Initialize the manager.

        Args:
            autoscale_config: Default autoscaling config for all models
        """
        self._autoscale_config = autoscale_config or AutoscaleConfig()

        self._pools: dict[str, ray.ActorHandle] = {}
        self._configs: dict[str, ModelConfig] = {}

        # Create registry actor
        self._registry = (
            ray.remote(ModelRegistry)
            .options(
                name=REGISTRY_ACTOR_NAME,
            )
            .remote()
        )
        ray.get(self._registry.start.remote())

        logger.info("ModelServiceManager initialized")

    @property
    def registry(self) -> ray.ActorHandle:
        """Registry ActorHandle — pass to ExternalLLMOperatorConfig."""
        return self._registry

    async def deploy_model(
        self,
        config: ModelConfig,
        wait_ready: bool = True,
        timeout: float = 600.0,
        autoscale_config: Optional[AutoscaleConfig] = None,
    ) -> dict[str, Any]:
        """Deploy a model.

        Creates a ModelPool for the model and scales to min_workers.
        Optionally waits for at least one worker to be ready.

        Args:
            config: Model configuration
            wait_ready: Whether to wait for workers to be ready
            timeout: Timeout for waiting (only if wait_ready=True)
            autoscale_config: Override autoscale config for this model

        Returns:
            Dict with deployment result:
            - model_id: Model identifier
            - status: "ready" | "deploying"
            - endpoints: List of endpoint URLs
            - duration_s: Time taken

        Raises:
            ValueError: If model already deployed
            TimeoutError: If wait_ready times out
        """
        model_id = config.model_id

        if model_id in self._pools:
            raise ValueError(f"Model {model_id} already deployed")

        result: dict[str, Any] = {
            "model_id": model_id,
            "status": "deploying",
            "started_at": time.time(),
        }

        logger.info(f"Deploying model {model_id}: {config.model_source}")

        # Initialize Ray if needed
        if not ray.is_initialized():
            ray.init(address="auto")

        # Create ModelPool actor — pass registry handle so pool holds a ref
        pool = (
            ray.remote(ModelPool)
            .options(
                name=get_pool_actor_name(model_id),
            )
            .remote(config, self._registry)
        )

        self._pools[model_id] = pool
        self._configs[model_id] = config

        # Scale to min_workers
        scale_result = await pool.scale_to.remote(config.min_workers)
        result["scale_result"] = scale_result

        # Start autoscaler inside pool actor
        effective_autoscale_config = autoscale_config or self._autoscale_config
        ray.get(pool.start_autoscaler.remote(effective_autoscale_config))

        if wait_ready:
            # Wait for at least one worker to be ready
            is_ready = await pool.wait_ready.remote(timeout=timeout)
            if not is_ready:
                raise TimeoutError(f"Model {model_id} not ready after {timeout}s")

        result["status"] = "ready"
        result["completed_at"] = time.time()
        result["duration_s"] = result["completed_at"] - result["started_at"]

        # Get endpoints from pool
        endpoints = ray.get(pool.get_endpoints.remote())
        result["endpoints"] = endpoints

        logger.info(
            f"Model {model_id} deployed: {len(endpoints)} endpoints, "
            f"took {result['duration_s']:.1f}s"
        )

        return result

    async def undeploy_model(self, model_id: str) -> dict[str, Any]:
        """Undeploy a model.

        Stops autoscaling, shuts down the pool, and removes all state.

        Args:
            model_id: Model identifier

        Returns:
            Dict with undeploy result

        Raises:
            ValueError: If model not found
        """
        if model_id not in self._pools:
            raise ValueError(f"Model {model_id} not found")

        result: dict[str, Any] = {
            "model_id": model_id,
            "started_at": time.time(),
        }

        logger.info(f"Undeploying model {model_id}")

        # Shutdown pool (stops autoscaler + all workers)
        pool = self._pools[model_id]
        await pool.shutdown.remote()

        # Kill the pool actor
        try:
            ray.kill(pool)
        except Exception:
            pass

        del self._pools[model_id]
        del self._configs[model_id]

        result["completed_at"] = time.time()
        result["duration_s"] = result["completed_at"] - result["started_at"]
        result["status"] = "undeployed"

        logger.info(f"Model {model_id} undeployed")

        return result

    async def scale_model(
        self,
        model_id: str,
        target: Optional[int] = None,
        min_workers: Optional[int] = None,
        max_workers: Optional[int] = None,
    ) -> dict[str, Any]:
        """Scale a model.

        Can either scale to a specific target or update min/max bounds.

        Args:
            model_id: Model identifier
            target: Target number of workers (immediate scaling)
            min_workers: Update minimum workers
            max_workers: Update maximum workers

        Returns:
            Dict with scaling result

        Raises:
            ValueError: If model not found
        """
        if model_id not in self._pools:
            raise ValueError(f"Model {model_id} not found")

        pool = self._pools[model_id]
        config = self._configs[model_id]

        # Update config bounds
        if min_workers is not None:
            config.min_workers = min_workers
        if max_workers is not None:
            config.max_workers = max_workers

        # Scale to target
        if target is not None:
            return await pool.scale_to.remote(target)

        return {
            "model_id": model_id,
            "status": "config_updated",
            "min_workers": config.min_workers,
            "max_workers": config.max_workers,
        }

    async def freeze_model(self, model_id: str) -> None:
        """Freeze autoscaling for a model."""
        pool = self._pools.get(model_id)
        if pool is None:
            raise ValueError(f"Model {model_id} not found")
        ray.get(pool.freeze_autoscaler.remote())

    async def unfreeze_model(self, model_id: str) -> None:
        """Unfreeze autoscaling for a model."""
        pool = self._pools.get(model_id)
        if pool is None:
            raise ValueError(f"Model {model_id} not found")
        ray.get(pool.unfreeze_autoscaler.remote())

    def get_endpoints(self, model_id: str) -> list[str]:
        """Get endpoint URLs for a model."""
        pool = self._pools.get(model_id)
        if pool is None:
            return []
        return ray.get(pool.get_endpoints.remote())

    def list_models(self) -> list[str]:
        """List all deployed model IDs.

        Returns:
            List of model identifiers
        """
        return list(self._pools.keys())

    async def shutdown(self) -> None:
        """Shutdown the manager and all deployed models.

        Undeploys all models and kills the registry actor.
        """
        logger.info("Shutting down ModelServiceManager")

        # Undeploy all models
        for model_id in list(self._pools.keys()):
            try:
                await self.undeploy_model(model_id)
            except Exception as e:
                logger.warning(f"Error undeploying {model_id}: {e}")

        # Kill the registry actor
        try:
            ray.kill(self._registry)
            logger.info("ModelRegistry actor killed")
        except Exception as e:
            logger.warning(f"Error killing registry: {e}")

        logger.info("ModelServiceManager shutdown complete")

    async def __aenter__(self) -> "ModelServiceManager":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.shutdown()
