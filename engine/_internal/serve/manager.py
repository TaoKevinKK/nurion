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

Two modes:
- **Attached** (default): Actors are reference-counted and die with the job.
- **Detached**: Actors use `lifetime="detached"` and survive job exit.
  Use `ModelServiceManager.connect()` to reconnect from a new job.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

import ray

from _internal.serve.config import AutoscaleConfig, ModelConfig
from _internal.serve.pool import ModelPool, get_pool_actor_name
from _internal.serve.registry import REGISTRY_ACTOR_NAME, ModelRegistry

logger = logging.getLogger(__name__)


class ModelServiceManager:
    """Control plane for multi-model inference service.

    Usage (attached — actors die with job):
        manager = ModelServiceManager()
        await manager.deploy_model(config)
        # ... run pipeline ...
        await manager.shutdown()

    Usage (detached — actors survive job exit):
        # Job 1: deploy models (slow, one-time)
        manager = ModelServiceManager(detached=True)
        await manager.deploy_model(config)
        # Job exits, models keep running

        # Job 2: reuse existing models (fast)
        manager = ModelServiceManager.connect()
        # manager.registry is ready to use
        # ... run pipeline ...
        # Don't call shutdown() — keep models alive for next job

        # Job N: tear down when done
        manager = ModelServiceManager.connect()
        await manager.shutdown()
    """

    def __init__(
        self,
        autoscale_config: Optional[AutoscaleConfig] = None,
        detached: bool = False,
    ) -> None:
        """Initialize the manager and create a new registry actor.

        Args:
            autoscale_config: Default autoscaling config for all models
            detached: If True, create detached actors that survive job exit
        """
        self._autoscale_config = autoscale_config or AutoscaleConfig()
        self._detached = detached

        self._pools: dict[str, ray.actor.ActorHandle] = {}
        self._configs: dict[str, ModelConfig] = {}

        # Create registry actor
        actor_options: dict[str, Any] = {"name": REGISTRY_ACTOR_NAME}
        if detached:
            actor_options["lifetime"] = "detached"

        self._registry = ray.remote(ModelRegistry).options(**actor_options).remote()
        ray.get(self._registry.start.remote())

        mode = "detached" if detached else "attached"
        logger.info(f"ModelServiceManager initialized (mode={mode})")

    @classmethod
    def connect(cls) -> "ModelServiceManager":
        """Connect to an existing detached ModelServiceManager.

        Looks up the registry and pool actors by their well-known names.
        Raises RuntimeError if no detached serve layer is running.
        """
        instance = object.__new__(cls)
        instance._autoscale_config = AutoscaleConfig()
        instance._detached = True
        instance._pools = {}
        instance._configs = {}

        # Look up existing registry
        try:
            instance._registry = ray.get_actor(REGISTRY_ACTOR_NAME)
        except ValueError:
            raise RuntimeError(
                "No detached serve layer found. "
                "Deploy models first with ModelServiceManager(detached=True)."
            )

        # Discover existing pools by querying registry for known models
        try:
            models = ray.get(instance._registry.list_models.remote())
        except Exception:
            models = []

        for model_id in models:
            try:
                pool = ray.get_actor(get_pool_actor_name(model_id))
                instance._pools[model_id] = pool
                logger.info(f"Reconnected to pool for model {model_id}")
            except ValueError:
                logger.warning(f"Pool actor for {model_id} not found, skipping")

        logger.info(f"Connected to detached serve layer: {len(instance._pools)} model(s) active")
        return instance

    @property
    def registry(self) -> ray.actor.ActorHandle:
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
            Dict with deployment result

        Raises:
            ValueError: If model already deployed
            RuntimeError: If workers fail to start
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

        # Create ModelPool actor
        pool_options: dict[str, Any] = {"name": get_pool_actor_name(model_id)}
        if self._detached:
            pool_options["lifetime"] = "detached"

        pool = (
            ray.remote(ModelPool)
            .options(**pool_options)
            .remote(config, self._registry, self._detached)
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
                elapsed = time.time() - result["started_at"]
                raise RuntimeError(
                    f"Model {model_id} failed to start after {elapsed:.1f}s. "
                    f"Check worker logs for details (e.g. model download errors, "
                    f"GPU OOM, missing architectures)."
                )

        result["status"] = "ready"
        result["completed_at"] = time.time()
        result["duration_s"] = result["completed_at"] - result["started_at"]

        # Get endpoints from pool status (fetched from registry)
        pool_status = await pool.get_status.remote()
        result["endpoints"] = pool_status.get("endpoints", [])

        logger.info(
            f"Model {model_id} deployed: {len(result['endpoints'])} endpoints, "
            f"took {result['duration_s']:.1f}s"
        )

        return result

    async def undeploy_model(self, model_id: str) -> dict[str, Any]:
        """Undeploy a model."""
        if model_id not in self._pools:
            raise ValueError(f"Model {model_id} not found")

        result: dict[str, Any] = {
            "model_id": model_id,
            "started_at": time.time(),
        }

        logger.info(f"Undeploying model {model_id}")

        pool = self._pools[model_id]
        await pool.shutdown.remote()

        try:
            ray.kill(pool)
        except Exception:
            pass

        del self._pools[model_id]
        self._configs.pop(model_id, None)

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
        """Scale a model."""
        if model_id not in self._pools:
            raise ValueError(f"Model {model_id} not found")

        pool = self._pools[model_id]
        config = self._configs.get(model_id)

        if config:
            if min_workers is not None:
                config.min_workers = min_workers
            if max_workers is not None:
                config.max_workers = max_workers

        if target is not None:
            return await pool.scale_to.remote(target)

        return {
            "model_id": model_id,
            "status": "config_updated",
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

    def list_models(self) -> list[str]:
        """List all deployed model IDs."""
        return list(self._pools.keys())

    async def shutdown(self) -> None:
        """Shutdown the manager and all deployed models.

        Kills all actors (including detached ones) and releases GPUs.
        """
        logger.info("Shutting down ModelServiceManager")

        for model_id in list(self._pools.keys()):
            try:
                await self.undeploy_model(model_id)
            except Exception as e:
                logger.warning(f"Error undeploying {model_id}: {e}")

        try:
            ray.kill(self._registry)
            logger.info("ModelRegistry actor killed")
        except Exception as e:
            logger.warning(f"Error killing registry: {e}")

        logger.info("ModelServiceManager shutdown complete")

    async def __aenter__(self) -> "ModelServiceManager":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.shutdown()
