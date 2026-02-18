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

The ModelServiceManager is a Ray actor that owns ModelPool instances (plain
objects) and a GPUAllocator (plain object). All scheduling and compaction
logic is local — no cross-actor RPC.

Two modes:
- **Attached** (default): Manager actor is reference-counted and dies with the job.
- **Detached**: Manager actor uses ``lifetime="detached"`` and survives job exit.
  Use ``ModelServiceManager.connect()`` to reconnect from a new job.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

import ray

from _internal.serve.allocator import GPUAllocator
from _internal.serve.config import AutoscaleConfig, ModelConfig
from _internal.serve.pool import ModelPool
from _internal.serve.registry import REGISTRY_ACTOR_NAME, SERVE_NAMESPACE, ModelRegistry
from _internal.webui.state.schema import encode_json, serve_model_key, serve_namespace

logger = logging.getLogger(__name__)

MANAGER_ACTOR_NAME = "nurion_model_service_manager"


@ray.remote
class ModelServiceManager:
    """Control plane for multi-model inference service.

    Ray actor that owns ModelPool instances (plain objects) and a
    GPUAllocator (plain object). All scheduling and compaction logic
    is local — no cross-actor RPC.

    Usage (attached — actor dies with job):
        manager = create_manager()
        await manager.deploy_model.remote(config)
        # ... run pipeline ...
        await manager.shutdown.remote()

    Usage (detached — actor survives job exit):
        # Job 1: deploy models (slow, one-time)
        manager = create_manager(detached=True)
        await manager.deploy_model.remote(configs)
        # Job exits, models keep running

        # Job 2: reuse existing models (fast)
        manager = ModelServiceManager.connect()
        registry = ray.get(manager.get_registry.remote())
        # ... run pipeline ...

        # Job N: tear down when done
        manager = ModelServiceManager.connect()
        await manager.shutdown.remote()
    """

    def __init__(
        self,
        autoscale_config: Optional[AutoscaleConfig] = None,
        detached: bool = False,
        broker_endpoint: Optional[str] = None,
    ) -> None:
        """Initialize the manager, allocator, and registry.

        Args:
            autoscale_config: Default autoscaling config for all models
            detached: Whether this manager is running in detached mode
            broker_endpoint: Optional WorkQueue broker URL for state persistence
        """
        self._autoscale_config = autoscale_config or AutoscaleConfig()
        self._detached = detached

        self._pools: dict[str, ModelPool] = {}
        self._configs: dict[str, ModelConfig] = {}
        self._allocator = GPUAllocator()
        self._allocator.refresh_nodes()

        # Optional state writer for serve monitoring
        self._state_writer: Optional[Any] = None
        if broker_endpoint:
            from _internal.queue import WorkQueueQueueClient

            self._state_writer = WorkQueueQueueClient(
                broker_endpoint,
                worker_id="serve-manager",
            )
            self._state_writer.start()

        # Create registry actor (still a separate actor for HTTP endpoint)
        actor_options: dict[str, Any] = {"name": REGISTRY_ACTOR_NAME}
        if detached:
            actor_options["lifetime"] = "detached"
            actor_options["namespace"] = SERVE_NAMESPACE

        self._registry = ray.remote(ModelRegistry).options(**actor_options).remote()
        ray.get(self._registry.start.remote())

        mode = "detached" if detached else "attached"
        logger.info(f"ModelServiceManager initialized (mode={mode})")

    @classmethod
    def connect(cls) -> ray.actor.ActorHandle:
        """Connect to an existing detached ModelServiceManager.

        Returns the Manager actor handle. All pools and allocator
        state live inside it — no need to discover N pool actors.

        Raises:
            RuntimeError: If no detached serve layer is running.
        """
        try:
            return ray.get_actor(MANAGER_ACTOR_NAME, namespace=SERVE_NAMESPACE)
        except ValueError:
            raise RuntimeError(
                "No detached serve layer found. "
                "Deploy models first with create_manager(detached=True)."
            )

    def get_registry(self) -> ray.actor.ActorHandle:
        """Get the registry actor handle."""
        return self._registry

    def _write_model_state(self, model_id: str, status: str) -> None:
        """Write model lifecycle metadata into WorkQueue state."""
        if self._state_writer is None:
            return
        import time

        data = {
            "model_id": model_id,
            "status": status,
            "timestamp": time.time(),
        }
        try:
            self._state_writer.state_put(
                serve_namespace(),
                puts={serve_model_key(model_id): encode_json(data)},
            )
        except Exception as e:
            logger.debug(f"Failed to write model state: {e}")

    # --- Deployment ---

    async def deploy_model(
        self,
        config: ModelConfig | list[ModelConfig],
        wait_ready: bool = True,
        timeout: float = 600.0,
        autoscale_config: Optional[AutoscaleConfig] = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """Deploy one or more models.

        Single config: creates a ModelPool (plain object) for the model,
        scales to min_workers, and optionally waits for readiness.

        List of configs: deploys with anti-fragmentation ordering —
        large models (needing many GPUs) go first sequentially, then
        small models deploy in parallel.

        Args:
            config: Single ModelConfig or list of ModelConfigs
            wait_ready: Whether to wait for workers to be ready
            timeout: Timeout for waiting (only if wait_ready=True)
            autoscale_config: Override autoscale config (single-model only)

        Returns:
            Single dict (one model) or list of dicts (multiple models)

        Raises:
            ValueError: If model already deployed
            RuntimeError: If workers fail to start
        """
        if isinstance(config, list):
            return await self._deploy_multiple(config, wait_ready, timeout)
        return await self._deploy_one(config, wait_ready, timeout, autoscale_config)

    async def _deploy_one(
        self,
        config: ModelConfig,
        wait_ready: bool,
        timeout: float,
        autoscale_config: Optional[AutoscaleConfig] = None,
    ) -> dict[str, Any]:
        """Deploy a single model."""
        model_id = config.model_id

        if model_id in self._pools:
            raise ValueError(f"Model {model_id} already deployed")

        result: dict[str, Any] = {
            "model_id": model_id,
            "status": "deploying",
            "started_at": time.time(),
        }

        logger.info(f"Deploying model {model_id}: {config.model_source}")

        # Pool is a plain object, not a Ray actor
        pool = ModelPool(
            config=config,
            registry=self._registry,
            detached=self._detached,
            allocator=self._allocator,
            state_writer=self._state_writer,
        )
        self._pools[model_id] = pool
        self._configs[model_id] = config

        # Scale to min_workers (direct call, no RPC)
        scale_result = await pool.scale_to(config.min_workers)
        result["scale_result"] = scale_result

        # Start autoscaler
        effective_autoscale_config = autoscale_config or self._autoscale_config
        pool.start_autoscaler(effective_autoscale_config)

        if wait_ready:
            is_ready = await pool.wait_ready(timeout=timeout)
            if not is_ready:
                elapsed = time.time() - result["started_at"]
                raise RuntimeError(
                    f"Model {model_id} failed to start after {elapsed:.1f}s. "
                    f"Check worker logs for details (e.g. model download errors, "
                    f"GPU OOM, missing architectures)."
                )

        self._write_model_state(model_id, "DEPLOYED")
        result["status"] = "ready"
        result["completed_at"] = time.time()
        result["duration_s"] = result["completed_at"] - result["started_at"]

        # Get endpoints from pool status (fetched from registry)
        pool_status = await pool.get_status()
        result["endpoints"] = pool_status.get("endpoints", [])

        logger.info(
            f"Model {model_id} deployed: {len(result['endpoints'])} endpoints, "
            f"took {result['duration_s']:.1f}s"
        )

        return result

    async def _deploy_multiple(
        self,
        configs: list[ModelConfig],
        wait_ready: bool,
        timeout: float,
    ) -> list[dict[str, Any]]:
        """Deploy multiple models with anti-fragmentation ordering.

        1. Refresh cluster topology in allocator.
        2. Sort by effective num_gpus descending.
        3. Phase 1: large models (num_gpus > threshold) deployed sequentially.
        4. Phase 2: small models deployed in parallel.
        """
        self._allocator.refresh_nodes()

        sorted_configs = sorted(
            configs,
            key=lambda c: c.get_worker_resources().get("num_gpus", 0),
            reverse=True,
        )

        # Dynamic threshold: half the largest node's GPU count
        max_node_gpus = max(self._allocator._node_total.values(), default=8)
        tp_threshold = max_node_gpus / 2

        large = [
            c for c in sorted_configs if c.get_worker_resources().get("num_gpus", 0) > tp_threshold
        ]
        small = [
            c for c in sorted_configs if c.get_worker_resources().get("num_gpus", 0) <= tp_threshold
        ]

        results: list[dict[str, Any]] = []

        # Phase 1: large models sequentially (need contiguous GPU blocks)
        for config in large:
            r = await self._deploy_one(config, wait_ready=wait_ready, timeout=timeout)
            results.append(r)

        # Phase 2: small models in parallel
        if small:
            small_results = await asyncio.gather(
                *(self._deploy_one(c, wait_ready=wait_ready, timeout=timeout) for c in small),
                return_exceptions=True,
            )
            for item in small_results:
                if isinstance(item, BaseException):
                    logger.warning(f"Failed to deploy model: {item}")
                    results.append({"status": "error", "error": str(item)})
                else:
                    results.append(item)

        return results

    # --- Compaction ---

    async def spawn_with_compaction(self, model_id: str) -> tuple[str, ray.actor.ActorHandle]:
        """Spawn a worker with one compaction retry on failure.

        Compaction is coordinated entirely within the Manager:
        1. Try normal spawn.
        2. On failure, plan compaction (local call to allocator).
        3. Freeze affected pools' autoscalers.
        4. Evict identified workers.
        5. Retry spawn.
        6. Unfreeze autoscalers (evicted pools auto-recover).
        """
        pool = self._pools[model_id]
        try:
            return await pool._spawn_worker()
        except Exception:
            gpus = float(pool._config.get_worker_resources().get("num_gpus", 0))
            if gpus <= 0:
                raise

            logger.warning(f"Spawn failed for {model_id} ({gpus} GPUs), attempting compaction...")

            plan = self._allocator.plan_compaction(gpus)  # direct call
            if plan is None:
                raise
            _, evict_wids = plan
            if not evict_wids:
                return await pool._spawn_worker()

            # Freeze autoscalers for affected pools
            affected_pools: list[ModelPool] = []
            for p in self._pools.values():
                owns = p.stop_workers_by_ids(evict_wids)
                if owns:
                    p.freeze_autoscaler()
                    affected_pools.append(p)

            # Evict workers
            for p in self._pools.values():
                owned_wids = p.stop_workers_by_ids(evict_wids)
                for wid in owned_wids:
                    await p._stop_worker(wid, graceful=True)

            # Retry spawn
            try:
                result = await pool._spawn_worker()
            finally:
                # Unfreeze — autoscalers will respawn evicted workers
                # on allocator-suggested nodes (not the cleared node)
                for p in affected_pools:
                    p.unfreeze_autoscaler()

            return result

    # --- Model management ---

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
        await pool.shutdown()

        del self._pools[model_id]
        self._configs.pop(model_id, None)

        self._write_model_state(model_id, "UNDEPLOYED")
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
            return await pool.scale_to(target)

        return {
            "model_id": model_id,
            "status": "config_updated",
        }

    def freeze_model(self, model_id: str) -> None:
        """Freeze autoscaling for a model."""
        pool = self._pools.get(model_id)
        if pool is None:
            raise ValueError(f"Model {model_id} not found")
        pool.freeze_autoscaler()

    def unfreeze_model(self, model_id: str) -> None:
        """Unfreeze autoscaling for a model."""
        pool = self._pools.get(model_id)
        if pool is None:
            raise ValueError(f"Model {model_id} not found")
        pool.unfreeze_autoscaler()

    def list_models(self) -> list[str]:
        """List all deployed model IDs."""
        return list(self._pools.keys())

    # --- Shutdown ---

    async def shutdown(self) -> None:
        """Shutdown the manager and all deployed models.

        1. Gracefully undeploys known pools.
        2. Kills the registry actor.
        3. In detached mode, sweeps nurion_serve namespace for orphan actors
           from previous failed deployments.
        """
        logger.info("Shutting down ModelServiceManager")

        for model_id in list(self._pools.keys()):
            try:
                await self.undeploy_model(model_id)
            except Exception as e:
                logger.warning(f"Error undeploying {model_id}: {e}")

        # Kill registry
        try:
            ray.kill(self._registry)
            logger.info("ModelRegistry actor killed")
        except Exception as e:
            logger.warning(f"Error killing registry: {e}")

        if self._detached:
            killed = 0
            for actor_info in ray.util.list_named_actors(all_namespaces=True):
                if actor_info.get("namespace") != SERVE_NAMESPACE:
                    continue
                name = actor_info.get("name", "")
                # Don't kill ourselves
                if name == MANAGER_ACTOR_NAME:
                    continue
                try:
                    handle = ray.get_actor(name, namespace=SERVE_NAMESPACE)
                    ray.kill(handle)
                    killed += 1
                except Exception:
                    pass

            if killed:
                logger.info(f"Killed {killed} orphan actor(s) in {SERVE_NAMESPACE}")

        logger.info("ModelServiceManager shutdown complete")


def create_manager(
    autoscale_config: Optional[AutoscaleConfig] = None,
    detached: bool = False,
    broker_endpoint: Optional[str] = None,
) -> ray.actor.ActorHandle:
    """Create a new ModelServiceManager actor.

    Args:
        autoscale_config: Default autoscaling config for all models
        detached: If True, create detached actor that survives job exit
        broker_endpoint: Optional WorkQueue broker URL for state persistence

    Returns:
        Actor handle for the ModelServiceManager
    """
    options: dict[str, Any] = {"name": MANAGER_ACTOR_NAME}
    if detached:
        options["lifetime"] = "detached"
        options["namespace"] = SERVE_NAMESPACE
    return (
        ray.remote(ModelServiceManager)
        .options(**options)
        .remote(autoscale_config, detached, broker_endpoint)
    )
