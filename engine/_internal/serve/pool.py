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

"""Model Pool - Manages InferenceWorkers for a single model with autoscaling.

Plain class owned by ModelServiceManager (Ray actor). Not a separate actor.
The Manager's event loop runs autoscaling as an asyncio.Task.

1. Worker lifecycle (spawn, stop, graceful shutdown)
2. Scaling operations (scale_to, freeze/unfreeze)
3. Autoscaling loop (background task that monitors and scales)
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any, Optional

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _internal.serve.config import AutoscaleConfig, ModelConfig
from _internal.serve.registry import SERVE_NAMESPACE
from _internal.serve.worker import InferenceWorker
from _internal.utils.network import find_free_port
from _internal.webui.state.schema import encode_json, serve_namespace, serve_worker_key

if TYPE_CHECKING:
    from _internal.queue import WorkQueueQueueClient
    from _internal.serve.allocator import GPUAllocator

logger = logging.getLogger(__name__)
_SPAWN_WAIT_TIMEOUT_SECONDS = 120.0


class ModelPool:
    """Manages InferenceWorkers for a single model with built-in autoscaling.

    Plain class owned by ModelServiceManager. Not a Ray actor.
    The Manager's event loop runs autoscaling as an asyncio.Task.
    """

    def __init__(
        self,
        config: ModelConfig,
        registry: ray.actor.ActorHandle,
        detached: bool = False,
        allocator: Optional[GPUAllocator] = None,
        state_writer: Optional["WorkQueueQueueClient"] = None,
    ) -> None:
        self._config = config
        self._registry = registry
        self._detached = detached
        self._allocator = allocator
        self._state_writer = state_writer
        self._workers: dict[str, ray.actor.ActorHandle] = {}
        self._worker_ports: dict[str, int] = {}
        self._worker_nodes: dict[str, str] = {}  # worker_id -> node_id
        self._spawning_workers = 0
        self._shutdown_event = asyncio.Event()
        self._last_scale_time = 0.0
        self._registry_url: Optional[str] = None

        # Autoscaler state
        self._autoscale_config: Optional[AutoscaleConfig] = None
        self._autoscale_task: Optional[asyncio.Task] = None
        self._autoscale_frozen = False
        self._last_idle_time = 0.0

        logger.info(f"ModelPool created for model {config.model_id} (detached={detached})")

    # --- State persistence ---

    def _write_serve_worker_state(self, worker_id: str, status: str) -> None:
        """Write serve worker lifecycle metadata into WorkQueue state."""
        if self._state_writer is None:
            return
        data = {
            "worker_id": worker_id,
            "model_id": self._config.model_id,
            "status": status,
            "timestamp": time.time(),
            "backend": self._config.backend,
            "tp_size": self._config.tensor_parallel_size,
        }
        try:
            self._state_writer.state_put(
                serve_namespace(),
                puts={serve_worker_key(self._config.model_id, worker_id): encode_json(data)},
            )
        except Exception as e:
            logger.debug(f"Failed to write serve worker state: {e}")

    # --- Worker lifecycle ---

    async def _spawn_worker(self) -> tuple[str, ray.actor.ActorHandle]:
        port = find_free_port()
        worker_id = f"{self._config.model_id}_worker_{port}"
        resources = self._config.get_worker_resources()

        actor_options: dict[str, Any] = {"name": worker_id, **resources}
        if self._detached:
            actor_options["lifetime"] = "detached"
            actor_options["namespace"] = SERVE_NAMESPACE

        # Best-fit node suggestion from allocator (direct call, no RPC)
        if self._allocator is not None:
            gpus = resources.get("num_gpus", 0)
            if gpus > 0:
                suggestions = self._allocator.suggest_nodes(float(gpus), 1)
                node_id = suggestions[0] if suggestions else None
                if node_id is not None:
                    actor_options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                        node_id=node_id, soft=True
                    )

        worker: Optional[ray.actor.ActorHandle] = None
        self._spawning_workers += 1
        try:
            worker = (
                ray.remote(InferenceWorker)
                .options(**actor_options)
                .remote(self._config, registry=self._registry, port=port, worker_id=worker_id)
            )
            # Bound actor-creation wait so unschedulable resources fail fast.
            await asyncio.wait_for(
                worker.start.remote(),
                timeout=_SPAWN_WAIT_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError as exc:
            if worker is not None:
                try:
                    ray.kill(worker)
                except Exception:
                    pass
            raise RuntimeError(
                f"Timed out spawning worker {worker_id} after "
                f"{_SPAWN_WAIT_TIMEOUT_SECONDS:.0f}s. "
                "Likely unschedulable resources or cluster capacity exhaustion."
            ) from exc
        finally:
            self._spawning_workers = max(0, self._spawning_workers - 1)

        assert worker is not None
        self._workers[worker_id] = worker
        self._worker_ports[worker_id] = port

        # Report actual placement back to allocator (direct call, no RPC)
        actual_node = await worker.get_node_id.remote()
        self._worker_nodes[worker_id] = actual_node
        if self._allocator is not None:
            gpus = resources.get("num_gpus", 0)
            self._allocator.record_placement(worker_id, actual_node, float(gpus))

        self._write_serve_worker_state(worker_id, "LOADING")
        logger.info(
            f"Spawned worker {worker_id} for {self._config.model_id} "
            f"(port={port}, node={actual_node}, resources={resources})"
        )
        return worker_id, worker

    async def _stop_worker(self, worker_id: str, graceful: bool = True) -> None:
        worker = self._workers.get(worker_id)
        if worker is None:
            return
        try:
            if graceful:
                await worker.shutdown.remote()
            else:
                ray.kill(worker)
        except Exception as e:
            logger.warning(f"Error stopping worker {worker_id}: {e}")

        self._workers.pop(worker_id, None)
        self._worker_ports.pop(worker_id, None)
        self._worker_nodes.pop(worker_id, None)

        # Direct call to allocator, no RPC
        if self._allocator is not None:
            self._allocator.record_removal(worker_id)

        self._write_serve_worker_state(worker_id, "STOPPED")
        logger.info(f"Stopped worker {worker_id}")

    # --- Public API ---

    async def scale_to(self, target: int) -> dict[str, Any]:
        """Scale to target number of workers."""
        target = max(self._config.min_workers, min(target, self._config.max_workers))
        current = len(self._workers)

        result: dict[str, Any] = {
            "model_id": self._config.model_id,
            "from_workers": current,
            "to_workers": target,
            "started_at": time.time(),
            "spawned": [],
            "stopped": [],
            "spawn_errors": [],
        }

        if target > current:
            tasks = [self._spawn_worker() for _ in range(target - current)]
            spawned = await asyncio.gather(*tasks, return_exceptions=True)
            for item in spawned:
                if isinstance(item, tuple):
                    result["spawned"].append(item[0])
                elif isinstance(item, Exception):
                    msg = str(item)
                    result["spawn_errors"].append(msg)
                    logger.warning(f"Failed to spawn worker for {self._config.model_id}: {msg}")

        elif target < current:
            count = current - target
            if self._allocator is not None:
                workers_to_stop = self._allocator.suggest_workers_to_stop(
                    list(self._workers.keys()), count
                )
            else:
                workers_to_stop = list(self._workers.keys())[:count]

            for worker_id in workers_to_stop:
                await self._stop_worker(worker_id, graceful=True)
                result["stopped"].append(worker_id)

        result["completed_at"] = time.time()
        result["duration_s"] = result["completed_at"] - result["started_at"]
        result["current_workers"] = len(self._workers)
        result["spawning_workers"] = self._spawning_workers
        self._last_scale_time = time.time()

        logger.info(
            f"Scaled {self._config.model_id}: {result['from_workers']} -> "
            f"{result['current_workers']} in {result['duration_s']:.1f}s"
        )
        return result

    def stop_workers_by_ids(self, worker_ids: list[str]) -> list[str]:
        """Return the subset of worker_ids that belong to this pool."""
        return [wid for wid in worker_ids if wid in self._workers]

    async def wait_ready(self, timeout: float = 600.0) -> bool:
        """Wait for at least one worker to be ready.

        Returns immediately with False if all workers have crashed.
        """
        start = time.time()
        while time.time() - start < timeout:
            all_failed = True
            for wid, worker in self._workers.items():
                try:
                    if await worker.is_ready.remote():
                        self._write_serve_worker_state(wid, "READY")
                        return True
                    if not await worker.is_failed.remote():
                        all_failed = False
                except Exception:
                    pass  # Actor dead — counts as failed

            if all_failed and self._workers:
                logger.error(
                    f"All workers for {self._config.model_id} have failed, aborting wait_ready"
                )
                return False

            await asyncio.sleep(2.0)
        return False

    async def get_status(self) -> dict[str, Any]:
        """Get pool status from registry (single HTTP call, not per-worker RPC)."""
        import httpx

        if self._registry_url is None:
            self._registry_url = ray.get(self._registry.get_http_url.remote())

        workers_status: list[dict[str, Any]] = []
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(
                    f"{self._registry_url}/endpoints_status",
                    params={"model_id": self._config.model_id},
                )
                resp.raise_for_status()
                workers_status = resp.json()
        except Exception as e:
            logger.warning(f"Failed to get status from registry: {e}")

        ready_count = sum(1 for w in workers_status if w.get("is_ready"))
        total_pending = sum(w.get("pending", 0) for w in workers_status)
        total_running = sum(w.get("running", 0) for w in workers_status)

        return {
            "model_id": self._config.model_id,
            "total_workers": len(self._workers),
            "spawning_workers": self._spawning_workers,
            "ready_workers": ready_count,
            "total_pending": total_pending,
            "total_running": total_running,
            "endpoints": [w.get("endpoint", "") for w in workers_status],
            "last_scale_time": self._last_scale_time,
            "autoscale_frozen": self._autoscale_frozen,
        }

    # --- Autoscaling ---

    def start_autoscaler(self, config: Optional[AutoscaleConfig] = None) -> None:
        """Start the autoscaling background loop."""
        self._autoscale_config = config or AutoscaleConfig()

        if not self._autoscale_config.enabled:
            logger.info(f"Autoscaler disabled for {self._config.model_id}")
            return

        if self._autoscale_task is not None:
            return  # Already running

        self._autoscale_task = asyncio.get_event_loop().create_task(self._autoscale_loop())
        logger.info(f"Started autoscaler for model {self._config.model_id}")

    def stop_autoscaler(self) -> None:
        """Stop the autoscaling loop."""
        if self._autoscale_task:
            self._autoscale_task.cancel()
            self._autoscale_task = None
        logger.info(f"Stopped autoscaler for {self._config.model_id}")

    def freeze_autoscaler(self) -> None:
        """Pause autoscaling decisions."""
        self._autoscale_frozen = True

    def unfreeze_autoscaler(self) -> None:
        """Resume autoscaling decisions."""
        self._autoscale_frozen = False

    async def _autoscale_loop(self) -> None:
        """Background loop that checks metrics and scales."""
        assert self._autoscale_config is not None
        cfg = self._autoscale_config

        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(cfg.check_interval_seconds)

                if self._autoscale_frozen:
                    continue

                status = await self.get_status()
                ready = status["ready_workers"]
                total = status["total_workers"]
                pending = status["total_pending"]
                running = status["total_running"]
                # Use total inflight (pending + running) as the load signal.
                # With vLLM continuous batching + chunked prefill, requests
                # move from "waiting" to "running" almost immediately, so
                # looking at pending alone misses overloaded workers whose
                # GPU is saturated but whose waiting queue is near-empty.
                inflight = pending + running
                now = time.time()

                # Cooldown
                if now - self._last_scale_time < cfg.cooldown_seconds:
                    continue

                # Scale up: total inflight exceeds threshold per ready worker
                threshold = cfg.scale_up_threshold * max(ready, 1)
                if inflight > threshold:
                    target = min(total + cfg.max_scale_step, self._config.max_workers)
                    if target > total:
                        logger.info(
                            f"Autoscale UP {self._config.model_id}: "
                            f"{total} -> {target} "
                            f"(inflight={inflight}, pending={pending}, running={running})"
                        )
                        await self.scale_to(target)
                    continue

                # Scale down: no requests at all for a sustained period
                if inflight == 0:
                    if self._last_idle_time == 0:
                        self._last_idle_time = now
                    elif now - self._last_idle_time > cfg.scale_down_idle_seconds:
                        if total > self._config.min_workers:
                            target = max(total - 1, self._config.min_workers)
                            logger.info(
                                f"Autoscale DOWN {self._config.model_id}: "
                                f"{total} -> {target} (idle)"
                            )
                            await self.scale_to(target)
                else:
                    self._last_idle_time = 0

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Autoscaler error: {e}")

    # --- Shutdown ---

    async def shutdown(self) -> None:
        """Shutdown autoscaler and all workers."""
        logger.info(f"Shutting down ModelPool for {self._config.model_id}")

        self._shutdown_event.set()
        self.stop_autoscaler()

        tasks = [self._stop_worker(wid, graceful=True) for wid in list(self._workers)]
        await asyncio.gather(*tasks, return_exceptions=True)

        logger.info(f"ModelPool for {self._config.model_id} shutdown complete")
