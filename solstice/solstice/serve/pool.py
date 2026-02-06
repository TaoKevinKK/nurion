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

Combines worker lifecycle management and autoscaling into one actor:
1. Worker lifecycle (spawn, stop, graceful shutdown)
2. Scaling operations (scale_to, freeze/unfreeze)
3. Autoscaling loop (background task that monitors and scales)
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

import ray

from solstice.serve.config import AutoscaleConfig, ModelConfig
from solstice.serve.worker import InferenceWorker
from solstice.utils.network import find_free_port

logger = logging.getLogger(__name__)


class ModelPool:
    """Manages InferenceWorkers for a single model with built-in autoscaling.

    Usage:
        pool = ray.remote(ModelPool).options(
            name=f"model_pool_{config.model_id}",
        ).remote(config)

        await pool.scale_to.remote(4)
        await pool.start_autoscaler.remote()
        await pool.shutdown.remote()
    """

    def __init__(self, config: ModelConfig, registry: "ray.ActorHandle") -> None:
        self._config = config
        self._registry = registry
        self._workers: dict[str, ray.ActorHandle] = {}
        self._worker_ports: dict[str, int] = {}
        self._shutdown_event = asyncio.Event()
        self._last_scale_time = 0.0

        # Autoscaler state
        self._autoscale_config: Optional[AutoscaleConfig] = None
        self._autoscale_task: Optional[asyncio.Task] = None
        self._autoscale_frozen = False
        self._last_idle_time = 0.0

        logger.info(f"ModelPool created for model {config.model_id}")

    # --- Worker lifecycle ---

    async def _spawn_worker(self) -> tuple[str, ray.ActorHandle]:
        port = find_free_port()
        worker_id = f"{self._config.model_id}_worker_{port}"
        resources = self._config.get_worker_resources()

        worker = (
            ray.remote(InferenceWorker)
            .options(
                name=worker_id,
                **resources,
            )
            .remote(self._config, registry=self._registry, port=port, worker_id=worker_id)
        )

        await worker.start.remote()

        self._workers[worker_id] = worker
        self._worker_ports[worker_id] = port

        logger.info(
            f"Spawned worker {worker_id} for {self._config.model_id} "
            f"(port={port}, resources={resources})"
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
        }

        if target > current:
            tasks = [self._spawn_worker() for _ in range(target - current)]
            spawned = await asyncio.gather(*tasks, return_exceptions=True)
            for item in spawned:
                if isinstance(item, tuple):
                    result["spawned"].append(item[0])

        elif target < current:
            # Stop workers (pick any, could be smarter with metrics)
            workers_to_stop = list(self._workers.keys())[: current - target]
            for worker_id in workers_to_stop:
                await self._stop_worker(worker_id, graceful=True)
                result["stopped"].append(worker_id)

        result["completed_at"] = time.time()
        result["duration_s"] = result["completed_at"] - result["started_at"]
        result["current_workers"] = len(self._workers)
        self._last_scale_time = time.time()

        logger.info(
            f"Scaled {self._config.model_id}: {result['from_workers']} -> "
            f"{result['current_workers']} in {result['duration_s']:.1f}s"
        )
        return result

    async def wait_ready(self, timeout: float = 600.0) -> bool:
        """Wait for at least one worker to be ready."""
        start = time.time()
        while time.time() - start < timeout:
            for worker in self._workers.values():
                try:
                    if await worker.is_ready.remote():
                        return True
                except Exception:
                    pass
            await asyncio.sleep(2.0)
        return False

    def get_endpoints(self) -> list[str]:
        """Get all worker endpoints."""
        endpoints = []
        for worker in self._workers.values():
            try:
                endpoints.append(ray.get(worker.get_endpoint.remote()))
            except Exception:
                pass
        return endpoints

    async def get_status(self) -> dict[str, Any]:
        """Get pool status."""
        worker_statuses = {}
        ready_count = 0
        total_pending = 0

        for worker_id, worker in self._workers.items():
            try:
                status = await worker.get_status.remote()
                worker_statuses[worker_id] = status
                if status.get("is_ready"):
                    ready_count += 1
            except Exception:
                worker_statuses[worker_id] = {"state": "unknown"}

        return {
            "model_id": self._config.model_id,
            "total_workers": len(self._workers),
            "ready_workers": ready_count,
            "total_pending": total_pending,
            "config": {
                "min_workers": self._config.min_workers,
                "max_workers": self._config.max_workers,
                "tensor_parallel_size": self._config.tensor_parallel_size,
            },
            "workers": worker_statuses,
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
                now = time.time()

                # Cooldown
                if now - self._last_scale_time < cfg.cooldown_seconds:
                    continue

                # Scale up
                threshold = cfg.scale_up_pending_threshold * max(ready, 1)
                if pending > threshold:
                    target = min(total + cfg.max_scale_step, self._config.max_workers)
                    if target > total:
                        logger.info(
                            f"Autoscale UP {self._config.model_id}: "
                            f"{total} -> {target} (pending={pending})"
                        )
                        await self.scale_to(target)
                    continue

                # Scale down
                if pending == 0:
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


def get_pool_actor_name(model_id: str) -> str:
    """Get the named actor name for a model pool."""
    return f"solstice_model_pool_{model_id}"
