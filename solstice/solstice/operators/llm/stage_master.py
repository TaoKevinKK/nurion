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

"""LLM Stage Master - orchestrates router and GPU workers for LLM inference.

Architecture:
    ┌─────────────────────────────────────────────────────────────────┐
    │                      LLMStageMaster                             │
    │                                                                 │
    │   ┌─────────────────────────────────────────────────────────┐   │
    │   │              SGLang Router Actor                        │   │
    │   │              - Load balancing                           │   │
    │   │              - Health checking                          │   │
    │   │              - Dynamic worker registration              │   │
    │   └────────────────────────┬────────────────────────────────┘   │
    │                            │ HTTP                               │
    │     ┌──────────────────────┼──────────────────────┐            │
    │     ▼                      ▼                      ▼            │
    │ ┌────────────┐       ┌────────────┐       ┌────────────┐       │
    │ │ GPU Worker │       │ GPU Worker │       │ GPU Worker │       │
    │ │   Actor 1  │       │   Actor 2  │       │   Actor N  │       │
    │ │  8 GPUs    │       │  8 GPUs    │       │  8 GPUs    │       │
    │ └────────────┘       └────────────┘       └────────────┘       │
    │                                                                 │
    │   ┌─────────────────────────────────────────────────────────┐   │
    │   │              CPU StageWorkers (from parent class)       │   │
    │   │              - Pull data from queue                     │   │
    │   │              - Call Router HTTP API                     │   │
    │   └─────────────────────────────────────────────────────────┘   │
    └─────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Dict, Optional

import ray

from solstice.core.stage_master import StageMaster
from solstice.operators.llm.operator import LLMOperatorConfig
from solstice.operators.llm.router_actor import SGLangRouterActor
from solstice.operators.llm.worker_actor import SGLangWorkerActor

if TYPE_CHECKING:
    from solstice.core.stage import Stage
    from solstice.core.stage_config import StageConfig
    from solstice.core.split_payload_store import SplitPayloadStore


class LLMStageMaster(StageMaster):
    """Stage Master for LLM inference that manages router and GPU workers.

    Extends the base StageMaster to add:
    - SGLang Router lifecycle management
    - GPU Worker Actor management with dynamic registration
    - Automatic endpoint injection into operators

    Usage:
        # Set master_class on LLMOperatorConfig
        config = LLMOperatorConfig(
            managed=True,
            num_workers=10,
            gpus_per_worker=8,
            worker_config=WorkerConfig(model_path="llama-3.1-70b", tensor_parallel_size=8),
        )
        config.master_class = LLMStageMaster
    """

    def __init__(
        self,
        job_id: str,
        stage: "Stage",
        config: "StageConfig",
        payload_store: "SplitPayloadStore",
    ):
        """Initialize LLM stage master.

        Args:
            job_id: Job identifier
            stage: Stage definition
            config: Stage configuration
            payload_store: Shared payload store
        """
        super().__init__(job_id, stage, config, payload_store)

        # LLM operator config
        assert isinstance(stage.operator_config, LLMOperatorConfig)
        self._operator_config: LLMOperatorConfig = stage.operator_config

        # Router and GPU worker management
        self._router_actor: Optional[ray.actor.ActorHandle] = None
        self._gpu_worker_actors: Dict[str, ray.actor.ActorHandle] = {}
        self._router_endpoint: Optional[str] = None

    def _infer_num_workers(self, gpus_per_worker: int) -> int:
        """Infer number of workers from cluster GPU resources.

        Args:
            gpus_per_worker: GPUs required per worker

        Returns:
            Number of workers that can be launched
        """
        try:
            cluster_resources = ray.cluster_resources()
            total_gpus = int(cluster_resources.get("GPU", 0))
            if total_gpus == 0:
                self.logger.warning("No GPUs found in cluster")
                return 0
            num_workers = total_gpus // gpus_per_worker
            self.logger.info(
                f"Cluster has {total_gpus} GPUs, "
                f"can launch {num_workers} workers with {gpus_per_worker} GPUs each"
            )
            return num_workers
        except Exception as e:
            self.logger.warning(f"Failed to get cluster resources: {e}, defaulting to 1 worker")
            return 1

    async def start(self) -> None:
        """Start the LLM stage with router and GPU workers."""
        if self._running:
            return

        # Start managed inference infrastructure if configured
        if self._operator_config.managed:
            await self._start_inference_infrastructure()

        # Call parent start (creates CPU stage workers)
        await super().start()

    async def _start_inference_infrastructure(self) -> None:
        """Start router and GPU worker actors."""
        self.logger.info(f"Starting LLM inference infrastructure for stage {self.stage_id}")

        router_config = self._operator_config.router_config
        worker_config = self._operator_config.worker_config
        gpus_per_worker = self._operator_config.gpus_per_worker

        # Auto-detect num_workers from cluster resources if not specified
        num_workers = self._operator_config.num_workers
        if num_workers is None:
            num_workers = self._infer_num_workers(gpus_per_worker)
            self.logger.info(f"Auto-detected num_workers={num_workers} from cluster resources")

        if num_workers <= 0:
            raise ValueError(
                f"No GPU workers to start. num_workers={num_workers}, "
                f"gpus_per_worker={gpus_per_worker}. "
                "Check cluster GPU availability or set num_workers explicitly."
            )

        # 1. Start Router
        self._router_actor = SGLangRouterActor.options(
            name=f"{self.job_id}_{self.stage_id}_router",
            num_cpus=1,
        ).remote(router_config, self.job_id)

        self._router_endpoint = await self._router_actor.start.remote()
        self.logger.info(f"Router started at {self._router_endpoint}")

        # 2. Start GPU Workers
        self.logger.info(f"Starting {num_workers} GPU workers ({gpus_per_worker} GPUs each)")

        start_tasks = []
        for i in range(num_workers):
            worker_id = f"{self.stage_id}_gpu_worker_{i}"
            worker = SGLangWorkerActor.options(
                name=f"{self.job_id}_{worker_id}",
                num_gpus=gpus_per_worker,
            ).remote(
                worker_config,
                self._router_actor,
                worker_id,
            )
            self._gpu_worker_actors[worker_id] = worker
            start_tasks.append(worker.start.remote())

        # Wait for all workers to start and register
        try:
            await asyncio.gather(*start_tasks)
            self.logger.info(f"All {num_workers} GPU workers started and registered")
        except Exception as e:
            self.logger.error(f"Failed to start GPU workers: {e}")
            # Stop any workers that did start
            await self._stop_inference_infrastructure()
            raise

        # 3. Inject router endpoint into operator config
        # This allows the HttpOperator to know where to send requests
        self._inject_router_endpoint()

    def _inject_router_endpoint(self) -> None:
        """Inject router endpoint into the operator config."""
        if not self._router_endpoint:
            return

        # Only inject if not already set (allow external URL override)
        if not self._operator_config.base_url:
            self._operator_config.base_url = self._router_endpoint
            self.logger.info(f"Injected router endpoint into operator: {self._router_endpoint}")

    async def _stop_inference_infrastructure(self) -> None:
        """Stop router and GPU worker actors."""
        # Stop GPU workers first
        stop_tasks = []
        for worker_id, worker in list(self._gpu_worker_actors.items()):
            try:
                stop_tasks.append(worker.stop.remote())
            except Exception as e:
                self.logger.warning(f"Error stopping GPU worker {worker_id}: {e}")

        if stop_tasks:
            try:
                await asyncio.gather(*stop_tasks, return_exceptions=True)
            except Exception as e:
                self.logger.warning(f"Error waiting for GPU workers to stop: {e}")

        self._gpu_worker_actors.clear()

        # Stop router
        if self._router_actor:
            try:
                await self._router_actor.stop.remote()
            except Exception as e:
                self.logger.warning(f"Error stopping router: {e}")
            self._router_actor = None

        self._router_endpoint = None
        self.logger.info("LLM inference infrastructure stopped")

    async def stop(self) -> None:
        """Stop the stage including inference infrastructure."""
        # Stop CPU stage workers first
        await super().stop()

        # Stop inference infrastructure
        await self._stop_inference_infrastructure()
