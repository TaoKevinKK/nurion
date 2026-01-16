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

"""SGLang Worker Actor for managing inference server lifecycle.

Each worker actor:
1. Starts an SGLang inference server process
2. Waits for the server to be ready
3. Registers with the router
"""

import asyncio
import logging
import os
import signal
import subprocess
import time
from typing import Optional

import aiohttp
import ray

from solstice.operators.llm.config import WorkerConfig
from solstice.utils.network import find_free_port, get_node_ip


@ray.remote
class SGLangWorkerActor:
    """Ray Actor that manages SGLang inference server lifecycle.

    Each worker actor starts an SGLang server process and
    automatically registers it with the router.

    Usage:
        worker = SGLangWorkerActor.options(
            name="my_job_worker_0",
            num_gpus=8,
        ).remote(
            config=WorkerConfig(model_path="llama-3.1-70b", tensor_parallel_size=8),
            router_actor=router,
            worker_id="worker_0",
        )

        endpoint = await worker.start.remote()
        await worker.stop.remote()
    """

    def __init__(
        self,
        config: WorkerConfig,
        router_actor: ray.actor.ActorHandle,
        worker_id: str,
    ):
        """Initialize worker actor.

        Args:
            config: Worker configuration
            router_actor: Handle to the router actor for registration
            worker_id: Unique worker identifier
        """
        self._config = config
        self._router = router_actor
        self._worker_id = worker_id
        self._process: Optional[subprocess.Popen] = None
        self._endpoint: Optional[str] = None
        self._started = False

        self._logger = logging.getLogger(f"SGLangWorker-{worker_id}")

    async def start(self) -> str:
        """Start the inference server and register with router.

        Returns:
            Worker endpoint URL

        Raises:
            RuntimeError: If server fails to start or register
        """
        if self._started:
            return self._endpoint or ""

        port = self._config.port or find_free_port()
        node_ip = get_node_ip()

        cmd = self._build_command(port)
        self._logger.info(f"Starting SGLang server: {' '.join(cmd)}")

        try:
            env = os.environ.copy()
            # Ray automatically sets CUDA_VISIBLE_DEVICES

            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                preexec_fn=os.setsid,
            )

            self._endpoint = f"http://{node_ip}:{port}"

            await self._wait_for_ready()

            success = await self._router.register_worker.remote(self._worker_id, self._endpoint)
            if not success:
                raise RuntimeError(f"Failed to register worker {self._worker_id}")

            self._started = True
            self._logger.info(f"SGLang server started at {self._endpoint}")
            return self._endpoint

        except Exception as e:
            self._logger.error(f"Failed to start SGLang server: {e}")
            await self.stop()
            raise RuntimeError(f"Failed to start SGLang server: {e}")

    def _build_command(self, port: int) -> list[str]:
        """Build SGLang server command."""
        cmd = [
            "python",
            "-m",
            "sglang.launch_server",
            "--model-path",
            self._config.model_path,
            "--tp",
            str(self._config.tensor_parallel_size),
            "--host",
            self._config.host,
            "--port",
            str(port),
            "--mem-fraction-static",
            str(self._config.gpu_memory_utilization),
        ]

        if self._config.max_model_len > 0:
            cmd.extend(["--context-length", str(self._config.max_model_len)])

        cmd.extend(self._config.additional_args)

        return cmd

    async def _wait_for_ready(self) -> None:
        """Wait for SGLang server to be ready."""
        start_time = time.time()
        timeout = self._config.startup_timeout
        health_endpoint = f"{self._endpoint}/health"

        self._logger.info(f"Waiting for server at {health_endpoint}")

        while (time.time() - start_time) < timeout:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        health_endpoint,
                        timeout=aiohttp.ClientTimeout(total=5),
                    ) as resp:
                        if resp.status == 200:
                            self._logger.info("Server is ready")
                            return
            except Exception:
                elapsed = time.time() - start_time
                if int(elapsed) % 30 == 0 and int(elapsed) > 0:
                    self._logger.info(f"Still waiting... ({elapsed:.0f}s)")

            if self._process and self._process.poll() is not None:
                stderr = ""
                if self._process.stderr:
                    stderr = self._process.stderr.read().decode()
                raise RuntimeError(f"Server process died: {stderr[:1000]}")

            await asyncio.sleep(1.0)

        raise RuntimeError(f"Server failed to become ready within {timeout}s")

    async def stop(self) -> None:
        """Stop the inference server and unregister from router."""
        self._started = False

        if self._router:
            try:
                await self._router.unregister_worker.remote(self._worker_id)
            except Exception as e:
                self._logger.warning(f"Failed to unregister from router: {e}")

        if self._process:
            try:
                os.killpg(os.getpgid(self._process.pid), signal.SIGTERM)
                try:
                    self._process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    self._logger.warning("Force killing server")
                    os.killpg(os.getpgid(self._process.pid), signal.SIGKILL)
                    self._process.wait(timeout=5)
            except Exception as e:
                self._logger.warning(f"Error stopping server: {e}")
            finally:
                self._process = None

        self._logger.info(f"Worker {self._worker_id} stopped")
