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

"""SGLang Router Actor for managing the router lifecycle.

The router provides:
- Load balancing across multiple SGLang workers
- Health checking and automatic worker removal
- Dynamic worker registration/unregistration
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

from solstice.operators.llm.config import RouterConfig
from solstice.utils.network import find_free_port, get_node_ip


@ray.remote(num_cpus=1)
class SGLangRouterActor:
    """Ray Actor that manages SGLang Router lifecycle.

    The router provides load balancing and health checking for multiple
    SGLang inference workers.

    Usage:
        # Create router
        router = SGLangRouterActor.options(
            name="my_job_router",
        ).remote(RouterConfig(), "my_job")

        # Start router
        endpoint = await router.start.remote()

        # Register workers
        await router.register_worker.remote("worker_0", "http://host:port")

        # Stop
        await router.stop.remote()
    """

    def __init__(self, config: RouterConfig, job_id: str):
        """Initialize router actor.

        Args:
            config: Router configuration
            job_id: Job identifier for logging
        """
        self._config = config
        self._job_id = job_id
        self._process: Optional[subprocess.Popen] = None
        self._endpoint: Optional[str] = None
        self._workers: dict[str, str] = {}  # worker_id -> url
        self._started = False

        self._logger = logging.getLogger(f"SGLangRouter-{job_id}")

    async def start(self) -> str:
        """Start the SGLang router.

        Returns:
            Router endpoint URL

        Raises:
            RuntimeError: If router fails to start
        """
        if self._started:
            return self._endpoint or ""

        port = self._config.port or find_free_port()
        node_ip = get_node_ip()

        # Build command for sglang router
        cmd = [
            "python",
            "-m",
            "sglang_router.launch_router",
            "--host",
            self._config.host,
            "--port",
            str(port),
            "--policy",
            self._config.policy,
        ]

        self._logger.info(f"Starting SGLang router: {' '.join(cmd)}")

        try:
            # Start the router process
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid,
            )

            self._endpoint = f"http://{node_ip}:{port}"

            # Wait for router to be ready
            await self._wait_for_ready()

            self._started = True
            self._logger.info(f"SGLang router started at {self._endpoint}")
            return self._endpoint

        except Exception as e:
            self._logger.error(f"Failed to start router: {e}")
            await self.stop()
            raise RuntimeError(f"Failed to start SGLang router: {e}")

    async def _wait_for_ready(self) -> None:
        """Wait for router to be ready to accept requests."""
        start_time = time.time()
        timeout = self._config.startup_timeout

        while (time.time() - start_time) < timeout:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{self._endpoint}/health",
                        timeout=aiohttp.ClientTimeout(total=5),
                    ) as resp:
                        if resp.status == 200:
                            return
            except Exception:
                pass

            # Check if process is still running
            if self._process and self._process.poll() is not None:
                stderr = ""
                if self._process.stderr:
                    stderr = self._process.stderr.read().decode()
                raise RuntimeError(f"Router process died: {stderr[:500]}")

            await asyncio.sleep(0.5)

        raise RuntimeError(f"Router failed to become ready within {timeout}s")

    async def register_worker(self, worker_id: str, worker_url: str) -> bool:
        """Register a worker with the router.

        Args:
            worker_id: Unique worker identifier
            worker_url: Worker's HTTP endpoint URL

        Returns:
            True if registration successful
        """
        if not self._started:
            self._logger.error("Cannot register worker: router not started")
            return False

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self._endpoint}/add_worker",
                    params={"url": worker_url},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 200:
                        self._workers[worker_id] = worker_url
                        self._logger.info(f"Registered worker {worker_id} at {worker_url}")
                        return True
                    else:
                        body = await resp.text()
                        self._logger.error(
                            f"Failed to register worker {worker_id}: HTTP {resp.status} - {body}"
                        )
                        return False
        except Exception as e:
            self._logger.error(f"Failed to register worker {worker_id}: {e}")
            return False

    async def unregister_worker(self, worker_id: str) -> bool:
        """Unregister a worker from the router.

        Args:
            worker_id: Worker identifier to remove

        Returns:
            True if unregistration successful
        """
        worker_url = self._workers.pop(worker_id, None)
        if not worker_url:
            return True  # Already removed

        if not self._started:
            return True  # Router stopped, nothing to do

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self._endpoint}/remove_worker",
                    params={"url": worker_url},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 200:
                        self._logger.info(f"Unregistered worker {worker_id}")
                        return True
                    else:
                        body = await resp.text()
                        self._logger.warning(
                            f"Failed to unregister worker {worker_id}: HTTP {resp.status} - {body}"
                        )
                        return False
        except Exception as e:
            self._logger.warning(f"Failed to unregister worker {worker_id}: {e}")
            return False

    async def stop(self) -> None:
        """Stop the router and clean up."""
        self._started = False
        self._workers.clear()

        if self._process:
            try:
                # Send SIGTERM to the process group
                os.killpg(os.getpgid(self._process.pid), signal.SIGTERM)
                # Wait for process to terminate
                try:
                    self._process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    # Force kill if it doesn't terminate
                    os.killpg(os.getpgid(self._process.pid), signal.SIGKILL)
                    self._process.wait(timeout=5)
            except Exception as e:
                self._logger.warning(f"Error stopping router process: {e}")
            finally:
                self._process = None

        self._logger.info("SGLang router stopped")
