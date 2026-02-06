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

"""Inference Worker - runs vLLM/SGLang HTTP server.

Each InferenceWorker:
1. Starts a vLLM or SGLang OpenAI-compatible HTTP server as a subprocess
2. Registers its endpoint with the ModelRegistry via HTTP
3. Periodically reports metrics (pending requests, etc.) via HTTP heartbeat
4. Gracefully shuts down when requested

Note: This class is NOT decorated with @ray.remote. Callers should create
actors using Ray's API directly, e.g.:
    worker = ray.remote(InferenceWorker).options(num_gpus=4).remote(config)
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import signal
import subprocess
import sys
import time
from typing import Any, Optional

import httpx
import ray

from solstice.serve.config import ModelConfig, WorkerState
from solstice.utils.network import find_free_port, get_node_ip

logger = logging.getLogger(__name__)


class InferenceWorker:
    """Ray Actor that runs a vLLM/SGLang HTTP server.

    Each worker:
    - Starts the inference server as a subprocess
    - Registers with ModelRegistry via HTTP on startup
    - Sends heartbeats with metrics via HTTP
    - Unregisters and stops the server on shutdown

    Usage:
        worker = ray.remote(InferenceWorker).options(
            num_gpus=config.tensor_parallel_size,
        ).remote(config, port=8001)

        # Wait for ready
        await worker.wait_ready.remote()

        # Get endpoint
        endpoint = await worker.get_endpoint.remote()

        # Shutdown
        await worker.shutdown.remote()
    """

    def __init__(
        self,
        config: ModelConfig,
        registry: "ray.ActorHandle",
        port: Optional[int] = None,
        worker_id: Optional[str] = None,
    ) -> None:
        """Initialize the worker.

        Args:
            config: Model configuration
            registry: Registry ActorHandle (holds ref + provides HTTP URL)
            port: Port to run the server on (auto-assigned if None)
            worker_id: Unique worker identifier (auto-generated if None)
        """
        self._config = config
        self._port = port or find_free_port()
        self._worker_id = worker_id or f"{config.model_id}_worker_{self._port}"
        self._host = "0.0.0.0"
        self._node_ip = get_node_ip()
        self._endpoint = f"http://{self._node_ip}:{self._port}"

        self._process: Optional[subprocess.Popen] = None
        self._state = WorkerState.STARTING
        self._is_ready = False
        self._pending_requests = 0
        self._running_requests = 0

        # Registry (ActorHandle for ref counting, URL for HTTP)
        self._registry = registry
        self._registry_url = ray.get(registry.get_http_url.remote())
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        # HTTP client for registry communication
        self._http_client: Optional[httpx.AsyncClient] = None

        # Background tasks (started lazily)
        self._monitor_task: Optional[asyncio.Task] = None
        self._ready_task: Optional[asyncio.Task] = None
        self._background_tasks_started = False

        logger.info(
            f"InferenceWorker {self._worker_id} initializing: "
            f"model={config.model_id}, endpoint={self._endpoint}"
        )

        # Start the server process (sync)
        self._start_server_process()

        # Background tasks will be started on first async method call
        logger.info(f"Worker {self._worker_id} process started, waiting for async init")

    def _get_http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=10.0)
        return self._http_client

    def _ensure_background_tasks(self) -> None:
        """Ensure background tasks are started (idempotent)."""
        if self._background_tasks_started:
            return
        self._background_tasks_started = True

        loop = asyncio.get_running_loop()
        self._monitor_task = loop.create_task(self._monitor_server())
        self._ready_task = loop.create_task(self._wait_for_ready())
        logger.info(f"Worker {self._worker_id} background tasks started")

    async def _registry_request(self, method: str, path: str, json: Optional[dict] = None) -> None:
        """Make HTTP request to registry.

        Args:
            method: HTTP method ("POST" or "GET")
            path: URL path (e.g., "/register")
            json: JSON body for POST requests
        """
        url = f"{self._registry_url}{path}"
        client = self._get_http_client()

        if method == "POST":
            response = await client.post(url, json=json)
        else:
            response = await client.get(url)
        response.raise_for_status()

    def _start_server_process(self) -> None:
        """Start the vLLM/SGLang HTTP server subprocess."""
        self._state = WorkerState.LOADING

        if self._config.backend == "vllm":
            cmd = self._build_vllm_command()
        else:
            cmd = self._build_sglang_command()

        logger.info(f"Starting inference server: {' '.join(cmd)}")

        self._process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            preexec_fn=self._child_preexec,
        )

    @staticmethod
    def _child_preexec() -> None:
        """Pre-exec function for subprocess.

        1. os.setsid() — new process group, so shutdown() can killpg() the whole tree.
        2. PR_SET_PDEATHSIG — kernel auto-kills this process when parent dies,
           even if parent is SIGKILL'd. Guarantees no orphan GPU processes.
        """
        os.setsid()
        try:
            import ctypes

            libc = ctypes.CDLL("libc.so.6", use_errno=True)
            PR_SET_PDEATHSIG = 1
            libc.prctl(PR_SET_PDEATHSIG, signal.SIGKILL)
        except Exception:
            pass  # Not on Linux, skip

    def _build_vllm_command(self) -> list[str]:
        """Build vLLM server command."""
        config = self._config

        cmd = [
            sys.executable,
            "-m",
            "vllm.entrypoints.openai.api_server",
            "--model",
            config.model_source,
            "--host",
            self._host,
            "--port",
            str(self._port),
            "--tensor-parallel-size",
            str(config.tensor_parallel_size),
            "--max-model-len",
            str(config.max_model_len),
            "--gpu-memory-utilization",
            str(config.gpu_memory_utilization),
            "--dtype",
            config.dtype,
        ]

        if config.trust_remote_code:
            cmd.append("--trust-remote-code")

        if config.quantization:
            cmd.extend(["--quantization", config.quantization])

        # Add extra engine kwargs as command line args
        for key, value in config.extra_engine_kwargs.items():
            arg_name = key.replace("_", "-")
            if isinstance(value, bool):
                if value:
                    cmd.append(f"--{arg_name}")
            else:
                cmd.extend([f"--{arg_name}", str(value)])

        return cmd

    def _build_sglang_command(self) -> list[str]:
        """Build SGLang server command."""
        config = self._config

        cmd = [
            sys.executable,
            "-m",
            "sglang.launch_server",
            "--model-path",
            config.model_source,
            "--host",
            self._host,
            "--port",
            str(self._port),
            "--tp-size",
            str(config.tensor_parallel_size),
        ]

        if config.trust_remote_code:
            cmd.append("--trust-remote-code")

        if config.quantization:
            cmd.extend(["--quantization", config.quantization])

        return cmd

    async def _wait_for_ready(self) -> None:
        """Wait for the server to be ready by polling the health endpoint."""
        health_url = f"{self._endpoint}/health"
        start_time = time.time()
        timeout = 600.0  # 10 minutes for model loading

        client = self._get_http_client()
        while not self._shutdown_event.is_set():
            if time.time() - start_time > timeout:
                logger.error(f"Worker {self._worker_id} startup timeout")
                self._state = WorkerState.STOPPED
                return

            try:
                response = await client.get(health_url, timeout=5.0)
                if response.status_code == 200:
                    self._state = WorkerState.READY
                    self._is_ready = True
                    logger.info(
                        f"Worker {self._worker_id} ready (took {time.time() - start_time:.1f}s)"
                    )

                    # Register with registry via HTTP
                    await self._registry_request(
                        "POST",
                        "/register",
                        json={
                            "model_id": self._config.model_id,
                            "endpoint": self._endpoint,
                            "status": {"is_ready": True, "pending": 0, "running": 0},
                        },
                    )

                    # Start heartbeat
                    self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                    return
            except Exception:
                pass

            await asyncio.sleep(2.0)

    async def _heartbeat_loop(self) -> None:
        """Periodically report status to registry via HTTP."""
        while not self._shutdown_event.is_set():
            try:
                metrics = await self._get_metrics()
                await self._registry_request(
                    "POST",
                    "/heartbeat",
                    json={
                        "endpoint": self._endpoint,
                        "status": {
                            "is_ready": self._is_ready,
                            "pending": metrics.get("pending", 0),
                            "running": metrics.get("running", 0),
                            "state": self._state.value,
                            "worker_id": self._worker_id,
                        },
                    },
                )
            except Exception as e:
                logger.warning(f"Heartbeat failed: {e}")

            await asyncio.sleep(2.0)

    async def _get_metrics(self) -> dict[str, Any]:
        """Get metrics from the inference server."""
        metrics_url = f"{self._endpoint}/metrics"

        try:
            client = self._get_http_client()
            response = await client.get(metrics_url, timeout=5.0)
            if response.status_code == 200:
                return self._parse_prometheus_metrics(response.text)
        except Exception:
            pass

        return {"pending": 0, "running": 0}

    def _parse_prometheus_metrics(self, text: str) -> dict[str, Any]:
        """Parse Prometheus metrics text format."""
        metrics: dict[str, Any] = {}

        # vLLM metrics patterns
        patterns = {
            "pending": r"vllm:num_requests_waiting\s+(\d+)",
            "running": r"vllm:num_requests_running\s+(\d+)",
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, text)
            if match:
                metrics[key] = int(match.group(1))

        return metrics

    async def _monitor_server(self) -> None:
        """Monitor the server subprocess, streaming its output to logger."""
        if self._process is None or self._process.stdout is None:
            return

        loop = asyncio.get_running_loop()
        prefix = f"[vllm:{self._worker_id}]"

        while not self._shutdown_event.is_set():
            # Read one line from subprocess stdout in a thread to avoid blocking
            line_bytes = await loop.run_in_executor(None, self._process.stdout.readline)

            if line_bytes:
                line = line_bytes.decode("utf-8", errors="replace").rstrip()
                # Use print for reliable Ray actor log capture
                print(f"{prefix} {line}", flush=True)
            else:
                # EOF - process has exited
                ret = self._process.wait()
                print(
                    f"{prefix} EXITED with code {ret}",
                    flush=True,
                )
                self._state = WorkerState.STOPPED
                self._is_ready = False
                break

    # Public API

    def get_endpoint(self) -> str:
        """Get the endpoint URL of this worker."""
        return self._endpoint

    def get_worker_id(self) -> str:
        """Get the worker ID."""
        return self._worker_id

    def get_state(self) -> str:
        """Get current worker state."""
        return self._state.value

    def is_ready(self) -> bool:
        """Check if the worker is ready to serve requests."""
        return self._is_ready

    async def start(self) -> None:
        """Start background tasks (must be called after actor creation)."""
        self._ensure_background_tasks()

    async def wait_ready(self, timeout: float = 600.0) -> bool:
        """Wait for the worker to become ready.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if ready, False if timeout
        """
        # Ensure background tasks are started
        self._ensure_background_tasks()

        start_time = time.time()
        while not self._is_ready:
            if time.time() - start_time > timeout:
                return False
            if self._state == WorkerState.STOPPED:
                return False
            await asyncio.sleep(1.0)
        return True

    def get_status(self) -> dict[str, Any]:
        """Get worker status."""
        return {
            "worker_id": self._worker_id,
            "model_id": self._config.model_id,
            "endpoint": self._endpoint,
            "state": self._state.value,
            "is_ready": self._is_ready,
            "port": self._port,
        }

    async def shutdown(self) -> None:
        """Gracefully shutdown the worker."""
        logger.info(f"Shutting down worker {self._worker_id}")

        self._state = WorkerState.DRAINING
        self._shutdown_event.set()

        # Cancel heartbeat task
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Unregister from registry via HTTP
        try:
            await self._registry_request(
                "POST",
                "/unregister",
                json={
                    "model_id": self._config.model_id,
                    "endpoint": self._endpoint,
                },
            )
        except Exception as e:
            logger.warning(f"Failed to unregister: {e}")

        # Close HTTP client
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

        # Stop the server process
        if self._process is not None:
            try:
                # Send SIGTERM to process group
                os.killpg(os.getpgid(self._process.pid), signal.SIGTERM)

                # Wait for graceful shutdown
                try:
                    self._process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    # Force kill
                    os.killpg(os.getpgid(self._process.pid), signal.SIGKILL)
                    self._process.wait(timeout=5)
            except Exception as e:
                logger.warning(f"Error stopping server process: {e}")

        self._state = WorkerState.STOPPED
        self._is_ready = False
        logger.info(f"Worker {self._worker_id} shutdown complete")
