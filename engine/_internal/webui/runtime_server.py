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

"""Embedded WebUI server for runtime mode."""

from __future__ import annotations

import socket
import threading
from typing import Optional

import uvicorn

from _internal.webui.app import create_webui_app
from _internal.webui.state.manager import JobStateManager
from _internal.utils.logging import create_ray_logger


def _find_available_port(host: str, start_port: int, max_tries: int = 200) -> int:
    for port in range(start_port, start_port + max_tries):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
            except OSError:
                continue
            return port
    raise RuntimeError(f"No available port found starting at {start_port}")


class EmbeddedWebUIServer:
    """Run WebUI inside the job driver process.

    Reads metadata directly from Anvil storage (pyO3) via JobStateManager.
    """

    def __init__(
        self,
        job_id: str,
        storage: JobStateManager,
        host: str = "0.0.0.0",
        port_base: int = 5000,
    ):
        self.job_id = job_id
        self.storage = storage
        self.host = host
        self.port_base = port_base
        self.port: Optional[int] = None
        self._server: Optional[uvicorn.Server] = None
        self._thread: Optional[threading.Thread] = None
        self.logger = create_ray_logger(f"WebUIRuntime-{job_id}")

    def start(self) -> int:
        """Start the embedded WebUI server."""
        if self._server:
            return self.port or self.port_base

        host = self.host
        port = _find_available_port(host, self.port_base)

        app = create_webui_app(
            self.storage,
            title=f"Nurion Job {self.job_id}",
            base_path="",
        )

        config = uvicorn.Config(
            app,
            host=host,
            port=port,
            log_level="info",
        )
        server = uvicorn.Server(config)
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()

        self._server = server
        self._thread = thread
        self.port = port
        self.logger.info(f"Embedded WebUI running on {host}:{port}")
        return port

    def stop(self) -> None:
        """Stop the embedded WebUI server."""
        if not self._server:
            return
        self._server.should_exit = True
        if self._thread:
            self._thread.join(timeout=5)
        self._server = None
        self._thread = None
        self.logger.info("Embedded WebUI stopped")
