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

"""Model Registry - High-throughput service discovery with embedded HTTP server.

Architecture:
- Ray Named Actor for lifecycle management and URL discovery
- Embedded aiohttp server for high-frequency data plane operations
- Ray async actor keeps the event loop running between method calls,
  so the aiohttp server stays alive as long as the actor lives
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

from aiohttp import web

from _internal.utils.network import find_free_port, get_node_ip

logger = logging.getLogger(__name__)

REGISTRY_ACTOR_NAME = "nurion_model_registry"


class ModelRegistry:
    """Model endpoint registry with embedded aiohttp server.

    This is a Ray async actor. The event loop runs persistently, keeping
    the aiohttp server alive between method calls.

    IMPORTANT: The actor is non-detached and reference-counted. At least one
    ActorHandle must be held (e.g., in ModelServiceManager._registry) to keep
    the actor alive. If all handles are GC'd, the actor dies.
    """

    def __init__(self, port: Optional[int] = None) -> None:
        self._endpoints: dict[str, list[str]] = {}
        self._worker_status: dict[str, dict[str, Any]] = {}
        self._last_heartbeat: dict[str, float] = {}

        self._port = port or find_free_port()
        self._node_ip = get_node_ip()
        self._http_url = f"http://{self._node_ip}:{self._port}"
        self._runner: Optional[web.AppRunner] = None
        self._started = False

    async def start(self) -> str:
        """Start the aiohttp server. Must be called after actor creation."""
        if self._started:
            return self._http_url

        app = web.Application()
        app.router.add_post("/register", self._handle_register)
        app.router.add_post("/unregister", self._handle_unregister)
        app.router.add_post("/heartbeat", self._handle_heartbeat)
        app.router.add_get("/endpoints", self._handle_get_endpoints)
        app.router.add_get("/endpoints_status", self._handle_get_endpoints_with_status)
        app.router.add_get("/models", self._handle_get_all_models)
        app.router.add_get("/status", self._handle_get_status)
        app.router.add_get("/health", self._handle_health)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self._port)
        await site.start()
        self._started = True

        logger.info(f"ModelRegistry HTTP server started on {self._http_url}")
        return self._http_url

    # HTTP Handlers

    async def _handle_register(self, request: web.Request) -> web.Response:
        data = await request.json()
        self._register(data["model_id"], data["endpoint"], data.get("status"))
        return web.json_response({"ok": True})

    async def _handle_unregister(self, request: web.Request) -> web.Response:
        data = await request.json()
        self._unregister(data["model_id"], data["endpoint"])
        return web.json_response({"ok": True})

    async def _handle_heartbeat(self, request: web.Request) -> web.Response:
        data = await request.json()
        self._update_status(data["endpoint"], data.get("status", {}))
        return web.json_response({"ok": True})

    async def _handle_get_endpoints(self, request: web.Request) -> web.Response:
        model_id = request.query.get("model_id", "")
        return web.json_response(self._get_endpoints(model_id))

    async def _handle_get_endpoints_with_status(self, request: web.Request) -> web.Response:
        model_id = request.query.get("model_id", "")
        return web.json_response(self._get_endpoints_with_status(model_id))

    async def _handle_get_all_models(self, request: web.Request) -> web.Response:
        return web.json_response(list(self._endpoints.keys()))

    async def _handle_get_status(self, request: web.Request) -> web.Response:
        return web.json_response(self._get_all_status())

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "healthy"})

    # Core logic

    def _register(
        self, model_id: str, endpoint: str, status: Optional[dict[str, Any]] = None
    ) -> None:
        if model_id not in self._endpoints:
            self._endpoints[model_id] = []
        if endpoint not in self._endpoints[model_id]:
            self._endpoints[model_id].append(endpoint)
            logger.info(f"Registered endpoint {endpoint} for model {model_id}")
        self._last_heartbeat[endpoint] = time.time()
        if status:
            self._worker_status[endpoint] = status

    def _unregister(self, model_id: str, endpoint: str) -> None:
        if model_id in self._endpoints:
            self._endpoints[model_id] = [e for e in self._endpoints[model_id] if e != endpoint]
            if not self._endpoints[model_id]:
                del self._endpoints[model_id]
        self._worker_status.pop(endpoint, None)
        self._last_heartbeat.pop(endpoint, None)
        logger.info(f"Unregistered endpoint {endpoint} for model {model_id}")

    def _update_status(self, endpoint: str, status: dict[str, Any]) -> None:
        self._worker_status[endpoint] = status
        self._last_heartbeat[endpoint] = time.time()

    def _get_endpoints(self, model_id: str) -> list[str]:
        return list(self._endpoints.get(model_id, []))

    def _get_endpoints_with_status(self, model_id: str) -> list[dict[str, Any]]:
        endpoints = self._endpoints.get(model_id, [])
        now = time.time()
        return [
            {
                "endpoint": ep,
                "pending": self._worker_status.get(ep, {}).get("pending", 0),
                "running": self._worker_status.get(ep, {}).get("running", 0),
                "is_ready": self._worker_status.get(ep, {}).get("is_ready", True),
                "last_heartbeat_age_s": now - self._last_heartbeat.get(ep, 0),
                **self._worker_status.get(ep, {}),
            }
            for ep in endpoints
        ]

    def _get_all_status(self) -> dict[str, Any]:
        return {
            "models": {
                mid: {"endpoints": eps, "worker_count": len(eps)}
                for mid, eps in self._endpoints.items()
            },
            "total_workers": sum(len(e) for e in self._endpoints.values()),
            "total_models": len(self._endpoints),
            "http_url": self._http_url,
        }

    # Ray Actor public API (control plane only, data plane uses HTTP)

    def get_http_url(self) -> str:
        """Get the HTTP URL. Only Ray method clients need to call."""
        return self._http_url

    def list_models(self) -> list[str]:
        """List all registered model IDs. Used by connect() for discovery."""
        return list(self._endpoints.keys())

    async def stop(self) -> None:
        """Stop the HTTP server."""
        if self._runner:
            await self._runner.cleanup()
        self._started = False
        logger.info("ModelRegistry stopped")
