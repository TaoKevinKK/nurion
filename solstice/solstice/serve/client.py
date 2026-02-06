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

"""Model Client - Async endpoint discovery and load-balanced selection."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx
import ray

logger = logging.getLogger(__name__)


@dataclass
class EndpointInfo:
    """Information about a model endpoint."""

    endpoint: str
    pending: int = 0
    running: int = 0
    is_ready: bool = True
    last_heartbeat_age_s: float = 0.0


@dataclass
class EndpointCache:
    """Cached endpoint information for a model."""

    endpoints: list[EndpointInfo] = field(default_factory=list)
    cached_at: float = 0.0

    def is_fresh(self, ttl: float) -> bool:
        return time.time() - self.cached_at < ttl

    @classmethod
    def from_registry_response(cls, response: list[dict[str, Any]]) -> "EndpointCache":
        endpoints = [
            EndpointInfo(
                endpoint=item["endpoint"],
                pending=item.get("pending", 0),
                running=item.get("running", 0),
                is_ready=item.get("is_ready", True),
                last_heartbeat_age_s=item.get("last_heartbeat_age_s", 0.0),
            )
            for item in response
        ]
        return cls(endpoints=endpoints, cached_at=time.time())


class ModelClient:
    """Async client for discovering and selecting model inference endpoints.

    Usage:
        client = ModelClient(registry=registry_handle)
        endpoint = await client.get_endpoint("caption_vlm")
        # → "http://10.1.48.251:8000"
    """

    def __init__(self, registry: ray.ActorHandle, cache_ttl_seconds: float = 30.0) -> None:
        self._registry = registry
        self._cache_ttl = cache_ttl_seconds
        self._registry_url: Optional[str] = None
        self._endpoint_cache: dict[str, EndpointCache] = {}
        self._http_client: Optional[httpx.AsyncClient] = None

    def _get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=10.0)
        return self._http_client

    async def _get_registry_url(self) -> str:
        if self._registry_url is None:
            self._registry_url = await self._registry.get_http_url.remote()
        return self._registry_url

    async def _fetch_endpoints(self, model_id: str) -> list[dict[str, Any]]:
        url = f"{await self._get_registry_url()}/endpoints_status"
        client = self._get_http_client()
        response = await client.get(url, params={"model_id": model_id})
        response.raise_for_status()
        return response.json()

    async def _get_endpoints(self, model_id: str) -> list[EndpointInfo]:
        cache = self._endpoint_cache.get(model_id)
        if cache and cache.is_fresh(self._cache_ttl):
            return cache.endpoints

        try:
            response = await self._fetch_endpoints(model_id)
            cache = EndpointCache.from_registry_response(response)
            self._endpoint_cache[model_id] = cache
            return cache.endpoints
        except httpx.ConnectError:
            logger.warning("Registry connection failed, re-resolving URL...")
            self._registry_url = None
            try:
                response = await self._fetch_endpoints(model_id)
                cache = EndpointCache.from_registry_response(response)
                self._endpoint_cache[model_id] = cache
                return cache.endpoints
            except Exception as e:
                logger.warning(f"Failed after re-resolve for {model_id}: {e}")
                if cache:
                    return cache.endpoints
                return []
        except Exception as e:
            logger.warning(f"Failed to get endpoints for {model_id}: {e}")
            if cache:
                return cache.endpoints
            return []

    async def get_endpoints(self, model_id: str) -> list[str]:
        """Get all ready endpoint URLs for a model.

        Returns:
            List of endpoint URLs (ready ones only, or all if none ready)

        Raises:
            RuntimeError: If no endpoints available
        """
        endpoints = await self._get_endpoints(model_id)
        if not endpoints:
            raise RuntimeError(f"No endpoints available for model {model_id}")

        ready = [e.endpoint for e in endpoints if e.is_ready]
        return ready or [e.endpoint for e in endpoints]

    def invalidate_cache(self, model_id: str) -> None:
        self._endpoint_cache.pop(model_id, None)

    async def close(self) -> None:
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None
