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

"""Reusable HTTP clients for OpenAI-compatible chat completions APIs.

Two clients:

1. ``ChatCompletionsClient`` — base client with endpoint discovery, retries,
   context-length error detection, and cache invalidation.
2. ``RoutedChatCompletionsClient`` — wraps the base client with length-based
   model routing and automatic context-length fallback to the next larger model.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx
import ray

from _internal.serve.client import ModelClient

logger = logging.getLogger(__name__)

_CONTEXT_LENGTH_KEYWORDS = (
    "maximum context length",
    "max_model_len",
    "input is too long",
    "exceed",
    "context length",
    "too many tokens",
)


class ContextLengthError(Exception):
    """Raised when the server rejects a request due to input length."""


# ---------------------------------------------------------------------------
# Routing config
# ---------------------------------------------------------------------------


@dataclass
class ModelRoute:
    """Routing rule: requests with estimated tokens <= max_tokens use this model."""

    model_id: str
    max_tokens: int


@dataclass
class ModelRoutingConfig:
    """Length-based routing across multiple model deployments.

    Routes are matched in order. Each request is routed to the first model
    whose max_tokens >= estimated token count. If no route fits, the request
    goes to the last (largest) route with a warning.

    Attributes:
        routes: Routing rules sorted by max_tokens ascending
        tokens_per_image: Estimated tokens per image (VLM)
        chars_per_token: Rough chars-to-token ratio for text
    """

    routes: list[ModelRoute] = field(default_factory=list)
    tokens_per_image: int = 1000
    chars_per_token: float = 3.5

    def __post_init__(self) -> None:
        if len(self.routes) < 1:
            raise ValueError("ModelRoutingConfig requires at least one route")
        for i in range(1, len(self.routes)):
            if self.routes[i].max_tokens <= self.routes[i - 1].max_tokens:
                raise ValueError("routes must have strictly increasing max_tokens")


# ---------------------------------------------------------------------------
# Base client
# ---------------------------------------------------------------------------


class ChatCompletionsClient:
    """HTTP client for OpenAI-compatible chat completions with retry and discovery.

    Features:
    - Endpoint discovery via ModelClient (or direct base_url)
    - Retry with linear backoff on transient errors (429, 5xx, connect, timeout)
    - Re-selects endpoint on connection failure
    - Detects context-length errors from HTTP 400 responses
    - Cache invalidation on connection failure

    Usage with ModelClient::

        client = ChatCompletionsClient(registry=registry_handle)
        content = await client.generate("my_model", body={"messages": [...]})

    Usage with direct base_url (no ModelClient)::

        client = ChatCompletionsClient(base_url="http://server:8000")
        content = await client.generate("my_model", body={"messages": [...]})
    """

    def __init__(
        self,
        registry: Optional[ray.actor.ActorHandle] = None,
        base_url: str = "",
        timeout: float = 120.0,
        max_retries: int = 3,
        cache_ttl_seconds: float = 30.0,
    ) -> None:
        self._base_url = base_url
        self._max_retries = max_retries
        self._http_client: Optional[httpx.AsyncClient] = None
        self._timeout = timeout

        self._model_client: Optional[ModelClient] = None
        if registry is not None:
            self._model_client = ModelClient(
                registry=registry,
                cache_ttl_seconds=cache_ttl_seconds,
            )

    def _get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=self._timeout)
        return self._http_client

    async def get_endpoints(self, model_id: str) -> list[str]:
        """Get all ready endpoint URLs for a model."""
        if self._model_client is not None:
            return await self._model_client.get_endpoints(model_id)
        return [self._base_url]

    def invalidate_cache(self, model_id: str) -> None:
        """Invalidate cached endpoints for a model."""
        if self._model_client is not None:
            self._model_client.invalidate_cache(model_id)

    async def post(
        self,
        model_id: str,
        body: dict[str, Any],
        path: str = "/v1/chat/completions",
    ) -> dict[str, Any]:
        """POST to an endpoint with retries. Returns the full response dict.

        On connection failure, invalidates the endpoint cache and re-selects.

        Raises:
            ContextLengthError: If the server returns HTTP 400 with context-length keywords
            RuntimeError: If all retries are exhausted
        """
        endpoints = await self.get_endpoints(model_id)
        endpoint = random.choice(endpoints)
        url = f"{endpoint}{path}"
        client = self._get_http_client()
        last_error: Optional[Exception] = None

        for attempt in range(self._max_retries):
            try:
                response = await client.post(url, json=body)
                response.raise_for_status()
                data = response.json()
                if "error" in data:
                    raise RuntimeError(f"API error: {data['error']}")
                return data
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                last_error = e
                logger.warning(
                    f"[{model_id}] attempt {attempt + 1}/{self._max_retries} "
                    f"failed: {type(e).__name__}: {e or 'timeout'}"
                )
                self.invalidate_cache(model_id)
                endpoints = await self.get_endpoints(model_id)
                endpoint = random.choice(endpoints)
                url = f"{endpoint}{path}"
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (429, 500, 502, 503, 504):
                    last_error = e
                    logger.warning(
                        f"[{model_id}] HTTP {e.response.status_code} "
                        f"(attempt {attempt + 1}/{self._max_retries})"
                    )
                elif e.response.status_code == 400:
                    body_text = e.response.text.lower()
                    if any(kw in body_text for kw in _CONTEXT_LENGTH_KEYWORDS):
                        raise ContextLengthError(e.response.text) from e
                    raise
                else:
                    raise
            await asyncio.sleep(0.5 * (attempt + 1))

        raise RuntimeError(f"All {self._max_retries} attempts failed for {model_id}: {last_error}")

    async def generate(
        self,
        model_id: str,
        body: dict[str, Any],
        path: str = "/v1/chat/completions",
    ) -> str:
        """POST and return choices[0].message.content.

        Convenience wrapper around ``post()`` for the common case.
        """
        resp = await self.post(model_id, body, path)
        return resp["choices"][0]["message"]["content"]

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None


# ---------------------------------------------------------------------------
# Routed client
# ---------------------------------------------------------------------------


class RoutedChatCompletionsClient:
    """ChatCompletionsClient with length-based model routing and fallback.

    Wraps a ``ChatCompletionsClient`` and adds:
    - Automatic model selection based on estimated token count
    - Context-length fallback: on ``ContextLengthError``, retries with the
      next larger model in the routing config

    The ``generate()`` signature matches ``ChatCompletionsClient`` so they
    can be used interchangeably::

        routed = RoutedChatCompletionsClient(client, routing_config)

        # model_id not in routes → auto-pick based on token estimate
        content = await routed.generate("default", body)

        # model_id IS a route → use it directly (still gets fallback)
        content = await routed.generate("small-ctx", body)
    """

    def __init__(
        self,
        client: ChatCompletionsClient,
        routing: ModelRoutingConfig,
    ) -> None:
        self._client = client
        self._routing = routing
        self._route_models = {r.model_id for r in routing.routes}

    # --- Proxy methods (same interface as ChatCompletionsClient) ---

    async def get_endpoints(self, model_id: str) -> list[str]:
        return await self._client.get_endpoints(model_id)

    def invalidate_cache(self, model_id: str) -> None:
        self._client.invalidate_cache(model_id)

    async def post(
        self,
        model_id: str,
        body: dict[str, Any],
        path: str = "/v1/chat/completions",
    ) -> dict[str, Any]:
        return await self._client.post(model_id, body, path)

    async def close(self) -> None:
        await self._client.close()

    # --- Routing logic ---

    def estimate_tokens(self, messages: list[dict]) -> int:
        """Rough token estimate for routing (not exact tokenization)."""
        total = 0.0
        for msg in messages:
            content = msg.get("content", "")
            if isinstance(content, str):
                total += len(content) / self._routing.chars_per_token
            elif isinstance(content, list):
                for item in content:
                    if item.get("type") == "text":
                        total += len(item.get("text", "")) / self._routing.chars_per_token
                    elif item.get("type") == "image_url":
                        total += self._routing.tokens_per_image
        return int(total)

    def pick_model(self, messages: list[dict]) -> str:
        """Pick the smallest model whose max_tokens fits the estimated token count."""
        estimated = self.estimate_tokens(messages)
        for route in self._routing.routes:
            if estimated <= route.max_tokens:
                return route.model_id
        logger.warning(
            f"Estimated {estimated} tokens exceeds max route "
            f"({self._routing.routes[-1].max_tokens}), "
            f"routing to largest deployment"
        )
        return self._routing.routes[-1].model_id

    def next_model(self, current_model_id: str) -> Optional[str]:
        """Return the next larger model in the routing config, or None."""
        for i, route in enumerate(self._routing.routes):
            if route.model_id == current_model_id and i + 1 < len(self._routing.routes):
                return self._routing.routes[i + 1].model_id
        return None

    # --- Generate with routing + fallback ---

    async def generate(
        self,
        model_id: str,
        body: dict[str, Any],
        path: str = "/v1/chat/completions",
    ) -> str:
        """Generate with automatic model routing and context-length fallback.

        If ``model_id`` is not a route model (e.g. a default placeholder),
        picks the optimal model based on estimated token count of messages
        in the body.

        On ``ContextLengthError``, automatically retries with the next larger
        model. Raises ``ContextLengthError`` only when all routes are exhausted.
        """
        # Pick model if caller didn't specify a route model
        if model_id not in self._route_models:
            messages = body.get("messages", [])
            model_id = self.pick_model(messages)
            body = {**body, "model": model_id}

        try:
            return await self._client.generate(model_id, body, path)
        except ContextLengthError:
            next_m = self.next_model(model_id)
            if next_m is None:
                raise
            logger.info(f"Context length exceeded on {model_id}, falling back to {next_m}")
            body = {**body, "model": next_m}
            return await self.generate(next_m, body, path)
