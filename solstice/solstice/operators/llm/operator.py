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

"""External LLM/VLM Operator for calling external inference services.

Uses OpenAI-compatible Chat Completions API (/v1/chat/completions).

Two modes for endpoint discovery:
1. Direct mode: Set `base_url` to call a specific endpoint
2. ModelClient mode: Set `use_model_client=True` for dynamic endpoint
   discovery and load balancing via solstice.serve
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any, ClassVar, Literal, Optional, Type

import httpx
import pyarrow as pa
import ray

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import Operator, OperatorConfig, OperatorRuntime, operator
from solstice.operators.llm.utils import (
    build_multi_image_message,
    build_single_image_message,
    extract_column,
    extract_messages,
    extract_prompts,
)
from solstice.serve.client import ModelClient


class EndpointSelectPolicy:
    """Policy for selecting endpoints from a list.

    Each instance has a random offset so that different workers in a
    distributed system don't all start from index 0.
    """

    def __init__(self, endpoints: list[str]) -> None:
        self._endpoints = endpoints
        self._offset = random.randint(0, max(len(endpoints) - 1, 0))
        self._counter = 0

    def next(self) -> str:
        """Return next endpoint using round-robin with random start offset."""
        idx = (self._offset + self._counter) % len(self._endpoints)
        self._counter += 1
        return self._endpoints[idx]


@dataclass
class ExternalLLMOperatorConfig(OperatorConfig):
    """Configuration for calling external LLM services via HTTP.

    Two modes:
    1. Direct mode (set `base_url`): Call a specific endpoint
    2. ModelClient mode (set `use_model_client=True`): Dynamic discovery

    Usage:
        # Direct mode
        config = ExternalLLMOperatorConfig(
            base_url="http://server:8000",
            model="Qwen/Qwen2.5-72B-Instruct",
            prompt="Describe this image.",
            image_field="image",
        )

        # ModelClient mode
        config = ExternalLLMOperatorConfig(
            use_model_client=True,
            model="caption_vlm",
            prompt="Describe this image.",
            image_field="image",
        )
    """

    operator_class: ClassVar[Type["ExternalLLMOperator"]]

    # Endpoint
    base_url: str = ""
    use_model_client: bool = False
    registry: Optional[ray.actor.ActorHandle] = None  # Required when use_model_client=True

    # Model name — used for both ModelClient discovery AND API request body.
    # For ModelClient mode, this must match the model_id in ModelServiceManager.
    # vLLM's served_model_name defaults to model_source (HuggingFace ID).
    model: str = ""

    # HTTP
    timeout: float = 120.0
    max_retries: int = 3

    # Generation parameters
    max_tokens: int = 512
    temperature: float = 0.7
    top_p: float = 0.95
    top_k: int = 0
    presence_penalty: float = 0.0
    frequency_penalty: float = 0.0

    # Output
    output_field: str = "response"

    # Batching (concurrent requests per split)
    batch_size: int = 32

    # --- Text-only mode ---
    messages_field: str = ""

    # --- Vision mode ---
    prompt: str = ""
    prompt_field: str = ""
    image_field: str = ""
    image_url_field: str = ""
    images_field: str = ""
    detail: Literal["auto", "low", "high"] = "auto"


@operator(ExternalLLMOperatorConfig)
class ExternalLLMOperator(Operator):
    """Operator for calling external LLM services via OpenAI-compatible API.

    Async process_split with concurrent batch requests via asyncio.gather.
    ModelClient handles endpoint discovery; the API call path is unified.
    """

    def __init__(self, config: ExternalLLMOperatorConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self._config: ExternalLLMOperatorConfig = config
        self._http_client: Optional[httpx.AsyncClient] = None
        self._model_client: Optional[Any] = None

    def _get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=self._config.timeout)
        return self._http_client

    def _get_model_client(self) -> ModelClient:
        if self._model_client is None:
            assert self._config.registry is not None, (
                "registry must be set in config when use_model_client=True"
            )
            self._model_client = ModelClient(
                registry=self._config.registry,
                cache_ttl_seconds=30.0,
            )
        return self._model_client

    async def _get_endpoints(self) -> list[str]:
        """Get endpoints — from ModelClient or base_url."""
        if self._config.use_model_client:
            return await self._get_model_client().get_endpoints(self._config.model)
        return [self._config.base_url]

    async def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Process a split by generating responses concurrently."""
        if payload is None:
            return None

        table = payload.to_table()

        # Extract messages
        if self._config.messages_field:
            messages_list = extract_messages(table, self._config.messages_field)
        else:
            messages_list = self._build_vision_messages(table)

        # Get endpoints once per split, round-robin with random offset
        endpoints = await self._get_endpoints()
        selector = EndpointSelectPolicy(endpoints)

        # Generate outputs in batches with asyncio.gather
        outputs: list[str] = []
        for i in range(0, len(messages_list), self._config.batch_size):
            batch = messages_list[i : i + self._config.batch_size]
            batch_results = await asyncio.gather(
                *(self._generate_one(m, selector.next()) for m in batch)
            )
            outputs.extend(batch_results)

        # Add outputs to table
        output_array = pa.array(outputs, type=pa.string())
        new_table = table.append_column(self._config.output_field, output_array)

        return SplitPayload(
            data=new_table,
            split_id=f"{split.split_id}_{self.worker_id}",
        )

    async def _generate_one(self, messages: list[dict], endpoint: str) -> str:
        """Generate response for a single message list with retries."""
        url = f"{endpoint}/v1/chat/completions"
        body = self._build_request_body(messages)

        try:
            return await self._call_api(url, body)
        except Exception as e:
            self.logger.error(f"Failed to generate response: {e}")
            if self._config.use_model_client:
                self._get_model_client().invalidate_cache(self._config.model)
            return f"[ERROR: {str(e)}]"

    async def _call_api(self, url: str, body: dict[str, Any]) -> str:
        """POST to chat/completions endpoint with retries."""
        client = self._get_http_client()
        last_error: Optional[Exception] = None

        for attempt in range(self._config.max_retries):
            try:
                response = await client.post(url, json=body)
                response.raise_for_status()
                return response.json()["choices"][0]["message"]["content"]
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                last_error = e
                self.logger.warning(
                    f"Request failed (attempt {attempt + 1}/{self._config.max_retries}): {e}"
                )
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (429, 500, 502, 503, 504):
                    last_error = e
                    self.logger.warning(
                        f"Retryable HTTP {e.response.status_code} "
                        f"(attempt {attempt + 1}/{self._config.max_retries})"
                    )
                else:
                    raise
            # Brief backoff before retry
            await asyncio.sleep(0.5 * (attempt + 1))

        raise RuntimeError(
            f"All {self._config.max_retries} attempts failed for {url}: {last_error}"
        )

    def _build_request_body(self, messages: list[dict]) -> dict[str, Any]:
        """Build OpenAI-compatible request body."""
        body: dict[str, Any] = {
            "messages": messages,
            "max_tokens": self._config.max_tokens,
            "temperature": self._config.temperature,
            "top_p": self._config.top_p,
        }

        if self._config.model:
            body["model"] = self._config.model
        if self._config.top_k > 0:
            body["top_k"] = self._config.top_k
        if self._config.presence_penalty != 0.0:
            body["presence_penalty"] = self._config.presence_penalty
        if self._config.frequency_penalty != 0.0:
            body["frequency_penalty"] = self._config.frequency_penalty

        return body

    def _build_vision_messages(self, table: pa.Table) -> list[list[dict]]:
        """Build OpenAI-compatible vision messages from table."""
        prompts = extract_prompts(table, self._config.prompt_field, self._config.prompt)
        detail = self._config.detail

        if self._config.images_field:
            if self._config.images_field not in table.column_names:
                raise ValueError(f"Images field '{self._config.images_field}' not found.")
            images_list = table[self._config.images_field].to_pylist()
            return [
                build_multi_image_message(prompt, images or [], detail)
                for prompt, images in zip(prompts, images_list)
            ]

        images = extract_column(table, self._config.image_field, len(prompts))
        image_urls = extract_column(table, self._config.image_url_field, len(prompts))
        return [
            build_single_image_message(prompt, image, url, detail)
            for prompt, image, url in zip(prompts, images, image_urls)
        ]

    def close(self) -> None:
        """Clean up resources."""
        if self._http_client:
            # Schedule async close if loop is running
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._http_client.aclose())
            except RuntimeError:
                pass
            self._http_client = None
        super().close()
