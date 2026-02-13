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
   discovery and load balancing via _internal.serve
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, ClassVar, Literal, Optional, Type, Union

import pyarrow as pa
import ray

from _internal.core.models import Split, SplitPayload
from _internal.core.operator import Operator, OperatorConfig, OperatorRuntime, operator
from _internal.operators.llm.client import (
    ChatCompletionsClient,
    ContextLengthError,
    ModelRoutingConfig,
    RoutedChatCompletionsClient,
)
from _internal.operators.llm.utils import (
    build_multi_image_message,
    build_single_image_message,
    extract_column,
    extract_messages,
    extract_prompts,
)

logger = logging.getLogger(__name__)


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

    # --- Length-based routing ---
    model_routing: Optional[ModelRoutingConfig] = None


@operator(ExternalLLMOperatorConfig)
class ExternalLLMOperator(Operator):
    """Operator for calling external LLM services via OpenAI-compatible API.

    Async process_split with concurrent batch requests via asyncio.gather.
    Uses ChatCompletionsClient (or RoutedChatCompletionsClient when model_routing
    is configured) for endpoint discovery, retries, and error detection.
    """

    def __init__(self, config: ExternalLLMOperatorConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self._config: ExternalLLMOperatorConfig = config
        self._client: Optional[Union[ChatCompletionsClient, RoutedChatCompletionsClient]] = None

    def _get_client(self) -> Union[ChatCompletionsClient, RoutedChatCompletionsClient]:
        if self._client is None:
            base = ChatCompletionsClient(
                registry=self._config.registry if self._config.use_model_client else None,
                base_url=self._config.base_url,
                timeout=self._config.timeout,
                max_retries=self._config.max_retries,
            )
            if self._config.model_routing is not None:
                self._client = RoutedChatCompletionsClient(base, self._config.model_routing)
            else:
                self._client = base
        return self._client

    async def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Process a split by generating responses concurrently."""
        if payload is None:
            return None

        table = payload.to_table()

        if self._config.messages_field:
            messages_list = extract_messages(table, self._config.messages_field)
        else:
            messages_list = self._build_vision_messages(table)

        outputs = []
        for i in range(0, len(messages_list), self._config.batch_size):
            batch = messages_list[i : i + self._config.batch_size]
            batch_results = await asyncio.gather(*(self._generate_one(m) for m in batch))
            outputs.extend(batch_results)

        output_array = pa.array(outputs, type=pa.string())
        new_table = table.append_column(self._config.output_field, output_array)

        return SplitPayload(
            data=new_table,
            split_id=f"{split.split_id}_{self.worker_id}",
        )

    async def _generate_one(self, messages: list[dict], model: Optional[str] = None) -> str:
        """Generate response for a single message list.

        When model_routing is configured, the RoutedChatCompletionsClient
        automatically picks the optimal model and handles context-length
        fallback. ContextLengthError only reaches here when all routes
        are exhausted.
        """
        body = self._build_request_body(messages, model=model)
        effective_model = model or self._config.model

        try:
            return await self._get_client().generate(effective_model, body)
        except ContextLengthError as e:
            self.logger.error(f"Context length exceeded: {e}")
            return "[ERROR: context length exceeded on largest model]"
        except Exception as e:
            self.logger.error(f"Failed to generate response: {e}")
            self._get_client().invalidate_cache(effective_model)
            return f"[ERROR: {str(e)}]"

    def _build_request_body(
        self, messages: list[dict], model: Optional[str] = None
    ) -> dict[str, Any]:
        """Build OpenAI-compatible request body."""
        body: dict[str, Any] = {
            "messages": messages,
            "max_tokens": self._config.max_tokens,
            "temperature": self._config.temperature,
            "top_p": self._config.top_p,
        }

        effective_model = model or self._config.model
        if effective_model:
            body["model"] = effective_model
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
        if self._client is not None:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._client.close())
            except RuntimeError:
                pass
            self._client = None
        super().close()
