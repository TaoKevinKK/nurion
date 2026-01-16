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

"""Unified LLM/VLM Operator for chat completions inference.

Supports:
- Text-only chat (messages_field)
- Single image + text (prompt_field + image_field/image_url_field)
- Multiple images + text (prompt_field + images_field)

Uses OpenAI-compatible Chat Completions API (/v1/chat/completions).
"""

from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Type, Union

import pyarrow as pa

from solstice.core.models import Split, SplitPayload
from solstice.operators.http.operator import HttpOperator, HttpOperatorConfig
from solstice.operators.llm.config import RouterConfig, WorkerConfig

if TYPE_CHECKING:
    from solstice.operators.llm.stage_master import LLMStageMaster


@dataclass
class LLMOperatorConfig(HttpOperatorConfig):
    """Configuration for LLM/VLM chat completions operator.

    Supports three modes based on which fields are set:

    1. Text-only chat: Set `messages_field` to column containing chat messages
       Input format: [{"role": "user", "content": "Hello"}]

    2. Single image + text: Set `prompt_field` and (`image_field` or `image_url_field`)
       Input: prompt string + image data/URL

    3. Multiple images + text: Set `prompt_field` and `images_field`
       Input: prompt string + list of images

    Attributes:
        model: Model name for the API
        max_tokens: Maximum tokens to generate
        temperature: Sampling temperature (0 = deterministic)
        top_p: Top-p (nucleus) sampling
        output_field: Output column for generated response
        batch_size: Number of requests to process concurrently

        # Text-only mode
        messages_field: Input column containing chat messages (list of dicts)

        # Vision mode (single or multi-image)
        prompt_field: Input column containing text prompts
        image_field: Input column containing single image (base64 or bytes)
        image_url_field: Input column containing single image URL
        images_field: Input column containing multiple images (list)
        detail: Image detail level for vision API

        # Managed inference infrastructure
        managed: Whether Solstice manages the inference servers
        router_config: Configuration for the SGLang router
        worker_config: Configuration for each SGLang worker
        num_workers: Number of GPU workers to start
        gpus_per_worker: GPUs allocated to each worker
    """

    operator_class: ClassVar[Type["LLMOperator"]]
    master_class: ClassVar[Optional[Type[LLMStageMaster]]] = None

    # Model configuration
    model: str = ""

    # Generation parameters
    max_tokens: int = 512
    temperature: float = 0.7
    top_p: float = 0.95

    # Output field
    output_field: str = "response"

    # Batching
    batch_size: int = 32

    # --- Text-only mode ---
    messages_field: str = ""  # Column containing chat messages

    # --- Vision mode ---
    prompt: str = ""  # Fixed prompt for all rows
    prompt_field: str = ""  # Column containing per-row prompts (overrides prompt)
    image_field: str = ""  # Column containing single image (base64/bytes)
    image_url_field: str = ""  # Column containing single image URL
    images_field: str = ""  # Column containing list of images
    detail: Literal["auto", "low", "high"] = "auto"

    # Managed inference infrastructure
    managed: bool = False
    router_config: RouterConfig = field(default_factory=RouterConfig)
    worker_config: WorkerConfig = field(default_factory=WorkerConfig)
    num_workers: Optional[int] = None  # None = auto-detect from cluster
    gpus_per_worker: int = 1


class LLMOperator(HttpOperator):
    """Unified LLM/VLM operator using OpenAI Chat Completions API.

    Supports three modes:
    1. Text-only: messages_field contains chat messages
    2. Single image: (prompt or prompt_field) + (image_field or image_url_field)
    3. Multi-image: (prompt or prompt_field) + images_field

    For vision modes, use `prompt` for a fixed prompt applied to all rows,
    or `prompt_field` for per-row prompts from a column.

    Usage:
        # Text-only chat
        config = LLMOperatorConfig(
            base_url="http://server:8000",
            model="Qwen/Qwen2.5-72B-Instruct",
            messages_field="messages",
        )

        # Single image + fixed prompt (VLM)
        config = LLMOperatorConfig(
            base_url="http://server:8000",
            model="Qwen/Qwen2.5-VL-72B-Instruct",
            prompt="Describe this image in detail.",
            image_field="image_base64",
        )

        # Single image + per-row prompt
        config = LLMOperatorConfig(
            base_url="http://server:8000",
            model="Qwen/Qwen2.5-VL-72B-Instruct",
            prompt_field="question",  # Each row has its own prompt
            image_url_field="image_url",
        )

        # Multiple images + fixed prompt
        config = LLMOperatorConfig(
            base_url="http://server:8000",
            model="Qwen/Qwen2.5-VL-72B-Instruct",
            prompt="Describe the sequence of events in these frames.",
            images_field="frames",
        )
    """

    def __init__(self, config: LLMOperatorConfig):
        super().__init__(config)
        self._config = config

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Process a split by generating responses."""
        if payload is None:
            return None

        table = payload.to_table()

        # Determine mode and extract data
        if self._config.messages_field:
            # Text-only mode
            messages_list = self._extract_messages(table)
        else:
            # Vision mode (single or multi-image)
            messages_list = self._extract_vision_messages(table)

        # Generate outputs
        outputs = asyncio.run(self._generate_all(messages_list))

        # Add outputs to table
        output_array = pa.array(outputs, type=pa.string())
        new_table = table.append_column(self._config.output_field, output_array)

        return SplitPayload(
            data=new_table,
            split_id=f"{split.split_id}_{self.worker_id}",
        )

    def _extract_messages(self, table: pa.Table) -> list[list[dict]]:
        """Extract chat messages from table (text-only mode)."""
        if self._config.messages_field not in table.column_names:
            raise ValueError(
                f"Messages field '{self._config.messages_field}' not found. "
                f"Available: {table.column_names}"
            )
        return table[self._config.messages_field].to_pylist()

    def _extract_vision_messages(self, table: pa.Table) -> list[list[dict]]:
        """Extract and build vision messages from table."""
        num_rows = table.num_rows

        # Get prompts: either from field or use fixed prompt
        if self._config.prompt_field:
            if self._config.prompt_field not in table.column_names:
                raise ValueError(
                    f"Prompt field '{self._config.prompt_field}' not found. "
                    f"Available: {table.column_names}"
                )
            prompts = table[self._config.prompt_field].to_pylist()
        elif self._config.prompt:
            prompts = [self._config.prompt] * num_rows
        else:
            raise ValueError("Either prompt or prompt_field must be set for vision mode")
        messages_list = []

        # Multi-image mode
        if self._config.images_field:
            if self._config.images_field not in table.column_names:
                raise ValueError(f"Images field '{self._config.images_field}' not found.")
            images_list = table[self._config.images_field].to_pylist()

            for prompt, images in zip(prompts, images_list):
                messages_list.append(self._build_multi_image_message(prompt, images or []))

        # Single image mode
        else:
            images = self._get_single_images(table, len(prompts))
            image_urls = self._get_image_urls(table, len(prompts))

            for prompt, image, url in zip(prompts, images, image_urls):
                messages_list.append(self._build_single_image_message(prompt, image, url))

        return messages_list

    def _get_single_images(self, table: pa.Table, count: int) -> list[Optional[Union[str, bytes]]]:
        """Get single images from table."""
        if self._config.image_field and self._config.image_field in table.column_names:
            return table[self._config.image_field].to_pylist()
        return [None] * count

    def _get_image_urls(self, table: pa.Table, count: int) -> list[Optional[str]]:
        """Get image URLs from table."""
        if self._config.image_url_field and self._config.image_url_field in table.column_names:
            return table[self._config.image_url_field].to_pylist()
        return [None] * count

    def _build_single_image_message(
        self,
        prompt: str,
        image: Optional[Union[str, bytes]],
        image_url: Optional[str],
    ) -> list[dict]:
        """Build message with single image."""
        content: list[dict] = []

        # Add image
        if image_url:
            content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": image_url, "detail": self._config.detail},
                }
            )
        elif image:
            image_b64 = base64.b64encode(image).decode() if isinstance(image, bytes) else image
            content.append(
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{image_b64}",
                        "detail": self._config.detail,
                    },
                }
            )

        # Add text
        content.append({"type": "text", "text": prompt})

        return [{"role": "user", "content": content}]

    def _build_multi_image_message(
        self, prompt: str, images: list[Union[str, bytes]]
    ) -> list[dict]:
        """Build message with multiple images."""
        content: list[dict] = []

        for image in images:
            image_b64 = base64.b64encode(image).decode() if isinstance(image, bytes) else image
            content.append(
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{image_b64}",
                        "detail": self._config.detail,
                    },
                }
            )

        content.append({"type": "text", "text": prompt})

        return [{"role": "user", "content": content}]

    async def _generate_all(self, messages_list: list[list[dict]]) -> list[str]:
        """Generate responses for all message lists."""
        batch_size = self._config.batch_size
        results: list[str] = []

        for i in range(0, len(messages_list), batch_size):
            batch = messages_list[i : i + batch_size]
            tasks = [self._generate_one(messages) for messages in batch]
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)

        return results

    async def _generate_one(self, messages: list[dict]) -> str:
        """Generate response for a single message list."""
        endpoint = f"{self._config.base_url}/v1/chat/completions"

        request_body: dict[str, Any] = {
            "messages": messages,
            "max_tokens": self._config.max_tokens,
            "temperature": self._config.temperature,
            "top_p": self._config.top_p,
        }

        if self._config.model:
            request_body["model"] = self._config.model

        try:
            response = await self._request("POST", endpoint, json=request_body)
            return response["choices"][0]["message"]["content"]
        except Exception as e:
            self.logger.error(f"Failed to generate response: {e}")
            return f"[ERROR: {str(e)}]"


# Set operator_class after definition
LLMOperatorConfig.operator_class = LLMOperator
