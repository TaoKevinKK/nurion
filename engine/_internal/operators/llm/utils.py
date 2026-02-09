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

"""Common utility functions for LLM operators.

Pure functions for extracting and validating data from PyArrow tables.
These are shared between EmbeddedLLMOperator and ExternalLLMOperator.
"""

from __future__ import annotations

import base64
from typing import Any, Optional, Union

import pyarrow as pa


def extract_prompts(
    table: pa.Table,
    prompt_field: str,
    fixed_prompt: str,
) -> list[str]:
    """Extract prompts from table or use fixed prompt.

    Args:
        table: PyArrow table containing data
        prompt_field: Column name containing per-row prompts (empty to use fixed)
        fixed_prompt: Fixed prompt to use for all rows (if prompt_field is empty)

    Returns:
        List of prompts, one per row

    Raises:
        ValueError: If neither prompt_field nor fixed_prompt is set, or field not found
    """
    if prompt_field:
        if prompt_field not in table.column_names:
            raise ValueError(
                f"Prompt field '{prompt_field}' not found. Available: {table.column_names}"
            )
        return table[prompt_field].to_pylist()
    elif fixed_prompt:
        return [fixed_prompt] * table.num_rows
    else:
        raise ValueError("Either prompt_field or fixed_prompt must be set")


def extract_messages(table: pa.Table, messages_field: str) -> list[list[dict]]:
    """Extract chat messages from table.

    Args:
        table: PyArrow table containing data
        messages_field: Column name containing chat messages

    Returns:
        List of message lists (OpenAI chat format)

    Raises:
        ValueError: If messages_field not found in table
    """
    if messages_field not in table.column_names:
        raise ValueError(
            f"Messages field '{messages_field}' not found. Available: {table.column_names}"
        )
    return table[messages_field].to_pylist()


def extract_column(
    table: pa.Table,
    field_name: str,
    default_count: int,
) -> list[Any]:
    """Extract column values from table, or return list of Nones.

    Args:
        table: PyArrow table containing data
        field_name: Column name to extract (empty string returns Nones)
        default_count: Number of None values to return if field is empty/not found

    Returns:
        List of column values, or list of Nones
    """
    if field_name and field_name in table.column_names:
        return table[field_name].to_pylist()
    return [None] * default_count


def extract_images(table: pa.Table, image_field: str) -> list[Any]:
    """Extract images from table.

    Args:
        table: PyArrow table containing data
        image_field: Column name containing images

    Returns:
        List of images

    Raises:
        ValueError: If image_field not found in table
    """
    if image_field not in table.column_names:
        raise ValueError(f"Image field '{image_field}' not found. Available: {table.column_names}")
    return table[image_field].to_pylist()


def encode_image_base64(image: Union[str, bytes]) -> str:
    """Encode image to base64 string.

    Args:
        image: Image bytes or already base64-encoded string

    Returns:
        Base64-encoded string
    """
    if isinstance(image, bytes):
        return base64.b64encode(image).decode()
    return image


def build_openai_image_content(
    image: Union[str, bytes],
    detail: str = "auto",
) -> dict[str, Any]:
    """Build OpenAI-compatible image content block.

    Args:
        image: Image bytes or base64 string
        detail: Image detail level ("auto", "low", "high")

    Returns:
        OpenAI image_url content block
    """
    image_b64 = encode_image_base64(image)
    return {
        "type": "image_url",
        "image_url": {
            "url": f"data:image/jpeg;base64,{image_b64}",
            "detail": detail,
        },
    }


def build_openai_image_url_content(
    url: str,
    detail: str = "auto",
) -> dict[str, Any]:
    """Build OpenAI-compatible image URL content block.

    Args:
        url: Image URL
        detail: Image detail level ("auto", "low", "high")

    Returns:
        OpenAI image_url content block
    """
    return {
        "type": "image_url",
        "image_url": {"url": url, "detail": detail},
    }


def build_openai_text_content(text: str) -> dict[str, str]:
    """Build OpenAI-compatible text content block.

    Args:
        text: Text content

    Returns:
        OpenAI text content block
    """
    return {"type": "text", "text": text}


def build_single_image_message(
    prompt: str,
    image: Optional[Union[str, bytes]],
    image_url: Optional[str],
    detail: str = "auto",
) -> list[dict]:
    """Build OpenAI-compatible message with single image.

    Args:
        prompt: Text prompt
        image: Image bytes or base64 string (optional)
        image_url: Image URL (optional, used if image is None)
        detail: Image detail level

    Returns:
        List with single user message containing image + text
    """
    content: list[dict] = []

    if image_url:
        content.append(build_openai_image_url_content(image_url, detail))
    elif image:
        content.append(build_openai_image_content(image, detail))

    content.append(build_openai_text_content(prompt))

    return [{"role": "user", "content": content}]


def build_multi_image_message(
    prompt: str,
    images: list[Union[str, bytes]],
    detail: str = "auto",
) -> list[dict]:
    """Build OpenAI-compatible message with multiple images.

    Args:
        prompt: Text prompt
        images: List of image bytes or base64 strings
        detail: Image detail level

    Returns:
        List with single user message containing images + text
    """
    content: list[dict] = []

    for image in images:
        content.append(build_openai_image_content(image, detail))

    content.append(build_openai_text_content(prompt))

    return [{"role": "user", "content": content}]
