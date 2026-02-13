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

"""Tests for length-based model routing.

Unit tests for ModelRoutingConfig validation, token estimation, model picking,
and RoutedChatCompletionsClient logic. No HTTP/Ray needed.
"""

from __future__ import annotations

import math
from unittest.mock import AsyncMock, MagicMock

import pytest

from _internal.operators.llm.client import (
    ContextLengthError,
    ModelRoute,
    ModelRoutingConfig,
    RoutedChatCompletionsClient,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_routed_client(
    routing_config: ModelRoutingConfig,
) -> RoutedChatCompletionsClient:
    """Create a RoutedChatCompletionsClient with a mock base client."""
    base = MagicMock()
    return RoutedChatCompletionsClient(base, routing_config)


# ---------------------------------------------------------------------------
# ModelRoutingConfig validation
# ---------------------------------------------------------------------------


class TestModelRoutingConfigValidation:
    def test_empty_routes_raises(self) -> None:
        with pytest.raises(ValueError, match="at least one route"):
            ModelRoutingConfig(routes=[])

    def test_single_route_valid(self) -> None:
        cfg = ModelRoutingConfig(routes=[ModelRoute("m1", 8192)])
        assert len(cfg.routes) == 1

    def test_non_increasing_max_tokens_raises(self) -> None:
        with pytest.raises(ValueError, match="strictly increasing"):
            ModelRoutingConfig(
                routes=[
                    ModelRoute("small", 8192),
                    ModelRoute("large", 4096),  # 4096 < 8192 → invalid
                ]
            )

    def test_equal_max_tokens_raises(self) -> None:
        with pytest.raises(ValueError, match="strictly increasing"):
            ModelRoutingConfig(
                routes=[
                    ModelRoute("a", 8192),
                    ModelRoute("b", 8192),  # equal → invalid
                ]
            )

    def test_valid_two_routes(self) -> None:
        cfg = ModelRoutingConfig(
            routes=[
                ModelRoute("small", 4096),
                ModelRoute("large", 32768),
            ]
        )
        assert cfg.routes[0].model_id == "small"
        assert cfg.routes[1].model_id == "large"


# ---------------------------------------------------------------------------
# Token estimation
# ---------------------------------------------------------------------------


class TestTokenEstimation:
    def _routing_config(self) -> ModelRoutingConfig:
        return ModelRoutingConfig(
            routes=[ModelRoute("m1", 8192)],
            chars_per_token=4.0,
            tokens_per_image=1000,
        )

    def test_text_only(self) -> None:
        rc = _make_routed_client(self._routing_config())
        messages = [{"role": "user", "content": "a" * 400}]
        tokens = rc.estimate_tokens(messages)
        # 400 chars / 4.0 chars_per_token = 100
        assert tokens == 100

    def test_image_content_block(self) -> None:
        rc = _make_routed_client(self._routing_config())
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "a" * 80},
                    {"type": "image_url", "image_url": {"url": "data:image/png;..."}},
                ],
            }
        ]
        tokens = rc.estimate_tokens(messages)
        # 80/4 + 1000 = 20 + 1000 = 1020
        assert tokens == 1020

    def test_multiple_messages(self) -> None:
        rc = _make_routed_client(self._routing_config())
        messages = [
            {"role": "system", "content": "a" * 40},
            {"role": "user", "content": "b" * 80},
        ]
        tokens = rc.estimate_tokens(messages)
        # 40/4 + 80/4 = 10 + 20 = 30
        assert tokens == 30

    def test_empty_messages(self) -> None:
        rc = _make_routed_client(self._routing_config())
        tokens = rc.estimate_tokens([])
        assert tokens == 0

    def test_multiple_images(self) -> None:
        rc = _make_routed_client(self._routing_config())
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": "img1"}},
                    {"type": "image_url", "image_url": {"url": "img2"}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
        tokens = rc.estimate_tokens(messages)
        # 2 * 1000 + len("describe")/4 = 2000 + 2 = 2002
        assert tokens == 2002


# ---------------------------------------------------------------------------
# Model picking
# ---------------------------------------------------------------------------


class TestModelPicking:
    def _routing_config(self) -> ModelRoutingConfig:
        return ModelRoutingConfig(
            routes=[
                ModelRoute("small-ctx", 4096),
                ModelRoute("medium-ctx", 16384),
                ModelRoute("large-ctx", 65536),
            ],
        )

    def _pick(self, rc: RoutedChatCompletionsClient, estimated_tokens: int) -> str:
        """Pick model by constructing messages with the right estimated length."""
        # chars_per_token defaults to 3.5; ceil to avoid int() truncation
        text_len = math.ceil(estimated_tokens * 3.5)
        messages = [{"role": "user", "content": "a" * text_len}]
        return rc.pick_model(messages)

    def test_fits_first_route(self) -> None:
        rc = _make_routed_client(self._routing_config())
        assert self._pick(rc, 100) == "small-ctx"

    def test_fits_exact_boundary(self) -> None:
        rc = _make_routed_client(self._routing_config())
        assert self._pick(rc, 4096) == "small-ctx"

    def test_fits_second_route(self) -> None:
        rc = _make_routed_client(self._routing_config())
        assert self._pick(rc, 4097) == "medium-ctx"

    def test_fits_last_route(self) -> None:
        rc = _make_routed_client(self._routing_config())
        assert self._pick(rc, 16385) == "large-ctx"

    def test_overflow_to_largest(self) -> None:
        rc = _make_routed_client(self._routing_config())
        assert self._pick(rc, 100000) == "large-ctx"

    def test_single_route_always_matches(self) -> None:
        cfg = ModelRoutingConfig(routes=[ModelRoute("only", 8192)])
        rc = _make_routed_client(cfg)
        assert self._pick(rc, 1) == "only"
        assert self._pick(rc, 8192) == "only"


# ---------------------------------------------------------------------------
# Next-model fallback
# ---------------------------------------------------------------------------


class TestNextModel:
    def _routing_config(self) -> ModelRoutingConfig:
        return ModelRoutingConfig(
            routes=[
                ModelRoute("small-ctx", 4096),
                ModelRoute("medium-ctx", 16384),
                ModelRoute("large-ctx", 65536),
            ],
        )

    def test_returns_next_larger(self) -> None:
        rc = _make_routed_client(self._routing_config())
        assert rc.next_model("small-ctx") == "medium-ctx"
        assert rc.next_model("medium-ctx") == "large-ctx"

    def test_largest_returns_none(self) -> None:
        rc = _make_routed_client(self._routing_config())
        assert rc.next_model("large-ctx") is None

    def test_unknown_model_returns_none(self) -> None:
        rc = _make_routed_client(self._routing_config())
        assert rc.next_model("nonexistent") is None


# ---------------------------------------------------------------------------
# Generate with routing + fallback
# ---------------------------------------------------------------------------


class TestRoutedGenerate:
    def _routing_config(self) -> ModelRoutingConfig:
        return ModelRoutingConfig(
            routes=[
                ModelRoute("small-ctx", 4096),
                ModelRoute("large-ctx", 65536),
            ],
            chars_per_token=4.0,
        )

    @pytest.mark.asyncio
    async def test_auto_picks_model(self) -> None:
        """Non-route model_id triggers automatic model selection."""
        base = MagicMock()
        base.generate = AsyncMock(return_value="ok")
        rc = RoutedChatCompletionsClient(base, self._routing_config())

        # Short message → should pick small-ctx
        body = {"messages": [{"role": "user", "content": "a" * 40}], "model": "default"}
        result = await rc.generate("default", body)

        assert result == "ok"
        call_args = base.generate.call_args
        assert call_args[0][0] == "small-ctx"
        assert call_args[0][1]["model"] == "small-ctx"

    @pytest.mark.asyncio
    async def test_explicit_route_model_skips_picking(self) -> None:
        """When model_id IS a route model, use it directly without re-picking."""
        base = MagicMock()
        base.generate = AsyncMock(return_value="ok")
        rc = RoutedChatCompletionsClient(base, self._routing_config())

        body = {"messages": [{"role": "user", "content": "hi"}]}
        result = await rc.generate("large-ctx", body)

        assert result == "ok"
        # Should use large-ctx as-is, not re-pick
        assert base.generate.call_args[0][0] == "large-ctx"

    @pytest.mark.asyncio
    async def test_fallback_on_context_length_error(self) -> None:
        """ContextLengthError on small model falls back to large model."""
        base = MagicMock()
        base.generate = AsyncMock(side_effect=[ContextLengthError("too long"), "fallback ok"])
        rc = RoutedChatCompletionsClient(base, self._routing_config())

        body = {"messages": [{"role": "user", "content": "hi"}], "model": "small-ctx"}
        result = await rc.generate("small-ctx", body)

        assert result == "fallback ok"
        assert base.generate.call_count == 2
        # Second call should be with large-ctx
        assert base.generate.call_args[0][0] == "large-ctx"

    @pytest.mark.asyncio
    async def test_raises_when_largest_model_fails(self) -> None:
        """ContextLengthError on the largest model propagates to caller."""
        base = MagicMock()
        base.generate = AsyncMock(side_effect=ContextLengthError("too long"))
        rc = RoutedChatCompletionsClient(base, self._routing_config())

        body = {"messages": [{"role": "user", "content": "hi"}], "model": "large-ctx"}
        with pytest.raises(ContextLengthError):
            await rc.generate("large-ctx", body)


# ---------------------------------------------------------------------------
# ContextLengthError
# ---------------------------------------------------------------------------


class TestContextLengthError:
    def test_is_exception(self) -> None:
        err = ContextLengthError("maximum context length exceeded")
        assert isinstance(err, Exception)

    def test_message_preserved(self) -> None:
        msg = "This model's maximum context length is 8192 tokens"
        err = ContextLengthError(msg)
        assert str(err) == msg
