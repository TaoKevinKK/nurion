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

"""Fake OpenAI-compatible server for testing.

Lightweight aiohttp server that mimics vLLM's OpenAI API. Used by
InferenceWorker when ``backend="fake"`` — starts instantly without GPU
or model loading, so integration tests exercise the real worker lifecycle
(subprocess management, health polling, registry registration, heartbeat).

Usage:
    python -m _internal.serve.fake_server --port 8000 --model-id test_model

Routes:
    GET  /health                  -> {"status": "ok"}
    GET  /metrics                 -> Prometheus-format text
    POST /v1/chat/completions     -> OpenAI-compatible chat completion
    POST /internal/set_metrics    -> Update pending/running counters
"""

from __future__ import annotations

import argparse
import os

from aiohttp import web

# Mutable metrics (updated via /internal/set_metrics)
_pending = 0
_running = 0

# Populated from CLI args
_model_id = "fake_model"
_tp_size = 1


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def handle_metrics(request: web.Request) -> web.Response:
    text = (
        "# HELP vllm:num_requests_waiting Number of requests waiting\n"
        "# TYPE vllm:num_requests_waiting gauge\n"
        f"vllm:num_requests_waiting {_pending}\n"
        "# HELP vllm:num_requests_running Number of requests running\n"
        "# TYPE vllm:num_requests_running gauge\n"
        f"vllm:num_requests_running {_running}\n"
    )
    return web.Response(text=text, content_type="text/plain")


async def handle_chat_completions(request: web.Request) -> web.Response:
    data = await request.json()
    model = data.get("model", _model_id)
    worker_id = f"{_model_id}_worker_{request.app['port']}"
    response = {
        "id": f"chatcmpl-fake-{worker_id}",
        "object": "chat.completion",
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": f"Fake response from {worker_id} (TP={_tp_size})",
                },
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30},
    }
    return web.json_response(response)


async def handle_set_metrics(request: web.Request) -> web.Response:
    global _pending, _running
    data = await request.json()
    _pending = data.get("pending", _pending)
    _running = data.get("running", _running)
    return web.json_response({"pending": _pending, "running": _running})


def main() -> None:
    global _model_id, _tp_size

    parser = argparse.ArgumentParser(description="Fake vLLM-compatible server")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--model-id", type=str, default="fake_model")
    parser.add_argument("--tp-size", type=int, default=1)
    args = parser.parse_args()

    _model_id = args.model_id
    _tp_size = args.tp_size

    # Clear proxy env vars so aiohttp binds correctly on loopback
    for key in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"):
        os.environ.pop(key, None)

    app = web.Application()
    app["port"] = args.port
    app.router.add_get("/health", handle_health)
    app.router.add_get("/metrics", handle_metrics)
    app.router.add_post("/v1/chat/completions", handle_chat_completions)
    app.router.add_post("/internal/set_metrics", handle_set_metrics)

    web.run_app(app, host="0.0.0.0", port=args.port, print=None)


if __name__ == "__main__":
    main()
