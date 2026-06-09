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

"""Tests for InferenceWorker command builders and metric parsing.

These exercise pure logic — no subprocess is spawned, no Ray needed. We
construct ``InferenceWorker`` via ``__new__`` to skip the real
``__init__`` (which would launch a server).
"""

from __future__ import annotations

from _internal.serve.config import ModelConfig
from _internal.serve.worker import InferenceWorker


def _make_bare_worker(config: ModelConfig) -> InferenceWorker:
    worker = InferenceWorker.__new__(InferenceWorker)
    worker._config = config
    worker._host = "0.0.0.0"
    worker._port = 8001
    worker._worker_id = "test_worker"
    return worker


class TestSglangCommand:
    def test_required_flags_present(self) -> None:
        config = ModelConfig(
            model_id="m",
            model_source="/models/foo",
            backend="sglang",
            tensor_parallel_size=2,
            max_model_len=4096,
            gpu_memory_utilization=0.85,
            dtype="bfloat16",
        )
        cmd = _make_bare_worker(config)._build_sglang_command()

        assert "sglang.launch_server" in cmd
        assert cmd[cmd.index("--model-path") + 1] == "/models/foo"
        assert cmd[cmd.index("--tp-size") + 1] == "2"
        assert cmd[cmd.index("--context-length") + 1] == "4096"
        assert cmd[cmd.index("--mem-fraction-static") + 1] == "0.85"
        assert cmd[cmd.index("--dtype") + 1] == "bfloat16"
        assert "--enable-metrics" in cmd

    def test_quantization_and_trust_remote_code(self) -> None:
        config = ModelConfig(
            model_id="m",
            model_source="/models/foo",
            backend="sglang",
            quantization="awq",
            trust_remote_code=False,
        )
        cmd = _make_bare_worker(config)._build_sglang_command()

        assert cmd[cmd.index("--quantization") + 1] == "awq"
        assert "--trust-remote-code" not in cmd

    def test_extra_engine_kwargs_passthrough(self) -> None:
        config = ModelConfig(
            model_id="m",
            model_source="/models/foo",
            backend="sglang",
            extra_engine_kwargs={
                "attention_backend": "fa3",
                "disable_radix_cache": True,
                "skip_tokenizer_init": False,
                "schedule_conservativeness": 0.3,
            },
        )
        cmd = _make_bare_worker(config)._build_sglang_command()

        assert cmd[cmd.index("--attention-backend") + 1] == "fa3"
        assert "--disable-radix-cache" in cmd
        # SGLang has no --no-* form; False bools are simply omitted.
        assert "--skip-tokenizer-init" not in cmd
        assert cmd[cmd.index("--schedule-conservativeness") + 1] == "0.3"


class TestPrometheusMetricParsing:
    def test_sglang_metrics_map_to_pending_running(self) -> None:
        worker = InferenceWorker.__new__(InferenceWorker)
        text = (
            "# TYPE sglang:num_queue_reqs gauge\n"
            "sglang:num_queue_reqs 7.0\n"
            "# TYPE sglang:num_running_reqs gauge\n"
            "sglang:num_running_reqs 3.0\n"
        )
        metrics = worker._parse_prometheus_metrics(text)
        assert metrics["pending"] == 7
        assert metrics["running"] == 3

    def test_vllm_metrics_still_work(self) -> None:
        worker = InferenceWorker.__new__(InferenceWorker)
        text = (
            "# TYPE vllm:num_requests_waiting gauge\n"
            "vllm:num_requests_waiting 5.0\n"
            "# TYPE vllm:num_requests_running gauge\n"
            "vllm:num_requests_running 2.0\n"
        )
        metrics = worker._parse_prometheus_metrics(text)
        assert metrics["pending"] == 5
        assert metrics["running"] == 2
