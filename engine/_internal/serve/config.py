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

"""Configuration classes for Multi-Model Inference Service."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal, Optional


class WorkerState(Enum):
    """Worker lifecycle states."""

    STARTING = "starting"  # Process starting
    LOADING = "loading"  # Model loading
    READY = "ready"  # Ready to serve
    BUSY = "busy"  # Processing requests
    DRAINING = "draining"  # Waiting for pending requests to complete
    STOPPED = "stopped"  # Stopped


@dataclass
class ModelConfig:
    """Configuration for a model deployment.

    Attributes:
        model_id: Unique identifier for the model (e.g., "decision", "generation")
        model_source: Model path or HuggingFace ID (e.g., "Qwen/Qwen2.5-7B-Instruct")
        backend: Inference backend ("vllm" or "sglang")
        tensor_parallel_size: Number of GPUs per worker for tensor parallelism

        min_workers: Minimum number of workers to maintain
        max_workers: Maximum number of workers allowed

        scale_up_pending_threshold: Scale up when pending > threshold * ready_workers
        scale_down_idle_seconds: Scale down after idle for this many seconds
        scale_cooldown_seconds: Cooldown period between scaling operations

        max_model_len: Maximum context length
        gpu_memory_utilization: Fraction of GPU memory to use (vLLM)
        quantization: Quantization method (e.g., "awq", "gptq", "fp8")
        dtype: Model data type ("bfloat16", "float16", "auto")
        trust_remote_code: Whether to trust remote code from HuggingFace

        worker_resources: Ray worker resource requirements (e.g., {"num_gpus": 4, "num_cpus": 8})
            If not specified, defaults to {"num_gpus": tensor_parallel_size}

        extra_engine_kwargs: Additional kwargs passed to vLLM/SGLang engine

    Usage:
        config = ModelConfig(
            model_id="decision",
            model_source="Qwen/Qwen2.5-7B-Instruct",
            tensor_parallel_size=1,
            min_workers=2,
            max_workers=8,
        )

        # With custom resources
        config = ModelConfig(
            model_id="generation",
            model_source="Qwen/Qwen2.5-72B-Instruct",
            tensor_parallel_size=8,
            worker_resources={"num_gpus": 8, "num_cpus": 16, "memory": 64 * 1024**3},
        )
    """

    # Model identification
    model_id: str
    model_source: str

    # Backend selection
    backend: Literal["vllm", "sglang"] = "vllm"
    tensor_parallel_size: int = 1

    # Scaling configuration
    min_workers: int = 1
    max_workers: int = 4

    # Scaling thresholds
    scale_up_pending_threshold: int = 10
    scale_down_idle_seconds: float = 60.0
    scale_cooldown_seconds: float = 30.0

    # Engine configuration
    max_model_len: int = 8192
    gpu_memory_utilization: float = 0.9
    quantization: Optional[str] = None
    dtype: str = "auto"
    trust_remote_code: bool = True

    # Worker resource requirements (passed to ray.remote().options())
    worker_resources: Optional[dict[str, Any]] = None

    # Extra engine kwargs
    extra_engine_kwargs: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate configuration."""
        if not self.model_id:
            raise ValueError("model_id must be specified")
        if not self.model_source:
            raise ValueError("model_source must be specified")
        if self.min_workers < 1:
            raise ValueError("min_workers must be >= 1")
        if self.max_workers < self.min_workers:
            raise ValueError("max_workers must be >= min_workers")
        if self.tensor_parallel_size < 1:
            raise ValueError("tensor_parallel_size must be >= 1")

    def get_worker_resources(self) -> dict[str, Any]:
        """Get Ray worker resource requirements.

        Returns worker_resources if specified, otherwise defaults to
        {"num_gpus": tensor_parallel_size}.
        """
        if self.worker_resources:
            return self.worker_resources
        return {"num_gpus": self.tensor_parallel_size}

    def to_engine_kwargs(self) -> dict[str, Any]:
        """Convert config to vLLM/SGLang engine kwargs."""
        if self.backend == "vllm":
            kwargs: dict[str, Any] = {
                "model": self.model_source,
                "tensor_parallel_size": self.tensor_parallel_size,
                "max_model_len": self.max_model_len,
                "gpu_memory_utilization": self.gpu_memory_utilization,
                "dtype": self.dtype,
                "trust_remote_code": self.trust_remote_code,
            }
            if self.quantization:
                kwargs["quantization"] = self.quantization
        else:  # sglang
            kwargs = {
                "model_path": self.model_source,
                "tp_size": self.tensor_parallel_size,
                "trust_remote_code": self.trust_remote_code,
            }
            if self.quantization:
                kwargs["quantization"] = self.quantization

        kwargs.update(self.extra_engine_kwargs)
        return kwargs


@dataclass
class AutoscaleConfig:
    """Configuration for autoscaling behavior.

    Attributes:
        enabled: Whether autoscaling is enabled
        check_interval_seconds: How often to check for scaling decisions
        scale_up_pending_threshold: Scale up when pending > threshold * ready_workers
        scale_down_idle_seconds: Scale down after idle for this many seconds
        cooldown_seconds: Cooldown period between scaling operations
        max_scale_step: Maximum workers to add/remove per scaling decision
    """

    enabled: bool = True
    check_interval_seconds: float = 5.0
    scale_up_pending_threshold: int = 10
    scale_down_idle_seconds: float = 60.0
    cooldown_seconds: float = 30.0
    max_scale_step: int = 2
