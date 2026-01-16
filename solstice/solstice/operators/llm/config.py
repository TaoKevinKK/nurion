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

"""Configuration classes for LLM inference components."""

from dataclasses import dataclass, field


@dataclass
class RouterConfig:
    """Configuration for SGLang Router.

    Attributes:
        host: Host to bind the router
        port: Port to bind (0 = auto-assign)
        policy: Load balancing policy
        health_check_interval: Seconds between health checks
        health_check_timeout: Timeout for health check requests
    """

    host: str = "0.0.0.0"
    port: int = 0  # 0 = auto-assign
    policy: str = "cache_aware"  # round_robin, random, cache_aware

    # Health check settings
    health_check_interval: float = 10.0
    health_check_timeout: float = 5.0

    # Startup settings
    startup_timeout: float = 60.0  # Max time to wait for router ready


@dataclass
class WorkerConfig:
    """Configuration for SGLang Worker (inference server).

    Attributes:
        model_path: Path or HuggingFace model ID
        tensor_parallel_size: Number of GPUs for tensor parallelism
        gpu_memory_utilization: Fraction of GPU memory to use
        max_model_len: Maximum sequence length (0 = auto)
        host: Host to bind the worker server
        port: Port to bind (0 = auto-assign)
        additional_args: Additional command line arguments for sglang
    """

    model_path: str = ""
    tensor_parallel_size: int = 1
    gpu_memory_utilization: float = 0.9
    max_model_len: int = 0  # 0 = auto

    # Server settings
    host: str = "0.0.0.0"
    port: int = 0  # 0 = auto-assign

    # Additional CLI arguments for SGLang server
    additional_args: list[str] = field(default_factory=list)

    # Startup settings
    startup_timeout: float = 600.0  # Model loading can be slow
