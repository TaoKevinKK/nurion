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

"""Embedded LLM Operator using vLLM/SGLang offline batch inference.

This module provides high-throughput LLM inference by directly embedding
the inference engine inside Solstice workers, eliminating HTTP overhead.

Key advantages over HTTP-based inference:
- Zero HTTP/serialization overhead
- Direct memory access via Ray Object Store
- Natural backpressure via Solstice's pull-based architecture
- Continuous batching handled by vLLM/SGLang engine

Supported backends:
- vLLM: https://docs.vllm.ai/en/latest/serving/offline_inference.html
- SGLang: https://docs.sglang.io/basic_usage/offline_engine_api.html
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal, Optional, Type

import pyarrow as pa

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import Operator, OperatorConfig, OperatorRuntime, operator
from solstice.operators.llm.utils import (
    extract_images,
    extract_messages,
    extract_prompts,
)


@dataclass
class EmbeddedLLMOperatorConfig(OperatorConfig):
    """Configuration for embedded LLM inference using vLLM or SGLang offline API.

    This operator embeds the inference engine directly in the worker,
    providing maximum throughput for batch processing scenarios.

    Note: This is for OFFLINE BATCH processing. PD disaggregation (prefill-decode
    separation) is NOT supported in offline mode - it's an online serving optimization.
    For batch processing, use KV cache quantization and chunked prefill instead.

    Attributes:
        backend: Inference backend ("vllm" or "sglang")
        model: Model name or path (e.g., "Qwen/Qwen2.5-VL-72B-Instruct")
        tensor_parallel_size: Number of GPUs for tensor parallelism
        max_model_len: Maximum context length
        gpu_memory_utilization: Fraction of GPU memory to use (vLLM only)
        quantization: Quantization method (e.g., "awq", "gptq", None)
        trust_remote_code: Whether to trust remote code from HuggingFace
        kv_cache_dtype: KV cache data type ("auto", "fp8_e4m3", "fp8_e5m2", "fp16")

        # vLLM-specific
        vllm_enable_chunked_prefill: Enable chunked prefill for long prompts
        vllm_kv_offloading_size_gb: Size in GB to offload KV cache to CPU
        vllm_kv_offloading_backend: Offloading backend ("native", "lmcache")

        # SGLang-specific
        sglang_mem_fraction_static: Fraction of GPU memory for static allocation
        sglang_attention_backend: Attention backend ("fa3", "flashinfer", etc.)

        # Generation parameters
        temperature: Sampling temperature (0 = deterministic)
        top_p: Top-p (nucleus) sampling
        max_tokens: Maximum tokens to generate
        stop: Stop sequences

        # Input/output fields
        prompt: Fixed prompt for all rows (used if prompt_field not set)
        prompt_field: Column containing per-row prompts
        messages_field: Column containing chat messages (text-only mode)
        image_field: Column containing image bytes (VLM mode)
        images_field: Column containing list of images (multi-image VLM)
        output_field: Column for generated responses

    Usage:
        # Basic text inference
        config = EmbeddedLLMOperatorConfig(
            model="Qwen/Qwen2.5-72B-Instruct",
            messages_field="messages",
        )

        # VLM with KV cache optimization (vLLM)
        config = EmbeddedLLMOperatorConfig(
            backend="vllm",
            model="Qwen/Qwen2.5-VL-72B-Instruct",
            tensor_parallel_size=4,
            prompt="Describe this image.",
            image_field="image",
            kv_cache_dtype="fp8_e4m3",  # ~50% KV cache memory reduction
            vllm_enable_chunked_prefill=True,  # Handle long prompts efficiently
        )

        # SGLang with memory optimization
        config = EmbeddedLLMOperatorConfig(
            backend="sglang",
            model="Qwen/Qwen2.5-VL-72B-Instruct",
            tensor_parallel_size=4,
            prompt="Describe this image.",
            image_field="image",
            kv_cache_dtype="fp8_e5m2",
            sglang_mem_fraction_static=0.85,
            sglang_attention_backend="fa3",
        )
    """

    operator_class: ClassVar[Type["EmbeddedLLMOperator"]]

    # Backend selection
    backend: Literal["vllm", "sglang"] = "vllm"

    # Model configuration
    model: str = ""
    tensor_parallel_size: int = 1
    max_model_len: int = 8192
    gpu_memory_utilization: float = 0.9
    quantization: Optional[str] = None
    trust_remote_code: bool = True

    # --- KV Cache optimization (both backends) ---
    kv_cache_dtype: Optional[str] = None  # "auto", "fp8_e4m3", "fp8_e5m2", "fp16"

    # --- vLLM-specific ---
    vllm_enable_chunked_prefill: bool = False  # Chunked prefill for long prompts
    vllm_kv_offloading_size_gb: Optional[float] = None  # GB to offload KV cache to CPU
    vllm_kv_offloading_backend: Optional[str] = None  # "native", "lmcache"

    # --- SGLang-specific ---
    sglang_mem_fraction_static: Optional[float] = None  # Static memory fraction
    sglang_attention_backend: Optional[str] = None  # "fa3", "flashinfer", etc.

    # Generation parameters
    temperature: float = 0.7
    top_p: float = 0.95
    max_tokens: int = 1024
    stop: list[str] = field(default_factory=list)

    # Input fields
    prompt: str = ""  # Fixed prompt for all rows
    prompt_field: str = ""  # Column with per-row prompts
    messages_field: str = ""  # Column with chat messages (text-only)
    image_field: str = ""  # Column with single image bytes
    images_field: str = ""  # Column with list of images

    # Output field
    output_field: str = "response"

    def __post_init__(self) -> None:
        if not self.model:
            raise ValueError("model must be specified")

        # Validate mode configuration
        has_messages = bool(self.messages_field)
        has_prompt = bool(self.prompt or self.prompt_field)
        has_image = bool(self.image_field or self.images_field)

        if has_messages and (has_prompt or has_image):
            raise ValueError(
                "Cannot mix messages_field with prompt/image fields. "
                "Use messages_field for text-only chat, or prompt+image for VLM."
            )

        if has_image and not has_prompt:
            raise ValueError(
                "VLM mode requires prompt or prompt_field to be set along with image_field/images_field"
            )

        if not has_messages and not has_prompt:
            raise ValueError(
                "Either messages_field (text chat) or prompt/prompt_field (VLM) must be set"
            )


@operator(EmbeddedLLMOperatorConfig)
class EmbeddedLLMOperator(Operator):
    """Embedded LLM operator using vLLM or SGLang offline batch inference.

    This operator loads the model directly into the worker process,
    providing the highest possible throughput for batch inference.

    The engine is initialized lazily on first use (in setup() or first process_split).
    This allows the model to be loaded on the GPU assigned to the worker.

    Supports:
    - Text-only chat (messages_field)
    - Single image VLM (prompt/prompt_field + image_field)
    - Multi-image VLM (prompt/prompt_field + images_field)
    """

    def __init__(self, config: EmbeddedLLMOperatorConfig, runtime: OperatorRuntime) -> None:
        super().__init__(config, runtime)
        self._llm_config = config
        self._engine: Any = None
        self._sampling_params: Any = None

    def _init_engine(self) -> None:
        """Initialize vLLM or SGLang engine."""
        if self._engine is not None:
            return

        cfg = self._llm_config

        if cfg.backend == "vllm":
            self._init_vllm_engine()
        else:
            self._init_sglang_engine()

        self.logger.info(
            f"Initialized {cfg.backend} engine for model {cfg.model} "
            f"(TP={cfg.tensor_parallel_size})"
        )

    def _init_vllm_engine(self) -> None:
        """Initialize vLLM engine."""
        try:
            from vllm import LLM, SamplingParams
        except ImportError as e:
            raise ImportError(
                "vLLM is required for embedded LLM inference. Install with: pip install vllm"
            ) from e

        cfg = self._llm_config

        engine_kwargs: dict[str, Any] = {
            "model": cfg.model,
            "tensor_parallel_size": cfg.tensor_parallel_size,
            "max_model_len": cfg.max_model_len,
            "gpu_memory_utilization": cfg.gpu_memory_utilization,
            "trust_remote_code": cfg.trust_remote_code,
        }

        if cfg.quantization:
            engine_kwargs["quantization"] = cfg.quantization

        # KV Cache configuration
        if cfg.kv_cache_dtype:
            engine_kwargs["kv_cache_dtype"] = cfg.kv_cache_dtype

        if cfg.vllm_enable_chunked_prefill:
            engine_kwargs["enable_chunked_prefill"] = True

        # KV Cache offloading (CPU offload for larger effective batch)
        if cfg.vllm_kv_offloading_size_gb is not None:
            # vLLM uses bytes, convert from GB
            engine_kwargs["kv_offloading_size"] = int(cfg.vllm_kv_offloading_size_gb * 1024**3)
            if cfg.vllm_kv_offloading_backend:
                engine_kwargs["kv_offloading_backend"] = cfg.vllm_kv_offloading_backend

        self._engine = LLM(**engine_kwargs)

        self.logger.info(
            f"vLLM engine initialized: kv_cache_dtype={cfg.kv_cache_dtype}, "
            f"chunked_prefill={cfg.vllm_enable_chunked_prefill}, "
            f"kv_offload={cfg.vllm_kv_offloading_size_gb}GB"
        )

        sampling_kwargs: dict[str, Any] = {
            "temperature": cfg.temperature,
            "top_p": cfg.top_p,
            "max_tokens": cfg.max_tokens,
        }

        if cfg.stop:
            sampling_kwargs["stop"] = cfg.stop

        self._sampling_params = SamplingParams(**sampling_kwargs)

    def _init_sglang_engine(self) -> None:
        """Initialize SGLang engine."""
        try:
            import sglang as sgl
        except ImportError as e:
            raise ImportError(
                "SGLang is required for embedded LLM inference. Install with: pip install sglang"
            ) from e

        cfg = self._llm_config

        engine_kwargs: dict[str, Any] = {
            "model_path": cfg.model,
            "tp_size": cfg.tensor_parallel_size,
            "trust_remote_code": cfg.trust_remote_code,
        }

        if cfg.quantization:
            engine_kwargs["quantization"] = cfg.quantization

        # KV Cache configuration
        if cfg.kv_cache_dtype:
            engine_kwargs["kv_cache_dtype"] = cfg.kv_cache_dtype

        # Memory fraction for static allocation
        if cfg.sglang_mem_fraction_static is not None:
            engine_kwargs["mem_fraction_static"] = cfg.sglang_mem_fraction_static

        # Attention backend (affects KV cache support)
        if cfg.sglang_attention_backend:
            engine_kwargs["attention_backend"] = cfg.sglang_attention_backend

        self._engine = sgl.Engine(**engine_kwargs)

        self.logger.info(
            f"SGLang engine initialized: kv_cache_dtype={cfg.kv_cache_dtype}, "
            f"mem_fraction_static={cfg.sglang_mem_fraction_static}, "
            f"attention_backend={cfg.sglang_attention_backend}"
        )

        # SGLang uses kwargs directly in generate()
        self._sampling_params = {
            "temperature": cfg.temperature,
            "top_p": cfg.top_p,
            "max_new_tokens": cfg.max_tokens,
        }

        if cfg.stop:
            self._sampling_params["stop"] = cfg.stop

    def process_split(
        self,
        split: Split,
        payload: Optional[SplitPayload] = None,
    ) -> Optional[SplitPayload]:
        """Process a split through the LLM engine."""
        if payload is None:
            return None

        # Ensure engine is initialized
        if self._engine is None:
            self._init_engine()

        table = payload.to_table()
        cfg = self._llm_config

        # Determine mode and generate
        if cfg.messages_field:
            responses = self._generate_from_messages(table)
        elif cfg.images_field:
            responses = self._generate_from_multi_images(table)
        elif cfg.image_field:
            responses = self._generate_from_single_image(table)
        else:
            responses = self._generate_from_prompts(table)

        # Append responses to table
        response_array = pa.array(responses, type=pa.string())
        result_table = table.append_column(cfg.output_field, response_array)

        return SplitPayload(
            data=result_table,
            split_id=f"{split.split_id}_{self.worker_id}",
        )

    def _generate_from_messages(self, table: pa.Table) -> list[str]:
        """Generate responses for text-only chat messages."""
        messages_list = extract_messages(table, self._llm_config.messages_field)

        # Convert chat messages to prompts (simplified)
        prompts = []
        for messages in messages_list:
            prompt_parts = []
            for msg in messages:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                prompt_parts.append(f"{role}: {content}")
            prompts.append("\n".join(prompt_parts))

        return self._generate_text_batch(prompts)

    def _generate_from_prompts(self, table: pa.Table) -> list[str]:
        """Generate responses for text prompts (no images)."""
        cfg = self._llm_config
        prompts = extract_prompts(table, cfg.prompt_field, cfg.prompt)
        return self._generate_text_batch(prompts)

    def _generate_from_single_image(self, table: pa.Table) -> list[str]:
        """Generate responses for single image + prompt."""
        cfg = self._llm_config
        prompts = extract_prompts(table, cfg.prompt_field, cfg.prompt)
        images = extract_images(table, cfg.image_field)
        return self._generate_vlm_batch(prompts, images)

    def _generate_from_multi_images(self, table: pa.Table) -> list[str]:
        """Generate responses for multiple images + prompt."""
        cfg = self._llm_config
        prompts = extract_prompts(table, cfg.prompt_field, cfg.prompt)
        images_list = extract_images(table, cfg.images_field)
        return self._generate_vlm_batch(prompts, images_list)

    def _generate_text_batch(self, prompts: list[str]) -> list[str]:
        """Generate responses for a batch of text prompts."""
        if self._llm_config.backend == "vllm":
            outputs = self._engine.generate(prompts, self._sampling_params)
            return [output.outputs[0].text for output in outputs]
        else:
            # SGLang
            outputs = self._engine.generate(prompts, **self._sampling_params)
            return [output["text"] for output in outputs]

    def _generate_vlm_batch(
        self,
        prompts: list[str],
        images: list[Any],
    ) -> list[str]:
        """Generate responses for VLM (vision-language) inputs."""
        if self._llm_config.backend == "vllm":
            return self._generate_vlm_vllm(prompts, images)
        else:
            return self._generate_vlm_sglang(prompts, images)

    def _generate_vlm_vllm(
        self,
        prompts: list[str],
        images: list[Any],
    ) -> list[str]:
        """Generate VLM responses using vLLM."""
        inputs = []
        for prompt, image_data in zip(prompts, images):
            if image_data is None:
                inputs.append(prompt)
            else:
                inputs.append(
                    {
                        "prompt": prompt,
                        "multi_modal_data": {"image": image_data},
                    }
                )

        outputs = self._engine.generate(inputs, self._sampling_params)
        return [output.outputs[0].text for output in outputs]

    def _generate_vlm_sglang(
        self,
        prompts: list[str],
        images: list[Any],
    ) -> list[str]:
        """Generate VLM responses using SGLang."""
        outputs = self._engine.generate(
            prompts,
            images=images,
            **self._sampling_params,
        )
        return [output["text"] for output in outputs]

    def teardown(self) -> None:
        """Clean up the inference engine."""
        if self._engine is not None:
            # SGLang has explicit shutdown
            if hasattr(self._engine, "shutdown"):
                try:
                    self._engine.shutdown()
                except Exception as e:
                    self.logger.warning(f"Error shutting down engine: {e}")

            self._engine = None
            self._sampling_params = None

        super().close()
