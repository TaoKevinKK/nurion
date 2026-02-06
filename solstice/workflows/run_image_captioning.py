#!/usr/bin/env python3
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

"""Run image captioning workflow with embedded vLLM inference.

This script uses EmbeddedLLMOperator to run vLLM directly inside Solstice workers,
eliminating HTTP overhead for maximum throughput.

Usage:
    # Submit as Ray Job:
    ray job submit --address http://localhost:8265 \
        --runtime-env-json "$(cat runtime_env.json)" \
        --working-dir . \
        -- python workflows/run_image_captioning.py

Debug:
    # Get Ray cluster pods:
    kubectl get po -l ray.io/cluster=<cluster-name>
    # Login to pod for logs:
    kubectl exec -it <pod-name> -- bash
"""

import argparse
import asyncio
import logging

import ray

# Structured captioning prompt for vision-language models
CAPTION_PROMPT = """You are an expert visual analyst and creative reconstructor. I will provide you with a single image. Your task is not just to describe it, but to reverse-engineer how the image was conceived — starting from its broad conceptual essence, then progressively fleshing out every layer of detail until you've reconstructed the full visual narrative.

Clearly present your analysis in a structured, machine-readable output. in Markdown format.

Finally, present a JSON structured caption that describes the image in a way that is easy to understand and use to regenerate the image via a text-to-image model or design tool.

Output Requirements:

Use clear section headers.
Include ALL relevant visual details — no assumptions, no omissions.
If uncertain about a detail, state "unclear" or "ambiguous" — don't hallucinate.
Ensure the final output could be used to regenerate the image via a text-to-image model or design tool."""

# Default sampling parameters
SAMPLING_PARAMS = {
    "temperature": 0.7,
    "top_p": 0.8,
    "max_tokens": 3072,
}

# Default model configuration (HuggingFace Hub model ID)
DEFAULT_MODEL_SOURCE = "Qwen/Qwen3-VL-32B-Instruct"


async def run_workflow(
    input_path: str,
    output_path: str,
    model_source: str,
    tensor_parallel_size: int,
    max_model_len: int,
    gpu_memory_utilization: float,
    image_field: str,
    split_size: int,
) -> None:
    """Run workflow with embedded vLLM inference."""
    from solstice.core.job import Job, JobConfig
    from solstice.core.stage import Stage
    from solstice.operators.llm import EmbeddedLLMOperatorConfig
    from solstice.operators.sinks import LanceSinkConfig
    from solstice.operators.sources import LanceTableSourceConfig

    logger = logging.getLogger(__name__)

    logger.info("Starting image captioning workflow with embedded vLLM...")
    logger.info(f"  Model: {model_source}")
    logger.info(f"  TP: {tensor_parallel_size}, max_model_len: {max_model_len}")
    logger.info(f"  GPU memory utilization: {gpu_memory_utilization}")

    # Create job
    job = Job(
        job_id="image_captioning",
        config=JobConfig(workqueue_db_path="memory://"),
    )

    # Source stage
    source_stage = Stage(
        stage_id="source",
        operator_config=LanceTableSourceConfig(
            dataset_uri=input_path,
            split_size=split_size,
        ),
        parallelism=1,
    )

    # Caption stage with embedded vLLM
    # Ray schedules the worker to a node with enough GPUs, vLLM uses multiprocessing internally
    caption_stage = Stage(
        stage_id="caption",
        operator_config=EmbeddedLLMOperatorConfig(
            backend="vllm",
            model=model_source,
            tensor_parallel_size=tensor_parallel_size,
            max_model_len=max_model_len,
            gpu_memory_utilization=gpu_memory_utilization,
            quantization="fp8",
            trust_remote_code=True,
            # KV cache optimization
            kv_cache_dtype="fp8_e4m3",
            vllm_enable_chunked_prefill=True,
            # Use multiprocessing (mp) executor within the actor, NOT Ray
            vllm_distributed_executor_backend="mp",
            # Generation parameters
            prompt=CAPTION_PROMPT,
            image_field=image_field,
            temperature=SAMPLING_PARAMS["temperature"],
            top_p=SAMPLING_PARAMS["top_p"],
            max_tokens=SAMPLING_PARAMS["max_tokens"],
            output_field="caption",
        ),
        parallelism=4,  # 4 workers x 4 GPUs/worker = 16 GPUs total
        # Request GPUs for this worker - Ray will schedule to a node with enough GPUs
        worker_resources={"num_gpus": tensor_parallel_size},
    )

    # Sink stage
    sink_stage = Stage(
        stage_id="sink",
        operator_config=LanceSinkConfig(
            table_path=output_path,
            mode="overwrite",
            buffer_size=100,
        ),
        parallelism=1,
    )

    job.add_stage(source_stage)
    job.add_stage(caption_stage, upstream_stages=["source"])
    job.add_stage(sink_stage, upstream_stages=["caption"])

    logger.info("Running captioning workflow...")
    logger.info(f"  Input: {input_path}")
    logger.info(f"  Output: {output_path}")

    runner = job.create_ray_runner()
    await runner.run()

    logger.info("Workflow completed!")


def main():
    parser = argparse.ArgumentParser(description="Run image captioning workflow")

    # Input/Output
    parser.add_argument(
        "--input",
        required=True,
        help="Input Lance table path (e.g. s3://bucket/input.lance)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output Lance table path (e.g. s3://bucket/output.lance)",
    )
    parser.add_argument(
        "--image-field",
        default="image_res_2048",
        help="Column name containing image bytes",
    )

    # Model configuration
    parser.add_argument(
        "--model-source",
        default=DEFAULT_MODEL_SOURCE,
        help="Path to model (S3 or local)",
    )

    # vLLM configuration
    parser.add_argument(
        "--tensor-parallel-size",
        type=int,
        default=4,
        help="Tensor parallel size (number of GPUs per model instance)",
    )
    parser.add_argument(
        "--max-model-len",
        type=int,
        default=32768,
        help="Maximum context length",
    )
    parser.add_argument(
        "--gpu-memory-utilization",
        type=float,
        default=0.9,
        help="Fraction of GPU memory to use",
    )

    # Source configuration
    parser.add_argument(
        "--split-size",
        type=int,
        default=10,
        help="Number of rows per split (batch size for inference)",
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Initialize Ray
    if not ray.is_initialized():
        ray.init(address="auto")
    logging.info(f"Connected to Ray cluster: {ray.cluster_resources()}")

    # Run workflow
    asyncio.run(
        run_workflow(
            input_path=args.input,
            output_path=args.output,
            model_source=args.model_source,
            tensor_parallel_size=args.tensor_parallel_size,
            max_model_len=args.max_model_len,
            gpu_memory_utilization=args.gpu_memory_utilization,
            image_field=args.image_field,
            split_size=args.split_size,
        )
    )


if __name__ == "__main__":
    main()
