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

"""Run image captioning workflow with external vLLM server.

This script demonstrates the new architecture:
1. Start vLLM inference server via nurion.serve (ModelServiceManager)
2. Run Nurion Runtime workflow using ExternalLLMOperator to call the server

Benefits over embedded mode:
- Independent scaling of inference servers
- Server can be shared across multiple workflows
- Easier debugging (server logs separate from workflow logs)
- Supports autoscaling based on load

Usage:
    # Submit as Ray Job:
    ray job submit --address http://localhost:8265 \
        --runtime-env-json "$(cat runtime_env.json)" \
        --working-dir . \
        -- python workflows/run_image_captioning_external.py

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


async def start_inference_server(
    model_id: str,
    model_source: str,
    tensor_parallel_size: int,
    max_model_len: int,
    gpu_memory_utilization: float,
    min_workers: int,
    max_workers: int,
):
    """Start vLLM inference server via _internal.serve.

    Returns:
        ModelServiceManager instance. Caller MUST hold this reference —
        it keeps the registry actor alive (non-detached, reference-counted).
    """
    from _internal.serve import (
        ModelConfig,
        ModelServiceManager,
    )

    logger = logging.getLogger(__name__)

    logger.info(f"Starting inference server for model: {model_id}")
    logger.info(f"  Model source: {model_source}")
    logger.info(f"  TP size: {tensor_parallel_size}, max_model_len: {max_model_len}")
    logger.info(f"  Workers: {min_workers} - {max_workers}")

    config = ModelConfig(
        model_id=model_id,
        model_source=model_source,
        backend="vllm",
        tensor_parallel_size=tensor_parallel_size,
        max_model_len=max_model_len,
        gpu_memory_utilization=gpu_memory_utilization,
        quantization="fp8",
        trust_remote_code=True,
        min_workers=min_workers,
        max_workers=max_workers,
        worker_resources={"num_gpus": tensor_parallel_size},
        extra_engine_kwargs={
            "kv_cache_dtype": "fp8_e4m3",
            "enable_chunked_prefill": True,
            "distributed_executor_backend": "mp",
        },
    )

    manager = ModelServiceManager()
    result = await manager.deploy_model(config, wait_ready=True)

    logger.info(f"Inference server ready: {result}")

    return manager


async def run_workflow(
    input_path: str,
    output_path: str,
    model_id: str,
    registry: ray.actor.ActorHandle,
    image_field: str,
    split_size: int,
) -> None:
    """Run captioning workflow using external LLM server."""
    from nurion import Job, JobConfig, LanceSinkConfig, LanceTableSourceConfig, Stage
    from _internal.operators.llm import ExternalLLMOperatorConfig

    logger = logging.getLogger(__name__)

    # Create job
    job = Job(
        job_id="image_captioning_external",
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

    # Caption stage using ExternalLLMOperator with ModelClient
    # ModelClient handles endpoint discovery and load balancing automatically
    caption_stage = Stage(
        stage_id="caption",
        operator_config=ExternalLLMOperatorConfig(
            use_model_client=True,
            registry=registry,
            model=model_id,
            # Generation parameters
            temperature=SAMPLING_PARAMS["temperature"],
            top_p=SAMPLING_PARAMS["top_p"],
            max_tokens=SAMPLING_PARAMS["max_tokens"],
            # Vision mode: fixed prompt + image field
            prompt=CAPTION_PROMPT,
            image_field=image_field,
            detail="high",
            # Output
            output_field="caption",
            batch_size=16,  # Concurrent requests to server
        ),
        parallelism=8,  # Multiple workers calling the server concurrently
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
    logger.info(f"  Model ID: {model_id} (using ModelClient for load balancing)")

    runner = job.create_ray_runner()
    await runner.run()

    logger.info("Workflow completed!")


async def main_async(args: argparse.Namespace) -> None:
    """Async main function.

    Actors are non-detached and reference-counted. The `manager` variable
    holds the registry ActorHandle, keeping it alive for the entire workflow.
    When the job exits, Ray automatically kills all actors and releases GPUs.
    """
    logger = logging.getLogger(__name__)

    # Step 1: Start inference server
    logger.info("=" * 60)
    logger.info("STEP 1: Starting vLLM inference server")
    logger.info("=" * 60)

    # IMPORTANT: keep `manager` alive — it holds the registry ActorHandle
    manager = await start_inference_server(
        model_id=args.model_id,
        model_source=args.model_source,
        tensor_parallel_size=args.tensor_parallel_size,
        max_model_len=args.max_model_len,
        gpu_memory_utilization=args.gpu_memory_utilization,
        min_workers=args.min_workers,
        max_workers=args.max_workers,
    )

    # Step 2: Run workflow
    logger.info("=" * 60)
    logger.info("STEP 2: Running captioning workflow")
    logger.info("=" * 60)

    await run_workflow(
        input_path=args.input,
        output_path=args.output,
        model_id=args.model_id,
        registry=manager.registry,
        image_field=args.image_field,
        split_size=args.split_size,
    )

    # manager stays alive until here, keeping registry + pools alive
    logger.info("Workflow done, shutting down serve layer...")
    await manager.shutdown()


def main():
    parser = argparse.ArgumentParser(
        description="Run image captioning workflow with external vLLM server"
    )

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
        "--model-id",
        default=DEFAULT_MODEL_SOURCE,
        help="Model identifier — used as both serve model_id and vLLM served_model_name",
    )
    parser.add_argument(
        "--model-source",
        default=DEFAULT_MODEL_SOURCE,
        help="HuggingFace model ID or path (defaults to model-id)",
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

    # Scaling configuration
    parser.add_argument(
        "--min-workers",
        type=int,
        default=2,
        help="Minimum number of inference workers",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of inference workers (for autoscaling)",
    )

    # Source configuration
    parser.add_argument(
        "--split-size",
        type=int,
        default=10,
        help="Number of rows per split (batch size for workflow)",
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
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
