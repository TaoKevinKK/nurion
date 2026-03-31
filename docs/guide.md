# Nurion Engine User Guide

This guide covers how to develop data processing workflows with Nurion Engine, including basic pipeline construction, embedded LLM offline inference, and multi-model serve mode.

---

## Table of Contents

1. [Workflow Development](#1-workflow-development)
2. [Embedded LLM Offline Inference](#2-embedded-llm-offline-inference)
3. [Multi-Model Serve Mode](#3-multi-model-serve-mode)
   - [Attached Mode](#31-attached-mode-default)
   - [Detached Mode](#32-detached-mode)

---

## 1. Workflow Development

### 1.1 Core Concepts

Nurion Engine is a Ray-based unified streaming/batch data processing framework. The core abstractions are:

| Concept | Description |
|---------|-------------|
| **Job** | A DAG pipeline containing multiple Stages |
| **Stage** | A processing step wrapping an Operator, with parallelism and resource settings |
| **Operator** | The unit of data processing logic, processes data via `process_split()` |
| **OperatorConfig** | Immutable configuration class that drives Operator behavior |
| **Split / SplitPayload** | Split metadata and actual data payload (backed by PyArrow Table) |

### 1.2 Data Flow Model

Nurion uses a **pull-based, queue-driven** data flow model:

```
Source Stage ──push──> [Queue] ──pull──> Transform Stage ──push──> [Queue] ──pull──> Sink Stage
```

- Each Stage's workers pull messages from the upstream Stage's output queue
- After processing, workers push results to their own Stage's output queue
- Natural backpressure via queue lag

### 1.3 Basic Pipeline: ETL Example

A minimal pipeline has three parts: Source (read data) -> Transform (process data) -> Sink (write data).

```python
import asyncio

from nurion import (
    FileSinkConfig,
    FilterOperatorConfig,
    Job,
    JobConfig,
    LanceTableSourceConfig,
    MapOperatorConfig,
    Stage,
)


def transform_record(record: dict) -> dict:
    """Transform each record."""
    record["processed"] = True
    if "value" in record:
        record["value_doubled"] = record["value"] * 2
    return record


def filter_predicate(record: dict) -> bool:
    """Filter: keep only records where value > 10."""
    return record.get("value", 0) > 10


async def main():
    # 1. Create a Job
    job = Job(
        job_id="simple_etl",
        config=JobConfig(anvil_db_path="memory://"),
    )

    # 2. Source Stage - read from a Lance table
    job.add_stage(Stage(
        stage_id="source",
        operator_config=LanceTableSourceConfig(
            dataset_uri="s3://bucket/input.lance",
            split_size=1000,  # 1000 rows per split
        ),
        parallelism=1,
    ))

    # 3. Map Stage - transform records (auto-scale between 2 and 8 workers)
    job.add_stage(
        Stage(
            stage_id="transform",
            operator_config=MapOperatorConfig(map_fn=transform_record),
            parallelism=(2, 8),  # (min_workers, max_workers)
        ),
        upstream_stages=["source"],
    )

    # 4. Filter Stage - filter records
    job.add_stage(
        Stage(
            stage_id="filter",
            operator_config=FilterOperatorConfig(filter_fn=filter_predicate),
            parallelism=2,
        ),
        upstream_stages=["transform"],
    )

    # 5. Sink Stage - write to file
    job.add_stage(
        Stage(
            stage_id="sink",
            operator_config=FileSinkConfig(
                output_path="s3://bucket/output.json",
                format="json",
            ),
            parallelism=1,
        ),
        upstream_stages=["filter"],
    )

    # 6. Run
    runner = job.create_ray_runner()
    await runner.run()


asyncio.run(main())
```

### 1.4 Stage Configuration

```python
Stage(
    stage_id="my_stage",             # Unique stage identifier
    operator_config=MyConfig(...),   # Operator configuration
    parallelism=4,                   # Fixed 4 workers
    # parallelism=(2, 10),           # Or auto-scale: min=2, max=10
    worker_resources={               # Resource requirements per worker
        "num_cpus": 1,
        "num_gpus": 0,
        "memory": 2 * 1024**3,      # 2 GB
    },
    batch_size=100,                  # Messages claimed per batch
    backpressure_threshold_lag=5000, # Backpressure activation threshold
)
```

**Parallelism parameter:**

- `int`: Fixed number of workers, no auto-scaling
- `(min, max)` tuple: Auto-scaling, worker count adjusts dynamically between min and max

### 1.5 Custom Operators

When built-in operators like `MapOperatorConfig` / `FilterOperatorConfig` are not sufficient, write a custom Operator:

```python
from dataclasses import dataclass
from typing import Optional

from nurion import Operator, OperatorConfig, OperatorRuntime, Split, SplitPayload, operator


@dataclass
class MyOperatorConfig(OperatorConfig):
    """Custom operator configuration."""
    multiplier: int = 2
    output_column: str = "result"


@operator(MyOperatorConfig)
class MyOperator(Operator):
    """Custom operator implementation."""

    def __init__(self, config: MyOperatorConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self._my_config = config

    def process_split(
        self,
        split: Split,
        payload: Optional[SplitPayload] = None,
    ) -> Optional[SplitPayload]:
        if payload is None:
            return None

        # Get the Arrow Table for processing
        table = payload.to_table()

        # Example: compute on a column
        import pyarrow.compute as pc
        values = table.column("value")
        result = pc.multiply(values, self._my_config.multiplier)
        new_table = table.append_column(self._my_config.output_column, result)

        return SplitPayload(
            data=new_table,
            split_id=f"{split.split_id}_{self.worker_id}",
        )
```

**Key points:**

- Define a config class with `@dataclass`, inheriting from `OperatorConfig`
- Use the `@operator(MyOperatorConfig)` decorator to bind Config to Operator
- Implement processing logic in `process_split()`
- Data flows in and out as `SplitPayload` (wrapping a PyArrow Table)
- Return `None` to drop the split (filter behavior)
- Async and generator return types are also supported (one-to-many)

### 1.6 Built-in Operators

| Operator | Purpose | Config Class |
|----------|---------|-------------|
| Lance Source | Read from Lance table | `LanceTableSourceConfig` |
| Iceberg Source | Read from Iceberg table | `IcebergSourceConfig` |
| File Source | Read from files | `FileSourceConfig` |
| Spark Source | Read via Spark SQL | `SparkSourceConfig` |
| Map | Per-record transform | `MapOperatorConfig` |
| FlatMap | Per-record transform (one-to-many) | `FlatMapOperatorConfig` |
| MapBatches | Batch transform | `MapBatchesOperatorConfig` |
| Filter | Filter records | `FilterOperatorConfig` |
| File Sink | Write to file (JSON/Parquet/CSV) | `FileSinkConfig` |
| Lance Sink | Write to Lance table | `LanceSinkConfig` |
| Print Sink | Print to stdout | `PrintSinkConfig` |
| Embedded LLM | In-process LLM inference | `EmbeddedLLMOperatorConfig` |
| External LLM | Call external LLM service | `ExternalLLMOperatorConfig` |

### 1.7 Submitting a Job to a Ray Cluster

```bash
# Submit a workflow as a Ray Job
ray job submit --address http://localhost:8265 \
    --runtime-env-json "$(cat runtime_env.json)" \
    --working-dir . \
    -- python workflows/my_workflow.py
```

`runtime_env.json` defines runtime dependencies (pip packages, environment variables, etc.).

---

## 2. Embedded LLM Offline Inference

Embedded mode runs the vLLM/SGLang inference engine directly inside Nurion worker processes, with **zero HTTP overhead**. This is ideal for large-scale offline batch processing.

### 2.1 Architecture

```
Lance Source ──> [Queue] ──> EmbeddedLLMOperator(vLLM engine) ──> [Queue] ──> Lance Sink
                             ^ Each worker embeds a full vLLM engine instance
                             ^ Ray schedules workers to nodes with enough GPUs
```

**Characteristics:**

- Each worker is a Ray Actor holding a complete vLLM/SGLang engine
- The engine is lazily initialized on the first `process_split()` call
- vLLM uses multiprocessing (mp) internally for tensor parallelism, not nested Ray
- `worker_resources={"num_gpus": N}` tells Ray to schedule the worker on a node with N GPUs

### 2.2 Full Example: Image Captioning

```python
import asyncio
import logging

import ray

from nurion import Job, JobConfig, LanceSinkConfig, LanceTableSourceConfig, Stage
from _internal.operators.llm import EmbeddedLLMOperatorConfig

CAPTION_PROMPT = "Describe this image in detail."


async def run():
    logging.basicConfig(level=logging.INFO)

    # Connect to Ray cluster
    if not ray.is_initialized():
        ray.init(address="auto")

    job = Job(
        job_id="image_captioning",
        config=JobConfig(anvil_db_path="memory://"),
    )

    # Source: read images from a Lance table
    job.add_stage(Stage(
        stage_id="source",
        operator_config=LanceTableSourceConfig(
            dataset_uri="s3://my-bucket/images.lance",
            split_size=10,  # 10 images per split
        ),
        parallelism=1,
    ))

    # Caption: embedded vLLM inference
    job.add_stage(
        Stage(
            stage_id="caption",
            operator_config=EmbeddedLLMOperatorConfig(
                backend="vllm",
                model="Qwen/Qwen3-VL-32B-Instruct",
                tensor_parallel_size=4,
                max_model_len=32768,
                gpu_memory_utilization=0.9,
                quantization="fp8",
                trust_remote_code=True,
                # KV cache optimization
                kv_cache_dtype="fp8_e4m3",
                vllm_enable_chunked_prefill=True,
                # Use multiprocessing for TP within the actor, NOT Ray
                vllm_distributed_executor_backend="mp",
                # Generation parameters
                prompt=CAPTION_PROMPT,
                image_field="image",         # Column containing image bytes
                output_field="caption",      # Output column name
                temperature=0.7,
                top_p=0.8,
                max_tokens=3072,
            ),
            parallelism=4,  # 4 workers x 4 GPUs/worker = 16 GPUs total
            worker_resources={"num_gpus": 4},  # Each worker needs 4 GPUs
        ),
        upstream_stages=["source"],
    )

    # Sink: write to Lance table
    job.add_stage(
        Stage(
            stage_id="sink",
            operator_config=LanceSinkConfig(
                table_path="s3://my-bucket/captioned.lance",
                mode="overwrite",
            ),
            parallelism=1,
        ),
        upstream_stages=["caption"],
    )

    runner = job.create_ray_runner()
    await runner.run()


asyncio.run(run())
```

### 2.3 EmbeddedLLMOperatorConfig Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `backend` | Inference backend: `"vllm"` or `"sglang"` | `"vllm"` |
| `model` | Model path or HuggingFace ID | Required |
| `tensor_parallel_size` | TP degree (GPUs per engine instance) | `1` |
| `max_model_len` | Maximum context length | `8192` |
| `gpu_memory_utilization` | GPU memory fraction (vLLM only) | `0.9` |
| `quantization` | Quantization method: `"fp8"`, `"awq"`, `"gptq"`, etc. | `None` |
| `trust_remote_code` | Trust remote code from HuggingFace | `True` |
| `kv_cache_dtype` | KV cache data type: `"fp8_e4m3"`, `"fp8_e5m2"` | `None` (auto) |
| `vllm_enable_chunked_prefill` | Enable chunked prefill (long prompt optimization) | `False` |
| `vllm_distributed_executor_backend` | Distributed executor backend: `"mp"` or `"ray"` | `None` |

**Input modes (choose one):**

| Mode | Configuration | Description |
|------|---------------|-------------|
| Text chat | `messages_field="messages"` | Column containing chat message lists |
| Single-image VLM | `prompt="..." + image_field="image"` | Fixed prompt + image column |
| Multi-image VLM | `prompt="..." + images_field="images"` | Fixed prompt + image list column |

You can also use `prompt_field` instead of `prompt` to specify a different prompt per row.

### 2.4 Using the SGLang Backend

Simply change `backend` to `"sglang"` and use SGLang-specific parameters:

```python
EmbeddedLLMOperatorConfig(
    backend="sglang",
    model="Qwen/Qwen3-VL-32B-Instruct",
    tensor_parallel_size=4,
    trust_remote_code=True,
    # SGLang-specific parameters
    kv_cache_dtype="fp8_e5m2",
    sglang_mem_fraction_static=0.85,
    sglang_attention_backend="fa3",
    # Generation parameters
    prompt="Describe this image.",
    image_field="image",
    max_tokens=3072,
)
```

### 2.5 Submitting to a Cluster

```bash
ray job submit --address http://localhost:8265 \
    --runtime-env-json "$(cat runtime_env.json)" \
    --working-dir . \
    -- python workflows/run_image_captioning.py \
        --input s3://bucket/images.lance \
        --output s3://bucket/captioned.lance \
        --tensor-parallel-size 4 \
        --split-size 10
```

---

## 3. Multi-Model Serve Mode

Use Serve mode when you need to deploy multiple models, scale them independently, or share inference services across multiple workflows.

### Architecture Overview

```
+-----------------------------------------------------------+
|                  ModelServiceManager                      |
|                                                           |
|   +--------------+   +--------------+                     |
|   |  ModelPool A  |   |  ModelPool B  |   ...              |
|   |  (model_id)  |   |  (model_id)  |                     |
|   | +----------+ |   | +----------+ |                     |
|   | | Worker 1 | |   | | Worker 1 | |                     |
|   | | (vLLM)   | |   | | (vLLM)   | |                     |
|   | +----------+ |   | +----------+ |                     |
|   | | Worker 2 | |   | | Worker 2 | |                     |
|   | | (vLLM)   | |   | | (vLLM)   | |                     |
|   | +----------+ |   | +----------+ |                     |
|   +------+-------+   +------+-------+                     |
|          |                  |                              |
|          v                  v                              |
|   +--------------------------------------+                 |
|   |         ModelRegistry                |                 |
|   |  (HTTP service discovery + routing)  |                 |
|   +--------------------------------------+                 |
+-----------------------------------------------------------+
                          |
                   HTTP (OpenAI API)
                          |
            +-------------+-------------+
            v             v             v
   +--------------+ +--------------+ +--------------+
   |   Nurion      | |   Nurion      | | Any OpenAI   |
   |   Pipeline    | |   Pipeline    | | compatible   |
   |   Worker      | |   Worker      | | client       |
   +--------------+ +--------------+ +--------------+
```

**Core components:**

| Component | Description |
|-----------|-------------|
| `ModelServiceManager` | Control plane: deploy, scale, undeploy models |
| `ModelConfig` | Model deployment configuration (model source, TP size, worker count, etc.) |
| `ModelPool` | Manages all InferenceWorkers for a single model |
| `InferenceWorker` | Ray Actor running a vLLM/SGLang HTTP server |
| `ModelRegistry` | Service discovery registry (embedded HTTP server) |
| `ModelClient` | Client-side service discovery with caching (used by pipeline workers) |

**Two lifecycle modes:**

| Mode | Lifecycle | Use Case |
|------|-----------|----------|
| **Attached** (default) | Actors die when the job exits | One-off tasks, single workflow runs |
| **Detached** | Actors survive job exit | Long-running services, cross-job reuse |

### 3.1 Attached Mode (Default)

In attached mode, all actors (Registry, Pool, Worker) are reference-counted and automatically destroyed when the job exits, releasing all GPUs.

**Best for:** Single workflow runs that don't need to share models across jobs.

```python
import asyncio
import logging

import ray

from nurion import (
    Job, JobConfig, LanceSinkConfig, LanceTableSourceConfig,
    ModelConfig, ModelServiceManager, Stage,
)
from _internal.operators.llm import ExternalLLMOperatorConfig


async def main():
    logging.basicConfig(level=logging.INFO)
    ray.init(address="auto")

    # ====== Step 1: Deploy model ======
    # Attached mode (default): actors die with the job
    manager = ModelServiceManager()

    # Deploy model
    await manager.deploy_model(
        ModelConfig(
            model_id="caption_vlm",
            model_source="Qwen/Qwen3-VL-32B-Instruct",
            backend="vllm",
            tensor_parallel_size=4,
            max_model_len=32768,
            gpu_memory_utilization=0.9,
            quantization="fp8",
            trust_remote_code=True,
            min_workers=2,
            max_workers=4,
            worker_resources={"num_gpus": 4},
            extra_engine_kwargs={
                "kv_cache_dtype": "fp8_e4m3",
                "enable_chunked_prefill": True,
                "distributed_executor_backend": "mp",
            },
        ),
        wait_ready=True,  # Wait for at least one worker to be ready
    )

    # ====== Step 2: Run pipeline ======
    job = Job(
        job_id="captioning",
        config=JobConfig(anvil_db_path="memory://"),
    )

    job.add_stage(Stage(
        stage_id="source",
        operator_config=LanceTableSourceConfig(
            dataset_uri="s3://bucket/images.lance",
            split_size=10,
        ),
        parallelism=1,
    ))

    job.add_stage(
        Stage(
            stage_id="caption",
            operator_config=ExternalLLMOperatorConfig(
                use_model_client=True,
                registry=manager.registry,  # Pass registry for service discovery
                model="caption_vlm",        # Must match ModelConfig.model_id
                prompt="Describe this image in detail.",
                image_field="image",
                detail="high",
                output_field="caption",
                max_tokens=3072,
                temperature=0.7,
                batch_size=16,  # Concurrent requests per worker
            ),
            parallelism=8,  # Multiple workers calling the model service concurrently
        ),
        upstream_stages=["source"],
    )

    job.add_stage(
        Stage(
            stage_id="sink",
            operator_config=LanceSinkConfig(
                table_path="s3://bucket/captioned.lance",
                mode="overwrite",
            ),
            parallelism=1,
        ),
        upstream_stages=["caption"],
    )

    runner = job.create_ray_runner()
    await runner.run()

    # ====== Step 3: Shutdown ======
    # In attached mode, explicit shutdown releases GPUs faster.
    # Even without calling shutdown(), actors are cleaned up when the job exits.
    await manager.shutdown()


asyncio.run(main())
```

**Key points:**

1. `ModelServiceManager()` defaults to attached mode
2. `manager.registry` is passed as an `ActorHandle` to `ExternalLLMOperatorConfig`
3. `ExternalLLMOperator` internally uses `ModelClient` for automatic endpoint discovery and round-robin load balancing
4. After the workflow completes, all actors are automatically destroyed -- no GPU leaks

### 3.2 Detached Mode

In detached mode, actors are created with `lifetime="detached"` and survive job exit. This is ideal when:

- Model loading is slow (large models can take 10-30 minutes to load from S3) and you want to load once, use many times
- Multiple workflows share the same inference services
- Models need to stay online, ready to accept new tasks at any time

**Detached mode has a three-step lifecycle:**

#### Step 1: Deploy Models (first run, slow)

```python
import asyncio
import ray
from nurion import ModelConfig, ModelServiceManager


async def deploy():
    ray.init(address="auto")

    # detached=True: actors survive job exit
    manager = ModelServiceManager(detached=True)

    # Deploy multiple models
    await manager.deploy_model(
        ModelConfig(
            model_id="ocr_model",
            model_source="deepseek-ai/DeepSeek-OCR",
            tensor_parallel_size=1,
            min_workers=1,
            max_workers=2,
            worker_resources={"num_gpus": 1},
        ),
        wait_ready=True,
    )

    await manager.deploy_model(
        ModelConfig(
            model_id="fusion_model",
            model_source="Qwen/Qwen3-VL-235B-A22B-Instruct",
            tensor_parallel_size=8,
            max_model_len=32768,
            quantization="fp8",
            min_workers=2,
            max_workers=4,
            worker_resources={"num_gpus": 8},
            extra_engine_kwargs={
                "kv_cache_dtype": "fp8_e4m3",
                "enable_chunked_prefill": True,
                "distributed_executor_backend": "mp",
            },
        ),
        wait_ready=True,
        timeout=1800.0,  # Large models may need longer to load
    )

    print("Models deployed and running (detached). Job can exit safely.")
    # Do NOT call manager.shutdown() -- keep models running


asyncio.run(deploy())
```

#### Step 2: Connect to Existing Models + Run Pipeline (subsequent runs, instant startup)

```python
import asyncio
import ray
from nurion import (
    Job, JobConfig, LanceSinkConfig, LanceTableSourceConfig,
    ModelServiceManager, Stage,
)
from _internal.operators.llm import ExternalLLMOperatorConfig


async def run_pipeline():
    ray.init(address="auto")

    # Connect to existing detached models
    manager = ModelServiceManager.connect()
    models = manager.list_models()
    print(f"Connected to {len(models)} model(s): {models}")

    # Build and run pipeline
    job = Job(
        job_id="ocr_pipeline",
        config=JobConfig(anvil_db_path="memory://"),
    )

    job.add_stage(Stage(
        stage_id="source",
        operator_config=LanceTableSourceConfig(
            dataset_uri="s3://bucket/images.lance",
            split_size=10,
        ),
        parallelism=1,
    ))

    job.add_stage(
        Stage(
            stage_id="ocr",
            operator_config=ExternalLLMOperatorConfig(
                use_model_client=True,
                registry=manager.registry,
                model="ocr_model",
                prompt="OCR this image.",
                image_field="image",
                output_field="ocr_result",
                max_tokens=8192,
                temperature=0.0,
            ),
            parallelism=4,
        ),
        upstream_stages=["source"],
    )

    job.add_stage(
        Stage(
            stage_id="sink",
            operator_config=LanceSinkConfig(
                table_path="s3://bucket/ocr_output.lance",
                mode="overwrite",
            ),
            parallelism=1,
        ),
        upstream_stages=["ocr"],
    )

    runner = job.create_ray_runner()
    await runner.run()

    # Do NOT shutdown -- models keep running for the next job
    print("Pipeline done. Models still running (detached).")


asyncio.run(run_pipeline())
```

#### Step 3: Tear Down Models (when no longer needed)

```python
import asyncio
import ray
from nurion import ModelServiceManager


async def teardown():
    ray.init(address="auto")

    manager = ModelServiceManager.connect()
    await manager.shutdown()  # Kill all actors, release GPUs

    print("All models shut down, GPUs released.")


asyncio.run(teardown())
```

### 3.3 ModelConfig Parameters

```python
ModelConfig(
    model_id="my_model",                  # Unique ID for service discovery
    model_source="Qwen/Qwen3-VL-32B",    # HuggingFace ID or local path
    backend="vllm",                        # "vllm" or "sglang"
    tensor_parallel_size=4,                # TP degree
    min_workers=1,                         # Minimum worker count
    max_workers=4,                         # Maximum worker count
    max_model_len=8192,                    # Maximum context length
    gpu_memory_utilization=0.9,            # GPU memory utilization
    quantization="fp8",                    # Quantization method
    dtype="auto",                          # Data type
    trust_remote_code=True,                # Trust remote code
    worker_resources={"num_gpus": 4},      # Resource requirements per worker
    extra_engine_kwargs={                  # Additional engine arguments
        "kv_cache_dtype": "fp8_e4m3",
        "enable_chunked_prefill": True,
    },
)
```

### 3.4 Autoscaling

The serve layer has built-in autoscaling, controlled via `AutoscaleConfig`:

```python
from nurion import ModelServiceManager
from _internal.serve.config import AutoscaleConfig

manager = ModelServiceManager(
    autoscale_config=AutoscaleConfig(
        enabled=True,
        check_interval_seconds=5.0,     # Check every 5 seconds
        scale_up_threshold=10,          # Scale up when inflight (pending+running) > 10 * ready_workers
        scale_down_idle_seconds=60.0,   # Scale down after 60s idle
        cooldown_seconds=30.0,          # Minimum interval between scaling ops
        max_scale_step=2,               # Max workers to add/remove per decision
    ),
)
```

Manual scaling is also available:

```python
# Scale to a specific number of workers
await manager.scale_model("my_model", target=6)

# Freeze autoscaling (useful for manual debugging)
await manager.freeze_model("my_model")

# Resume autoscaling
await manager.unfreeze_model("my_model")
```

### 3.5 Multi-Model Deployment Example

The following example deploys 3 models that work together in a single pipeline (OCR + fusion scenario):

```python
import asyncio
import ray
from nurion import ModelConfig, ModelServiceManager


async def deploy_multi_model():
    ray.init(address="auto")

    manager = ModelServiceManager(detached=True)

    # Model 1: Small OCR model (7B, TP=1, single GPU)
    await manager.deploy_model(
        ModelConfig(
            model_id="deepseek_ocr",
            model_source="deepseek-ai/DeepSeek-OCR",
            tensor_parallel_size=1,
            max_model_len=4096,
            min_workers=1,
            max_workers=2,
            worker_resources={"num_gpus": 1},
            extra_engine_kwargs={"enforce_eager": True},
        ),
        wait_ready=True,
    )

    # Model 2: Small OCR model (1B, TP=1, single GPU)
    await manager.deploy_model(
        ModelConfig(
            model_id="hunyuan_ocr",
            model_source="tencent/HunyuanOCR",
            tensor_parallel_size=1,
            max_model_len=4096,
            min_workers=1,
            max_workers=2,
            worker_resources={"num_gpus": 1},
        ),
        wait_ready=True,
    )

    # Model 3: Large fusion model (235B, TP=8, 8 GPUs, FP8 quantization)
    await manager.deploy_model(
        ModelConfig(
            model_id="qwen3_vl_fusion",
            model_source="Qwen/Qwen3-VL-235B-A22B-Instruct",
            tensor_parallel_size=8,
            max_model_len=32768,
            quantization="fp8",
            min_workers=2,
            max_workers=4,
            worker_resources={"num_gpus": 8},
            extra_engine_kwargs={
                "kv_cache_dtype": "fp8_e4m3",
                "enable_chunked_prefill": True,
                "distributed_executor_backend": "mp",
            },
        ),
        wait_ready=True,
        timeout=1800.0,
    )

    print(f"Deployed {len(manager.list_models())} models: {manager.list_models()}")


asyncio.run(deploy_multi_model())
```

In your pipeline, use a custom Operator to call multiple models via `ModelClient` for service discovery:

```python
from dataclasses import dataclass
from typing import Optional

import httpx
import ray

from nurion import (
    ModelClient, Operator, OperatorConfig, OperatorRuntime,
    Split, SplitPayload, operator,
)


@dataclass
class MultiModelConfig(OperatorConfig):
    registry: Optional[ray.actor.ActorHandle] = None
    image_field: str = "image"


@operator(MultiModelConfig)
class MultiModelOperator(Operator):

    def __init__(self, config: MultiModelConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self._client = ModelClient(registry=config.registry)

    async def process_split(self, split, payload):
        if payload is None:
            return None

        table = payload.to_table()

        # Discover endpoints for each model
        ocr_endpoints = await self._client.get_endpoints("deepseek_ocr")
        fusion_endpoints = await self._client.get_endpoints("qwen3_vl_fusion")

        # Call OCR model
        # ... use httpx to POST to ocr_endpoints[0] + "/v1/chat/completions"

        # Call fusion model
        # ... use httpx to POST to fusion_endpoints[0] + "/v1/chat/completions"

        return SplitPayload(data=result_table, split_id=split.split_id)
```

### 3.6 Attached vs Detached: Choosing the Right Mode

| Dimension | Attached | Detached |
|-----------|----------|----------|
| Actor lifecycle | Destroyed automatically when job exits | Must call `shutdown()` manually |
| Startup speed | Model loaded every run | Load once, instant reconnect |
| Resource management | Auto-released, no leak risk | Manual management; forgetting `shutdown()` leaks GPUs |
| Use case | One-off tasks, CI/CD pipelines | Long-running services, multi-job reuse, interactive dev |
| Multi-job sharing | Not supported | Supported via `connect()` |
| Code complexity | Simple | Must manage deploy / connect / shutdown lifecycle |

**Rules of thumb:**

- Development & debugging -> Attached (simple, auto-cleanup on exit)
- Production batch processing -> Detached (avoid reloading large models)
- Single workflow -> Attached
- Multiple workflows sharing models -> Detached

---

## Appendix

### A. Submitting a Ray Job

```bash
# Basic submission
ray job submit --address http://localhost:8265 \
    --runtime-env-json "$(cat runtime_env.json)" \
    --working-dir . \
    -- python workflows/my_workflow.py --input ... --output ...
```

### B. Example runtime_env.json

```json
{
  "working_dir": ".",
  "excludes": ["tests/", ".git/", "*.lance", "__pycache__/"],
  "pip": [
    "pylance>=1.0.1",
    "pyarrow>=18.0.0",
    "s3fs>=2024.6.0",
    "httpx",
    "aiohttp",
    "vllm==0.15.1",
    "pillow",
    "nurion-anvil"
  ],
  "env_vars": {
    "VLLM_WORKER_MULTIPROC_METHOD": "spawn",
    "HF_HOME": "/fsx/huggingface_cache"
  }
}
```

### C. Debugging Tips

```bash
# Check Ray cluster resources
python -c "import ray; ray.init(address='auto'); print(ray.cluster_resources())"

# Open Ray Dashboard
# Navigate to http://localhost:8265

# List pods on Kubernetes
kubectl get po -l ray.io/cluster=<cluster-name>

# View worker logs
kubectl exec -it <pod-name> -- bash
```
