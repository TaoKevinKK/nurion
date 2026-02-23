# Nurion Runtime

Nurion Runtime is a **high-throughput batch processing framework** with a **streaming-style execution model** and built-in multimodal operators.

It is designed for large-scale, production pipelines where:
- You want **streaming-style execution** (no stage-wide barriers, no long tails).
- You need to **fully utilise CPU + GPU** on heterogeneous workloads.
- You process **multimodal data** (video, images, embeddings, text, binary blobs) rather than only tabular data.

## Positioning

Conceptually, Nurion Runtime is a **batch processing engine**: jobs are finite DAGs processing finite input data sets.  
Implementation-wise, it uses a **streaming-style, pull-based execution model** inside the job to minimise stage barriers and long-tail latency.

Nurion Runtime focuses on **simple, elastic, and observable high-throughput pipelines**, not on being a full analytics platform.

- **Elastic workers with stateless design**:  
  - Workers are stateless Ray actors; StageMasters coordinate worker pools and manage output queues.  
  - Worker pools can scale up/down dynamically at runtime without stopping the job.  
  - Combined with backpressure, the system can find a good stage-by-stage resource mix automatically.

- **Backpressure by design**:  
  - Downstream workers pull from upstream queues, so slow consumers naturally throttle producers.  
  - The runtime uses queue lag metrics to detect bottlenecks and adapt throughput.

- **Minimal dependencies**:  
  - Runtime only requires **Ray**; WorkQueue broker is embedded (no external service).  
  - No heavy external services are required to start a pipeline.

- **Streaming-style execution model**:  
  - Stages do not wait for each other to complete; data flows continuously through the DAG.  
  - This avoids classic batch-style stage barriers and long-tail stragglers.

- **Queue-based data flow**:  
  - Stage-to-stage communication uses WorkQueue (embedded broker; `memory://` or `file://` storage).
  - Message IDs enable recovery and exactly-once semantics (when fully implemented).

## How Nurion Runtime compares

### vs Apache Spark

- Spark is fundamentally a **batch-oriented** system with stage barriers; even in streaming mode, many workloads suffer from **stage wait and long tails**.  
- Nurion Runtime is a **batch engine with a streaming-style execution model**:
  - No global stage barriers between operators.
  - Continuous pulling between stages keeps data flowing and avoids long-tail tasks.
  - Better at **saturating CPU + GPU** on pipelines that mix heavy compute with I/O.

### vs Ray Data

- Ray Data is primarily built around **in-memory object store shuffle**:
  - Great for smaller tabular workloads, but costly for **huge multimodal binaries** (e.g. video frames, model inputs).  
- Nurion Runtime:
  - Uses **WorkQueue** (embedded broker) for stage-to-stage coordination with message ID tracking.  
  - Offers a **transparent, explicit runtime model** (stages, splits, queues, backpressure) instead of opaque auto-tuning knobs.  
  - Works better when your data is large, binary, and long-lived.

### vs Daft

- Daft provides a **DataFrame API** optimised for analytics and table-centric workloads.  
- Nurion Runtime intentionally **does not** expose a DataFrame-centric interface:
  - Large-scale, multimodal pipelines do not always benefit from DataFrame abstractions.  
  - Operator-based DAGs (sources / transforms / sinks) map more directly to multimodal processing graphs and model serving pipelines.  
  - This keeps the core minimal while still allowing you to build higher-level APIs on top if needed.

## Non-goals

Nurion Runtime is **not** trying to be:

- A **full SQL engine** with complete SQL coverage.
- A **general-purpose DataFrame platform** (like Spark SQL / Pandas / Daft) for interactive analytics.
- A BI / ad-hoc analytics tool.

Instead, it is focused on:

- High-throughput, long-running **batch jobs with streaming-style dataflow**.
- **Multimodal data processing** pipelines.
- Operational simplicity with strong runtime guarantees (backpressure, elasticity).

## Components

- **engine/**: Nurion Runtime core streaming framework (Ray-based distributed processing)
- **workflows/**: Example workflows
- **design-docs/**: Architecture and design documents
- **todo/**: Feature implementation tracking

### Shared Libraries (in `/lib`)

- **lib/workqueue-rs/**: Embedded WorkQueue broker + Python client
- **lib/raydp/**: Run Spark on Ray with distributed execution
- **lib/raydp/java/**: Scala/Java components for Spark integration

## Quick Start

### Prerequisites

```bash
# Install using uv (recommended)
cd /path/to/nurion/engine
uv sync --dev

# Or install with pip
pip install -e .
```

Use the `nurion` CLI to run jobs.

### Python API

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
# Create a job with configuration
job = Job(
    job_id='my_pipeline',
    config=JobConfig(
        workqueue_db_path="memory://",  # Use file:// for local persistence
    ),
)

# Add source stage
job.add_stage(Stage(
    stage_id='source',
    operator_config=LanceTableSourceConfig(table_path='/data/input'),
    parallelism=1,
))

# Add transform stage with auto-scaling
job.add_stage(Stage(
    stage_id='transform',
    operator_config=MapOperatorConfig(map_fn=lambda row: row),
    parallelism=(2, 8),  # Auto-scale between 2 and 8 workers
), upstream_stages=['source'])

# Add filter stage
job.add_stage(Stage(
    stage_id='filter',
    operator_config=FilterOperatorConfig(filter_fn=lambda row: row.get('valid', True)),
    parallelism=4,
), upstream_stages=['transform'])

# Add sink stage
job.add_stage(Stage(
    stage_id='sink',
    operator_config=FileSinkConfig(output_path='/data/output.json'),
    parallelism=1,
), upstream_stages=['filter'])

# Run the job
async def main():
    runner = job.create_ray_runner()
    await runner.run()

asyncio.run(main())
```

## Key Features

✅ **Elastic Scaling**: Auto-scale workers based on load  
✅ **Backpressure**: Automatic rate adaptation via queue lag detection  
✅ **DAG Pipelines**: Complex multi-stage workflows  
✅ **Queue-Based Flow**: WorkQueue (embedded broker) for stage coordination  
✅ **Zero Config Files**: All configuration in Python code  
✅ **Multimodal Operators**: Video processing, LLM inference, deduplication

## Operators

### Built-in Sources

| Operator | Description |
|----------|-------------|
| `LanceTableSource` | Read from Lance tables |
| `FileSource` | Read from JSON/Parquet/CSV files |
| `IcebergSource` | Read from Apache Iceberg tables |
| `SparkSource` | Read via Spark DataFrame |
| `SparkSourceV2` | Optimized Spark source with direct queue writes |

### Built-in Transforms

| Operator | Description |
|----------|-------------|
| `MapOperator` | 1-to-1 row transformations |
| `MapBatchesOperator` | Batch-level transformations (Arrow tables) |
| `FlatMapOperator` | 1-to-N row transformations |
| `FilterOperator` | Filter rows by predicate |
| `RepartitionOperator` | Repartition data by hash key |
| `HashDedupeOperator` | Exact deduplication by key columns |
| `MinHashComputeOperator` | Compute MinHash signatures for fuzzy dedup |

### Built-in Sinks

| Operator | Description |
|----------|-------------|
| `FileSink` | Write to JSON/Parquet/CSV files |
| `LanceSink` | Write to Lance tables |
| `PrintSink` | Print to stdout (debugging) |

### Specialized Operators

| Operator | Description |
|----------|-------------|
| `HttpOperator` | HTTP API calls with rate limiting |
| `LLMOperator` | LLM inference (managed or external service) |
| `FFmpegSceneDetectOperator` | Video scene detection |
| `FFmpegSliceOperator` | Video slicing |

### Custom Operators

```python
from dataclasses import dataclass
from typing import Optional, ClassVar, Type

from nurion import Operator, OperatorConfig, OperatorRuntime, Split, SplitPayload


@dataclass
class MyOperatorConfig(OperatorConfig):
    """Configuration for MyOperator."""
    multiplier: int = 2
    operator_class: ClassVar[Type["MyOperator"]]


class MyOperator(Operator):
    """Example custom operator."""

    def __init__(self, config: MyOperatorConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self.multiplier = config.multiplier

    def process_split(
        self,
        split: Split,
        payload: Optional[SplitPayload] = None,
    ) -> Optional[SplitPayload]:
        if payload is None:
            return None

        table = payload.to_table()
        # Apply your transformation to the Arrow table
        # ...
        return SplitPayload(data=table, split_id=split.split_id)


# Link config to operator class
MyOperatorConfig.operator_class = MyOperator
```

## Architecture

Nurion Runtime uses a **pull-based, queue-driven execution model**:

- **RayJobRunner**: Orchestrates the job lifecycle, manages stage masters
- **StageMaster**: Manages workers for a stage, owns the output queue
- **StageWorker**: Stateless Ray actor that pulls from upstream queue, processes data, writes to output queue
- **Queue Backend**: WorkQueue (embedded broker; `memory://` or `file://` storage)

```
┌─────────────────────────────────────────────────────────────────┐
│                       RayJobRunner                               │
│  - Topological stage ordering                                    │
│  - Stage lifecycle management                                    │
│  - Optional autoscaling                                          │
└───────────────────────────┬─────────────────────────────────────┘
                            │
            ┌───────────────┼───────────────┐
            ▼               ▼               ▼
      ┌──────────┐   ┌──────────┐   ┌──────────┐
      │  Stage   │   │  Stage   │   │  Stage   │
      │  Master  │   │  Master  │   │  Master  │
      │ (Source) │   │(Transform)│  │  (Sink)  │
      └────┬─────┘   └────┬─────┘   └────┬─────┘
           │              │              │
      ┌────┴────┐    ┌────┴────┐    ┌────┴────┐
      │ Workers │    │ Workers │    │ Workers │
      └────┬────┘    └────┬────┘    └────┬────┘
           │              │              │
           ▼              ▼              ▼
      ┌─────────┐    ┌─────────┐    ┌─────────┐
      │  Queue  │───▶│  Queue  │───▶│  Queue  │
      │(Output) │    │(Output) │    │(Output) │
      └─────────┘    └─────────┘    └─────────┘
                          │
              Workers PULL from upstream queues
```

### Component Details

**StageMaster** coordinates core managers:
- `WorkerManager`: Worker lifecycle (spawn, stop, status)
- `RecoveryManager`: Failure tracking and worker recovery
- Backpressure/autoscaling use job-level WorkQueue stats

**Queue Backend**:
- `WorkQueue`: Embedded broker with claim/ack semantics

## WorkQueue Storage Options

### In-memory (testing)

```python
job = Job(
    job_id='test_job',
    config=JobConfig(workqueue_db_path="memory://"),
)
```

### File-backed (local persistence)

```python
job = Job(
    job_id='prod_job',
    config=JobConfig(workqueue_db_path="file:///tmp/workqueue"),
)
```

## WebUI (Debugging Interface)

Nurion Runtime includes an optional web-based debugging interface:

```python
from nurion import Job, JobConfig, WebUIConfig

job = Job(
    job_id='my_job',
    config=JobConfig(
        workqueue_db_path="file:///tmp/workqueue",
        webui=WebUIConfig(
            enabled=True,
        ),
    ),
)
```

Access at: `http://localhost:<port>/jobs/{job_id}/` (port starts at 5000)

See `engine/webui/README.md` for details.

## Documentation

- `README.md` - This file (overview and usage)
- `PROJECT_OVERVIEW.md` - Extended project overview
- `design-docs/` - Architecture and design documents
- `todo/` - Feature implementation tracking
- `engine/webui/README.md` - WebUI documentation

## Examples

See `workflows/` and `examples/` directories:

- `examples/video_slice_demo.py` - Video processing pipeline
- `examples/minhash_dedup_example.py` - Fuzzy deduplication with MinHash

## Development

```bash
# Run tests (unit tests, no external dependencies)
cd engine
uv run pytest tests/ -v --tb=short -m "not integration and not distributed and not chaos and not slow and not stability and not workflow"

# Run integration tests (requires Java 11; Control Plane for Iceberg; RayDP JARs for Spark)
uv run pytest tests/ -v --tb=short -m "integration"

# Lint and format
uv run ruff check engine/
uv run ruff format --check engine/
```

## License

Apache License 2.0
