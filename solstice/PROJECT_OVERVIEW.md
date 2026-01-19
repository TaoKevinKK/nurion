# Solstice - Project Overview

## What is Solstice?

Solstice is a Ray-based **high-throughput batch processing framework** whose internal execution model is **streaming-style and pull-based**, featuring elastic scaling, backpressure, and fault tolerance.

## Key Characteristics

- **Streaming-Style Execution**: Pull-based data flow, no stage barriers
- **Elastic Workers**: Dynamic worker scaling based on queue lag
- **Queue-Based Communication**: Tansu (Kafka-compatible) or in-memory queues
- **Fault-Tolerant Design**: Offset-based recovery (scaffolding implemented)
- **Minimal Dependencies**: Only Ray and optional Tansu broker

## Directory Structure

```
solstice/
├── solstice/                # Framework implementation
│   ├── core/                # Core abstractions
│   │   ├── job.py           # Job and JobConfig
│   │   ├── stage.py         # Stage definition
│   │   ├── stage_master.py  # StageMaster orchestration
│   │   ├── stage_worker.py  # StageWorker execution
│   │   ├── stage_config.py  # Configuration classes
│   │   ├── operator.py      # Operator base class
│   │   ├── models.py        # Split, SplitPayload
│   │   └── managers/        # Component managers
│   │       ├── partition_manager.py
│   │       ├── worker_manager.py
│   │       ├── recovery_manager.py
│   │       └── backpressure_monitor.py
│   ├── runtime/             # Runtime components
│   │   ├── ray_runner.py    # RayJobRunner
│   │   ├── autoscaler.py    # SimpleAutoscaler
│   │   └── state_push.py    # StatePushManager (WebUI)
│   ├── queue/               # Queue backends
│   │   ├── protocols.py     # QueueProducer, QueueConsumer, etc.
│   │   ├── memory.py        # MemoryBackend
│   │   └── tansu.py         # TansuBrokerManager, TansuQueueClient
│   ├── operators/           # Built-in operators
│   │   ├── sources/         # Source operators
│   │   ├── sinks/           # Sink operators
│   │   ├── map.py           # Map/FlatMap operators
│   │   ├── filter.py        # Filter operator
│   │   ├── shuffle.py       # Shuffle/Repartition
│   │   ├── dedupe.py        # Hash deduplication
│   │   ├── minhash/         # MinHash operators
│   │   ├── connected_components.py  # CC algorithm
│   │   ├── http/            # HTTP operator
│   │   ├── llm/             # LLM inference
│   │   └── video.py         # Video processing
│   ├── state/               # State management
│   ├── checkpoint/          # Checkpoint storage
│   ├── webui/               # Debug WebUI
│   └── utils/               # Utilities
│
├── raydp/                   # Spark on Ray integration
├── java/                    # Scala/Java Spark components
├── tansu-py/                # Tansu PyO3 bindings
├── workflows/               # Example workflows
├── examples/                # Example scripts
├── tests/                   # Test suite
├── design-docs/             # Architecture documents
└── todo/                    # Feature tracking
```

## Core Concepts

### 1. Job

A complete processing pipeline with a DAG of stages.

```python
from solstice.core.job import Job, JobConfig
from solstice.queue import QueueType

job = Job(
    job_id='my_pipeline',
    config=JobConfig(
        queue_type=QueueType.TANSU,
        tansu_storage_url='memory://',
    ),
)
```

### 2. Stage

A processing step with an operator configuration and parallelism.

```python
from solstice.core.stage import Stage

# Fixed parallelism (4 workers)
Stage('transform', MyOperatorConfig(...), parallelism=4)

# Auto-scaling parallelism (2 to 10 workers)
Stage('scale', MyOperatorConfig(...), parallelism=(2, 10))
```

### 3. Operator

The logic that processes data. Operators are stateless and config-driven.

```python
from dataclasses import dataclass
from typing import Optional, ClassVar, Type

from solstice.core.operator import Operator, OperatorConfig
from solstice.core.models import Split, SplitPayload


@dataclass
class MyOperatorConfig(OperatorConfig):
    """Configuration for MyOperator."""
    param: str = "default"
    operator_class: ClassVar[Type["MyOperator"]]


class MyOperator(Operator):
    def __init__(self, config: MyOperatorConfig):
        super().__init__(config)
        self.param = config.param

    def process_split(
        self,
        split: Split,
        payload: Optional[SplitPayload] = None,
    ) -> Optional[SplitPayload]:
        if payload is None:
            return None

        table = payload.to_table()
        # Transform the Arrow table...
        return SplitPayload(data=table, split_id=split.split_id)


MyOperatorConfig.operator_class = MyOperator
```

### 4. Queue Backend

Where messages flow between stages:
- `TansuBackend`: Kafka-compatible broker (production)
- `MemoryBackend`: In-process queue (testing)

## Built-in Operators

| Category | Operator | Description |
|----------|----------|-------------|
| **Sources** | `LanceTableSource` | Read from Lance tables |
| | `FileSource` | Read from JSON/Parquet/CSV |
| | `IcebergSource` | Read from Iceberg tables |
| | `SparkSource` | Read via Spark DataFrame |
| | `SparkSourceV2` | Optimized Spark with direct queue writes |
| **Transforms** | `MapOperator` | 1-to-1 transformation |
| | `MapBatchesOperator` | Batch-level transformation |
| | `FlatMapOperator` | 1-to-N transformation |
| | `FilterOperator` | Filter records |
| | `RepartitionOperator` | Repartition by hash key |
| | `HashDedupeOperator` | Exact deduplication |
| | `MinHashComputeOperator` | MinHash signature computation |
| | `CandidatePairOperator` | LSH candidate pair generation |
| **Sinks** | `FileSink` | Write to files |
| | `LanceSink` | Write to Lance |
| | `PrintSink` | Print to stdout |
| **Specialized** | `HttpOperator` | HTTP API calls |
| | `LLMOperator` | LLM inference |
| | `FFmpegSceneDetectOperator` | Video scene detection |
| | `FFmpegSliceOperator` | Video slicing |

## Parallelism Modes

### Fixed Parallelism
```python
parallelism=4  # Always 4 workers
```

Use for:
- Source/Sink operations with known concurrency limits
- Predictable workloads
- When you want exact resource control

### Auto-Scaling Parallelism
```python
parallelism=(2, 10)  # Scale between 2 and 10 workers
```

Use for:
- Variable workloads
- CPU/GPU intensive operations
- When you want optimal resource utilization

## Architecture Layers

```
┌─────────────────────────────────────────┐
│ Layer 4: User API                       │
│ (Job, Stage, OperatorConfig)            │
├─────────────────────────────────────────┤
│ Layer 3: Runtime                        │
│ (RayJobRunner, SimpleAutoscaler)        │
├─────────────────────────────────────────┤
│ Layer 2: Execution                      │
│ (StageMaster, StageWorker)              │
├─────────────────────────────────────────┤
│ Layer 1: Queue                          │
│ (TansuBackend, MemoryBackend)           │
├─────────────────────────────────────────┤
│ Layer 0: Ray                            │
│ (Actors, Object Store)                  │
└─────────────────────────────────────────┘
```

## StageMaster Component Managers

StageMaster delegates to specialized managers:

| Manager | Responsibility |
|---------|----------------|
| `PartitionManager` | Partition assignment and rebalancing |
| `WorkerManager` | Worker lifecycle (spawn, stop, status) |
| `RecoveryManager` | Failure tracking and worker recovery |
| `BackpressureMonitor` | Queue lag monitoring and scaling signals |

## Running a Pipeline

```python
import asyncio
from solstice.core.job import Job, JobConfig
from solstice.core.stage import Stage
from solstice.operators.sources import LanceTableSourceConfig
from solstice.operators.map import MapOperatorConfig
from solstice.operators.sinks import FileSinkConfig
from solstice.queue import QueueType

# 1. Create job
job = Job(
    job_id='my_job',
    config=JobConfig(queue_type=QueueType.MEMORY),
)

# 2. Add stages
job.add_stage(Stage(
    'source',
    LanceTableSourceConfig(table_path='/data/input'),
    parallelism=1,
))

job.add_stage(Stage(
    'transform',
    MapOperatorConfig(map_fn=lambda x: x),
    parallelism=(2, 8),
), upstream_stages=['source'])

job.add_stage(Stage(
    'sink',
    FileSinkConfig(output_path='/data/output.json'),
    parallelism=1,
), upstream_stages=['transform'])

# 3. Run
async def main():
    runner = job.create_ray_runner()
    await runner.run()

asyncio.run(main())
```

## Feature Comparison

| Feature | Solstice | Flink | Spark Streaming |
|---------|----------|-------|-----------------|
| Pull-Based Flow | ✅ | Push-based | Push-based |
| Dynamic Scaling | ✅ | Limited | Limited |
| No External Deps | ✅ (Ray only) | ❌ (Kafka, ZK) | ❌ (HDFS) |
| Lance Integration | ✅ | ❌ | ❌ |
| Python-First | ✅ | ❌ | ✅ |
| Queue Backend | Tansu/Memory | Kafka | HDFS/Kafka |

## Current Implementation Status

| Feature | Status |
|---------|--------|
| Queue-based execution | ✅ Complete |
| Worker auto-scaling | ✅ Complete |
| Backpressure detection | ✅ Complete |
| WebUI monitoring | ✅ Complete |
| Multi-partition queues | ✅ Complete |
| Partition assignment | ✅ Complete |
| Offset-based recovery | 🚧 Scaffolding only |

## Documentation

- **`README.md`** - Quick start and overview
- **`PROJECT_OVERVIEW.md`** - This file
- **`design-docs/`** - Architecture decisions and designs
- **`todo/`** - Implementation status tracking
- **`solstice/webui/README.md`** - WebUI documentation

## Next Steps

1. Read `README.md` for quick start
2. Explore `examples/` for sample pipelines
3. Check `design-docs/` for architecture details
4. See `todo/` for implementation status

---

*Last updated: 2026-01-19*
