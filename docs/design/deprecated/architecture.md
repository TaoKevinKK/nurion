# Solstice Runtime Architecture

> NOTE: This document references the former Tansu/Kafka queue model. The current
> implementation uses the embedded Anvil backend. See
> `../work-queue-redesign.md`.

## Overview

Solstice implements a **high-throughput dataflow engine** on top of Ray actors. Conceptually it is a **batch processing engine** (jobs are finite DAGs over finite inputs), but its internal execution model is **streaming-style and pull-based**. It is designed to run long-lived, multimodal pipelines (video, images, embeddings, text, binary blobs) with:

- **Streaming-style execution** – no global stage barriers, no batch-style long tails.
- **Stateless workers + queue-based coordination** – workers can be scaled in/out freely, while stage masters manage output queues.
- **Built-in backpressure** – workers pull from upstream queues, so the system naturally throttles producers and can adapt resource usage.
- **Queue-based data flow** – stages exchange data via Tansu (Kafka-compatible) or in-memory queues with offset tracking.

Workflows are expressed as directed acyclic graphs (DAGs) of stages. Each stage owns a user-defined operator and a pool of stateless `StageWorker` actors, while the `StageMaster` manages the output queue and coordinates workers. Stages exchange *Splits*, which are metadata records describing batches of data stored in the `SplitPayloadStore`. This orchestration keeps hot data off the control plane and allows the pipeline to scale horizontally across workers.

```
       +-------------+       +-------------+       +-------------+
       | SourceStage |  -->  |  MapStage   |  -->  | SinkStage   |
       +-------------+       +-------------+       +-------------+
             |                     |                      |
   (produce to queue)       (pull → process → produce)   (pull → write)
       Output Queue           Output Queue              Output Queue
```

## Data Flow Model: Pull-Based Architecture

Solstice uses a **Pull-based** data flow model where workers actively pull data from upstream queues. This design provides natural backpressure and reduces coupling between stages.

### Key Characteristics

1. **Workers pull from upstream queues**: Each worker fetches messages from its upstream stage's output queue, processes them, and writes results to its own stage's output queue.

2. **Natural backpressure**: If a downstream stage is slow, its workers pull less frequently. The upstream's output queue fills up, and the upstream stage naturally slows down when its buffer is full.

3. **Single-direction dependency**: Workers know about their upstream queue (to pull from), but upstream stages don't need to know about their downstreams.

4. **Offset-based consumption**: Each consumer group tracks its read offset, enabling multiple consumers and recovery from failures.

```
Pull-Based Data Flow:

Source.output_queue <── pull ── Transform.workers ── produce ──> Transform.output_queue
                                                                        ↑
                              Sink.workers ── pull ── from ─────────────┘
```

## Components

### Job Definition

* `Job`: Declarative DAG specification. Tracks stages, edges, and configuration (queue type, autoscaling, WebUI).
* `Stage`: Wraps an operator config, parallelism configuration, and resource requirements.
* `Split`: Control-plane record representing a unit of work (batch metadata, lineage, status).
* `SplitPayload`: The actual data (Arrow table) associated with a split.

### Runtime

* `RayJobRunner`: Orchestrates the execution lifecycle. Responsibilities:
  - Initialize Ray and payload store.
  - Create stage masters in topological order.
  - Configure upstream references for each stage.
  - Monitor stage counters to detect when the DAG is quiescent.
  - Coordinate shutdown.

* `StageMaster`: Manages the output queue and a pool of StageWorkers. Delegates to component managers:
  - `WorkerManager`: Worker lifecycle (spawn, stop, status tracking)
  - `RecoveryManager`: Failure tracking and worker recovery
  - Backpressure/autoscaling use job-level Anvil stats

* `StageWorker`: Executes the user operator over batches. Responsibilities:
  - Pull messages from upstream queue.
  - Fetch payload from `SplitPayloadStore`.
  - Invoke operator's `process_split()`.
  - Store output payload and produce message to output queue.
  - Commit offset after successful processing.

### Queue Backends

* `TansuBrokerManager`: Manages embedded Tansu broker lifecycle (start, stop, health check).
* `TansuQueueClient`: Kafka client for Tansu (produce, fetch, commit offset).
* `MemoryBroker` / `MemoryClient`: In-process queue for testing.

The queue layer follows the **Interface Segregation Principle** with focused protocols:
- `QueueProducer`: Message production
- `QueueConsumer`: Message consumption and offset management
- `QueueAdmin`: Topic management
- `QueueBroker`: Broker lifecycle
- `QueueClient`: Combined interface

### Payload Store

* `SplitPayloadStore`: Protocol for storing and retrieving split payloads.
* `RaySplitPayloadStore`: Implementation using Ray Object Store with a registry actor.
  - Payloads stored as Arrow tables via `ray.put()`.
  - Registry actor tracks key → ObjectRef mapping.
  - Auto-converts Arrow IPC bytes from JVM writers.

## Dataflow

### 1. Source Ingestion

- Source stages generate splits via `SourceOperator.plan_splits()` or similar.
- Splits are processed by workers and written to the source stage's output queue.
- Completed payloads are stored in `SplitPayloadStore`.

### 2. Stage Processing (Pull-Based)

Each worker runs a processing loop:

```python
# Pseudocode for worker processing loop
while running:
    # Pull batch from upstream queue
    records = upstream_queue.fetch(topic, offset, max_records)
    
    for record in records:
        message = QueueMessage.from_bytes(record.value)
        
        # Fetch payload from store
        payload = payload_store.get(message.payload_key)
        
        # Process with operator
        output_payload = operator.process_split(split, payload)
        
        # Store output and produce to output queue
        output_key = payload_store.store(output_payload)
        output_queue.produce(output_topic, output_message)
    
    # Commit offset after batch
    upstream_queue.commit_offset(group, topic, last_offset + 1)
```

### 3. Queue-Based Buffering

- Each stage maintains an output queue (Tansu topic or in-memory).
- Workers produce messages after processing; downstream workers consume.
- Queue provides durability (Tansu) and backpressure signal (lag).

### 4. Completion Detection

A stage is complete when:
- All upstreams have marked themselves as finished.
- No pending messages in input queue.
- All workers have processed their assigned work.

When all stages are complete, the runner stops the job.

## Scheduling & Backpressure

### Natural Backpressure (Pull Model)

* **Queue lag-based throttling**: When downstream workers can't keep up, upstream queue fills up, naturally throttling producers.
* **Lag monitoring**: Job-level Anvil stats drive backpressure/autoscaling decisions.
* **No explicit backpressure signals needed**: Downstream controls the flow rate by its pull frequency.

### Worker Scheduling

* Workers are Ray actors with configurable resources (num_cpus, num_gpus, memory).
* `WorkerManager` handles spawning and stopping workers.
* `PartitionManager` can assign specific partitions to workers (when multi-partition is enabled).

## Elasticity

* `SimpleAutoscaler` adjusts worker pool size according to queue lag.
* Workers can be added/removed without stopping the job.
* When scaling in, workers complete current batch before stopping.

## Fault Tolerance

### Current Implementation (Scaffolding)

1. **Offset tracking**: Each worker tracks committed offset in queue.
2. **Checkpoint storage**: `FsspecCheckpointStorage` can read/write checkpoint files.
3. **Recovery loading**: `recover_from_checkpoint()` can load checkpoint data.

### Not Yet Implemented

- Periodic checkpoint saving during execution
- Passing recovered offsets to workers on restart
- Workers seeking to recovered offset

### Planned Recovery Flow

```
1. Job starts
2. Load checkpoint from storage (if exists)
3. For each stage:
   - Get partition offsets from checkpoint
   - Pass offsets to workers
4. Workers:
   - Seek queue consumer to recovered offset
   - Resume processing
5. Periodic checkpoint:
   - Collect offsets from all workers
   - Save to checkpoint storage
```

## CLI Lifecycle

1. Create `Job` via workflow module or direct API.
2. Build `RayJobRunner`.
3. `await runner.run()`:
   - Initialize stages in topological order.
   - Configure upstream references.
   - Start stage processing loops.
   - Monitor until all stages are complete.
   - Stop job and report status/metrics.
4. `runner.shutdown()` cleans up actors and Ray services.

## ASCII Architecture Diagram

```
                    ┌─────────────────────────────────────────┐
                    │            RayJobRunner                  │
                    │  - Stage lifecycle management            │
                    │  - Optional autoscaling                  │
                    │  - Optional WebUI integration            │
                    └───────────────────┬─────────────────────┘
                                        │
              ┌─────────────────────────┼─────────────────────────┐
              │                         │                         │
              ▼                         ▼                         ▼
      ┌───────────────┐        ┌───────────────┐        ┌───────────────┐
      │  StageMaster  │        │  StageMaster  │        │  StageMaster  │
      │   (Source)    │        │  (Transform)  │        │    (Sink)     │
      │               │        │               │        │               │
      │ ┌───────────┐ │        │ ┌───────────┐ │        │ ┌───────────┐ │
      │ │WorkerMgr  │ │        │ │WorkerMgr  │ │        │ │WorkerMgr  │ │
      │ │PartMgr    │ │        │ │PartMgr    │ │        │ │PartMgr    │ │
      │ │RecoveryMgr│ │        │ │RecoveryMgr│ │        │ │RecoveryMgr│ │
      │ │BackpresMon│ │        │ │BackpresMon│ │        │ │BackpresMon│ │
      │ └───────────┘ │        │ └───────────┘ │        │ └───────────┘ │
      └───────┬───────┘        └───────┬───────┘        └───────┬───────┘
              │                        │                        │
         [Workers]                [Workers]                [Workers]
              │                        │                        │
              ▼                        │                        │
      ┌───────────────┐                │                        │
      │ Output Queue  │◄───── pull ────┤                        │
      │   (Tansu)     │                │                        │
      └───────────────┘                ▼                        │
                               ┌───────────────┐                │
                               │ Output Queue  │◄───── pull ────┤
                               │   (Tansu)     │                │
                               └───────────────┘                ▼
                                                        ┌───────────────┐
                                                        │ Output Queue  │
                                                        │   (Tansu)     │
                                                        └───────────────┘

                        ┌─────────────────────────────────────────┐
                        │          SplitPayloadStore              │
                        │  (Ray Object Store + Registry Actor)    │
                        └─────────────────────────────────────────┘
```

## Configuration

### StageConfig Options

| Option | Description | Default |
|--------|-------------|---------|
| `min_workers` | Minimum worker count | 1 |
| `max_workers` | Maximum worker count | 4 |
| `batch_size` | Messages per fetch batch | 100 |
| `poll_interval_ms` | Polling interval when queue empty | 100 |
| `failure_policy` | How to handle failures (FAIL_FAST, SKIP, RETRY) | FAIL_FAST |
| `max_retries` | Max retries per message (if RETRY) | 3 |

### JobConfig Options

| Option | Description | Default |
|--------|-------------|---------|
| `queue_type` | TANSU or MEMORY | TANSU |
| `tansu_storage_url` | Tansu storage backend | memory:// |
| `autoscale_config` | Autoscaling configuration | None |
| `webui` | WebUI configuration | disabled |
| `checkpoint_path` | Checkpoint storage path | /tmp/solstice-checkpoints/ |

## Known Gaps & Issues

* **Checkpoint recovery not functional**: Scaffolding exists but checkpoints are not saved during execution and not restored on restart.
* **Iceberg ingestion not streaming**: `IcebergSource.read()` materialises data up front.
* **Buffer persistence**: If a stage master restarts, in-flight messages may be lost.

## Future Improvements

* Implement full checkpoint save/restore cycle.
* Add multi-partition support for higher parallelism.
* Add long-polling support for reduced latency.
* Implement adaptive batch sizing based on throughput metrics.
* Extend checkpointing to support partial DAG snapshots.

---

*Last updated: 2026-01-19*
