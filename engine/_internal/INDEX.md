# Engine Internal Module Index

> **Update this file whenever you add, remove, or rename a module.**
> Read this first when navigating `_internal/` — it maps every file to its purpose and key symbols.

---

## Core Execution Framework (`core/`)

### `core/job.py`
- **`Job`** — top-level pipeline definition; `job.run()` submits to `RayJobRunner`
- **`JobConfig`** — dataclass: `job_id`, `max_workers`, `failure_policy`, `workqueue_db_path`
- **`WebUIConfig`** — dataclass: WebUI host/port settings

### `core/stage.py`
- **`Stage`** — defines one pipeline stage: `operator_config`, `num_workers`, `stage_id`
- **`StageRuntime`** — runtime context passed to `StageMaster`: queue endpoints, store config

### `core/operator.py`
- **`Operator`** (ABC) — base class; implement `process_split(split, payload) -> PayloadResult`
- **`OperatorConfig`** (ABC dataclass) — base config; override `get_merge_upstream()`, `create_source()`, `create_sink_committer()`
- **`OperatorRuntime`** (frozen dataclass) — `job_id`, `stage_id`, `worker_id`, `broker_endpoint`
- **`@operator(Config)`** — decorator that binds `Config.operator_class ↔ Operator.config_class`
- **`@master_callable`** — marks operator methods remotely callable by `StageMaster`
- **`PayloadResult`** — Union type for all valid `process_split` return values

### `core/source_operator.py`
- **`SourceOperator`** — base for source stages; implement `plan_splits() -> list[Split]`
- Config must implement `create_source()` → `SourceStrategy` (either `SplitPlanner` or `DirectProducer`)

### `core/sink_operator.py`
- **`SinkOperator`** — base for sink stages
- Config may implement `create_sink_committer()` → `SinkCommitter` for batched commits

### `core/models.py`
- **`Split`** — scheduling metadata: `split_id`, `stage_id`, `data_range`, `parent_split_ids`
  - `derive_output_split()` — create downstream split from parent
- **`SplitPayload`** — `split_id`, `data: pa.Table`, `metadata: dict`
- **`QueueMessage`** — message envelope: `msg_id`, `split_id`, `payload_ref`
- **`QueueEndpoint`** — `broker_url`, `queue_name`
- **`StageStatus`** — enum: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`
- **`FailurePolicy`** — enum: `FAIL_FAST`, `CONTINUE`
- **`FailureTracker`** — tracks worker failures, checks against policy
- **`BackpressureSignal`** — `from_stage`, `to_stage`, pressure level
- **`RawOutputBytes`** — wraps raw bytes for sink commit messages

### `core/source.py`
- **`Source`** (Protocol) — interface for source adapters
- **`SourceStrategy`** (Protocol) — `SplitPlanner` or `DirectProducer` variant

### `core/sink.py`
- **`Sink`** (Protocol) — interface for sink adapters
- **`SinkCommitter`** (Protocol) — commit coordinator for batched sinks

### `core/stage_master.py`
- **`StageMaster`** — orchestrates one pipeline stage
  - `start()` → SourceManager.start + SinkManager.start + WorkerManager.start
  - `run()` → monitor loop: recovery check, autoscale tick, completion detection
  - `stop()` → finalize sink, drain workers
- **`BackpressureProvider`** (Protocol) — `is_backpressure_active()`, `should_pause()`
- Delegates to: `WorkerManager`, `RecoveryManager`, `SourceManager`, `SinkManager`

### `core/stage_worker.py`
- **`StageWorker`** (Ray actor) — stateless claim-process-ack worker
  - Claim-process-ack loop: `claim(N)` → merge payloads → `process_split()` → `ack_and_forward`
  - `merge_upstream=N`: merges N upstream messages (Arrow concat) before calling operator once
  - `invoke_operator(method, *args)` — calls `@master_callable` method on operator
- **`WorkerRuntime`** (frozen dataclass) — StageWorker init params

### `core/split_payload_store.py`
- **`SplitPayloadStore`** (Protocol) — get/put `SplitPayload` objects
- **`RaySplitPayloadStore`** — Ray object store backend (in-cluster)
- **`FsspecSplitPayloadStore`** — S3/GCS backend (cross-cluster / persistent)

### `core/fault_tolerance.py`
- **`CheckpointManager`** — write/restore operator checkpoints
- Checkpoints stored in WorkQueue state store (atomic with ack)

### `core/managers/worker_manager.py`
- **`WorkerManager`** — spawn/stop `StageWorker` actors, track actor handles
- `scale_up(n)` / `scale_down(n)` — add/remove workers
- `get_active_workers()` — returns live worker handles

### `core/managers/source_manager.py`
- **`SourceManager`** — start/stop `SourceStrategy`
- Handles `SplitPlanner` (push splits to queue) and `DirectProducer` (bypass queue)

### `core/managers/sink_manager.py`
- **`SinkManager`** — run `SinkCommitter` background commit loop
- `finalize()` — flush all pending commits before stage completion

### `core/managers/recovery_manager.py`
- **`RecoveryManager`** — detect worker failure (Ray actor death)
- On failure: nack claimed messages → re-enqueue for retry
- Tracks consecutive failure count for `FailurePolicy` enforcement

---

## Operators (`operators/`)

### Transform Operators

#### `operators/map.py`
- **`MapConfig`** / **`Map`** — apply fn to each row; `fn: Callable[[dict], dict]`
- **`MapBatchesConfig`** / **`MapBatches`** — apply fn to `pa.Table` batch
- **`FlatMapConfig`** / **`FlatMap`** — explode rows; fn returns list

#### `operators/filter.py`
- **`FilterConfig`** / **`Filter`** — predicate fn; returns None to drop

#### `operators/dedupe.py`
- **`DedupeConfig`** / **`Dedupe`** — bucket-based dedup using Union-Find
- Config: `key_cols: list[str]`, `similarity_threshold: float`

#### `operators/shuffle.py`
- **`ShuffleConfig`** / **`Shuffle`** — repartition splits across workers

#### `operators/video.py`
- **`VideoSliceConfig`** / **`VideoSlice`** — decode video, emit frame batches
- Config: `fps: float`, `start_sec: float`, `end_sec: float`

### Sources (`operators/sources/`)

#### `sources/file.py`
- **`FileSourceConfig`** / **`FileSource`** — local / S3 / GCS file source
- `plan_splits()` lists files, creates one Split per file (or configurable chunk size)

#### `sources/iceberg.py`
- **`IcebergSourceConfig`** / **`IcebergSource`** — Apache Iceberg table
- Config: `catalog_uri`, `table_identifier`, `snapshot_id` (optional)

#### `sources/lance.py`
- **`LanceSourceConfig`** / **`LanceSource`** — LanceDB dataset
- Config: `uri`, `version` (optional), `columns` (optional projection)

#### `sources/spark.py`
- **`SparkSourceConfig`** / **`SparkSource`** — Spark DataFrame (v1)
- Config: `table`, `spark_conf` dict

#### `sources/sparkv2.py`
- **`SparkSourceV2Config`** / **`SparkSourceV2`** — Spark Source V2 with predicate pushdown
- Design doc: `engine/design-docs/spark-source-v2.md`

### Sinks (`operators/sinks/`)

#### `sinks/file.py`
- **`FileSinkConfig`** / **`FileSink`** — write Arrow tables to files (Parquet, JSON, CSV)
- Config: `output_path`, `format: str`, `partition_by: list[str]`

#### `sinks/lance.py`
- **`LanceSinkConfig`** / **`LanceSink`** — write rows into LanceDB dataset
- `get_merge_upstream()` returns large N to build bigger fragments
- Returns `RawOutputBytes(commit_metadata)` after writing fragment

#### `sinks/lance_commit.py`
- **`LanceCommitSinkConfig`** / **`LanceCommitSink`** — finalize LanceDB write
- Reads commit metadata from queue, calls `dataset.commit()`
- Always a separate stage after `LanceSink`

#### `sinks/print.py`
- **`PrintSinkConfig`** / **`PrintSink`** — print splits to stdout; debug only

### HTTP Operator (`operators/http/`)

#### `http/operator.py`
- **`HttpOperatorConfig`** / **`HttpOperator`** — send HTTP request per split
- Config: `url`, `method`, `headers`, `timeout_secs`, `max_retries`

#### `http/circuit_breaker.py`
- **`CircuitBreaker`** — open on error rate threshold; auto-close after recovery window

#### `http/rate_limiter.py`
- **`RateLimiter`** — token-bucket algorithm; `requests_per_second: float`

### LLM Operator (`operators/llm/`)

#### `llm/operator.py`
- **`LlmOperatorConfig`** / **`LlmOperator`** — run LLM inference per split
- Connects to serve module via `LlmClient`
- Config: `model_id`, `prompt_template`, `max_tokens`, `temperature`

#### `llm/client.py`
- **`LlmClient`** — HTTP client to `InferenceWorker`; resolves URL via `ModelRegistry`

#### `llm/embedded.py`
- **`EmbeddedInference`** — in-process inference (no serve actor); for single-node use

#### `llm/utils.py`
- Prompt formatting, response parsing helpers

### Dedup Utilities (`operators/dedup/`)

#### `dedup/bucket_union.py`
- **`BucketUnionFind`** — Union-Find for merging near-duplicate buckets

#### `dedup/encoder.py`
- **`FeatureEncoder`** — feature extraction for similarity hashing

#### `dedup/filter.py`
- **`DedupeFilter`** — filter records based on Union-Find membership

### MinHash (`operators/minhash/`)

#### `minhash/compute.py`
- **`MinHashComputer`** — compute MinHash signatures (used by dedup workflow)
- Config: `num_perm: int`, `ngram_size: int`

---

## Runtime (`runtime/`)

### `runtime/ray_runner.py`
- **`RayJobRunner`** — top-level orchestrator
  - `run(job)` → start broker → create store → start stages → monitor → return
  - Manages `StageMaster` actors and stage ordering
- **`JobStatus`** — `job_id`, `is_running`, `stages`, `elapsed_time`, `error`

### `runtime/autoscaler.py`
- **`SimpleAutoscaler`** — backpressure-driven worker scaling
  - Reads queue depth from `QueueStatsClient`
  - Calls `WorkerManager.scale_up/down()` on each tick
  - Config: `min_workers`, `max_workers`, `scale_up_threshold`, `scale_down_threshold`

### `runtime/backpressure.py`
- **`JobBackpressureController`** — monitor queue depths across all stages
  - Sends `BackpressureSignal` to upstream `StageMaster` to pause/resume source

### `runtime/queue_stats.py`
- **`QueueStatsClient`** — collect queue stats from WorkQueue broker
- **`StageQueueConfig`** — queue name → stage mapping

---

## Queue (`queue/`)

### `queue/workqueue.py`
- **`WorkQueueQueueClient`** — Python client for queue operations
  - `claim(queue, timeout)` → `WorkQueueRecord`
  - `ack_and_forward(msg_id, output_queue, payload)` — atomic
  - `nack(msg_id)` — re-enqueue
  - `state_get(ns, key)` / `state_put(ns, key, value)`
- **`WorkQueueBrokerManager`** — start/stop the Rust broker process

### `queue/workqueue_storage.py`
- **`WorkQueueStorage`** — higher-level storage abstraction over WorkQueue

### `queue/backend.py`
- **`QueueBackend`** (Protocol) — abstract backend; `InMemoryBackend` for tests

---

## Serve Module (`serve/`)

### `serve/config.py`
- **`ModelConfig`** — `model_id: str`, `model_source: str`, `tensor_parallel_size: int`, `min_workers: int`, `max_workers: int`
- **`AutoscaleConfig`** — `target_qps`, `scale_up_threshold`, `scale_down_threshold`

### `serve/manager.py`
- **`ModelServiceManager`** (Ray actor) — control plane
  - `deploy_model(config)` → allocate GPUs → create pool → register
  - `undeploy_model(model_id)` → stop pool → release GPUs → deregister
  - `shutdown()` → undeploy all
  - Two modes: attached (default) / detached (`lifetime="detached"`)
  - `ModelServiceManager.connect()` — reconnect to detached manager

### `serve/pool.py`
- **`ModelPool`** (plain object) — per-model worker pool
  - `scale_up(n)` / `scale_down(n)` → spawn/stop `InferenceWorker` actors
  - `get_worker_urls()` → list of active worker HTTP URLs

### `serve/worker.py`
- **`InferenceWorker`** (Ray actor) — runs vLLM or SGLang server
  - Exposes HTTP endpoint for generation requests
  - Reports health, handles graceful shutdown

### `serve/allocator.py`
- **`GPUAllocator`** (plain object) — GPU bin-packing
  - `allocate(model_id, tp_size)` → list of GPU IDs
  - `release(model_id)` → return GPUs to pool
  - Anti-fragmentation: prefers filling existing nodes before spreading

### `serve/registry.py`
- **`ModelRegistry`** (Ray Named Actor) — service discovery
  - `register(model_id, urls)` / `deregister(model_id)`
  - `get_workers(model_id)` → list of URLs
  - Actor name: `REGISTRY_ACTOR_NAME` in namespace `SERVE_NAMESPACE`

### `serve/client.py`
- **`ModelClient`** (plain object) — HTTP client with round-robin LB
  - `generate(prompt, max_tokens, ...)` → async HTTP POST to a worker URL

### `serve/fake_server.py`
- **`FakeInferenceServer`** — HTTP stub for tests; returns configurable responses
  - Monkeypatch target: replace `InferenceWorker` in tests

---

## WebUI (`webui/`)

### `webui/app.py`
- **`create_webui_app()`** — FastAPI factory for WebUI server

### `webui/job_webui.py`
- **`JobWebUI`** — per-job monitoring interface; reads state from WorkQueue

### `webui/portal.py`
- **`PortalServer`** — multi-job dashboard; lists active and historical jobs

### `webui/history_server.py`
- **`HistoryServer`** — historical job viewer; reads archived state

### `webui/runtime_server.py`
- **`RuntimeServer`** / **`EmbeddedWebUIServer`** — live metrics endpoint embedded in job

### `webui/api/`
- REST endpoints for frontend: job list, stage status, split events, queue stats

### `webui/collectors/`
- Metric collectors: pull queue stats and actor status from Ray

### `webui/state/`
- **`WorkQueueStateWriter`** — writes job/stage/split events to WorkQueue state store
- **`schema.py`** — key namespace helpers: `job_namespace`, `stage_key`, `worker_key`, `split_key`, `event_key`

---

## Utilities (`utils/`)

### `utils/logging.py`
- **`create_ray_logger(name)`** — Ray-compatible structured logger

### `utils/network.py`
- Port discovery, address formatting helpers

### `utils/remote.py`
- Helpers for Ray remote call patterns

### `utils/union_find.py`
- **`UnionFind`** — generic Union-Find data structure (also used in dedup)

---

## Other

### `compute/duckdb_engine.py`
- **`DuckDBEngine`** — SQL query execution via DuckDB; used in ETL transforms

### `state/`
- Operator persistent state management helpers

### `testing/fault_injection.py`
- **`check_fault()`**, `FAULT_BEFORE_PROCESS`, `FAULT_AFTER_PROCESS` — inject failures at controlled points for chaos tests
