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
- Config must implement `create_source()` → `SplitPlanner` or `DirectProducer`

### `core/sink_operator.py`
- **`SinkOperator`** — base for sink stages
- Config may implement `create_sink_committer()` → `SinkCommitter` for batched commits

### `core/models.py`
- **`Split`** — scheduling metadata: `split_id`, `stage_id`, `data_range`, `parent_split_ids`
  - `derive_output_split()` — create downstream split from parent
- **`SplitPayload`** — `split_id`, `data: pa.Table`, `metadata: dict`
- **`SourceQueueMessage`** — message envelope for source splits
- **`DataQueueMessage`** — message envelope for inter-stage data: `msg_id`, `split_id`, `payload_key`
- **`MessageType`** — enum: `SOURCE`, `DATA`
- **`Record`** — generic queue record wrapper
- **`QueueStats`** — queue depth statistics
- **`QueueEndpoint`** — `broker_url`, `queue_name`
- **`StageStatus`** — enum: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`
- **`FailurePolicy`** — enum: `FAIL_FAST`, `CONTINUE`
- **`FailureTracker`** — tracks worker failures, checks against policy
- **`BackpressureSignal`** — `from_stage`, `to_stage`, pressure level
- **`RawOutputBytes`** — wraps raw bytes for sink commit messages

### `core/source.py`
- **`SplitPlanner`** (Protocol) — plans splits and pushes to queue
- **`DirectProduceContext`** (dataclass) — context for direct producers
- **`DirectProducer`** (Protocol) — bypasses queue, produces directly to workers

### `core/sink.py`
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
- **`OutputRouting`** — routes output to downstream queue(s)
- **`PayloadMissingError`** — raised when payload not found in store

### `core/split_payload_store.py`
- **`SplitPayloadStore`** (ABC) — get/put `SplitPayload` objects
- **`RaySplitPayloadStore`** — Ray object store backend (in-cluster)
- **`FsspecSplitPayloadStore`** — S3/GCS backend (cross-cluster / persistent)

### `core/nvme_payload_store.py`
- **`NvmeSplitPayloadStore`** — two-tier NVMe + S3 payload store
  - `get_with_hint()` — three-tier fallback: local NVMe → remote Flight → S3
  - `get_location()` — returns `{"flight": ..., "s3": ...}` for downstream hints
  - `flush_pending_writes()` — wait for async S3 writes to complete
- **`WritePolicy`** (Enum) — `WRITE_THROUGH` (S3 first), `WRITE_BACK` (NVMe first)
- **`NvmeDisk`** — single disk: hash-prefix bucketing, atomic write (tmp+rename), mmap read
- **`NvmeDiskPool`** — multi-disk: write to most-free, key-to-disk tracking
- **`FlightPayloadServer`** — per-process Arrow Flight gRPC server for cross-node reads
- **`parse_nvme_uri()`** — parse `nvme://` URI with comma-separated paths and query params

### `core/partition.py`
- **`split_table_by_column()`** — split Arrow table into partitions by column values

### `core/fault_tolerance.py`
- **`NodeBlacklistConfig`** — config for node blacklisting on repeated failures
- **`NodeBlacklist`** — track and blacklist nodes with excessive failures
- **`TimeoutConfig`** — config for worker/split timeout monitoring
- **`TimeoutMonitor`** — detect and handle timed-out workers/splits

### `core/managers/worker_manager.py`
- **`WorkerManager`** — spawn/stop `StageWorker` actors, track actor handles
- `scale_up(n)` / `scale_down(n)` — add/remove workers
- `get_active_workers()` — returns live worker handles

### `core/managers/source_manager.py`
- **`SourceManager`** — start/stop source strategy
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
- **`MapOperatorConfig`** / **`MapOperator`** — apply fn to each row; `fn: Callable[[dict], dict]`
- **`MapBatchesOperatorConfig`** / **`MapBatchesOperator`** — apply fn to `pa.Table` batch
- **`FlatMapOperatorConfig`** / **`FlatMapOperator`** — explode rows; fn returns list

#### `operators/filter.py`
- **`FilterOperatorConfig`** / **`FilterOperator`** — predicate fn; returns None to drop

#### `operators/dedupe.py`
- **`HashDedupeConfig`** / **`HashDedupeOperator`** — hash-based dedup (extends `ShuffleOperatorConfig`)
- Config: `key_cols: list[str]`

#### `operators/shuffle.py`
- **`ShuffleOperatorConfig`** / **`ShuffleOperator`** — repartition splits across workers
- **`RepartitionConfig`** / **`RepartitionOperator`** — change partition count

#### `operators/video.py`
- **`FFmpegSceneDetectConfig`** / **`FFmpegSceneDetectOperator`** — detect scene boundaries in video
- **`FFmpegSliceConfig`** / **`FFmpegSliceOperator`** — slice video into segments
- Utilities: `attach_slice_hash()`, `keep_every_n()`

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
- Design doc: `docs/design/spark-source-v2.md`

#### `sources/anti_join.py`
- **`AntiJoinSourceConfig`** — wraps an inner source + exclude source with key columns
- **`AntiJoinSplitPlanner`** — injects payload key into data_range, validates key columns
- **`AntiJoinSourceOperator`** — DuckDB ANTI JOIN against lazily-fetched exclude table

#### `sources/union.py`
- **`UnionSourceConfig`** — wraps N source configs with schema validation
- **`UnionSplitPlanner`** — concatenates splits from all sub-sources with globally unique IDs
- **`UnionSourceOperator`** — dispatches reads to correct sub-source based on split_id index

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
- **`RetryableError`** — raised for retriable HTTP failures
- Config: `url`, `method`, `headers`, `timeout_secs`, `max_retries`

#### `http/circuit_breaker.py`
- **`CircuitBreaker`** — open on error rate threshold; auto-close after recovery window

#### `http/rate_limiter.py`
- **`RateLimiter`** — token-bucket algorithm; `requests_per_second: float`

### LLM Operator (`operators/llm/`)

#### `llm/operator.py`
- **`ExternalLLMOperatorConfig`** / **`ExternalLLMOperator`** — run LLM inference per split via external serve endpoint
- Direct mode (`base_url`) or ModelClient mode (`use_model_client` + registry)
- Config: `model_id`, `system_prompt`, `max_tokens`, `temperature`, `model_routing: Optional[ModelRoutingConfig]`

#### `llm/client.py`
- **`ChatCompletionsClient`** — HTTP client for OpenAI-compatible chat completions API
  - Retry logic, context-length error detection, configurable endpoints
- **`RoutedChatCompletionsClient`** — wraps `ChatCompletionsClient` with length-based model routing
  - `estimate_tokens(messages)` → token count estimate
  - `pick_model(messages)` → select model by token length
  - Context-length fallback: on `ContextLengthError`, retry with next larger model
- **`ModelRoutingConfig`** — routing config: list of `ModelRoute(model_id, max_tokens)`
- **`ContextLengthError`** — raised when input exceeds model context window

#### `llm/embedded.py`
- **`EmbeddedLLMOperatorConfig`** / **`EmbeddedLLMOperator`** — in-process inference (vLLM/SGLang offline); for single-node use
- Config: `model_source`, `engine` (vllm/sglang), `kv_cache_dtype`, `max_model_len`

#### `llm/utils.py`
- OpenAI message construction and image handling utilities
- `extract_prompts()`, `extract_messages()`, `extract_images()`
- `build_openai_image_content()`, `build_single_image_message()`, `build_multi_image_message()`
- `encode_image_base64()`, `extract_column()`

### Dedup Operators (`operators/dedup/`)

#### `dedup/bucket_union.py`
- **`BucketUnionOperatorConfig`** / **`BucketUnionOperator`** — sends (band_hash, doc_id) to UFClient for union
- Side-effect operator: returns None (union happens in UnionFind service)

#### `dedup/encoder.py`
- **`MinHashEncoderConfig`** / **`MinHashEncoderOperator`** — compute MinHash signatures
- xxhash64, numpy vectorization, word n-grams
- Output: `(doc_id, bucket_id, band_hash)`

#### `dedup/filter.py`
- **`DedupFilterOperatorConfig`** / **`DedupFilterOperator`** — filter records based on Union-Find cluster membership
- Two modes: lookup (via UFClient) or preloaded (cluster_table)

### MinHash (`operators/minhash/`)

#### `minhash/compute.py`
- **`MinHashComputeConfig`** / **`MinHashComputeOperator`** — compute MinHash signatures (shuffle-based)
- Config: `num_perm: int`, `ngram_size: int`
- `jaccard_similarity()` — utility for comparing signatures

---

## Runtime (`runtime/`)

### `runtime/ray_runner.py`
- **`RayJobRunner`** — top-level orchestrator
  - `run(job)` → start broker → create store → start stages → monitor → return
  - Manages `StageMaster` actors and stage ordering
- **`JobStatus`** — `job_id`, `is_running`, `stages`, `elapsed_time`, `error`

### `runtime/autoscaler.py`
- **`SimpleAutoscaler`** — queue-depth-driven worker scaling
  - Reads queue depth from `QueueStatsClient`
  - Calls `StageMaster.scale_up/down()` on each tick
  - Proactive resource check via `ray.available_resources()` before scale-up
- **`StageAutoscaleConfig`** — `enabled`, `check_interval_s`, `scale_up_lag_threshold`, `scale_down_lag_threshold`, `cooldown_s`, `max_scale_step`
- **`StageMetrics`** — per-stage metrics snapshot for scaling decisions

### `runtime/backpressure.py`
- **`JobBackpressureController`** — monitor queue depths across all stages
  - Sends `BackpressureSignal` to upstream `StageMaster` to pause/resume source

### `runtime/queue_stats.py`
- **`QueueStatsClient`** — collect queue stats from WorkQueue broker
- **`StageQueueConfig`** — queue name → stage mapping
- **`QueueRef`** — reference to a specific queue for stats collection

---

## Queue (`queue/`)

### `queue/workqueue.py`
- **`WorkQueueQueueClient`** — Python client for queue operations
  - `claim(queue, timeout)` → `WorkQueueRecord`
  - `ack_and_forward(msg_id, output_queue, payload)` — atomic
  - `ack_and_scatter(...)` — atomic ack + push to QueueGroup partitions
  - `claim_from_group(...)` — claim from QueueGroup with work-stealing
  - `nack(msg_id)` — re-enqueue
  - `state_get(ns, key)` / `state_put(ns, key, value)`
- **`WorkQueueBrokerManager`** — start/stop the Rust broker process

### `queue/workqueue_storage.py`
- **`WorkQueueStorageReader`** — read-only access to WorkQueue storage via PyO3 bindings

### `queue/backend.py`
- **`Record`** — generic queue record dataclass

---

## Serve Module (`serve/`)

### `serve/config.py`
- **`ModelConfig`** — `model_id`, `model_source`, `tensor_parallel_size`, `min_workers`, `max_workers`
  - `get_worker_resources()` — auto-infer GPU resources (fractional when TP=1 + low utilization)
- **`ServeAutoscaleConfig`** — `enabled`, `check_interval_seconds`, `scale_up_threshold`, `scale_down_idle_seconds`, `cooldown_seconds`, `max_scale_step`
- **`WorkerState`** (Enum) — LOADING, READY, STOPPED

### `serve/manager.py`
- **`ModelServiceManager`** (Ray actor) — control plane
  - `deploy_model(config)` → allocate GPUs → create pool → register
  - `undeploy_model(model_id)` → stop pool → release GPUs → deregister
  - `shutdown()` → undeploy all
  - Two modes: attached (default) / detached (`lifetime="detached"`)
  - `ModelServiceManager.connect()` — reconnect to detached manager
- **`create_manager()`** — factory function with optional `detached` and `broker_endpoint` params

### `serve/pool.py`
- **`ModelPool`** (plain object) — per-model worker pool
  - `scale_up(n)` / `scale_down(n)` → spawn/stop `InferenceWorker` actors
  - `get_worker_urls()` → list of active worker HTTP URLs
  - Built-in autoscaler as asyncio task with freeze/unfreeze

### `serve/worker.py`
- **`InferenceWorker`** (Ray actor) — runs vLLM or SGLang server
  - Subprocess management with PR_SET_PDEATHSIG, health polling, heartbeat
  - `get_node_id()` → node identification for allocator tracking

### `serve/allocator.py`
- **`GPUAllocator`** (plain object) — GPU bin-packing
  - `suggest_nodes(gpus, count)` — best-fit allocation with random tiebreaker
  - `suggest_workers_to_stop(ids, count)` — emptiest-node-first eviction
  - `plan_compaction(gpus_needed)` — fewest-eviction compaction planning
  - `reconcile(active_ids)` — prune stale placements

### `serve/registry.py`
- **`ModelRegistry`** (Ray Named Actor) — service discovery
  - Embedded aiohttp server for HTTP-based endpoint resolution
  - `register(model_id, urls)` / `deregister(model_id)`
  - `get_workers(model_id)` → list of URLs

### `serve/client.py`
- **`ModelClient`** (plain object) — HTTP client for model inference
  - Async endpoint discovery with caching via `EndpointCache`
- **`EndpointInfo`** — cached endpoint data with TTL
- **`EndpointCache`** — TTL-based cache for registry lookups

### `serve/fake_server.py`
- Standalone aiohttp server script for tests — no class, just `main()` with routes
- Monkeypatch target: replace `InferenceWorker` subprocess in tests

### `serve/union_find/config.py`
- **`UFClusterConfig`** — config for Union-Find service: `num_shards`, `checkpoint_interval`

### `serve/union_find/client.py`
- **`UFClient`** — batch operations against Union-Find shards
  - `batch_match_and_union()` — route by `band_hash % num_shards`, send to shards
  - `batch_find()` — resolve cluster membership

### `serve/union_find/manager.py`
- **`UnionFindServiceManager`** — lifecycle management for UF cluster
  - `deploy()`, `resolve_cross_shard()`, `export_clusters()`, `force_checkpoint()`, `shutdown()`

### `serve/union_find/shard.py`
- **`UFShard`** (Ray actor) — one shard of the distributed Union-Find
  - `band_hash_index` for fast lookup, cross-shard edge tracking
  - Checkpoint/restore via PayloadStore

---

## WebUI (`webui/`)

### `webui/app.py`
- **`create_webui_app()`** — FastAPI factory for WebUI server

### `webui/job_webui.py`
- **`JobWebUI`** — per-job monitoring interface; reads state from WorkQueue

### `webui/portal.py`
- **`NurionPortal`** — multi-job dashboard; lists active and historical jobs
- **`create_portal_app()`** / **`start_portal()`** — factory and launcher
- **`portal_exists()`** — check if portal is already running

### `webui/history_server.py`
- **`history_server()`** — click CLI command for historical job viewer

### `webui/runtime_server.py`
- **`EmbeddedWebUIServer`** — live metrics endpoint embedded in running job

### `webui/api/`
- REST endpoints: `jobs.py`, `stages.py`, `workers.py`, `events.py`, `lineage.py`, `serve.py`
- 12 endpoints total covering job/stage/worker/event/lineage/serve queries

### `webui/collectors/`
- Metric collectors: pull queue stats and actor status from Ray

### `webui/state/writer.py`
- **`WorkQueueStateWriter`** — writes job/stage/worker/event data to WorkQueue state store

### `webui/state/manager.py`
- **`JobStateManager`** — reads job/stage/worker/event/lineage data from WorkQueue storage

### `webui/state/schema.py`
- Key namespace helpers: `job_namespace`, `job_index_key`, `stage_key`, `worker_key`, `split_key`, `event_key`
- Serve keys: `serve_namespace`, `serve_model_key`, `serve_worker_key`, `serve_event_key`

---

## Utilities (`utils/`)

### `utils/logging.py`
- **`create_ray_logger(name)`** — Ray-compatible structured logger

### `utils/network.py`
- **`get_node_ip()`** — portable node IP detection
- Port discovery, address formatting helpers

### `utils/remote.py`
- S3 configuration helpers: `get_s3_storage_options()`, `get_lance_storage_options()`
- **`ensure_local_file()`** — download remote files to local cache
- **`restore_s3_object()`** — restore archived S3 objects from Glacier

### `utils/union_find.py`
- **`UnionFind`** — generic Union-Find data structure with path compression and rank
- Arrow serialization support for checkpoint/restore

---

## Other

### `compute/duckdb_engine.py`
- **`DuckDBEngine`** — SQL query execution via DuckDB; used in ETL transforms

### `state/`
- Operator persistent state management helpers

### `testing/fault_injection.py`
- **`InjectedFaultError`** — exception raised by fault injection
- **`check_fault()`** — check and trigger fault at a named point
- **`is_fault_injection_enabled()`** / **`reset_fault_injector()`** — control fault injection state
- Constants: `FAULT_BEFORE_PROCESS`, `FAULT_AFTER_PROCESS`, `FAULT_QUEUE_PRODUCE`, `FAULT_QUEUE_FETCH`, `FAULT_QUEUE_COMMIT`, etc.
