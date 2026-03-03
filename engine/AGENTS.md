# Nurion Engine - Agent Notes

## Purpose

High-throughput batch processing with streaming-style execution and multimodal operators.
Ray-based distributed computing framework.

## Architecture

```
          RayJobRunner (orchestrator)
               |
    +----------+----------+
    |          |          |
StageMaster StageMaster StageMaster
 (Source)   (Transform)   (Sink)
    |          |          |
 Workers    Workers    Workers
    |          |          |
 [Queue] --pull--> [Queue] --pull--> [Queue]
```

**Key Components**:
- **Job**: DAG pipeline definition (configured via `JobConfig`)
- **Stage**: Processing step wrapping `OperatorConfig` with parallelism settings
- **StageMaster**: Manages output queue and worker pool (via WorkerManager, RecoveryManager, BackpressureMonitor)
- **StageWorker**: Stateless Ray Actor executing Operator logic
- **Operator**: Data processing logic (configured via `OperatorConfig` subclasses)
- **Split/SplitPayload**: Metadata and data for a unit of work
- **Queue Backend**: WorkQueue (embedded broker; `memory://` for tests, `file://` for persistence)

**Data Flow**: Pull-based, queue-driven. Workers pull from upstream queue, process, write to own queue. Natural backpressure via queue lag.

## Key Paths

- `_internal/core/` — Job, Stage, Operator, StageMaster, StageWorker, managers/
- `_internal/operators/` — sources/, sinks/, map.py, filter.py, http/, llm/, dedup/, minhash/, video.py
- `_internal/queue/` — WorkQueue backend (embedded broker)
- `_internal/runtime/` — RayJobRunner, autoscaler, backpressure
- `_internal/serve/` — Model serving (manager, pool, worker, allocator, client)
- `_internal/webui/` — Debug UI (see `_internal/webui/README.md`)
- `nurion/` — Public API package (`nurion/__init__.py` for all exports)
- `workflows/`, `examples/` — Example pipelines
- `../docs/design/` — Architecture decisions
- `../docs/todo/` — Implementation tracking

## Dev Commands

- `uv sync --dev`
- `uv run pytest tests/ -v --tb=short -m "not integration and not distributed and not chaos and not slow and not stability and not workflow"`
- `uv run pytest tests/ -v --tb=short -m "integration"`
- `uv run ruff check _internal/`
- `uv run ruff format --check _internal/`

## Test Conventions

### File Naming
- Unit tests: `tests/<subdir>/test_<module>.py`
- Integration tests: `tests/<subdir>/test_integration_<scope>.py`
- Workflow tests: `tests/test_<name>_workflow.py`
- Stability/chaos tests: `tests/test_stability_<scenario>.py`, `tests/test_chaos_<scenario>.py`

### Markers (defined in `pyproject.toml` + `tests/conftest.py`)
| Marker | Meaning | CI Behavior |
|--------|---------|-------------|
| `integration` | Requires external services | Excluded by default |
| `distributed` | Multi-worker Ray pipelines | — |
| `workflow` | End-to-end pipeline tests | — |
| `slow` | Takes >30s | — |
| `chaos` | Random failure injection | Not run in CI |
| `benchmark` | Performance benchmarks | Excluded by default |

### Fixtures
- `ray_cluster` (function-scoped) — Ray local cluster, no GPUs
- `ray_cluster_with_gpus` (function-scoped) — Ray local cluster with 16 fake GPUs

## Engine-Specific Patterns

1. **Operators are config-driven, stateless**: `Operator.__init__` only takes `OperatorConfig` + `OperatorRuntime`. Runtime context (`job_id`, `stage_id`, `worker_id`) lives in `OperatorRuntime`. No `set_*()` methods for injecting dependencies. See `engine/examples/` for full examples.

2. **No uncertain fallback patterns**: Logic should be deterministic — don't chain "try A, if not B, if not C". Use clear conditional selection instead.

3. **Minimize instance state (`self._*`)**: If a value is only used during init or can be recomputed, use local variables. Only store long-lived, cleanup-requiring state on `self`.

4. **Keep API responses minimal**: Return canonical data, let clients compute derived values. No redundant `is_running`/`is_finished` alongside `status`.

5. **No stats/counter fields**: Avoid `_total_xxx_count` instance state. Use logging for observability, not in-memory counters.

## Adding Features

- **New Operator**: Inherit `nurion.Operator`, implement `process_split()`. Optionally `checkpoint()`/`restore()`.
- **New Source**: Inherit `nurion.SourceOperator`, implement `plan_splits()`. Export in `__init__.py`.
- **New WebUI feature**: See `_internal/webui/README.md` for API, collector, and template patterns.

## Quick Notes

- Pull-based, queue-driven execution; workers are stateless.
- WorkQueue is embedded; use `workqueue_db_path="memory://"` for tests.
- Use `create_ray_logger()` for logging.
