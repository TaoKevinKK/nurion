# File Navigation Guide

> Quick reference: "I need to do X" → "go to file Y, follow pattern Z"
> **Update this file when module layout or common task patterns change.**

---

## Finding Things Fast

| I need to... | File(s) | Notes |
|---|---|---|
| Change operator base class / contract | `engine/_internal/core/operator.py` | Also update `architecture.md` |
| Change `process_split` signature | `core/operator.py` + `core/stage_worker.py` | Both must stay in sync |
| Add a transform operator | `_internal/operators/<name>.py` | Use `@operator(Config)` decorator |
| Add a source | `_internal/operators/sources/<name>.py` | Config must implement `create_source()` |
| Add a sink | `_internal/operators/sinks/<name>.py` | Config may implement `create_sink_committer()` |
| Change split routing / stage lifecycle | `core/stage_master.py` | See architecture.md for StageMaster lifecycle |
| Change claim-process-ack loop | `core/stage_worker.py` | Also update `architecture.md` data flow |
| Change worker scaling | `runtime/autoscaler.py` | Uses `QueueStatsClient` for decisions |
| Change backpressure monitoring | `runtime/backpressure.py` | Sends `BackpressureSignal` to `StageMaster` |
| Change job orchestration / stage ordering | `runtime/ray_runner.py` | |
| Change checkpoint / recovery | `core/fault_tolerance.py` + `core/managers/recovery_manager.py` | See design doc |
| Change how workers spawn/die | `core/managers/worker_manager.py` | |
| Change source lifecycle | `core/managers/source_manager.py` | |
| Change sink commit loop | `core/managers/sink_manager.py` | |
| Change public API exports | `engine/nurion/__init__.py` | Only file users import from |
| Change WorkQueue hot path | `lib/workqueue-rs/src/queue.rs` | Must stay O(1) — see workqueue.md |
| Change WorkQueue state ops | `lib/workqueue-rs/src/state.rs` | Atomic with ack |
| Change WorkQueue GC / recovery | `lib/workqueue-rs/src/gc.rs`, `recovery.rs` | O(n) is OK here |
| Change serve model lifecycle | `_internal/serve/manager.py` | |
| Change serve GPU allocation | `_internal/serve/allocator.py` | Bin-packing logic |
| Change serve service discovery | `_internal/serve/registry.py` | Named actor |
| Change serve client routing | `_internal/serve/client.py` | Round-robin LB |
| Add a FastAPI endpoint (control) | `control/control/api/routes/` + schema + service | See control-plane.md |
| Add a DB model (control) | `control/control/models/` + alembic migration | |
| Understand full module layout | `engine/_internal/INDEX.md` | Read this first |
| Understand execution pipeline | `.claude/rules/architecture.md` | Diagrams + key invariants |
| Find architecture decision | `docs/design/*.md` | Before proposing changes |

---

## Common Tasks: Step by Step

### Add a New Transform Operator

1. Create `engine/_internal/operators/<name>.py`
   - `@dataclass class <Name>Config(OperatorConfig)` — all settings as fields
   - `@operator(<Name>Config) class <Name>(Operator)` — decorator binds Config ↔ Operator
   - Implement `process_split(self, split, payload) -> PayloadResult`
   - Override `get_merge_upstream()` if batching benefits performance
2. Export: `engine/nurion/__init__.py` — add import + `__all__` entry
3. Test: `engine/tests/operators/test_<name>.py`
4. Update: `engine/_internal/INDEX.md` (Operators section)

### Add a New Source

1. Create `engine/_internal/operators/sources/<name>.py`
   - Config: inherit `OperatorConfig`, implement `create_source() -> SourceStrategy`
   - Operator: inherit `SourceOperator`, implement `plan_splits() -> list[Split]`
   - Use `@operator(Config)` decorator
2. Export: `engine/nurion/__init__.py`
3. Integration test: `engine/tests/test_integration_<name>.py` with `pytestmark = pytest.mark.integration`
4. Update: `engine/_internal/INDEX.md` (Sources section)

### Add a New Sink

1. Create `engine/_internal/operators/sinks/<name>.py`
   - Config: `get_merge_upstream()` → larger N for bigger output fragments
   - Config: optionally `create_sink_committer()` if two-phase commit needed
   - Operator: return `RawOutputBytes(metadata)` to push commit message downstream
2. If two-phase: create `sinks/<name>_commit.py` for the commit stage
3. Export both in `nurion/__init__.py`
4. Update: `engine/_internal/INDEX.md`

### Add a Control Plane Endpoint

1. Route: `control/control/api/routes/<name>.py` — FastAPI router
2. Schema: `control/control/schemas/<name>.py` — Pydantic request/response models
3. Service: `control/control/services/<name>_service.py` — business logic
4. If DB: `control/control/models/<name>.py` → `cd control && alembic revision --autogenerate -m "add <name>"`
5. Register: add `include_router(...)` in `control/control/app.py`
6. Test: `control/tests/test_<name>_api.py`

### Add a Serve Feature

1. Config change → `engine/_internal/serve/config.py`
2. Lifecycle (deploy/undeploy) → `serve/manager.py`
3. Worker scaling → `serve/pool.py`
4. GPU allocation → `serve/allocator.py`
5. Service discovery → `serve/registry.py`
6. Test: use `ray_cluster_with_gpus` fixture; monkeypatch `InferenceWorker` with `FakeInferenceServer`

### Modify WorkQueue Hot Path

1. Identify the operation in `lib/workqueue-rs/src/queue.rs` or `state.rs`
2. **Verify O(1) complexity**: must not introduce scans; use counters in `meta.rs`
3. Atomicity: use `WriteBatch` for multi-key updates
4. Rebuild: `cd lib/workqueue-rs && cargo build` + Python bindings
5. See `lib/workqueue-rs/AGENTS.md` for full constraints

### Debug a Stage That's Stuck

1. Check WebUI (`JobWebUI`) → stage status, pending/claimed counts
2. Check claimed messages: `get_queue_stats()` → high `claimed_count` suggests stuck workers
3. Recovery: `RecoveryManager` re-enqueues on worker death (Ray actor crash)
4. Backpressure: `BackpressureController` may have paused source — check `BackpressureSignal`

---

## Running Commands

```bash
# Engine: unit + workflow tests (fast, no Ray cluster, no external deps)
cd engine && uv run pytest tests/ -v --tb=short -m "not integration and not distributed and not chaos and not slow and not stability and not workflow"

# Engine: distributed tests (needs Ray cluster, slow)
cd engine && uv run pytest tests/ -v -m "distributed"

# Engine: integration tests (needs external services)
cd engine && uv run pytest tests/ -v -m "integration"

# Engine: serve tests only
cd engine && uv run pytest tests/serve/ -v

# Engine: lint + format check
cd engine && uv run ruff check _internal/ && uv run ruff format --check _internal/

# Control: run dev server
cd control && uv run uvicorn control.app:create_app --factory --reload

# Control: tests
cd control && uv run pytest tests/ -v --cov=control

# WorkQueue: build Rust
cd lib/workqueue-rs && cargo build --release
```

---

## Design Doc Quick Reference

Check before proposing architectural changes:

| Topic | File |
|---|---|
| Checkpoint & recovery | `docs/design/checkpoint-and-recovery.md` |
| Worker auto-scaling | `docs/design/dynamic-worker-scaling.md` |
| GPU scheduling | `docs/design/gpu-scheduling-and-routing.md` |
| LLM inference | `docs/design/llm-inference.md` |
| Exactly-once semantics | `docs/design/exactly-once-semantics.md` |
| WorkQueue semantics | `docs/design/workqueue-semantics.md` |
| WorkQueue redesign | `docs/design/work-queue-redesign.md` |
| MinHash dedup | `docs/design/minhash-dedup.md` |
| Backpressure | `docs/design/deprecated/partition-backpressure-improvements.md` |
| Multi-upstream join | `docs/design/multi-upstream-join.md` |
| WebUI v1 / v2 | `docs/design/webui.md`, `webui-api-v2.md` |
| Spark Source V2 | `docs/design/spark-source-v2.md` |

---

## Memory Update Trigger

| Code change | Update |
|---|---|
| Add / remove / rename a module | `engine/_internal/INDEX.md` |
| Change operator contract (`process_split` sig, decorators) | `operator-patterns.md` + `architecture.md` |
| Change execution pipeline or actor model | `architecture.md` |
| New common task pattern | This file (`file-navigation.md`) |
| New test fixture or marker | `test-conventions.md` |
| Serve component responsibilities change | `serve-module.md` |
| WorkQueue complexity or schema change | `workqueue.md` |
