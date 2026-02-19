---
globs:
  - engine/tests/**
---

# Test Conventions

> Test structure, markers, and fixtures for the engine module.
> **Update this file when new test patterns or fixtures are introduced.**

---

## File Naming

| Type | Pattern | Example |
|---|---|---|
| Unit | `tests/<subdir>/test_<module>.py` | `tests/core/test_operator.py` |
| Integration | `tests/<subdir>/test_integration_<scope>.py` | `tests/test_integration_iceberg.py` |
| Workflow | `tests/test_<name>_workflow.py` | `tests/test_minhash_dedup_workflow.py` |
| Chaos | `tests/test_chaos_<scenario>.py` | `tests/test_chaos_worker_crash.py` |
| Stability | `tests/test_stability_<scenario>.py` | `tests/test_stability_long_run.py` |

---

## Markers

Declare at module level: `pytestmark = pytest.mark.<marker>`

| Marker | Default CI | When to use |
|---|---|---|
| `integration` | **excluded** | Requires external service (Iceberg, Lance, Spark, S3) |
| `distributed` | included | Multi-worker Ray pipeline test |
| `workflow` | included | End-to-end pipeline test |
| `slow` | included | Runtime > 30s |
| `chaos` | **not in CI** | Random failure injection |
| `benchmark` | **excluded** | Performance measurement |

---

## Fixtures

```python
# No-GPU Ray cluster (unit/workflow tests)
def test_pipeline(ray_cluster):
    job = Job(...)
    job.run()

# 16 fake GPUs (serve module tests)
def test_serve(ray_cluster_with_gpus):
    manager = create_manager()
    ...

# In-memory WorkQueue (fast, no disk)
def test_queue():
    queue = WorkQueue(db_path="memory://")
    # or pass workqueue_db_path="memory://" to fixtures that accept it
```

---

## Run Commands

```bash
# Default: unit + workflow + distributed (fast, no external deps)
cd engine && uv run pytest tests/ -v --tb=short -m "not integration"

# Integration tests (needs external services)
cd engine && uv run pytest tests/ -v -m "integration"

# Specific serve tests
cd engine && uv run pytest tests/serve/ -v

# All tests including chaos (local dev only)
cd engine && uv run pytest tests/ -v

# With coverage
cd engine && uv run pytest tests/ -v --cov=_internal --cov-report=term-missing -m "not integration"
```

---

## Writing Tests for New Operators

```python
# tests/operators/test_my_operator.py
import pytest
from nurion import MyOperator, MyOperatorConfig
from _internal.core.models import Split, SplitPayload
import pyarrow as pa

def make_runtime(job_id="test", stage_id="s0", worker_id="w0"):
    from _internal.core.operator import OperatorRuntime
    return OperatorRuntime(job_id=job_id, stage_id=stage_id, worker_id=worker_id)

def test_basic_transform():
    config = MyOperatorConfig(threshold=0.5)
    op = config.setup(make_runtime())
    split = Split(split_id="s1", stage_id="s0", data_range={})
    payload = SplitPayload(split_id="s1", data=pa.table({"x": [1, 2, 3]}), metadata={})
    result = op.process_split(split, payload)
    assert result is not None
```

---

## Control Plane Tests

```bash
cd control && uv run pytest tests/ -v --cov=control
```

Fixtures in `control/tests/conftest.py` — async test client, DB session.
