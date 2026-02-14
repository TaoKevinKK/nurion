---
globs:
  - engine/tests/**
---

# Test Conventions

- Unit: `tests/<subdir>/test_<module>.py`, Integration: `tests/<subdir>/test_integration_<scope>.py`
- Workflow: `tests/test_<name>_workflow.py`, Chaos: `tests/test_chaos_<scenario>.py`
- Markers: `integration` (excluded by default), `distributed`, `workflow`, `slow`, `chaos` (not in CI), `benchmark` (excluded)
- Use `pytestmark = pytest.mark.<marker>` at module level
- Fixtures: `ray_cluster` (no GPU), `ray_cluster_with_gpus` (16 fake GPUs for serve tests)
- WorkQueue: `workqueue_db_path="memory://"` for in-memory tests
- Run: `cd engine && uv run pytest tests/ -v --tb=short -m "not integration"`
