# Solstice - Agent Notes

## Purpose
High-throughput batch processing with a streaming-style execution model and
multimodal operators.

## Key Paths
- `solstice/core/` Job, Stage, Operator, StageMaster/Worker
- `solstice/operators/` built-in sources, transforms, sinks
- `solstice/queue/` WorkQueue backend (embedded broker)
- `solstice/runtime/` Ray runner and autoscaling
- `workflows/` examples
- `tests/`, `design-docs/`, `todo/`
- `solstice/webui/` debug UI (see `solstice/webui/README.md`)

## Dev Commands
- `uv sync --dev`
- `uv run pytest tests/ -v --tb=short -m "not integration"`
- `uv run pytest tests/ -v --tb=short -m "integration"`
- `uv run ruff check solstice/`
- `uv run ruff format --check solstice/`

## Quick Notes
- Pull-based, queue-driven execution; workers are stateless.
- WorkQueue is embedded; use `workqueue_db_path="memory://"` for tests and
  `workqueue_db_path="file://..."` for local persistence.
