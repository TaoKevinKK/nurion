# Nurion Runtime - Agent Notes

## Purpose
High-throughput batch processing with a streaming-style execution model and
multimodal operators.

## Key Paths
- `engine/core/` Job, Stage, Operator, StageMaster/Worker
- `engine/operators/` built-in sources, transforms, sinks
- `engine/queue/` WorkQueue backend (embedded broker)
- `engine/engine/` Ray runner and autoscaling
- `workflows/` examples
- `tests/`, `design-docs/`, `todo/`
- `engine/webui/` debug UI (see `engine/webui/README.md`)

## Dev Commands
- `uv sync --dev`
- `uv run pytest tests/ -v --tb=short -m "not integration"`
- `uv run pytest tests/ -v --tb=short -m "integration"`
- `uv run ruff check engine/`
- `uv run ruff format --check engine/`

## Quick Notes
- Pull-based, queue-driven execution; workers are stateless.
- WorkQueue is embedded; use `workqueue_db_path="memory://"` for tests and
  `workqueue_db_path="file://..."` for local persistence.
