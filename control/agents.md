# Nurion Control Plane - Agent Notes

## Purpose
FastAPI control plane for Nurion. Provides task management, Kubernetes
integration, and data lake catalog APIs.

## Key Paths
- `control/api/routes/` HTTP routes
- `control/services/` business logic and integrations
- `control/models/` SQLAlchemy models
- `control/schemas/` Pydantic schemas
- `alembic/` database migrations
- `tests/` unit tests

## Dev Commands
- `uv venv` then `uv sync`
- `uv run uvicorn control.app:create_app --factory --reload`
- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run pytest tests/ -v --cov=control --cov-report=term-missing`

## CI Notes
PR titles must follow Conventional Commits: `<type>: <description>`.
