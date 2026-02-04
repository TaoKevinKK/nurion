# Aether - Agent Notes

## Purpose
FastAPI orchestration service for Nurion. Provides task management, Kubernetes
integration, and data lake catalog APIs.

## Key Paths
- `aether/api/routes/` HTTP routes
- `aether/services/` business logic and integrations
- `aether/models/` SQLAlchemy models
- `aether/schemas/` Pydantic schemas
- `alembic/` database migrations
- `tests/` unit tests

## Dev Commands
- `uv venv` then `uv sync`
- `uv run uvicorn aether.app:create_app --factory --reload`
- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run pytest tests/ -v --cov=aether --cov-report=term-missing`

## CI Notes
PR titles must follow Conventional Commits: `<type>: <description>`.
