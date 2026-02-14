# Nurion Control Plane - Agent Notes

## Purpose

FastAPI control plane for Nurion. Provides task management, Kubernetes
integration, and data lake catalog APIs.

## Key Paths

- `control/api/routes/` — HTTP routes
- `control/services/` — Business logic and integrations
- `control/models/` — SQLAlchemy models
- `control/schemas/` — Pydantic schemas
- `alembic/` — Database migrations
- `tests/` — Unit tests

## Dev Commands

- `uv sync --dev`
- `uv run uvicorn control.app:create_app --factory --reload`
- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run pytest tests/ -v --cov=control --cov-report=term-missing`

## Adding Features

- **New API Endpoint**: Routes in `control/api/routes/`, schemas in `control/schemas/`, logic in `control/services/`
- **New DB Model**: Define in `control/models/`, create migration with `alembic revision --autogenerate`

## CI Notes

PR titles must follow Conventional Commits: `<type>: <description>`.
