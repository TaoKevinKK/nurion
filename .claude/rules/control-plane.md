---
globs:
  - control/**
---

# Control Plane

- FastAPI + SQLAlchemy + AsyncPG + Alembic
- Routes: `control/api/routes/`, Schemas: `control/schemas/`, Logic: `control/services/`
- Models: `control/models/`, Migrations: `alembic revision --autogenerate`
- Run: `cd control && uv run uvicorn control.app:create_app --factory --reload`
- Test: `cd control && uv run pytest tests/ -v --cov=control`
