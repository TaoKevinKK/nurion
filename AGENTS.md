# Nurion - AI Agent Guidelines

## Project Overview

**Nurion** is a modern data platform monorepo combining orchestration and multimodal data processing.

| Component | Path | Description |
|-----------|------|-------------|
| **Engine** | `/engine` | Ray-based distributed streaming processing framework with multimodal operators |
| **Control Plane** | `/control` | FastAPI orchestration service (task management, K8s integration, data catalog) |
| **Shared Libs** | `/lib` | WorkQueue broker (Rust), RayDP Spark-on-Ray integration |

## Monorepo Structure

```
nurion/
├── engine/                   # Data processing framework
│   ├── nurion/               # Public API package
│   ├── _internal/            # Implementation (core, operators, runtime, serve, webui)
│   ├── workflows/            # Example workflows
│   └── tests/                # Unit/integration tests
├── docs/                     # Documentation
│   ├── design/               # Architecture decision records
│   ├── todo/                 # Feature tracking
│   └── lessons/              # Post-mortems and learnings
├── control/                  # Orchestration service
│   ├── control/              # FastAPI app (api/, models/, schemas/, services/)
│   ├── alembic/              # Database migrations
│   └── tests/
├── lib/                      # Shared libraries
│   ├── workqueue-rs/         # Rust WorkQueue broker + Python bindings
│   └── raydp/                # Spark on Ray (Python + JVM)
└── scripts/                  # CI/dev scripts
```

## Tech Stack

- **Languages**: Python 3.12+, Rust (WorkQueue), Java/Scala (Spark)
- **Runtime**: Ray (distributed computing), Apache Spark
- **API Framework**: FastAPI
- **Package Manager**: uv (monorepo workspace)
- **Code Quality**: Ruff (linting + formatting)
- **Testing**: pytest
- **CI/CD**: GitHub Actions

## Development Standards

### Commit Messages
Follow [Conventional Commits](https://conventionalcommits.org/): `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`

### Code Quality
- **Ruff**: `uv run ruff check .` and `uv run ruff format --check .` in each subproject
- **Type Hints**: Always include type annotations; packages are `py.typed`
- **Async by Default**: Use async/await for I/O operations

## Anti-Patterns

1. **Don't over-engineer**: Only implement current requirements, keep it simple
2. **Don't create unused APIs**: Only implement endpoints with actual callers
3. **Don't worry about pre-1.0 compatibility**: Breaking changes are acceptable before 1.0
4. **Don't skip types**: Add appropriate type annotations
5. **Don't hardcode config**: Use config classes and environment variables
6. **Don't import operator-layer types in core**: `_internal/core/` must never import from `_internal/operators/`. Use `OperatorConfig` hooks to let operators declare capabilities; core dispatches generically

## Preferred Patterns

1. **Protocol over ABC**: Use `typing.Protocol` for structural subtyping instead of `abc.ABC`
2. **Exception handling by layer**: Lower layers let exceptions propagate; only catch at boundaries (top-level loops, HTTP handlers)
3. **Dataclasses over plain dicts**: Use `@dataclass` for structured data
4. **Type hints always**: Better IDE support and documentation

## Agent Working Tips

1. **Design Docs**: Check `docs/design/` for architecture decisions
2. **TODO Tracking**: Check `docs/todo/` for implementation status
3. **Core Abstractions**: Start with `engine/_internal/core/` to understand the framework
4. **Examples**: Reference `engine/workflows/` and `engine/examples/`

## Subdirectory Guides

Each subproject has its own `AGENTS.md` with specific context:

- `engine/AGENTS.md` — Engine architecture, operator patterns, test conventions
- `control/AGENTS.md` — Control plane development, API patterns
- `lib/AGENTS.md` — Shared libraries overview
- `lib/workqueue-rs/AGENTS.md` — WorkQueue Rust development (O(1) I/O principles)

## Resources

- **Architecture Decisions**: `docs/design/`
- **Implementation Status**: `docs/todo/`
- **WebUI Guide**: `engine/_internal/webui/README.md`
- **CI Pipeline**: `.github/workflows/ci.yml`

---

*Last updated: 2026-02-14*
