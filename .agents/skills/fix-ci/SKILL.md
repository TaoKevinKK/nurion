---
name: fix-ci
description: Fix CI-discovered code quality (ruff) and mypy type check failures for the engine and/or control modules.
---

# Fix CI Code Quality and Type Checks

## Exact CI commands

```bash
# Lint + format (control)
cd control && uv run ruff check . && uv run ruff format --check .

# Lint + format (engine)
cd engine && uv run ruff check . && uv run ruff format --check .

# mypy (control)
cd control && uv run mypy control/

# mypy (engine)
cd engine && uv run mypy _internal/ nurion/
```

## Workflow

1. **Determine scope** — which modules have changes (engine, control, or both)
2. **Auto-fix ruff** — `ruff check --fix` then `ruff format`
3. **Run mypy** — read all errors, then fix them in source files
4. **Re-run all checks** — confirm fully clean before finishing

### Commands to fix and verify

```bash
# Fix ruff (engine)
cd engine && uv run ruff check . --fix && uv run ruff format .

# Fix ruff (control)
cd control && uv run ruff check . --fix && uv run ruff format .

# Check mypy (engine)
cd engine && uv run mypy _internal/ nurion/

# Check mypy (control)
cd control && uv run mypy control/
```

## Key rules

- Engine source lives in `engine/_internal/` — never `engine/engine/`
- mypy is gradual (`disallow_untyped_defs = false`); tests are excluded from mypy
- Prefer fixing the actual type error over adding `# type: ignore`
- Only use `# type: ignore[code]` as last resort (e.g. untyped third-party stubs)
- ruff: line-length = 100, target-version = py313
- After all fixes, every check must exit 0 with no output
