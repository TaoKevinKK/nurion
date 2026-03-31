# Claude Code Instructions

Read `AGENTS.md` for full project context (architecture, patterns, conventions).

## Quick Reference

- **Monorepo**: `engine/` (Ray processing), `control/` (FastAPI), `lib/` (anvil-rs, raydp)
- **Engine source**: `engine/_internal/` (NOT `engine/engine/`)
- **Public API**: `engine/nurion/__init__.py`
- **Package manager**: uv
- **Linting**: `cd engine && uv run ruff check _internal/`
- **Tests**: `cd engine && uv run pytest tests/ -v --tb=short -m "not integration and not distributed and not chaos and not slow and not stability and not workflow"`

## Rules

- Communicate in Chinese unless user switches to English
- Commit messages: Conventional Commits (`feat:`, `fix:`, `refactor:`, etc.)
- Don't over-engineer; only implement what's requested
- Operators are config-driven and stateless (`OperatorConfig` + `OperatorRuntime`)
- Anvil hot paths must be O(1) — never scan
- Check `docs/design/` before proposing architectural changes
