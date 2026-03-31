# lib - Agent Notes

## Purpose
Shared libraries used by the Nurion runtime and related tooling.

## Subprojects
- `raydp/` Spark-on-Ray integration (Python and JVM)
- `anvil-rs/` Rust work queue storage and server
  - See `anvil-rs/AGENTS.md` for detailed constraints

## Dev Notes
Each subproject has its own build system and `pyproject.toml` or `Cargo.toml`.
Prefer the local README or `AGENTS.md` for specific setup and commands.
