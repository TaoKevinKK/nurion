# lib - Agent Notes

## Purpose
Shared libraries used by Solstice and related tooling.

## Subprojects
- `raydp/` Spark-on-Ray integration (Python and JVM)
- `workqueue-rs/` Rust work queue storage and server
  - See `workqueue-rs/agents.md` for detailed constraints

## Dev Notes
Each subproject has its own build system and `pyproject.toml` or `Cargo.toml`.
Prefer the local README or `agents.md` for specific setup and commands.
