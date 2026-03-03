# Nurion TODO Tracking

This directory tracks implementation status and strategic priorities.

## Directory Structure

```
todo/
├── README.md                               # This file
├── roadmap.md                             # Strategic roadmap (business-value-driven)
├── runtime-prod-hardening.md              # Runtime production hardening backlog
├── serve.md                               # Serve module (GPU scheduling, LLM ops)
├── dedup.md                               # Dedup operators and Union-Find service
└── dedup-and-fault-tolerance-deprecated.md # Archived historical TODO
```

## File Roles

- **`roadmap.md`** — Strategic layer: what to build and why, ordered by business value. Read this first.
- **Per-module TODOs** — Tactical layer: specific implementation items with acceptance criteria.
- **`../design/`** — Describes "how it should work" (design intent); TODO files describe "what's done" and "what's pending" (implementation status).

## Current TODO Files

| File | Description | Last Updated |
|------|-------------|--------------|
| [roadmap.md](./roadmap.md) | Strategic roadmap — phases, priorities, deprioritized items | 2026-03-02 |
| [runtime-prod-hardening.md](./runtime-prod-hardening.md) | Runtime hardening backlog (scale, correctness, operability) | 2026-03-02 |
| [serve.md](./serve.md) | Serve module — GPU scheduling, model routing, LLM operators | 2026-03-02 |
| [dedup.md](./dedup.md) | Dedup operators and Union-Find service tracking | 2026-03-02 |
| [dedup-and-fault-tolerance-deprecated.md](./dedup-and-fault-tolerance-deprecated.md) | Archived old CC/legacy fault-tolerance notes | 2026-02-23 (deprecated) |

## Conventions

- Start from `roadmap.md` to understand priorities before diving into module TODOs
- Use `[x]` for completed items, `[ ]` for pending
- Cross-reference between files when items span modules (e.g., shuffle routing appears in both `dedup.md` and `runtime-prod-hardening.md`)
- Deprioritized items stay visible (with reasoning) rather than being deleted
