# Nurion TODO Tracking

This directory tracks implementation status and strategic priorities.

## Directory Structure

```
todo/
├── README.md                               # This file
├── 01-roadmap.md                          # Strategic roadmap (business-value-driven)
├── 02-serve.md                            # Serve module (GPU scheduling, LLM ops)
├── 03-dedup.md                            # Dedup operators and Union-Find service
├── 04-runtime-prod-hardening.md           # Runtime production hardening backlog
└── 05-xenna-inspirations.md               # Cosmos-Xenna design inspirations
```

## File Roles

- **`01-roadmap.md`** — Strategic layer: what to build and why, ordered by business value. Read this first.
- **Per-module TODOs** — Tactical layer: specific implementation items with acceptance criteria.
- **`../design/`** — Describes "how it should work" (design intent); TODO files describe "what's done" and "what's pending" (implementation status).

## Current TODO Files

| # | File | Description | Created | Last Updated |
|---|------|-------------|---------|--------------|
| 01 | [01-roadmap.md](./01-roadmap.md) | Strategic roadmap — phases, priorities, deprioritized items | 2026-02 | 2026-03-24 |
| 02 | [02-serve.md](./02-serve.md) | Serve module — GPU scheduling, model routing, LLM operators | 2026-02 | 2026-03-24 |
| 03 | [03-dedup.md](./03-dedup.md) | Dedup operators and Union-Find service tracking | 2026-02 | 2026-03-06 |
| 04 | [04-runtime-prod-hardening.md](./04-runtime-prod-hardening.md) | Runtime hardening backlog (scale, correctness, operability) | 2026-03 | 2026-03-06 |
| 05 | [05-xenna-inspirations.md](./05-xenna-inspirations.md) | Cosmos-Xenna design inspirations (GPU scheduling, two-level init, autoscaling) | 2026-03 | 2026-03-24 |

## Conventions

- Start from `01-roadmap.md` to understand priorities before diving into module TODOs
- Use `[x]` for completed items, `[ ]` for pending
- Cross-reference between files when items span modules (e.g., shuffle routing appears in both `03-dedup.md` and `04-runtime-prod-hardening.md`)
- Deprioritized items stay visible (with reasoning) rather than being deleted
- New files use next available sequence number (e.g., `06-*.md`)
