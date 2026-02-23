# Nurion Runtime TODO Tracking

This directory tracks implementation status of features.

## Directory Structure

```
todo/
├── README.md                               # This file
├── runtime-prod-hardening.md               # Runtime production hardening backlog
├── dedup.md                                # Active dedup/union-find tracking
├── webui.md                                # WebUI tracking
└── dedup-and-fault-tolerance-deprecated.md # Archived historical TODO (deprecated)
```

## File Format Guidelines

Each feature TODO file should include:

### 1. Completed (✅)
List implemented features using `[x]` markers.

### 2. In Progress (🚧)
Features currently under development.

### 3. TODO (📋)
Categorized by priority:
- **High Priority** - Blocking other work or urgently needed
- **Medium Priority** - Important but not urgent
- **Low Priority** - Nice-to-have

### 4. Design Changes (🔄)
Document differences from `design-docs/`:
- What changed
- Why it changed
- Whether design doc needs update

### 5. Next Iteration Suggestions (📝)
Guidance and priorities for next development cycle.

## Usage Guidelines

### When to Create a TODO File

- Starting a new feature
- Discovering discrepancies between design doc and implementation
- Need to track many pending items

### When to Update a TODO File

- After completing a feature, move to "Completed"
- When discovering new TODOs, add them
- When design changes occur, document them

### Relationship with design-docs

- `design-docs/` - Describes "how it should work" (design intent)
- `todo/` - Describes "what's done" and "what's pending" (implementation status)

Sync periodically. When implementation diverges from design:
1. Document the change in TODO file
2. Evaluate if design doc needs update
3. If design is better, implement per design; if implementation is better, update design doc

## Current TODO Files

| File | Description | Last Updated |
|------|-------------|--------------|
| [runtime-prod-hardening.md](./runtime-prod-hardening.md) | Runtime production hardening backlog (1B+/1000+ scale) | 2026-02-23 |
| [dedup.md](./dedup.md) | Dedup operators and Union-Find service tracking | 2026-02-23 |
| [webui.md](./webui.md) | WebUI feature tracking | 2026-02-23 |
| [dedup-and-fault-tolerance-deprecated.md](./dedup-and-fault-tolerance-deprecated.md) | Archived old CC/legacy fault-tolerance notes | 2026-02-23 (deprecated cleanup) |