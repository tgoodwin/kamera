# Architecture Decision Records

ADRs document significant architectural and design decisions.

## Index

| # | Decision | Status |
|---|----------|--------|

No ADRs have been added yet in this repository.

## When to Write an ADR

Write an ADR when making decisions that:
- Change the architecture or core design patterns
- Introduce new dependencies or technologies
- Affect multiple components or the public API
- Have long-term maintenance implications
- Future maintainers will ask "why did we do it this way?"

Not for: minor implementation details, bug fixes, refactoring,
configuration changes.

## How to Write an ADR

```bash
NEXT_NUM=0001  # or next sequence number you use
FILE="docs/adr/${NEXT_NUM}-short-title.md"
cp docs/adr/adr_tempalte.md "$FILE"
```

Edit the generated file: fill in **Context**, **Decision**, and
**Consequences**. Commit the ADR with the related code changes.

ADRs can reference each other:
- `Supersedes`: replaces an older decision
- `Amends`: modifies an earlier decision
