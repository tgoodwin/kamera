# Examples — Agent Guide

This guide documents the common headless workflow for running Kamera examples and capturing output for debugging.

## Common flags
- `-depth <int>`: maximum exploration depth (default: 10).
- `-timeout <duration>`: abort exploration after this duration (e.g., `10s`, `2m`). Set `0` to disable.
- `-log-level <level>`: `debug`, `info`, `warn`, or `error` (default: `info`).
- `-interactive <bool>`: launch the inspector TUI (`true` by default). Set `-interactive=false` for headless runs.
- `-dump-output <path>`: write converged + aborted states to a dump file (works with `-interactive=false`).
- `-emit-stats`: record and print reconcile performance stats at the end, and embed them under `stats` in the `-dump-output` file.

## Determinize + cache notes (when needed)
Some examples depend on upstream code that introduces nondeterministic values (timestamps, IDs). To reduce noise:
- Use `determinize_deps.sh` when available to rewrite dependencies for deterministic simulation.
- Run with explicit `GOCACHE` and `GOMODCACHE` so builds use the determinized deps.

Example (adjust paths for the example you're running):
```sh
$REPO_ROOT/determinize_deps.sh -c ~/tmp -t ./examples/<example> -m <module-prefix>

GOCACHE=~/tmp/gocache \
GOMODCACHE=~/tmp/gomodcache \
go run . \
  -depth 25 \
  -timeout 60s \
  -interactive=false \
  -dump-output /tmp/kamera-results.jsonl \
  -log-level info \
  -emit-stats
```

## Suggested headless workflow
```sh
# Run headless and write a dump
GOCACHE=~/tmp/gocache \
GOMODCACHE=~/tmp/gomodcache \
go run . \
  -depth 25 \
  -timeout 60s \
  -interactive=false \
  -dump-output /tmp/kamera-results.jsonl \
  -log-level info \
  -emit-stats
```

Inspect the dump:
```sh
go run ./cmd/inspect --dump /tmp/kamera-results.jsonl --interactive=false
```

## Suggested debugging flow
When debugging:
1. Add targeted logging to verify suspected behavior.
2. Re-run headless and dump output to a tempfile.
3. Inspect the output before changing behavior.
