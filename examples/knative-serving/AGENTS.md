# Knative Serving Example — Agent Guide

This example drives Kamera’s `Explorer` against a minimal Knative Serving setup. Agents can use it to exercise `explore.go` behavior without the TUI.

## Flags
- `-depth <int>`: maximum exploration depth (default: 10).
- `-timeout <duration>`: abort exploration after this duration. Accepts Go duration strings (e.g., `10s`, `2m`). Set to `0` to disable.
- `-log-level <level>`: `debug`, `info`, `warn`, or `error` (default: `info`).
- `-interactive <bool>`: launch the TUI inspector (`true` by default). Set `-interactive=false` for headless runs.
- `-dump-output <path>`: write converged + aborted states to a file (works even when `-interactive=false`).
- `-emit-stats`: record and print reconcile performance stats at the end.

## Suggested headless workflow
The explorer can run for a while on deep searches. Use a timeout and dump results to inspect them offline:
```sh
# Abort after 60s, limit depth to 25, disable TUI, log at info, dump results.
go run ./examples/knative-serving \
  -depth 25 \
  -timeout 60s \
  -interactive=false \
  -dump-output /tmp/kamera-results.jsonl \
  -log-level info \
  -emit-stats
```

- Inspect `/tmp/kamera-results.jsonl` directly or feed it into your own tooling.
- Increase `-depth` gradually; deeper searches grow quickly. In this Knative example, convergence requires a depth of ~30.
- Keep a timeout on while iterating to avoid long-running explorations.
