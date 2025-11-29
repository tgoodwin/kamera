# Knative Serving Example — Agent Guide

This example drives Kamera’s `Explorer` against a minimal Knative Serving setup. Agents can use it to exercise `explore.go` behavior without the TUI.

## Flags
- `-depth <int>`: maximum exploration depth (default: 10).
- `-timeout <duration>`: abort exploration after this duration. Accepts Go duration strings (e.g., `10s`, `2m`). Set to `0` to disable.
- `-log-level <level>`: `debug`, `info`, `warn`, or `error` (default: `info`).
- `-interactive <bool>`: launch the TUI inspector (`true` by default). Set `-interactive=false` for headless runs.
- `-dump-output <path>`: write converged + aborted states to a file (works even when `-interactive=false`).
- `-emit-stats`: record and print reconcile performance stats at the end.

## preparing knative code for deterministic simulation testing
Kamera's `Explorer` simulation searches the reconciliation execution space and identifies possible converged states. Because Knative code modifies Knative resources with nondeterministic values such as timestamps, these timestamp values can cause two semantically equivalent states to appear distinct, which is no good for our deterministic simulation strategy. So, we fix this by preprocessing Knative code + dependencies to be deterministic.
```
$REPO_ROOT/determinize_deps.sh -c ~/tmp -t ./examples/knative-serving -m knative.dev
```

## Suggested headless workflow
Run the explore routine using the "determinized" dependencies (from previous step) by setting `GOCACHE` and `GOMODCACHE` accordingly. The explorer can run for a while on deep searches, so use a timeout and dump results to inspect them offline:
```sh
# Abort after 60s, limit depth to 25, disable TUI, log at info, dump results.
GOCACHE=~/tmp/gocache \
GOMODCACHE=~/tmp/gomodcache \
go run ./examples/knative-serving \
  -depth 25 \
  -timeout 60s \
  -interactive=false \
  -dump-output /tmp/kamera-results.jsonl \
  -log-level info \
  -emit-stats
```

- Inspect `/tmp/kamera-results.jsonl` directly or feed it into your own tooling.
- Increase `-depth` gradually if the search is hitting max depth before finding any converged states. In this Knative example, convergence requires a depth of ~30.
- Keep a timeout on while iterating to avoid long-running, exhaustive explorations. The explore routine collects the convergence paths it finds along the way (DFS), so setting a timeout lets you inspect any convergence paths that were found without waiting for the routine to cover the entire state space. This faster feedback loop is useful when iterating / debugging.

## handling `AlreadyExists` errors
- Knative Service reconciliation may issue CREATEs for resources (e.g., Configuration/Route) that already exist; the API returns `AlreadyExists` to signal optimistic concurrency, not a fatal error.
- If the explorer aborts a branch on `AlreadyExists` from the ServiceReconciler, treat it as expected concurrency and adjust the harness/strategy to swallow the error so exploration can continue.
