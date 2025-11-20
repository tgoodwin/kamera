# Context
- Knative controllers invoked by the explorer set timestamps via `time.Now()`/`metav1.Now()`, leading to nondeterministic state hashes between runs (e.g., `routingStateModified` on Revisions).
- We want deterministic explore outputs and converged-state dumps without re-implementing controller logic or post-hoc scrubbing every timestamp field.
- Sleeve object IDs were randomized; we already made them deterministic. Timestamps remain a source of nondeterminism.

# Goal
Provide a deterministic, depth-aware simulation clock that controllers use instead of `time.Now()`, so explore runs yield stable timestamps (e.g., per-depth fixed values) and reproducible converged states.

# Plan
- Add a `simtime` package that exposes `Now()` and a depth setter; `Now()` returns `base + depth*step` with a sane default (e.g., Unix epoch + 1h per depth).
- In `Explorer` reconcile paths, set the current depth in `simtime` before invoking a controller, restore afterward.
- Fork/replace the knative controller code we use, swapping `time.Now()`/`metav1.Now()` call sites to `simtime.Now()` (or a helper that returns `metav1.Time`), minimal blast radius.
- Wire a `replace` in go.mod to point at the patched knative fork.
- Re-run knative example/bench to confirm converged state hashes stabilize across runs.
- Use an automated AST rewriter to replace `time.Now` → `simclock.Now` and `metav1.Now` → `simclock.NowMeta` across the knative fork, adding the import, then `goimports` to clean up.

# Work Log
- TODO
