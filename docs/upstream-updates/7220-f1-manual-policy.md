# Draft update for crossplane/crossplane#7220 (F1 manual policy)

> **Not posted by sprint executor.** This is staged for tgoodwin's manual review and posting.

**Re-run date:** 2026-04-28
**Harness HEAD:** `crossplane-reeval` @ `89acd8a` (kamera repo, branched off `main`)
**Crossplane version under test:** v2.2.0
**Classification: still-reproduces.**

## Fidelity context

This re-run was performed on a fidelity-hardened Kamera harness composed of:
- `911b3bd` core resourceVersion tracking + 409-conflict checking (`pkg/tracecheck/manager.go`, `pkg/replay/preconditions.go`)
- `cb1c43e` detect `MergeFromWithOptimisticLock` and extract base RV
- `7ba2045` seed initial RVs and bump RV on CRD status patches
- `1def992` tests for the above
- `e4daf33` (cherry-pick of `6cd7396`) `Client.Delete` preserves finalizers/spec/status; new `GarbageCollectorReconciler` cascade-deletes dependents on REMOVE
- `b12542d` `init()` hard assertion of `mergeFromPatch` struct layout
- `89acd8a` deterministic initial RV seeding

These fixes close the C2 fidelity gap that forced the retraction of #7224 and add tighter API-server-semantics fidelity for any scenario that relies on optimistic concurrency or ownership preservation across DELETEs.

## Inputs

- `examples/crossplane/scenarios/workflow_crossplane-policy_manual-update-policy-composition-switch.json`
- `examples/crossplane/scenarios/workflow_crossplane-policy_manual-update-policy-composition-switch-stale.json`

## Run command

```
crossplane-harness -interactive=false -closed-loop=true -depth 100 \
  -inputs <scenario.json> -output /tmp/crossplane-reeval-89acd8a/f1/<variant>/
```

Note: `--closed-loop=true` was used to match the conditions under which the original baselines were generated. The plan's `--closed-loop=false` default produced trivial 1-state runs because the user action fired at depth 0 and no further reconciles were scheduled in direct mode.

## Campaign metrics

| Variant | Unique nodes | Total visits | Resource states | Max-depth aborted |
|---|---|---|---|---|
| primary | 42 | 210 | 21 | 2 |
| stale | 354 | 754 | 36 | 5 |

## Terminal-state evidence

- **primary:** 1 converged state with 0 differing objects.
- **stale:** 5 converged states with 0 differing objects, 9 identical.

## Trace evidence (the bug)

The dump for the primary run contains a state with both:
- `compositionRef.name = "widget-composition-beta"`
- `compositionRevisionRef.name = "widget-composition-alpha-rev-1"`

This is the F1 mismatched-refs pattern reported in the original issue. The reconciler proceeds through this state without raising an error, then later self-corrects to `compositionRevisionRef.name = "widget-composition-beta-c34ead1"`.

The hardened harness now reaches convergence (the original analysis showed cycling at max depth) because of no-op-write suppression and RV conflict checking reducing the API churn that drove F2-style cycling. This makes the post-bug behavior more visible but does not change the bug.

## Conclusion

The F1 bug — `APIRevisionFetcher.Fetch` performing a bare `Get(currentRevision)` under `compositionUpdatePolicy: Manual` with no validation that the revision belongs to the Composition pointed at by `compositionRef` — **still reproduces** identically on the fidelity-hardened harness. The bug is pure logic in `internal/controller/apiextensions/composite/api.go:161-167` and is independent of any harness-side fidelity considerations.

## Suggested comment text for #7220

> Re-validated on 2026-04-28 against a fidelity-hardened Kamera harness (RV conflict checking + DELETE/GC fix). The mismatched-refs trace pattern (`compositionRef=beta` + `compositionRevisionRef=alpha-rev-1` after the user UPDATE) reproduces identically. The bug is in `APIRevisionFetcher.Fetch`'s Manual-policy branch, which does a bare `Get(currentRevision.Name)` without verifying the revision's `crossplane.io/composition-name` label matches the Composition referenced by `compositionRef`. The Automatic path (lines 170-196 of `api.go`) already filters revisions by that label; mirroring the check in the Manual path would close the issue.
