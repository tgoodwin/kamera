# Draft update for crossplane/crossplane#7222 (F3 composition deletion)

> **Not posted by sprint executor.** This is staged for tgoodwin's manual review and posting.

**Re-run date:** 2026-04-28
**Trace audit date:** 2026-04-28 (post-rewrite)
**Harness HEAD:** `crossplane-reeval` @ `89acd8a`
**Crossplane version under test:** v2.2.0
**Classification: shifts (still-reproduces, with narrowed error-family coverage).**

> **Audit history.** This draft was originally framed as a "provisional retraction with a maintainer ask" based on hash-only evidence (the new XWidget hashes didn't match the baseline). The R-4 trace audit ([`../crossplane-audits/R4-f3-trace-audit.md`](../crossplane-audits/R4-f3-trace-audit.md)) refuted the retraction: all 4 unique XWidget hashes in the hardened-harness terminals carry the same `Synced=False / reason=ReconcileError` condition with the original F3 `errSelectComp` message ("cannot select Composition: no compatible Compositions found"). The hash drift is cosmetic (`metadata.generation` and `metadata.resourceVersion` re-sequencing under the new GC reconciler), not a change in the underlying error pathology.

## Fidelity context

The two fidelity changes most relevant to F3:
- `e4daf33` (cherry-pick of `6cd7396`) — `Client.Delete` reads current object state before setting `deletionTimestamp`, preserving existing finalizers/spec/status (matches real K8s DELETE which patches metadata, not replaces). Adds `GarbageCollectorReconciler` for cascade-deletion of dependents on REMOVE; REMOVE events queue the GC controller.
- `911b3bd / cb1c43e / 7ba2045 / 1def992` — RV tracking + conflict checking; reduces API write churn that previously drove cycling.

Both changes alter the post-DELETE state space, but per the trace audit neither closes the F3 pathology: the XR ends up in the same `errSelectComp` `ReconcileError` permanent loop.

## Inputs and runs

| Input | Output dir | Wall-clock |
|---|---|---|
| `workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json` | `f3/hypothesis-1/` | ~5s |
| `workflow_crossplane-deletion_composition-deleted-while-xr-bound.json` | `f3/primary/` | ~1s |
| `workflow_crossplane-deletion_xr-deleted-with-active-composition.json` | `f3/related-xr-delete/` | ~1s |

Run command pattern (closed-loop):
```
crossplane-harness -interactive=false -closed-loop=true -depth 100 \
  -inputs <scenario.json> -output <dump-dir>/
```

## Campaign metrics

| Variant | Unique | Total | Resource states | Max-depth aborted |
|---|---|---|---|---|
| **hypothesis-1 baseline** | 44 | 324 | 8 | 3 |
| **hypothesis-1 hardened** | 86 | 622 | 13 | 6 |
| primary hardened | 88 | 259 | 24 | 2 |
| related-xr-delete hardened | 182 | 548 | 30 | 4 |

State space roughly doubled in hypothesis-1 — the `GarbageCollectorReconciler` and DELETE-state preservation explore more orderings.

## Terminal-state evidence

### hypothesis-1 (the primary scenario)

`kamera analyze diff` output:

```
6 converged states with 1 differing object(s), 3 identical

XWidget/default/example:
  State aborted-2azhpneh: 302b727
  State aborted-2uhk5wqu: 6b6a9d3
  State aborted-2uxzakid: 4390fb0
  State aborted-3ehm2of7: 855c129
  State aborted-3jxjqcsk: 6b6a9d3
  State aborted-hz2c0tz0: 855c129
```

**Per-hash trace audit** (R-4):

| State ID(s) | XWidget hash | `Synced` condition | `Ready` | `compositionRef` / `compositionRevisionRef` | `metadata.generation` / `resourceVersion` |
|---|---|---|---|---|---|
| `aborted-2azhpneh` | `302b727` | **False / ReconcileError** "cannot select Composition: no compatible Compositions found" | True / Available | both cleared | 3 / 12 |
| `aborted-2uhk5wqu`, `aborted-3jxjqcsk` | `6b6a9d3` | (same) | True / Available | both cleared | 3 / 13 |
| `aborted-2uxzakid` | `4390fb0` | (same) | True / Available | both cleared | 4 / 19 |
| `aborted-3ehm2of7`, `aborted-hz2c0tz0` | `855c129` | (same) | True / Available | both cleared | 4 / 20 |

All 4 unique hashes encode the **same** ReconcileError condition. The hash differences are entirely explained by `metadata` drift across paths.

`ConfigMap/default/xr-config` is identical across all 6 terminals (single hash `709d71b`); `CompositionRevision` is identical (single hash `f353fca`); `Composition` is absent (the trigger).

### primary

1 converged state, 0 differing objects across both reference and rerun phases. The cycling-at-max-depth-without-firing-DELETE behavior (baseline never fired the external DELETE) is gone — the run now reaches convergence after the DELETE. This is a fidelity-driven change, not bug resolution.

### related-xr-delete

3 converged states with 0 differing objects, 6 identical. XR deletion path remains clean (consistent with the original analysis).

## What shifted vs the original report

| Original report claim | Hardened-harness result |
|---|---|
| 3 distinct aborted XWidget hashes (`93d750b`, `9dc61c9`) | 4 distinct aborted hashes (`302b727`, `6b6a9d3`, `4390fb0`, `855c129`); all carry `errSelectComp` ReconcileError. |
| `ConfigMap/default/xr-config` differs across orderings (`709d71b` vs missing) | ConfigMap identical across all 6 terminals (`709d71b`). |
| Two error families: `errFetchComp` ("Composition not found") and `errSelectComp` ("no compatible Compositions found") | Only `errSelectComp` reaches a max-depth terminal in this dump. `errFetchComp` does not appear anywhere. |
| Permanent ReconcileError loop with no self-recovery | **Confirmed.** All 4 unique terminals carry `Synced=False / ReconcileError`. The XR has stale `Ready=True` (a separate finding consistent with R-3's F6 stale-Ready analysis — `Ready` is a system condition not cleared on the error path). |

## Interpretation

The bug remains real and reproduces on the fidelity-hardened harness with the same error message family. What changed:

1. The `errFetchComp` ordering (CleanupReconciler-first → `Composition not found`) no longer reaches a max-depth terminal within the depth budget. The new GC controller reshapes the ordering space such that only `errSelectComp` (CompositeReconciler-first) terminals are explored.
2. The `ConfigMap` divergence collapsed because the dependent is no longer GC'd in any path — consistent with R-8's note that the harness GC behavior matches Background-style cascade for single-owner dependents and the dependent here has a single owner that hasn't been deleted.
3. The XR ends up with `compositionRef` and `compositionRevisionRef` *cleared* in all paths (visible in the trace) rather than orphaned with stale references — this is a harness-side change driven by the new RV+DELETE semantics, but it does not close the bug; the cleared-ref XR still hits `errSelectComp` because the Cleanup path doesn't re-select an alternative Composition.

The XR-side permanent error loop is the bug. The above shifts are evidence-shape changes, not bug resolution.

## Recommendation

**Frame as "shifts" with refreshed evidence.** Drop the retraction-candidate framing entirely — it was based on hash-only evidence and is contradicted by the trace audit.

The strongest action item for the maintainer remains: there is no automatic recovery path on the XR when its Composition is deleted. `compositionRef` is cleared but `SelectComposition` finds no compatible Composition, so the XR stays in `Synced=False / ReconcileError` indefinitely. The TODO at `api.go:251-252` ("need to block the deletion of composition via finalizer once it's selected") in the original report still applies.

## Suggested comment text for #7222

> Re-validated on 2026-04-28 against a fidelity-hardened Kamera harness with two changes specifically relevant to this issue: (a) the simulated `Client.Delete` now preserves finalizers/spec/status across external user DELETEs (previously a Kamera fidelity gap), and (b) a new GC controller cascade-deletes dependents on REMOVE, modeling production behavior the original analysis explicitly noted was missing. After a trace-level audit of all 6 terminal states:
>
> **The bug still reproduces.** All 4 unique XWidget terminal hashes (`302b727`, `6b6a9d3`, `4390fb0`, `855c129`) encode the same `Synced=False / reason=ReconcileError` condition with message `"cannot select Composition: no compatible Compositions found"` — the original `errSelectComp` family. The hash drift from the original baseline (`93d750b` / `9dc61c9`) is cosmetic, driven by `metadata.generation` and `resourceVersion` re-sequencing under the new GC reconciler.
>
> What shifted: the `errFetchComp` ("Composition not found") ordering no longer reaches a max-depth terminal — the new GC reshapes the ordering space such that only `errSelectComp` is observed. The `ConfigMap/default/xr-config` divergence (was differing in baseline) has collapsed to identical-across-all-terminals — the dependent is no longer GC'd in any path.
>
> The XR-side permanent error loop is intact: `compositionRef` is cleared, `SelectComposition` finds no compatible Composition, and the XR stays in `Synced=False / ReconcileError` indefinitely. There is no self-recovery within the depth budget. The `api.go:251-252` TODO ("need to block the deletion of composition via finalizer once it's selected") remains the natural fix surface.
>
> Open question for the maintainer: is the `errSelectComp` permanent-error pathology accepted as the expected behavior pending the planned finalizer fix, or is there an alternative recovery path (e.g. re-selecting an alternative Composition with matching labels) that should kick in?

## Caveats / follow-ups

- **R-14 (real-cluster F3 reproduction)** is the strongest possible audit and remains the highest-leverage outstanding item. A 5-minute observation of an XR after `kubectl delete composition` will give the strongest possible confirmation that this is a production-real loop and not a Kamera artifact. Plan: [`../crossplane-audits/kind-cluster-plan.md`](../crossplane-audits/kind-cluster-plan.md) §R-14.
- The `errFetchComp` family is unreachable in this hardened-harness dump within depth 100. Whether it is *production*-unreachable or merely depth-budget-unreachable is undetermined. Either way, `errSelectComp` alone is a sufficient permanent-error pathology, so this doesn't affect the posting decision.
- Stale `Ready=True` while `Synced=False` is also present in the F3 terminals (it appears alongside the F6 stale-Ready finding documented in [`7223-f5-f6-reframe.md`](./7223-f5-f6-reframe.md)). If the maintainer accepts the F6 stale-Ready fix, F3 also benefits.
