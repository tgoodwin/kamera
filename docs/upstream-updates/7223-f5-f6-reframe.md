# Draft update for crossplane/crossplane#7223 (F5 + F6 reframe)

> **Not posted by sprint executor.** This is staged for tgoodwin's manual review and posting.

**Re-run date:** 2026-04-28
**Harness HEAD:** `crossplane-reeval` @ `89acd8a`
**Crossplane version under test:** v2.2.0

This issue combined three claims with different maintainer dispositions in the original review by jbw976. The re-run separates them.

---

## F5: Stale `ValidPipeline` ordering race — **still-reproduces (shifts)**

### Fidelity context relevant to F5

- `911b3bd / cb1c43e / 7ba2045 / 1def992` — RV tracking + 409-conflict checking + `MergeFromWithOptimisticLock` detection.

### Inputs

- `workflow_crossplane-staleness_function-capability-removed-hypothesis-1.json` (corrected window `staleAt=4, catchUpAt=8`)
- `workflow_crossplane-staleness_function-capability-removed.json`
- `interval_function-capability-removed.json`

### Campaign metrics

| Variant | Unique | Total | Resource states | Max-depth aborted |
|---|---|---|---|---|
| hypothesis-1 (hardened) | 44 | 328 | 16 | 3 |
| primary (hardened) | 115 | 576 | 33 | 5 |
| interval (hardened) | similar | similar | similar | varies |

### Terminal-state evidence

**hypothesis-1 rerun:**
```
3 converged states with 2 differing object(s), 4 identical

ConfigMap/default/xr-config:
  (missing), 709d71b, 709d71b
XWidget/default/example:
  6c7ce03, 667e63a, d09c2d4
```

**primary rerun:**
```
4 converged states with 2 differing object(s), 4 identical

XWidget/default/example:
  fdc5c69, 95062fa, 303b7bb
ConfigMap/default/xr-config:
  (missing), 709d71b, 709d71b
```

**interval rerun:** 9 converged states, same 2 differing objects, same family of values.

### Interpretation

Original baseline categories:
- **A** (correct, error-only): `XWidget=579a4db`, ConfigMap missing — 432 paths
- **B** (bug, 2 compositions): `XWidget=5333e65`, `ConfigMap=60f2920` — 355 paths
- **C** (bug, 1 composition): `XWidget=93d750b`, `ConfigMap=709d71b` — 287 paths

Hardened harness:
- **Category A still present** (ConfigMap missing terminals).
- **Category C still present** (`ConfigMap=709d71b` matches baseline exactly — same buggy 1-composition outcome).
- **Category B is GONE** — no `60f2920` ConfigMap, no `5333e65` XWidget appears in any run.
- **XWidget hashes shifted** for both categories A and C. The bug content (which wrong-function-output ends up in the ConfigMap) is preserved (`709d71b`); the XWidget metadata around it differs.

The most likely cause for Category B disappearance: the second redundant compose that produced `60f2920` was a RV-conflict-eligible write that previously slipped through; the new RV checking returns a 409 on that write, closing the window. Category C (single buggy composition) is preserved because the first compose hasn't yet been written when the cached `ValidPipeline=True` is read.

### Conclusion for F5

**Bug is real and still ordering-dependent.** The "1-composition with stale ValidPipeline" outcome reproduces with the same `ConfigMap=709d71b` content as the baseline. The "2-compositions" variant is suppressed by the new RV checking, which is the expected production behavior under optimistic concurrency.

### Suggested wording for the F5 portion

> F5 re-validated on 2026-04-28. The stale `ValidPipeline=True` race still produces buggy compositions in the hardened harness — specifically the 1-composition outcome with `ConfigMap` content `709d71b` (matches the original Category C). The 2-compositions variant from the original report is no longer reachable, presumably because the new RV-conflict checking in the harness returns a 409 on the redundant second write (which is the expected production behavior). The core race remains: `CompositeReconciler` reading the cached `ValidPipeline=True` condition on `CompositionRevision` and proceeding to compose with a function whose capabilities have been removed.

---

## F6 orphan persistence — **shifts; original GC fix proposal withdrawn**

### Fidelity context relevant to F6

- `e4daf33` (cherry-pick of `6cd7396`) — DELETE preserves finalizers; new `GarbageCollectorReconciler` cascade-deletes dependents on REMOVE.

### Input

- `workflow_crossplane-function-failure_composition-switches-to-fatal.json`

### Campaign metrics

- 341 unique / 1,212 total / 112 resource states / 12 max-depth aborted (~2m35s wall-clock).

### Terminal evidence (per-trial ConfigMap presence proxy)

| Trial | ConfigMap in terminal? |
|---|---|
| 1 | yes (orphan) |
| 2 | yes (orphan) |
| 3 | no (cleaned) |
| 4 | no (cleaned) |
| 5 | yes (orphan) |
| 6 | yes (orphan) |
| 7 | yes (orphan) |
| 8 | yes (orphan) |
| 9 | no (cleaned) |

**Hardened: 6/9 (67%) orphan, 3/9 (33%) cleaned.**
**Baseline: 42/49 (86%) orphan, 7/49 (14%) cleaned.**

### Interpretation

Orphan rate dropped meaningfully (86% → 67%) but remains the dominant outcome. The new GC controller and DELETE-semantics fix close some orderings that previously created orphans, but the underlying Crossplane behavior — `composition_functions.go:404` returning on `SEVERITY_FATAL` before `GarbageCollectComposedResources` runs — is unchanged. So the bug-shape persists in 67% of orderings.

### Position on the GC fix

I'm withdrawing the original "call `GarbageCollectComposedResources` before the SEVERITY_FATAL early-return" suggestion. jbw976's review pointed out that `desired` is potentially incomplete at that point (later pipeline steps may still add resources still in use), so cleaning up everything not in the partial-`desired` set could delete in-use resources. **That argument holds and I'm not contesting it.**

The orphan-persistence behavior is a real production artifact — but the safe-fix surface is narrower than I originally proposed. Possible directions (not pre-judged):
- Only GC resources whose owner-ref or label is unambiguously this XR's previous-revision composition output. Doesn't help if `desired` is meant to be authoritative.
- Surface the orphan inventory in XR status so users know what to clean up manually.
- Document the orphan-on-fatal semantics so users aren't surprised.

I don't have a recommendation that's both safe and trivial, so I'm leaving this with the data above.

---

## F6 stale `Ready=True` — **status: insufficient evidence in this re-run**

The original report noted that ConfigMap-present terminal states (the 86% orphan cases) showed `Synced=False` + `Ready=True` on the XR — the latter a stale carryover from the prior successful composition that the error path never clears (`reconciler.go:739` only sets `Synced=False`).

This re-run did not extract per-terminal XR conditions from the JSONL dumps. The condition information is folded into the converged-state hash and isn't surfaced by `kamera analyze diff`. Confirming whether the stale `Ready=True` still appears in the hardened-harness terminals requires either:
1. A trace-level grep across all trial dumps for `XWidget` `conditions` blocks; or
2. A custom analyzer pass.

This is captured as a follow-up; the issue update should not assert on F6-stale-Ready until that evidence lands.

### Suggested wording for the F6 portion

> Re-validated on 2026-04-28. Withdrawing the original GC-on-fatal proposal — the maintainer's argument that `desired` is potentially incomplete at the early-return point holds, and "clean up everything not in `desired`" is unsafe.
>
> The orphan-persistence behavior itself still reproduces in 6/9 trials on the hardened harness (down from ~86% → ~67% baseline), so the issue is real but the safe-fix surface is narrower than I first proposed. I don't have a clean recommendation; happy to leave this open as documentation-of-behavior or close it pending a different design.
>
> The stale `Ready=True` portion of the original report is unverified in this re-run because per-terminal XR condition extraction from the harness dumps requires more analysis than was feasible. Will re-validate that piece separately.

---

## Recommendation for #7223 issue body

Two options:

### Option A: split into two narrower issues
- **#XXXX-new-1** — F5 ordering race only (with the refreshed Category-A/C evidence). Mark severity P2.
- **#XXXX-new-2** — F6 stale `Ready=True` only (after the trace audit lands). Mark severity P2.
- Close #7223 referencing the two new issues. The orphan-persistence claim is withdrawn (replaced by "documentation request" comment if anything).

### Option B: rewrite #7223 body in place
Edit #7223 to keep only F5 (with new evidence) and F6-stale-Ready (with "evidence pending" caveat). Drop the F6-orphan-persistence claim and the GC-on-fatal proposal entirely.

**Recommend Option A** — cleaner separation of concerns; each new issue can be triaged independently; the original #7223 closure trail is informative for future work.

## Inputs for both options

For whichever path: cite `crossplane-reeval` @ `89acd8a` and the relevant fidelity SHAs (`911b3bd`, `cb1c43e`, `7ba2045`, `1def992`, `e4daf33`, `b12542d`, `89acd8a`) so a future re-validator knows which harness state produced the evidence.
