# F3 — Composition deletion produces a permanent error loop on the bound XR

**Issue:** [crossplane/crossplane#7222](https://github.com/crossplane/crossplane/issues/7222)
**Status:** 🔄 Shifts (still reproduces, broader than the original framing). Confidence: HIGH.
**Audits:** [R-4](../crossplane-audits/R4-f3-trace-audit.md), [R-8](../crossplane-audits/R8-k8s-gc-propagation.md), [R-14](../crossplane-audits/R14-f3-real-cluster.md).

## TL;DR

Deleting a Composition while an XR is bound to it puts the XR into a permanent `Synced=False / ReconcileError` loop with no automatic recovery. Crossplane v2.2.0 ships no finalizer guarding Composition deletion; the owned `CompositionRevision` is GC'd by Kubernetes via `ownerReferences`. Composed dependents (the resources the XR previously composed) persist untouched — Crossplane does not cascade-delete them.

The error family observed depends on the XR's update policy: **Manual** policy holds `compositionRevisionRef` and the reconciler hits `errFetchComp` ("cannot fetch Composition: cannot get CompositionRevision: ... not found"). **Automatic** policy clears the refs on the cleanup path and the reconciler hits `errSelectComp` ("cannot select Composition: no compatible Compositions found"). Both error families are real production pathologies; both leave the XR stuck.

This one already had a TODO add finalizer

## What's actually wrong

`internal/controller/apiextensions/composite/api.go:251-252` carries an explicit TODO: "need to block the deletion of composition via finalizer once it's selected." That guard does not exist in v2.2.0.

When the user runs `kubectl delete composition`, the API server deletes it immediately (no finalizer). The Kubernetes garbage collector then deletes the owned `CompositionRevision` via `ownerReferences`. From this point onward, every reconcile of the bound XR hits one of two failure modes depending on update policy and ordering:

1. **`errFetchComp`** (Manual policy path): `compositionRevisionRef` still points at the deleted revision name. `APIRevisionFetcher.Fetch` calls `Get(...)` on it and returns NotFound.
2. **`errSelectComp`** (Automatic policy / cleanup path): `compositionRef` and `compositionRevisionRef` get cleared on the cleanup pass. `SelectComposition` then runs and finds no compatible Composition (because the user deleted it), so the reconciler errors before it can pick anything.

In both cases the XR ends up with `Synced=False / reason=ReconcileError`. There is no automatic recovery and no signal to the user about the orphaned composed dependents.

## Audit history (for the talk)

This finding's audit trail is the most interesting in the batch. It's the canonical example of how kamera's state-space output can be misread on hash-only evidence and what trace-level audits then look like.

1. **Original report** (March 2026): based on baseline harness output, the report named two error families (`errFetchComp` and `errSelectComp`) plus a divergence in the composed `ConfigMap` dependent across orderings.
2. **Hardened-harness re-run** (April 2026): under the fidelity-improved harness (RV checking, fixed DELETE semantics, new `GarbageCollectorReconciler`), the XWidget terminal hashes drifted (`302b727`, `6b6a9d3`, `4390fb0`, `855c129` instead of the originals), the `errFetchComp` family disappeared from terminals, and the `ConfigMap` divergence collapsed.
3. **Provisional retraction draft** (April 2026): based on hash-only evidence, the bug looked like it might have been an artifact of the harness's earlier DELETE / GC behavior. A retraction-candidate draft was prepared.
4. **R-4 trace audit** refuted the retraction. Parsing all 6 terminal states' XWidget objects out of the 211 MB JSONL dump showed all 4 unique terminal hashes carry `Synced=False / reason=ReconcileError` with the original `errSelectComp` message ("cannot select Composition: no compatible Compositions found"). The hash drift was entirely cosmetic — explained by `metadata.generation` (3 vs 4) and `metadata.resourceVersion` (12, 13, 19, 20) re-sequencing under the new GC reconciler.
5. **R-14 real-cluster reproduction** confirmed the bug AND surfaced the `errFetchComp` family that the hardened harness had stopped exercising — showing the report should be **broadened**, not narrowed.

The shift from "retraction candidate" to "broader than originally framed" is the lesson: hash-equivalent state changes are not bug resolution unless you read the trace.

## How Kamera surfaced it

**Scenario:** [`workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json`](../../examples/crossplane/scenarios/workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json) — a steady-state XR is bound to a Composition; an external user DELETE removes the Composition.

The harness explored 622 unique node visits across 13 resource states with 6 max-depth-aborted terminals. Six terminal states with one differing object (the XWidget) collapsed to 4 unique hashes. Trace analysis showed each unique hash carried the same Synced=False / ReconcileError condition.

The harness produces this finding without needing a real cluster because the only mechanism in play is the Crossplane controller code itself reacting to a deleted Composition — pure logic, no timing dependence.

## How we validated it

### Tier 1 — trace audit (R-4)

The R-4 audit parsed all 6 terminal states' XWidget objects out of the JSONL dump. Per-hash result:

| State ID(s) | Hash | `Synced` | `metadata.generation` / `resourceVersion` |
|---|---|---|---|
| `aborted-2azhpneh` | `302b727` | **False / ReconcileError** "no compatible Compositions found" | 3 / 12 |
| `aborted-2uhk5wqu`, `aborted-3jxjqcsk` | `6b6a9d3` | (same) | 3 / 13 |
| `aborted-2uxzakid` | `4390fb0` | (same) | 4 / 19 |
| `aborted-3ehm2of7`, `aborted-hz2c0tz0` | `855c129` | (same) | 4 / 20 |

All 4 unique hashes encode the same ReconcileError condition. The hash differences are entirely explained by `metadata` drift across orderings.

### Tier 2 — closing the GC fidelity threat (R-8)

The hardened harness's `GarbageCollectorReconciler` was new. Could it be cascading composed dependents in a way that masks an F3 fidelity issue? R-8 audited the harness GC against the K8s `garbagecollector` controller. It approximates Background-style cascade for single-owner dependents — the dependent in this scenario has a single owner (the Composition has been deleted; the dependent's owner is the XR, which is still alive). The ConfigMap-collapse (no longer GC'd in any path within depth budget) is consistent with the harness GC behavior matching production behavior, not with a fidelity bug masking the original divergence.

### Tier 3 — direct real-cluster reproduction (R-14)

Setup: same kind cluster as R-11, with `widget-composition-alpha` bound to `XWidget/example` under Manual policy.

Probe:
```bash
kubectl delete composition widget-composition-alpha
# returns immediately, no finalizer hang
```

5-minute observation, sampling every 30s:

| T+ | Synced | message |
|---|---|---|
| 0s | True (ReconcileSuccess) | (last successful reconcile cached) |
| 60s | True | (still cached) |
| **90s** | **False (ReconcileError)** | **`cannot fetch Composition: cannot get CompositionRevision: CompositionRevision.apiextensions.crossplane.io "widget-composition-alpha-a39f01a" not found`** |
| 120s..300s | False (ReconcileError) | (same message, every sample) |

Crossplane is actively re-reconciling and re-writing the same error condition (XR `resourceVersion` bumped from 928 → 1062 over 5 minutes — confirming the "permanent error loop" framing). Composed `alpha-output` ConfigMap is **untouched** (rv constant at 924).

Notably, this real-cluster run surfaced the **`errFetchComp`** family — the family that the hardened-harness re-run had stopped exercising within the depth budget. Explanation: harness scenarios all use Automatic update policy (clears refs on cleanup → exercises `errSelectComp`); the real-cluster probe used Manual policy (holds refs → exercises `errFetchComp`). Both are real F3 pathologies.

Bonus probe: re-applying the byte-identical Composition yaml causes the XR to recover, because Crossplane derives `CompositionRevision` names from a deterministic spec hash and produces a revision with the same name (`widget-composition-alpha-a39f01a`). This is a fortunate accident of identical re-creation, not a recovery path. **If the user re-creates the Composition with any spec change** (the realistic scenario), the new revision has a different hash and the XR stays stuck in `errFetchComp` indefinitely.

Trace artifacts at [`../crossplane-audits/artifacts/tier3/r14/`](../crossplane-audits/artifacts/tier3/r14/). Smoking-gun XR yaml: [`xr-after-composition-delete-T+100s.yaml`](../crossplane-audits/artifacts/tier3/r14/xr-after-composition-delete-T+100s.yaml).

## Suggested fix (per the original issue + R-14)

The `api.go:251-252` TODO is the right surface. A finalizer on Composition deletion blocks deletion while bound XRs exist; the user is forced to migrate XRs to a different Composition (or delete the XRs) before the Composition can be removed. This converts the permanent `ReconcileError` failure mode into a stuck-deletion failure mode that's at least diagnosable via `kubectl get composition` and `kubectl describe`.

R-14 also raises a secondary observation: composed dependents persist as orphans even after the Composition is gone, regardless of recovery. A finalizer alone doesn't fix that; an explicit "Composition deletion implies cascade" semantic would, but it's a larger design question.

## Talking points for the audience

- The interesting story is the audit arc: hardened-harness output looked like it might invalidate the original report; trace audit refuted that; real-cluster reproduction broadened the bug rather than narrowing it.
- This is exactly the kind of finding the harness is good at: a multi-step user action against the Crossplane control plane (deploy XR, bind to Composition, delete Composition) where the failure mode is "controller silently enters a permanent error loop" — easy to overlook in production until users complain about XRs being stuck.
- The two error families is the most non-obvious part. Manual and Automatic policy take different cleanup paths, but both end at the same user-visible state (`Synced=False / ReconcileError`, no recovery). A maintainer who only ever tests one policy could miss the other family.
- The deterministic-hash recovery bonus probe is a fun footnote: re-applying byte-identical Composition spec saves you, but only by accident. Any real edit produces a new revision name and the XR stays stuck.
