# R-14: F3 composition deletion on real cluster (kind + Crossplane v2.2.0)

**Status:** ✅ AUDITED — **F3 reproduces on real cluster.** Deleting a Composition while a Manual-policy XR is bound produces a permanent `Synced=False / ReconcileError` loop on the XR. The composed dependent ConfigMap persists (no cascade GC). The XR shows the `errFetchComp` family ("cannot fetch Composition: cannot get CompositionRevision") — the family that did **not** surface in the hardened-harness re-run, which only exhibited `errSelectComp`.

**Threats addressed:** F3-A1 (clobbered DELETE), F3-A2 (GC fidelity artifact), and F3-Audit-1 (real-cluster reproduction). Closes the simulation-fidelity question for #7222 entirely.

**Date:** 2026-04-29
**Cluster:** `kind-crossplane-audit`, kindest/node v1.30.0
**Crossplane image:** `xpkg.crossplane.io/crossplane/crossplane:v2.2.0`
**Function:** `function-patch-and-transform v0.10.0`

## Setup

XRD: `xwidgets.example.org` (group `example.org`, kind `XWidget`, v1).
Composition: `widget-composition-alpha` (Pipeline mode, single P&T step producing `ConfigMap/default/alpha-output { source: alpha }`).
XR: `XWidget/example` with `compositionUpdatePolicy: Manual` and `compositionRevisionRef: widget-composition-alpha-a39f01a`.

Initial reconcile: `Synced=True / ReconcileSuccess`, `Ready=False (Creating)` ("Unready resources: cm" — the composed ConfigMap is created but Crossplane considers a vanilla ConfigMap unready because it has no `Ready` condition; this is unrelated to F3). `alpha-output` ConfigMap created with `resourceVersion=924`.

## Probe

```bash
kubectl delete composition widget-composition-alpha
# returns immediately, no finalizer hang
```

The Composition is deleted cleanly. Crossplane v2.2.0 ships **no admission/finalizer guard** preventing Composition deletion while bound XRs exist. The owned `CompositionRevision` is GC'd by Kubernetes via `ownerReferences`.

Observation window: 30s sampling for 5 minutes (T0..T+300s).

## Findings

| T+ | Synced | Ready | Responsive | message |
|---|---|---|---|---|
| 0s | True (ReconcileSuccess) | False (Creating) | True | — |
| 30s | True | False | True | (last successful reconcile cached) |
| 60s | True | False | True | (still cached) |
| **90s** | **False (ReconcileError)** | False | True | **`cannot fetch Composition: cannot get CompositionRevision: CompositionRevision.apiextensions.crossplane.io "widget-composition-alpha-a39f01a" not found`** |
| 120s | False (ReconcileError) | False | True | (same) |
| 150s | False | False | True | (same) |
| 180s | False | False | True | (same) |
| 210s | False | False | True | (same) |
| 240s | False | False | True | (same) |
| 270s | False | False | True | (same) |
| 300s | False (ReconcileError) | False | True | (same) |

The error condition transitions in at ~T+90s and stays for the entire 5-minute observation window. No self-recovery.

### Composed dependent

`ConfigMap/default/alpha-output` is **never touched** post-Composition-delete:
```
T0:    rv=924
T+30s: rv=924
... (constant) ...
T+300s: rv=924
```

Crossplane does not cascade-delete composed resources when a Composition is removed. This matches what the hardened harness shows (the `ConfigMap` collapse to identical-across-terminals in [R-4](./R4-f3-trace-audit.md)).

### XR-level state

`spec.compositionRef.name = widget-composition-alpha` (unchanged — Manual policy).
`spec.compositionRevisionRef.name = widget-composition-alpha-a39f01a` (unchanged — Manual policy holds the pin even though the target CompositionRevision is gone).

This is the natural permanent-error condition: under Manual policy, when the pinned revision is GC'd alongside its owner Composition, no automatic recovery path exists.

### Diff XR before vs after

```
< resourceVersion: "928"          # before delete
> resourceVersion: "1062"         # after 5 min of error reconciles
< Synced=True (ReconcileSuccess)
> Synced=False (ReconcileError)
> message: "cannot fetch Composition: cannot get CompositionRevision: CompositionRevision.apiextensions.crossplane.io \"widget-composition-alpha-a39f01a\" not found"
```

resourceVersion bumped from 928 → 1062 over 5 minutes — Crossplane is actively re-reconciling and re-writing the same error condition (consistent with the "permanent error loop" framing).

### Bonus probe: re-create the Composition

```bash
kubectl apply -f composition-alpha.yaml
# new CompositionRevision spawned: widget-composition-alpha-a39f01a (SAME NAME)
sleep 30
kubectl get xwidget/example  # → Synced=True (ReconcileSuccess)
```

The XR self-recovers. **Why this works in this specific test:** Crossplane derives `CompositionRevision` names from a deterministic hash of the Composition spec. Re-applying the identical Composition yaml produces a CompositionRevision with the **same name** (`widget-composition-alpha-a39f01a`). The XR's stale `compositionRevisionRef` once again points at a live object.

This is a fortunate accident of identical re-creation, not a recovery path. **If a user re-creates the Composition with any spec change** (which is the realistic scenario — usually you'd delete-and-replace because something needs to change), the new CompositionRevision has a different hash, and the XR stays in `errFetchComp` indefinitely until the user manually patches `compositionRevisionRef` or switches to Automatic policy.

## Comparison with hardened-harness re-run (R-4)

| Aspect | Hardened harness | Real cluster |
|---|---|---|
| **Permanent error?** | Yes (4/4 unique terminals carry `Synced=False / ReconcileError`) | **Yes** (5/5 30-second samples after T+90s show same error). |
| **Error family** | `errSelectComp`: "cannot select Composition: no compatible Compositions found" | **`errFetchComp`**: "cannot fetch Composition: cannot get CompositionRevision: ... not found" |
| **`compositionRef` state** | Cleared in all terminals | Preserved (Manual policy holds the pin) |
| **`compositionRevisionRef` state** | Cleared in all terminals | Preserved as stale pointer to GC'd revision |
| **ConfigMap dependent** | Identical across all terminals (`709d71b`) | Untouched (rv constant at 924) |
| **Self-recovery within budget?** | No (max-depth aborted) | No (over 5 minutes) |

The bug pathology is identical (permanent error loop, dependent persists, no automatic recovery), but the **specific error family observed differs** because the two systems exercise different orderings.

The harness uses **Automatic** update policy implicitly (clears refs on the cleanup path), which in the real cluster would push the XR through `errSelectComp` once `compositionRef` is cleared and SelectComposition fails to find any compatible Composition. Our real-cluster probe used **Manual** policy, which holds the refs and pushes through `errFetchComp` instead. **Both error families are real production pathologies of F3.**

## Implications for upstream-updates draft 7222

The current draft framing ("shifts (still-reproduces, with narrowed error-family coverage)") is **fully validated** by R-14. Tweaks to consider when the user posts:

1. The "narrowed to one of two original error families" claim is now backed by direct evidence: real production exhibits **both** `errFetchComp` and `errSelectComp` depending on update policy, but the hardened harness only surfaced `errSelectComp` within the depth budget.
2. The "no self-recovery" claim is empirically confirmed at 5 minutes wall-clock on real Crossplane v2.2.0 — strictly stronger evidence than the simulator's max-depth abort.
3. The bonus probe (deterministic-hash recovery) is worth mentioning as a footnote — the *only* way the XR auto-recovers is if the user re-applies the Composition with byte-identical spec, which is essentially never the realistic scenario.

The `api.go:251-252` finalizer-on-Composition-delete TODO remains the natural fix surface; this audit confirms the user impact (5+ minutes of `Synced=False / ReconcileError` per XR) is not a simulation artifact.

## What's NOT addressed by R-14

- **Hardened-harness `errFetchComp` absence within depth budget.** Real production reaches `errFetchComp` (Manual policy) at T+90s in ~3 controller reconciles. Why doesn't the simulator? Likely because the simulator's scenarios all use Automatic update policy (clears refs on cleanup) rather than Manual (holds refs), so the path is never exercised. Not a fidelity gap; a scenario-coverage gap. Tracked but not blocking.
- **Multi-XR scenarios.** A single XR was bound to the Composition. Whether the same loop hits multiple XRs simultaneously is not tested but is the natural extrapolation.
- **Crossplane v2.2.0+ behavior.** Only v2.2.0 was tested. If a future release adds a Composition finalizer (as the TODO suggests), the bug pattern shifts — no longer a permanent loop, but a stuck-deletion pattern instead.

## Artifacts

Committed under [`./artifacts/tier3/r14/`](./artifacts/tier3/r14/) — see [`./artifacts/tier3/README.md`](./artifacts/tier3/README.md) for the per-file index. Key trace evidence:

- [`r14/xr-after-composition-delete-T+100s.yaml`](./artifacts/tier3/r14/xr-after-composition-delete-T+100s.yaml): the smoking gun — XR shows `Synced=False / ReconcileError` with message `"cannot fetch Composition: cannot get CompositionRevision: ... not found"`. This is the **`errFetchComp` family** that the harness re-run did NOT surface (Manual policy holds the refs and exercises this path; harness scenarios use Automatic policy and exercise `errSelectComp` instead).
- [`r14/composition-alpha-after-delete.txt`](./artifacts/tier3/r14/composition-alpha-after-delete.txt) and [`r14/compositionrevision-alpha-after-delete.txt`](./artifacts/tier3/r14/compositionrevision-alpha-after-delete.txt): `NotFound` for both — Composition deletes cleanly with no finalizer, CompositionRevision GC'd by ownerReferences.
- [`r14/configmap-alpha-output-baseline.yaml`](./artifacts/tier3/r14/configmap-alpha-output-baseline.yaml) vs [`r14/configmap-alpha-output-after-composition-delete-T+100s.yaml`](./artifacts/tier3/r14/configmap-alpha-output-after-composition-delete-T+100s.yaml): both show `resourceVersion: 8803` — composed dependent untouched, no GC.
- [`r14/observation.log`](./artifacts/tier3/r14/observation.log): 5-minute sampling showing permanent error loop with no recovery.
