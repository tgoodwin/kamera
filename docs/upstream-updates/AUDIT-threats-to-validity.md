# Threats-to-validity audit for SPRINT-0001 findings

> Adversarial critique of the three claims that still stand as Crossplane bugs after SPRINT-0001 (F1, F5, F6-orphan-persistence) plus the F3 retraction-candidate. For each, list the simulation assumptions the claim load-bears on, the specific ways those assumptions could be wrong (Kamera fidelity gaps), and what evidence would resolve each threat.
>
> Goal: before posting any of the upstream-update drafts, walk the checklist for that finding to completion. The maintainer-ask framing in #7222 already implicitly does this for F3; this doc is the explicit version for all four.

**Status legend:**
- ✅ AUDITED — threat checked against source/docs, no fidelity issue found. Per-audit detail in [`../crossplane-audits/`](../crossplane-audits/).
- ⚠️ PENDING — threat identified, needs external research or a code read.
- ❓ UNKNOWN — threat identified, no plan yet.

**Audit progress:** Tier 1 complete. See [`../crossplane-audits/README.md`](../crossplane-audits/README.md) for the per-audit index.

---

## Cross-cutting fidelity threats (apply to every finding)

These are properties of the Kamera harness that affect *all* Crossplane scenarios, not any one finding. Audit each before signing off on any individual finding.

### CC-1. Function runtime stub fidelity

**Claim:** `examples/crossplane/functions_stub.go` correctly models the Crossplane function runtime contract (gRPC `RunFunctionRequest`/`Response`, capability advertisement, severity levels).

**Threat:** Real function runtime is a gRPC service that runs out-of-process. Our `stubFunctionRunner` returns hardcoded outputs synchronously. Specifically:
- Are we returning the exact `RunFunctionResponse` shape that Crossplane expects?
- Does our stub correctly populate `Results[].Severity = SEVERITY_FATAL` such that the Crossplane `composition_functions.go` early-return path actually fires?
- Capability advertisement: real functions advertise capabilities in their `Capabilities` field. Are we advertising the same way?

**Audit:**
- ✅ AUDITED — see [R-2](../crossplane-audits/R2-scenario-fixtures.md). The fatal-stub return shape triggers the documented production code path; even if production fatal functions populate `Desired`, the early-return at `composition_functions.go:439` short-circuits before desired-state processing.
- ✅ AUDITED — see [R-3](../crossplane-audits/R3-fatal-branch.md). SEVERITY_FATAL early-return is at `composition_functions.go:439` in v2.2.0; GC call at line 538 is unreachable from the fatal path.

### CC-2. CompositionRevision creation and labeling

**Claim:** Our harness creates `CompositionRevision` objects with the same ownerRefs, labels, and status fields that real Crossplane would, so the controllers under test see them identically.

**Threat:** F1 specifically depends on `crossplane.io/composition-name` label-based filtering in the Automatic policy path. If our `CompositionReconciler` (which we wire from real Crossplane code) creates revisions correctly, this is fine. But if any seeding step in our harness pre-creates `CompositionRevision` objects without the right labels/ownerRefs, label-based filtering would behave differently.

**Audit:**
- ✅ AUDITED — see [R-2](../crossplane-audits/R2-scenario-fixtures.md). Fixtures don't pre-create CompositionRevisions; the wired-from-real-source `composition.NewReconciler` creates them with production labels and ownerRefs. No harness shortcut.

### CC-3. Watch fanout and re-enqueue ordering

**Claim:** Our scheduler's permutation of controller invocations is a meaningful approximation of real controller-runtime watch fanout.

**Threat:** Real CR watches debounce and coalesce via the workqueue. Our permutation explores orderings that real CR would never produce because the rate limiter would collapse them. Conversely, real CR can produce orderings ours can't (e.g., multiple controllers reacting to the same watch event with different debounce timing).

This matters most for F5, which is purely an ordering claim.

**Audit:**
- ⚠️ PENDING (R-7). Web-search controller-runtime workqueue semantics (specifically: does `RequeueAfter` and the rate limiter collapse rapid re-enqueues from the same key?). Look at `sigs.k8s.io/controller-runtime/pkg/internal/controller/controller.go`.
- ⚠️ PENDING (R-7). Audit `pkg/explore/parallel_runner.go` for the permutation strategy. Confirm it represents *causally possible* orderings, not arbitrary ones.
- ✅ PARTIALLY AUDITED — see [R-1](../crossplane-audits/R1-functionrevision-watch.md). FunctionRevision → CompositionRevision watch IS wired in production, and we mirror it. Workqueue *timing* (not topology) is the remaining concern.

### CC-4. ownerReference auto-fixup

**Claim:** Our `EnsureDeterministicIdentity` + the post-pass that auto-fills empty ownerReference UIDs (commits `d2935ba`/`19916a0`) preserves the production semantics of ownerReferences.

**Threat:** Real K8s rejects `ownerReferences` with empty UIDs at the API server. Our auto-fixup post-processes them after assigning deterministic UIDs. If the timing of when ownerRefs are visible to other controllers differs from production, we could see causally impossible behaviors.

Specifically for the new `GarbageCollectorReconciler` (cherry-pick `e4daf33`): does our GC fire on REMOVE in a way that matches production K8s GC?

**Audit:**
- ⚠️ PENDING. Read `sleevectrl/pkg/controller/gc_controller.go`. Compare against the K8s `garbagecollector` controller source: `https://github.com/kubernetes/kubernetes/blob/v1.30.0/pkg/controller/garbagecollector/garbagecollector.go`. Specifically: does our GC respect `propagationPolicy` (Foreground / Background / Orphan)? Production GC has different timing for each.
- ⚠️ PENDING. Verify our GC doesn't cascade-delete things that real K8s GC wouldn't (e.g., resources without an explicit blockOwnerDeletion=true and a Foreground policy).

### CC-5. Status subresource write semantics

**Claim:** Our simulated `Status().Update()` and `Status().Patch()` paths preserve the spec/status separation that real K8s enforces.

**Threat:** The triage notes mention a known fidelity gap: "applyEffects replaces entire object on status subresource update, causing XR to lose spec fields. Affects workflow 4b (manual-policy-switch-stale)." This is exactly the F1-stale variant we re-ran and is still a known limitation.

**Audit:**
- ✅ AUDITED — see [R-5](../crossplane-audits/R5-status-writer.md). `mergeStatusSubresourceObject` correctly preserves `spec`/`metadata`; only `status` is replaced. Matches production for `Update` (the only path Crossplane uses in F1/F5/F6). The earlier triage note's "loses spec fields" claim doesn't apply to current code.

### CC-6. Generation / observedGeneration

**Claim:** Our harness correctly bumps `metadata.generation` on spec changes and exposes `status.observedGeneration` consistent with that.

**Threat:** Many Crossplane controllers gate on `observedGeneration` to skip work. If our harness doesn't bump `generation` on spec-only updates, controllers might do MORE work than they would in production, exposing races that production never sees.

**Audit:**
- ⚠️ PENDING. Read `pkg/replay/client.go` `Update`, `Patch`, and `Apply` paths. Confirm we bump `generation` only on spec changes (not on status changes, not on metadata-only changes).

### CC-7. Conditions list merge semantics

**Claim:** Our harness models `status.conditions` as a list-with-merge-by-`type` semantics matching K8s convention.

**Threat:** F5 specifically depends on `ValidPipeline=True` being set on `CompositionRevision`. In production, this condition is upserted by `type` (one entry per type, replaced not appended). If our simulator appends instead of upserts, condition history in our trace might be wrong.

**Audit:**
- ✅ AUDITED — see [R-5](../crossplane-audits/R5-status-writer.md). Conditions are set in-memory via Crossplane's own `xpv1.SetConditions` (which upserts by `type`), then the full object goes through `Status().Update()` which our harness handles correctly (full status replacement, matching production). The conditions-merge gap exists for `Status().Patch()` callers but Crossplane doesn't use that path in F1/F5/F6.

### CC-8. Manual update policy semantics — what does CR actually do here?

**Claim:** With `compositionUpdatePolicy: Manual`, the XR's `compositionRef` and `compositionRevisionRef` are *both* user-controlled and the reconciler uses them as-is.

**Threat:** Maybe in production, controller-runtime or Crossplane has admission/defaulting logic that auto-syncs `compositionRevisionRef` to a valid revision under `compositionRef` whenever `compositionRef` changes. Our harness doesn't simulate webhooks or defaulting — if such logic exists upstream, our F1 trace would be impossible in production.

**Audit:**
- ⚠️ PENDING. Web-search Crossplane v2.2.0 docs for `compositionUpdatePolicy: Manual` semantics. Is there a mutating webhook on XR that re-resolves `compositionRevisionRef` when `compositionRef` changes? Check `apiextensions.crossplane.io` CRD definitions for any webhook conversion or defaulting logic.
- ⚠️ PENDING. Try the exact F1 scenario on a real cluster: install Crossplane v2.2.0, create alpha + beta Compositions, create an XR with `Manual` policy pointing at alpha-rev-1, then `kubectl edit` to set `compositionRef = beta` while leaving `compositionRevisionRef = alpha-rev-1`. Observe the result.

---

## Per-finding threat models

### F1 (#7220) — Manual update policy wrong revision

**The claim being made upstream:** When `compositionUpdatePolicy: Manual` is set and a user changes `compositionRef` from `alpha` to `beta` while leaving `compositionRevisionRef = alpha-rev-1`, the reconciler composes resources from the `alpha` Composition's pipeline despite `compositionRef` saying `beta`. No error is raised. Source: `internal/controller/apiextensions/composite/api.go:161-167` does a bare `Get(currentRevision.Name)` with no validation.

**Strongest evidence:** The trace in `/tmp/crossplane-reeval-89acd8a/f1/primary/` contains a state where `compositionRef.name = "widget-composition-beta"` AND `compositionRevisionRef.name = "widget-composition-alpha-rev-1"`, and the reconciler does NOT error on this state.

#### Load-bearing simulation assumptions

| ID | Assumption | If wrong, F1 is... |
|---|---|---|
| F1-A1 | Our XR object delivered to `CompositeReconciler` carries the user-set `compositionRef` and `compositionRevisionRef` exactly as the user wrote them. | invalid (we'd be reading our own corrupted state) |
| F1-A2 | The external-user `UPDATE` event we model is semantically equivalent to a `kubectl edit` on the XR. | invalid if production runs admission/webhooks/defaulting we don't model. |
| F1-A3 | `APIRevisionFetcher.Fetch` is called with the same arguments in our harness as in production. | low risk — we wire the real Crossplane code, so this should be exact. |
| F1-A4 | No mutating webhook in production rewrites `compositionRevisionRef` to match `compositionRef` automatically. | invalid if such a webhook exists in v2.2.0. |

#### Threats

- **F1-T1 (CC-8 specialization):** Production might have a defaulting webhook that auto-corrects mismatched refs.
- **F1-T2:** Our harness might pre-create both `widget-composition-alpha-rev-1` and `widget-composition-beta-c34ead1` revisions in initial state, which a real cluster wouldn't have until the corresponding Compositions had been reconciled at least once. If `widget-composition-alpha-rev-1` doesn't actually exist when the user UPDATE fires, the bug doesn't fire.

#### Audits

- ✅ **F1-Audit-1:** Done in [R-2](../crossplane-audits/R2-scenario-fixtures.md). Initial XR has consistent refs (alpha/alpha-rev-1); external UPDATE creates the mismatch. CompositionRevisions are created by real Crossplane code, not pre-seeded.
- ⚠️ **F1-Audit-2 (R-6):** Web-search "Crossplane v2.2 manual update policy webhook" and "compositionRevisionRef defaulting". Read the XRD schema for any default values.
- ⚠️ **F1-Audit-3 (R-11):** Real-cluster experiment (highest-confidence): install Crossplane v2.2.0 in a kind cluster, set up alpha + beta Compositions, run the F1 scenario by hand. **This is the smoking-gun audit. If the bug reproduces on a real cluster, all simulation threats are moot.**

#### What would convince a skeptical maintainer

A `kubectl` reproduction on a real cluster, showing the XR ending up with composed resources from alpha while `compositionRef` says beta. Without that, the maintainer can argue the simulation is the source of the divergence.

---

### F5 (#7223 / F5 portion) — Stale `ValidPipeline` race

**The claim:** When a `FunctionRevision`'s capabilities are removed, there's a window where the `CompositionRevision` still has `ValidPipeline=True` cached on it. If `CompositeReconciler` reads that stale condition before `CompositionRevisionReconciler` re-evaluates, the XR gets composed using the now-invalid function. Reproduces in the trace as `ConfigMap=709d71b` matching the original baseline.

#### Load-bearing simulation assumptions

| ID | Assumption | If wrong, F5 is... |
|---|---|---|
| F5-A1 | Our scheduler can produce orderings where `CompositeReconciler` runs after the FunctionRevision change but before `CompositionRevisionReconciler` re-evaluates. | invalid if real CR's watch+workqueue semantics rule this ordering out. |
| F5-A2 | The cached `ValidPipeline=True` condition on `CompositionRevision` survives unchanged across the FunctionRevision capability change until `CompositionRevisionReconciler` next runs. | invalid if real K8s API server or some webhook/controller invalidates the cached condition automatically (unlikely but worth checking). |
| F5-A3 | `CompositeReconciler` actually reads `rev.GetCondition(v1.TypeValidPipeline)` and acts on it without independent verification. | low risk — sourced from `reconciler.go:631` directly. |
| F5-A4 | Our `FunctionRevision` capability-removal external event is semantically equivalent to a real package-manager-driven capability change. | invalid if production package manager has additional sync that we don't model. |

#### Threats

- **F5-T1 (CC-3 specialization):** Real controller-runtime might re-enqueue `CompositionRevisionReconciler` *immediately* on the FunctionRevision watch fanout, with priority/timing such that `CompositeReconciler` never has a chance to read the stale condition. Our scheduler's permutation might be exploring an ordering production would never produce.
- **F5-T2:** The `crossplane.io/.../FunctionRevision` watch wiring on `CompositionRevisionReconciler` may not be modeled. If so, real CR would re-enqueue on FunctionRevision change but our harness wouldn't, making the race wider in our model than in reality.
- **F5-T3:** In production, when a FunctionRevision's capabilities change, the package manager may also bump the FunctionRevision's `generation`. If `CompositionRevisionReconciler` gates on `observedGeneration`, our harness might miss the production trigger that closes the window.

#### Audits

- ✅ **F5-Audit-1:** Done in [R-1](../crossplane-audits/R1-functionrevision-watch.md). Production wires `Watches(&pkgv1.FunctionRevision{}, EnqueueCompositionRevisionsForFunctionRevision(...))` at `revision/reconciler.go:67`. The mapper lists all CompositionRevisions and enqueues those whose pipeline references the changed function.
- ✅ **F5-Audit-2:** Done in [R-1](../crossplane-audits/R1-functionrevision-watch.md). Our harness wires the watch (scenario.go:66) but with a hardcoded single-revision target. For F5's single-revision scenario, this matches production behavior.
- ⚠️ **F5-Audit-3 (R-7):** Web-search controller-runtime workqueue rate-limiting. Does the rapid re-enqueue from FunctionRevision watch coalesce in such a way that `CompositionRevisionReconciler` always wins the race against `CompositeReconciler`?
- ⚠️ **F5-Audit-4 (R-12):** Real-cluster experiment: deploy Crossplane v2.2.0, install a function with `composition` capability, deploy a Composition using it, then *remove* the capability from the function package. Watch the timeline in `kubectl get events` and `kubectl describe compositionrevision`. Does the race actually happen, or does CR re-evaluate before the next compose?

#### What would convince a skeptical maintainer

Either:
1. A real-cluster reproduction showing a successful XR composition after the capability is removed, with a `ValidPipeline=False` arriving milliseconds later, OR
2. A code-level argument: pointing at the *exact* line in v2.2.0 where `CompositionRevisionReconciler` does NOT watch `FunctionRevision` (closing the watch fanout that would otherwise prevent the race), AND showing that `CompositeReconciler`'s read of `ValidPipeline` happens *before* its read of `FunctionRevision` (so a fresh FunctionRevision read wouldn't help).

---

### F6 (#7223 / F6 orphan portion) — Fatal function leaves orphans

**The claim:** When a Composition switches to a function returning `SEVERITY_FATAL`, the early-return at `composition_functions.go:404` (or the v2.2.0 equivalent) skips `GarbageCollectComposedResources`, leaving previously-composed resources as orphans. Reproduces in 6/9 trials in the hardened-harness re-run.

#### Load-bearing simulation assumptions

| ID | Assumption | If wrong, F6 is... |
|---|---|---|
| F6-A1 | The early-return path at `composition_functions.go:404` is reached when our stub returns SEVERITY_FATAL. | invalid if our stub doesn't trigger that branch. |
| F6-A2 | Real Crossplane has no fallback GC path that runs on subsequent reconciles to clean up orphans. | invalid if there's a periodic resync or alternative GC trigger we're missing. |
| F6-A3 | The "orphan" we observe (ConfigMap with stale ownerRef) is what would actually persist in production. | invalid if production K8s GC would remove it independently of Crossplane logic. |
| F6-A4 | Our stub's return shape (no `Desired.Resources` populated, only `Results[].Severity = SEVERITY_FATAL`) matches what a real fatal-function would return. | invalid if real fatal functions return partial `Desired.Resources` that would tell Crossplane what to keep. |

#### Threats

- **F6-T1 (CC-1 specialization):** Our stub might be returning fatal in a way that triggers the wrong branch. If we return `SEVERITY_FATAL` without populating `Desired`, Crossplane might treat it as a "no-output success" rather than a fatal error. The "no orphan cleanup" behavior is then a stub artifact.
- **F6-T2:** Production Crossplane *might* have a periodic resync (e.g., `RequeueAfter` on the XR) that re-attempts composition on a fresh function. If the function is later fixed, the orphan would be cleaned up. Our trace ends at depth 100; in production the cluster keeps running indefinitely. The "permanent orphan" claim is overstated if production eventually self-heals.
- **F6-T3:** Real K8s GC works on ownerReferences. If the orphan ConfigMap has `ownerReference: {kind: XWidget, ...}`, and the XWidget itself is deleted, K8s GC would cascade-delete the ConfigMap. The "orphan persists forever" claim assumes the XR isn't deleted, which is a narrow assumption.
- **F6-T4 (CC-4 specialization):** Our new `GarbageCollectorReconciler` might be over-eager (cleaning up ConfigMaps it shouldn't) or under-eager (missing real K8s GC behavior that would clean up some of the "orphan" cases). The 67% orphan rate is therefore not directly comparable to a real cluster.

#### Audits

- ✅ **F6-Audit-1:** Done in [R-3](../crossplane-audits/R3-fatal-branch.md). SEVERITY_FATAL early-return is at `composition_functions.go:439` in v2.2.0. GC call at line 538 is unreachable from the fatal path. Within a single reconcile, the orphan is not GC'd.
- ✅ **F6-Audit-2:** Done in [R-2](../crossplane-audits/R2-scenario-fixtures.md). Our `fatalResponse()` stub returns `Severity_SEVERITY_FATAL` with no `Desired` populated. Production code path branches on `Severity` first; whatever else is in the response is irrelevant.
- ⚠️ **F6-Audit-3 (R-9):** Trace the XR's reconcile loop after a fatal compose. Does Crossplane re-enqueue the XR? If yes, does the next reconcile call GC? — **Partially answered by R-3:** while the function stays fatal, every reconcile retakes the same SEVERITY_FATAL early-return. GC is never called. The "permanent orphan" claim holds for as long as the function stays fatal. Once fixed, the next successful reconcile WILL GC. The draft is already scoped this way.
- ⚠️ **F6-Audit-4 (R-13):** Real-cluster experiment: deploy Crossplane v2.2.0, set up a Composition with a fatal function, observe whether the orphan persists across many reconcile cycles.

#### F6 stale-Ready-True audits (separate sub-finding)

- ✅ **F6-Ready-Audit-1:** Done in [R-3](../crossplane-audits/R3-fatal-branch.md). The error path at `reconciler.go:738-754` sets `Synced=False` and marks all *non-system* conditions Unknown via `IsSystemConditionType` filter at line 744. `Ready` is a system condition and is therefore preserved verbatim from prior state. **High-confidence claim.**

#### What would convince a skeptical maintainer

Either:
1. A real-cluster reproduction showing the orphan ConfigMap surviving for >1 hour with the XR still present and the function still fatal, OR
2. A trace screenshot from the simulation showing the XR's reconcile loop firing N times post-fatal with no GC ever invoked, AND a code-level argument that no other code path can invoke GC.

---

### F3 (#7222) — formerly retraction-candidate; **REFUTED by R-4**

**Update 2026-04-28:** [R-4](../crossplane-audits/R4-f3-trace-audit.md) refuted the retraction. All 4 unique XWidget terminal hashes carry the original F3 `errSelectComp` `ReconcileError` condition. The hash drift from baseline is cosmetic (`metadata.generation` / `resourceVersion` drift under the new GC reconciler). F3 reclassified as **shifts** with the same error pathology. The 7222 draft has been rewritten accordingly.

**The (now refuted) retraction claim was:** F3's original divergence pattern (`93d750b`/`9dc61c9` XWidget + `709d71b`/missing ConfigMap) does not reproduce on the hardened harness because the original divergence was driven by Kamera's pre-`e4daf33` DELETE behavior clobbering finalizers/spec.

#### Load-bearing simulation assumptions for the retraction

| ID | Assumption | Status |
|---|---|---|
| F3-A1 | The original baseline divergence was *primarily* driven by clobbered DELETE state, not by a real Crossplane bug. | **REFUTED by R-4.** All 4 unique terminal hashes carry the original `errSelectComp` ReconcileError. The bug is real. |
| F3-A2 | The new XWidget hashes (`302b727`, `6b6a9d3`, `4390fb0`, `855c129`) we observe post-fix are not new bugs, but new fidelity artifacts from the GC controller. | **REFUTED by R-4.** They encode the same error condition; differences are `metadata.generation` and `resourceVersion` drift, not behavioral changes. |
| F3-A3 | The `ConfigMap` divergence collapse means orphans no longer persist after a Composition delete. | STANDS — the dependent ConfigMap is identical across all 6 terminals. But this doesn't bear on the XR-side error loop, which is the actual bug. |

#### Threats

- **F3-T1:** The new XWidget hashes might encode genuine `ReconcileError` conditions we haven't audited. We're calling them "fidelity artifacts" without trace-level confirmation.
- **F3-T2:** Our `GarbageCollectorReconciler` might be cascade-deleting the orphaned `CompositionRevision` on Composition REMOVE in a way that closes the original "permanent error loop" for spurious reasons (the loop existed because the revision survived the parent's deletion; if our GC removes the revision, the loop's mechanism is gone — but a real cluster might or might not exhibit this depending on `propagationPolicy`).

#### Audits

- ✅ **F3-Audit-1:** Done in [R-4](../crossplane-audits/R4-f3-trace-audit.md). All 4 unique terminal XWidget hashes carry `Synced=False / reason=ReconcileError` with message "cannot select Composition: no compatible Compositions found" — original `errSelectComp` family. **Bug reproduces; retraction refuted.**
- ✅ **F3-Audit-2:** Done in [R-8](../crossplane-audits/R8-k8s-gc-propagation.md). The harness GC is more eager than production for multi-owner deps but not relevant to F3 (the ConfigMap dependent here has a single owner).
- ⚠️ **F3-Audit-3 (R-14):** Real-cluster experiment — see [`../crossplane-audits/kind-cluster-plan.md`](../crossplane-audits/kind-cluster-plan.md) §R-14. Now even higher leverage: confirms whether the `errSelectComp` permanent loop happens on a real cluster, locks down the upstream comment text.

#### What would convince a skeptical maintainer

The trace audit (R-4) showed all 4 unique XWidget terminals carry `Synced=False / ReconcileError` with the original `errSelectComp` message. The bug is source-grounded and harness-confirmed.

The remaining argument a skeptical maintainer could make is that the harness's depth-100 budget is artificial — production might recover via paths the harness can't explore. **R-14 (real-cluster reproduction)** addresses that by observing a real XR over 5+ minutes after `kubectl delete composition`. If the real XR also stays in `Synced=False / ReconcileError`, the case is airtight.

---

## Research checklist (in priority order)

This is the work needed before posting any of the upstream-update drafts. Items are ordered by how much each unblocks.

### Tier 1 — high-confidence wins (do first)

- [ ] **R-1 (covers F5-Audit-1, F5-Audit-2):** Read v2.2.0 `internal/controller/apiextensions/revision/reconciler.go` `SetupWithManager` for FunctionRevision watches. **Pure code read, ~30 min.**
- [ ] **R-2 (covers F1-Audit-1, F6-Audit-2):** Read `examples/crossplane/scenario.go`, `functions_stub.go`, and the F1 + F6 scenario JSON initial states. Confirm seeded objects and stub behavior. **Pure code read, ~45 min.**
- [ ] **R-3 (covers F6-Audit-1):** Read v2.2.0 `internal/controller/apiextensions/composite/composition_functions.go` around the SEVERITY_FATAL branch. **Pure code read, ~20 min.**
- [ ] **R-4 (covers F3-Audit-1):** Trace-level audit of F3 hypothesis-1 terminal-state XWidget conditions. Requires writing a small Python/jq script to extract conditions from JSONL dumps. **~1 hour.**
- [ ] **R-5 (covers CC-7, F6-Audit-2):** Audit our `Status()` writer paths in `pkg/replay/client.go` for conditions list merge vs replace. **Pure code read, ~30 min.**

### Tier 2 — research and web search

- [ ] **R-6 (covers CC-8, F1-Audit-2):** Web-search Crossplane v2.2.0 manual update policy semantics. Look for: defaulting webhooks, conversion webhooks, mutating admission. **~1 hour.**
- [ ] **R-7 (covers CC-3, F5-Audit-3):** Web-search controller-runtime workqueue + watch ordering semantics. Confirm whether our permutation explores causally-impossible orderings. **~1-2 hours.**
- [ ] **R-8 (covers CC-4, F3-Audit-2):** Web-search K8s GC propagationPolicy + the production GC controller's timing. Compare to our `gc_controller.go`. **~1 hour.**
- [ ] **R-9 (covers F6-Audit-3):** Web-search Crossplane composition reconciler post-fatal re-reconcile behavior. **~30 min.**
- [ ] **R-10 (covers CC-1):** Web-search the gRPC `RunFunctionResponse` schema and Crossplane function runtime contract. Diff against our stub. **~1 hour.**

### Tier 3 — real-cluster experiments (highest fidelity but highest cost)

These are the smoking-gun audits. Doing even one of them substantially de-risks the whole findings batch.

- [ ] **R-11 (covers F1-Audit-3):** Real-cluster F1 reproduction. Install Crossplane v2.2.0 in kind, set up alpha + beta Compositions with `Manual` policy, perform the mismatched-refs UPDATE, observe XR result. **2-4 hours including environment setup.**
- [ ] **R-12 (covers F5-Audit-4):** Real-cluster F5 reproduction. Install Crossplane v2.2.0, deploy a function with `composition` capability, then remove the capability. Watch reconcile timing. **2-4 hours.**
- [ ] **R-13 (covers F6-Audit-4):** Real-cluster F6 reproduction. Install Crossplane v2.2.0 with a fatal-function Composition, observe orphan persistence. **2-4 hours.**
- [ ] **R-14 (covers F3-Audit-3):** Real-cluster F3 reproduction. Delete a Composition with bound XR, observe XR over time. **2-4 hours.**

### Tier 4 — deeper systematic audits (defensive)

- [ ] **R-15 (covers CC-2):** Audit all CompositionRevision objects we create or seed for correct ownerRefs and `crossplane.io/composition-name` label.
- [ ] **R-16 (covers CC-6):** Audit `generation` bumping in our replay client.
- [ ] **R-17 (covers CC-5):** Audit status-subresource-replacement gap (the known F1-stale issue).

---

## Decision rules for posting upstream

Use this as a gate before posting each draft.

### Post `7220-f1-manual-policy.md` if:

- ✅ R-1 + R-2 (Tier 1 code reads) confirm the F1 trace path uses real Crossplane code with no harness-specific shortcuts, AND
- ✅ Either R-6 (web search rules out auto-defaulting) OR R-11 (real-cluster reproduction).

If R-6 finds defaulting webhooks, F1 may not reproduce in production — hold the post until R-11.

### Post `7222-f3-composition-deletion.md` (the "shifts" version after R-4) if:

- ✅ R-4 has confirmed (DONE) that all 4 unique XWidget terminals carry the `errSelectComp` ReconcileError. Bug reproduces.
- 📋 R-14 (real-cluster F3) is highly recommended for the strongest framing but no longer blocking. Without R-14, post with the existing "open question for the maintainer" wording. With R-14, post with a stronger "confirmed in production" claim.

The draft has been rewritten to drop the retraction framing and present F3 as **shifts** (still-reproduces with narrowed error-family coverage). Defensible to post now.

### Post the F5 portion of `7223-f5-f6-reframe.md` if:

- ✅ R-1 + R-2 confirm our FunctionRevision watch wiring matches production AND our scheduler explores valid orderings, AND
- ✅ R-7 (workqueue semantics) doesn't reveal a coalescing behavior that would close the race in production, AND
- ✅ ideally R-12 (real-cluster F5).

If R-1 reveals our harness *doesn't* watch FunctionRevision (i.e., we're missing a watch production has), F5 might not reproduce on a real cluster. Hold the post until R-12.

### Post the F6 portion of `7223-f5-f6-reframe.md` (with the GC-on-fatal proposal already withdrawn) if:

- ✅ R-3 (composition_functions.go read) confirms the early-return branch does what we claim, AND
- ✅ R-9 (post-fatal re-reconcile audit) confirms there's no fallback GC path, AND
- ✅ ideally R-13.

The orphan-persistence claim is weakest because of F6-T2 (production might re-attempt and self-heal). Hold the post until R-9 + R-13.

### F6 stale-Ready-True portion: do not post yet. Per the run log, evidence wasn't extracted. Tier 1 task R-4 (extending it to F6 trials) plus a separate trace audit are needed first.

### Post `7224-c2-claim-deletion-validation-not-for-posting.md`: never. It's internal-only by design.

---

## Summary

The findings most defensible without external work: **F1** (pure logic, code path traceable) and the **F5 ordering claim** (assuming our watch wiring matches production).

The findings most at risk: **F6 orphan persistence** (could be production-self-healing we don't simulate) and the **F3 retraction** (relies on a "fidelity artifact" judgment we haven't trace-audited).

The single highest-leverage external audit: any **real-cluster reproduction** (R-11, R-12, R-13, R-14). Doing one removes ~all simulation-fidelity threats for that finding. If you want the absolute strongest community-presentation posture, R-11 and R-13 are the must-haves.
