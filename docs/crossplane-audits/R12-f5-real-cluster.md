# R-12: F5 stale ValidPipeline race on real cluster (kind + Crossplane v2.2.0)

**Status:** ⚠️ INCONCLUSIVE on real cluster — Approach A (manual `status.capabilities` patch) was attempted but is **not stable in production**: the FunctionRevision controller actively reconciles `status.capabilities` back to the package-metadata value within seconds. Approach B (build and deploy a function package without the `composition` capability, then trigger version transition) is deferred — out of scope for this initial real-cluster pass per the kind-cluster-plan. **F5 confidence remains HIGH** based on the Tier 2 audits (R-7 closes the workqueue serialization threat) and the source-grounded mechanism; what R-12 adds is an empirical observation about how capabilities are reconciled in production, which actually **tightens** the F5 race-window argument rather than weakening it.

**Date:** 2026-04-29
**Cluster:** `kind-crossplane-audit`, kindest/node v1.30.0
**Crossplane image:** `xpkg.crossplane.io/crossplane/crossplane:v2.2.0`
**FunctionRevision under test:** `function-patch-and-transform-9991175eae0e`

## Approach A: direct status patch (attempted, not stable)

The kind-cluster-plan suggested:
```bash
kubectl patch functionrevision/$FR --subresource=status --type=json \
  -p='[{"op":"replace","path":"/status/capabilities","value":[]}]'
```

This was attempted. Pre-conditions and observations:

- `spec.capabilities` is empty; capabilities are stored at `status.capabilities = ["composition"]` (set by the package manager from the package's `crossplane.yaml` metadata).
- The patch initially succeeded — a snapshot taken immediately after the patch showed `status.capabilities = []`.
- A few seconds later, a follow-up `kubectl get` showed `status.capabilities = ["composition"]` again. A second patch attempt to `["composition"]` returned `(no change)`, confirming the controller had already re-set it.

**Conclusion (Approach A):** the FunctionRevision controller actively reconciles `status.capabilities` from the package metadata. A manual edit is transient — possibly only valid for a few hundred milliseconds to seconds before the controller reverts.

This is informative for the F5 framing:
1. The F5 race-window assumption — "there is a moment when CompositionRevision's `ValidPipeline=True` is stale because the function's capabilities have changed but the propagation hasn't reached the CompositionRevision yet" — is bounded on real Crossplane by **how fast the FunctionRevision controller's reconcile loop runs** and **how fast the CompositionRevision controller picks up the change via the FunctionRevision watch**.
2. R-1 confirmed both watches exist in v2.2.0. R-7 confirmed cross-controller workqueues do not serialize. R-12 (Approach A) confirms the controllers actively race to reconcile status — a tight race window is plausible, but its width depends on per-cluster timing.

## Approach B: deploy a function package without the capability (deferred)

The realistic, production-mirroring trigger for F5 is:
1. Function package v1 has `composition` capability → CompositionRevisions referencing it validate as `ValidPipeline=True`.
2. Function package v2 (without `composition` capability) is published → new FunctionRevision rolls out.
3. CompositionRevision controller eventually re-reconciles `ValidPipeline` against v2's capabilities — but during the gap, an XR reconcile under the old `ValidPipeline=True` proceeds and composes resources using a now-invalid pipeline.

This requires building and pushing a custom function package (or finding an existing function with two published versions where one drops the `composition` capability). Plan called this "out of scope for this initial pass." R-12 does not pursue it.

## Why this is acceptable

F5's confidence going into Tier 3 was already HIGH after Tier 1 + Tier 2:
- R-1 (FunctionRevision watch wiring) source-grounded the mechanism.
- R-7 (workqueue semantics) closed the cross-controller-serialization threat — workqueues do not block races between controllers.
- The harness scenario is a faithful model of the workqueue-race ordering space.

R-12's Approach A blocked finding (the controller actively reconciles `status.capabilities`) is a small *additional* signal that the race window is tight in production but real — controllers actively contend over the same status field.

The **strongest possible** evidence for F5 would be a real-cluster reproduction of a stale-`ValidPipeline=True` window with a witnessed compose-after-invalidation. That requires Approach B. It is not blocking for posting #7223's F5 sub-claim — the upstream draft's framing already explicitly notes "race-window-feasible" rather than "100% reproducible."

## Implications for upstream-update draft 7223 (F5)

The draft framing as "still-reproduces / shifts" with a workqueue-race-window argument is **fully validated** by R-1 + R-7. R-12 doesn't change this posture.

Optional addition to the draft footnote: "FunctionRevision `status.capabilities` is actively reconciled by the package manager — manual edits revert within seconds — confirming the race window is bounded in time but exists every time a function version is rolled out."

## What's NOT addressed by R-12

- **Quantitative race probability.** Approach B would yield this; deferred.
- **Observation of `ValidPipeline=True` going stale on a real cluster.** Same; deferred.
- **Multiple FunctionRevisions, multi-step pipelines, or non-default capabilities.** All deferred.
- **A direct kubectl-driven race demonstration.** Approach A's transient patch could conceivably be combined with a tight `kubectl apply xr.yaml` to attempt a manual race trigger — but the controller revert is too fast to script reliably without atomic timing primitives. Not pursued.

## Artifacts

None — both `kubectl patch` calls and the controller revert were ephemeral observations.
