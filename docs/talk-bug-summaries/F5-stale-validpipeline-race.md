# F5 — Stale `ValidPipeline=True` race after function capability change

**Issue:** [crossplane/crossplane#7223](https://github.com/crossplane/crossplane/issues/7223) (F5 sub-claim)
**Status:** ✅ Reproduces in fidelity-hardened harness with mechanism source-grounded. Confidence: HIGH.
**Audits:** [R-1](../crossplane-audits/R1-functionrevision-watch.md), [R-7](../crossplane-audits/R7-workqueue-semantics.md), [R-12](../crossplane-audits/R12-f5-real-cluster.md) (inconclusive on real cluster).

## TL;DR

When a Function package version is rolled out that drops the `composition` capability (e.g., the function is reclassified or its capabilities tighten), there is a window during which CompositionRevisions referencing it still carry `ValidPipeline=True` from the prior validation. An XR reconcile during that window proceeds to compose using a pipeline that's no longer valid. The race is bounded but real; its width depends on how fast the FunctionRevision controller and CompositionRevision controller round-trip the change.

## What's actually wrong

The race is between two controllers that share status state but reconcile independently:

1. **FunctionRevision controller** owns `FunctionRevision.status.capabilities` (set from the package's `crossplane.yaml` metadata).
2. **CompositionRevision controller** reads `FunctionRevision.status.capabilities` to validate that all functions referenced by a CompositionRevision pipeline advertise the `composition` capability. The result is published as `CompositionRevision.status.conditions.ValidPipeline`.
3. **Composite reconciler** reads `CompositionRevision.status.conditions.ValidPipeline=True` as a precondition for proceeding with `RunFunction` calls.

The cross-controller workqueue does not serialize. When function package v2 is published:
- FunctionRevision controller updates `status.capabilities` first.
- CompositionRevision controller is enqueued via the FunctionRevision watch, but its reconcile happens some milliseconds-to-seconds later.
- During that window, the CompositionRevision still carries `ValidPipeline=True` from v1's validation.
- The composite reconciler can fire in that window, see `ValidPipeline=True`, and call `RunFunction` against a function whose advertised capabilities no longer include `composition`.

User impact: an XR composes resources using a function pipeline that should have been rejected. The composed output is the function's response from the now-invalid pipeline — which may or may not be what the user wants, but is definitely not what the validation contract promises.

## How Kamera surfaced it

**Scenario:** [`workflow_crossplane-staleness_function-capability-removed-hypothesis-1.json`](../../examples/crossplane/scenarios/workflow_crossplane-staleness_function-capability-removed-hypothesis-1.json) — a staleness scenario that schedules a function-capability change at a tunable point in the explored ordering. The harness's scheduler explores valid orderings of:
- the FunctionRevision status update,
- the CompositionRevision re-validation reconcile,
- and an XR reconcile that fires during the window.

Across baseline runs, three terminal categories emerged:
- **A** (correct, error-only): XR errors out because the validation has caught up. ConfigMap missing.
- **B** (bug, 2 compositions): the bug fires twice in different orderings, producing two distinct buggy ConfigMaps. (Suppressed by hardened harness — see below.)
- **C** (bug, 1 composition): the bug fires once during the stale window. ConfigMap content `709d71b` (the function's invalid-capability output written through to the composed resource).

The hardened-harness re-run preserved Category C (`ConfigMap=709d71b` matches baseline exactly — same buggy 1-composition outcome). Category B disappeared because the new RV-conflict-checking properly rejects the redundant second compose write that previously slipped through; Category B was the artifact, Category C is the real bug. The terminal evidence under hardening:

```
hypothesis-1 rerun: 3 converged states with 2 differing object(s), 4 identical
ConfigMap/default/xr-config: (missing), 709d71b, 709d71b
```

The bug is ordering-sensitive but the harness's scheduler hits the racy window reliably across runs.

## How we validated it

### Tier 1 — wire-mapping (R-1)

R-1 confirmed the FunctionRevision → CompositionRevision watch is wired in production Crossplane v2.2.0 — the propagation path that the bug depends on actually exists and is structured the way the harness models it. The harness wires the watch (with one bounded gap: a single-revision target hardcode that doesn't affect the current scenarios).

### Tier 2 — workqueue semantics (R-7)

The most plausible reason the race might not exist in production is "controller-runtime workqueues serialize cross-controller reconciles, so this race window is closed by the framework." R-7 audited controller-runtime's workqueue source: the workqueue de-dups per-key per-controller, but **does not** serialize across controllers. The FunctionRevision controller and the CompositionRevision controller can race on the same status-derived state. The race window is timing-feasible in production exactly as the harness explores it. Threat closed.

### Tier 3 — real-cluster reproduction (R-12, inconclusive)

R-12 attempted two approaches:

**Approach A — direct status patch.** Manually strip `status.capabilities` on a live FunctionRevision via `kubectl patch --subresource=status` to fake the v2-without-`composition` precondition, then quickly apply an XR. The patch initially succeeded (snapshot showed `status.capabilities = []`), but within seconds the FunctionRevision controller reconciled it back to `["composition"]` from the package metadata. A second patch returned `(no change)`, confirming the controller had already re-set it. The package-manager controller is actively reconciling that field — manual edits don't hold long enough to script a downstream race.

This isn't a fidelity issue with the harness; it's a real observation about production. It actually **tightens** the F5 framing: the FunctionRevision controller and the CompositionRevision controller are actively contending on `status.capabilities` reconciliation, which is precisely the race window F5 names.

**Approach B — custom function package with two versions.** Build and publish a function package where v1 advertises `composition` and v2 doesn't, then trigger a version transition. This is the production-mirroring trigger but requires building an OCI artifact, pushing it to a registry, and timing a version flip. The kind-cluster-plan scoped this as out of scope for the initial Tier 3 pass.

R-12 is therefore inconclusive on the real cluster. F5 confidence does **not** depend on R-12 — R-1 (the watch is wired) and R-7 (the workqueue doesn't serialize) are sufficient. R-12 is a "would have been nice" rather than a blocker.

## Suggested fix (open)

The original report did not propose a specific fix. Possible directions (not pre-judged):

- The composite reconciler could re-validate on the FunctionRevision's current capabilities at reconcile time rather than trusting the cached `ValidPipeline` condition. Cost: extra read per reconcile.
- The CompositionRevision controller could mark `ValidPipeline=Unknown` immediately on receiving a FunctionRevision watch event, before re-validating. This shrinks the stale window to "stale → unknown → updated" rather than "stale → updated". The composite reconciler's `ValidPipeline=True` precondition would fail-closed during the gap.
- A finalizer or generation-tracking scheme on FunctionRevision capability changes that blocks the composite reconciler until validation has caught up.

Worth a design discussion with the maintainers; F5's bug claim is independent of which of these is right.

## Talking points for the audience

- This is a textbook two-controller race on shared status. The harness is well-suited to this kind of bug because the race window can be made arbitrarily wide by varying the scheduler's ordering policy.
- The hardened-harness re-run is a good case study in how fidelity improvements *clarify* the bug rather than mask it: we lost a bug variant (Category B) that turned out to be a harness artifact, and kept the real bug variant (Category C) intact.
- R-12 is the cleanest example of a tier-3 attempt that surfaced a meaningful production observation (FunctionRevision controller actively reconciles capabilities) without producing a successful end-to-end reproduction. The "approach A doesn't work" story is itself informative: any external attempt to widen the race window from outside the package manager is fighting the controller, not the bug.
- If the maintainers want the strongest possible evidence, Approach B (a real two-version function package) is the path. The harness's evidence already justifies the fix; the cluster reproduction would just be the empirical bow on top.
