# Workflow Analysis: manual-update-policy-composition-switch-stale

## What was run

Two workflows were executed and compared:

1. **Non-stale variant (4a):** `workflow_crossplane-policy_manual-update-policy-composition-switch.json`
   - `permuteControllers: [CompositeReconciler]`
   - No staleness configuration

2. **Stale variant (4a-stale):** `workflow_crossplane-policy_manual-update-policy-composition-switch-stale.json`
   - `permuteControllers: [CompositeReconciler, CompositionReconciler]`
   - `staleReads: {CompositeReconciler: [CompositionRevision]}`
   - `staleLookback: {CompositionRevision: 2}`

Both use: FunctionRevision + 2 Compositions (alpha, beta) + 2 CompositionRevisions + XWidget bound to alpha with Manual update policy. User input switches `compositionRef` to beta while keeping `compositionRevisionRef` pointing to `widget-composition-alpha-rev-1`.

Commands:
```
go run . -inputs workflow_crossplane-policy_manual-update-policy-composition-switch.json -interactive=false -log-level=info -output=/tmp/workflow-manual-policy-clean -depth 10
go run . -inputs workflow_crossplane-policy_manual-update-policy-composition-switch-stale.json -interactive=false -log-level=info -output=/tmp/workflow-manual-policy-stale -depth 10
```

## Observations

### Non-stale variant
- 3 distinct states, 3 total states, 3 resource states
- First reconcile (depth 1) succeeded: Manual policy path fetched alpha-rev-1 directly, applied it
- Second reconcile (depth 2) errored: "cannot fetch Composition: no compatible CompositionRevisions found"
- The second reconcile error is caused by a harness limitation (see Harness Issues below)

### Stale variant
- All branches aborted at depth 0 with: "cannot own CompositionRevision: widget-composition-alpha-rev-1 is already controlled by Composition widget-composition-alpha (UID )"
- CompositionReconciler fails because the ownerReference UIDs in the workflow JSON are empty strings and don't match the Kamera-assigned UIDs
- This prevents any exploration of the staleness scenario

## Harness Issues Discovered

Two issues with the Kamera test harness prevented full exploration:

### Issue 1: Status subresource updates replace entire object (blocks non-stale variant)

The Crossplane FunctionComposer's `Compose` method (composition_functions.go:605) calls `xfn.FromStruct(xr, ...)` which **replaces the XR's entire backing map** with the desired composite resource state. It then restores only apiVersion, kind, namespace, name, and UID -- but NOT spec fields like `compositionRef`, `compositionRevisionRef`, or `compositionUpdatePolicy`. The subsequent `Status().Patch()` call sends this stripped-down XR.

In real Kubernetes, `Status().Patch` with server-side apply only modifies the `/status` subresource. But the Kamera explorer's `applyEffects` (explore.go:1547-1588) treats UPDATE/APPLY effects as full-object replacements, so the status-only patch overwrites the spec, losing all composition-related fields.

After the first successful reconcile, the XR's stored state has no spec fields. The second reconcile reads this spec-less XR, finds `GetCompositionUpdatePolicy()` returns nil, enters the Automatic code path, and fails because `LatestRevision` returns nil (due to ownerRef UID mismatch).

### Issue 2: OwnerReference UID mismatch (blocks stale variant)

The workflow JSON sets ownerReference UIDs to empty strings (`"uid": ""`), but the Kamera harness assigns real UIDs to compositions at runtime. `metav1.IsControlledBy` compares UIDs, so it always returns false. The CompositionReconciler tries to re-add the controller reference but `meta.AddControllerReference` fails because there's already a controller ownerReference with a different (empty) UID.

Fix: Either omit ownerReferences from the workflow JSON (let CompositionReconciler add them), or have the harness fix up ownerReference UIDs after assigning object UIDs.

## Crossplane Bug Hypothesis (from code analysis)

Despite the harness limitations, the first reconcile in the non-stale variant successfully demonstrated the core bug, and code analysis confirms the staleness amplification.

### Bug: Manual policy allows cross-reference inconsistency with wrong revision metadata persisted on XR

**Location:** `APIRevisionFetcher.Fetch` in `internal/controller/apiextensions/composite/api.go:161-167`

**Mechanism:**

When `compositionUpdatePolicy` is Manual and `compositionRevisionRef` is set, the Fetch method (lines 161-167) returns the referenced revision without any validation that it belongs to the current `compositionRef`:

```go
if current != nil && pol != nil && *pol == xpv1.UpdateManual {
    rev := &v1.CompositionRevision{}
    err := f.client.Get(ctx, types.NamespacedName{Name: current.Name}, rev)
    return rev, errors.Wrap(err, errGetCompositionRevision)
}
```

After this, `APIConfigurator.Configure` (api.go:398-419) uses the revision's `writeConnectionSecretsToNamespace` to set the XR's `writeConnectionSecretToRef.namespace`. Since the revision belongs to the old composition (alpha), the XR gets alpha's connection secret namespace ("default") instead of beta's ("crossplane-system").

**Evidence from trace:** Object hash `9320fc55...` shows the XR after Configure with:
- `compositionRef.name: "widget-composition-beta"` (beta)
- `compositionRevisionRef.name: "widget-composition-alpha-rev-1"` (alpha's revision)
- `writeConnectionSecretToRef.namespace: "default"` (alpha's namespace, should be "crossplane-system")

### Staleness amplification (from code analysis)

With stale reads enabled for CompositionRevision on CompositeReconciler:

If the CompositionReconciler runs first and updates the alpha revision (e.g., incrementing its revision number or changing its hash), and then the CompositeReconciler runs with a stale read of CompositionRevision, it would fetch an older version of `widget-composition-alpha-rev-1`. This could mean:
- The `ValidPipeline` condition might not be True on the stale version, causing the reconciler to reject it (reconciler.go:633)
- The stale revision's `writeConnectionSecretsToNamespace` or pipeline configuration might differ from the current version

However, in this specific scenario, the staleness amplification is limited because:
1. The Manual policy path fetches the revision by exact name (not by label/list), so it always gets a specific revision
2. The staleness would need to produce a version of the revision that has different content, which requires prior mutations to that revision

The more significant interaction is: if the CompositionReconciler creates a **new** revision for beta (e.g., `widget-composition-beta-rev-2`), but the CompositeReconciler reads a stale state that doesn't include this new revision, the Manual policy path would still fetch the old alpha revision by name. This means the XR stays pinned to the wrong revision even when a correct one exists -- but this is actually the *intended* behavior of Manual policy. The bug is that Manual policy doesn't validate the revision matches the composition, not that staleness changes the behavior.

### Severity

**P1 (High):** The cross-reference inconsistency is a real Crossplane bug that can lead to:
- Connection secrets written to the wrong namespace
- Pipeline configuration from the wrong composition being applied
- Silent data corruption where the XR appears healthy but uses mismatched configuration

The staleness amplification is **minimal** for this specific scenario because the Manual policy path uses exact-name Get (not List), so stale reads don't change which revision is fetched.

### Suggested fix (in Crossplane)

In `APIRevisionFetcher.Fetch`, validate that the referenced revision belongs to the current composition:

```go
if current != nil && pol != nil && *pol == xpv1.UpdateManual {
    rev := &v1.CompositionRevision{}
    err := f.client.Get(ctx, types.NamespacedName{Name: current.Name}, rev)
    if err != nil {
        return nil, errors.Wrap(err, errGetCompositionRevision)
    }
    // Validate revision belongs to the current composition
    compName := cr.GetCompositionReference().Name
    if rev.Labels[v1.LabelCompositionName] != compName {
        return nil, errors.Errorf("compositionRevisionRef %q does not belong to composition %q", current.Name, compName)
    }
    return rev, nil
}
```
