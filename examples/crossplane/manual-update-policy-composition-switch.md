# Manual Update Policy Composition Switch: Bug Analysis

## Scenario

When `compositionUpdatePolicy=Manual`, a user switches `compositionRef` from `widget-composition-alpha` to `widget-composition-beta` but keeps the old `compositionRevisionRef` pointing to `widget-composition-alpha-rev-1`. The question: does Crossplane detect and reject this cross-reference inconsistency, or does it silently use the wrong composition's revision?

## What was run

```bash
cd examples/crossplane
go run . -inputs workflow_crossplane-policy_manual-update-policy-composition-switch.json \
  -interactive=false -log-level=info -output=/tmp/workflow-manual-policy -depth 10
```

The workflow sets up:
- Two Compositions: `widget-composition-alpha` (writeConnectionSecretsToNamespace: `default`) and `widget-composition-beta` (writeConnectionSecretsToNamespace: `crossplane-system`)
- Two corresponding CompositionRevisions, each labeled with their parent composition
- An XWidget initially bound to alpha with Manual update policy
- User input: UPDATE XWidget to switch compositionRef to beta, keep revisionRef pointing to alpha-rev-1

## Source Code Analysis

The bug is in `APIRevisionFetcher.Fetch` at:
`~/go/pkg/mod/github.com/crossplane/crossplane/v2@v2.1.0/internal/controller/apiextensions/composite/api.go:157-168`

```go
func (f *APIRevisionFetcher) Fetch(ctx context.Context, cr resource.Composite) (*v1.CompositionRevision, error) {
    current := cr.GetCompositionRevisionReference()
    pol := cr.GetCompositionUpdatePolicy()

    // We've already selected a revision, and our update policy is manual.
    // Just fetch and return the selected revision.
    if current != nil && pol != nil && *pol == xpv1.UpdateManual {
        rev := &v1.CompositionRevision{}
        err := f.client.Get(ctx, types.NamespacedName{Name: current.Name}, rev)
        return rev, errors.Wrap(err, errGetCompositionRevision)
    }
    // ... automatic path follows with proper validation
```

The Manual policy code path performs a bare `Get(current revision)` with **zero validation** that the fetched revision belongs to the composition referenced by `compositionRef`. In contrast, the Automatic path (lines 170-196) fetches the Composition first, then lists revisions filtered by `crossplane.io/composition-name` label, ensuring the revision is associated with the correct composition.

## Trace Analysis

The Kamera trace output (`/tmp/workflow-manual-policy/`) confirms the bug:

### First reconcile (depth 1): Silently applies wrong revision

The CompositeReconciler runs and:
1. `SelectComposition` is a no-op (compositionRef already set to beta)
2. `Fetch` takes the Manual path at line 163, fetching `widget-composition-alpha-rev-1` -- a revision belonging to the **alpha** composition
3. `APIConfigurator.Configure` runs using alpha's revision, setting `writeConnectionSecretToRef.namespace = "default"` (from alpha's `writeConnectionSecretsToNamespace`)
4. The reconcile succeeds with log message: "Successfully composed resources"

Evidence from trace object 12 (XWidget state after first reconcile):
```json
{
  "spec": {
    "compositionRef": {"name": "widget-composition-beta"},
    "compositionRevisionRef": {"name": "widget-composition-alpha-rev-1"},
    "compositionUpdatePolicy": "Manual",
    "writeConnectionSecretToRef": {"name": "2kq9nx12", "namespace": "default"}
  }
}
```

The `namespace: "default"` value comes from alpha's revision. If the correct revision (beta's) had been used, the namespace would be `"crossplane-system"`. The XR reports `Synced=True, Ready=True, phase=Composed` -- no error, no warning.

### Second reconcile (depth 2): Cascading failure

The second reconcile fails with "no compatible CompositionRevisions found". This appears to be caused by the XR's spec being partially updated (the Kamera harness captures granular spec/status patches), leading to a state where `compositionUpdatePolicy` is absent, causing `Fetch` to fall through to the Automatic path. The Automatic path then tries to find revisions for beta but encounters issues. This cascading failure is a secondary consequence of the initial inconsistency.

## Bug Hypothesis

**Confirmed: Cross-reference inconsistency silently accepted under Manual update policy.**

`APIRevisionFetcher.Fetch` (api.go:163) does not validate that a manually-referenced CompositionRevision belongs to the Composition referenced by `compositionRef`. When a user switches `compositionRef` without updating `compositionRevisionRef`, the XR silently uses the old composition's revision and pipeline.

### Impact

- **Data misrouting**: Connection secrets are written to the wrong namespace (demonstrated: `default` instead of `crossplane-system`)
- **Wrong pipeline execution**: The XR executes the old composition's pipeline steps, which may produce entirely different composed resources
- **Silent failure**: No error, no warning event, no condition set. The XR reports `Synced=True, Ready=True` while using the wrong composition's logic
- **User confusion**: The XR's `compositionRef` says "beta" but the actual behavior comes from "alpha"

### Root cause

The Manual policy code path was written assuming that if a `compositionRevisionRef` exists, it must be valid and consistent. There is no cross-referencing check between `compositionRef` and `compositionRevisionRef`. The fix would be to verify that the fetched revision's `crossplane.io/composition-name` label matches `compositionRef.name` before returning it.

### Severity: P0

This is a pure logic bug -- no race conditions or staleness required. Any user who changes `compositionRef` under Manual policy without also updating `compositionRevisionRef` will hit this silently.
