> **STALE**: This analysis contains outdated conclusions from multiple investigation rounds. See [ANALYSIS.md](ANALYSIS.md) for the current evidence-grounded analysis.

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

## Re-run (2026-03-11)

### What was run

```bash
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go run . -inputs workflow_crossplane-policy_manual-update-policy-composition-switch.json \
  -interactive=false -log-level=info -depth 30 \
  -output=/tmp/rerun-manual-policy-clean
```

Ran at depth 10, 20, and 30 to check for convergence trends.

### Campaign metrics

**Reference run (depth=30):**
```
  unique node visits:        19
  total node visits:         31
  unique resource states:    9
  aborted states:            1
  max-depth aborted states:  1
```

**Rerun (depth=30):**
```
  unique node visits:        2,766
  total node visits:         3,541
  unique resource states:    86
  aborted states:            65
  max-depth aborted states:  65
```

**Depth progression (rerun):**

| Depth | Unique nodes | Aborted states | Resource states |
|-------|-------------|----------------|-----------------|
| 10    | 1,880       | 308            | 86              |
| 20    | 2,696       | 66             | 86              |
| 30    | 2,766       | 65             | 86              |

### Answers to key questions

1. **Did the reference run converge?** No. It cycles (19 unique nodes at both depth=20 and depth=30; total visits grow linearly with depth).
2. **Did the reference run hit max depth?** Yes -- pure cycle at 19 unique states.
3. **Did the perturbed runs converge?** No. All 65 terminal states are max-depth aborts.
4. **Did the perturbed runs hit max depth?** Yes. The rerun unique node count plateaus around 2,766, suggesting the state space is fully explored but all paths cycle.
5. **Are there multiple distinct converged states?** N/A -- no states converged.
6. **How do the campaign-metrics compare?** The rerun explores vastly more state space (2,766 vs 19 unique nodes, 86 vs 9 resource states) due to controller permutations, but both cycle.

### What changed vs previous conclusions

Previously, the P0 bug was confirmed: the first reconcile silently applied the wrong revision (alpha's revision while compositionRef pointed to beta). The second reconcile failed with "no compatible CompositionRevisions found" due to a harness issue with the status subresource.

Now with recent commits:
- **Commit `af250b25` (fix status subresource effect handling):** This was expected to fix the second-reconcile failure. The exploration now reaches 2,766 unique nodes (vs the handful from before), meaning significantly more state space is explored. The second reconcile no longer crashes the harness.
- **Commit `38ff304d` (error tolerance):** Errors that previously terminated paths are now re-enqueued, allowing further exploration.
- **Commit `d2935ba9` (auto-fixup ownerReference UIDs):** Removes the need for manual UID correction in the workflow JSON.

The rerun explores 86 distinct resource states (a large variety), but all paths eventually cycle. The cycling is caused by the unconditional `Status().Update()` bug (Bug #1) which generates infinite resource version increments.

**The P0 bug finding (cross-reference inconsistency silently accepted) is still valid.** The traces still show "Successfully composed resources" using alpha's revision while `compositionRef` points to beta. The much larger state space (86 resource states) now shows many more variations of how this bug manifests across different controller orderings.

### Previously-reported issues resolved by recent commits

- **Second reconcile failure (harness status subresource issue):** Commit `af250b25` fixes this. The exploration now proceeds well past the first reconcile, reaching thousands of unique nodes.
- **OwnerReference UID mismatch:** Commit `d2935ba9` auto-fixes UIDs.
- **The P0 cross-reference bug is unchanged** -- `APIRevisionFetcher.Fetch` still performs no validation in the Manual policy path.

## Re-run (2026-03-12, depth=100)

### What was run

```bash
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go run . -inputs scenarios/workflow_crossplane-policy_manual-update-policy-composition-switch.json \
  -interactive=false -log-level=info \
  -output=/tmp/depth100-manual-policy-clean
```

Also ran at depth 400 to confirm cycling.

### Campaign metrics

**Depth 100:**
```
invocation: f16ffd52-86dd-4a4e-929f-5cbdf3b782da
  unique node visits:        19
  total node visits:         101
  unique resource states:    9
  duration:                  0s
  aborted states:            1
  max-depth aborted states:  1
```

**Depth 400:**
```
invocation: 3b1eaef2-cb44-4834-9c06-57d66b86171d
  unique node visits:        19
  total node visits:         401
  unique resource states:    9
  duration:                  1s
  aborted states:            1
  max-depth aborted states:  1
```

Only the reference file was produced (63MB at depth 100, ~1GB at depth 400). No rerun file generated.

### Answers to key questions

1. **Did the reference run converge?** No. Cycles through 19 unique nodes and 9 resource states indefinitely.
2. **Did the reference run hit max depth?** Yes -- at depth 100 and 400. Unique node count stays fixed at 19.
3. **Did the perturbed run(s) converge?** N/A -- no rerun generated (reference did not converge).
4. **Did the perturbed runs hit max depth?** N/A.
5. **Are there multiple distinct converged states?** No -- zero converged states.
6. **How do the campaign-metrics compare?** Only reference available. Identical to 2026-03-11 reference run (19 nodes, 9 resource states).

### Comparison with previous runs

| Metric | Original | 2026-03-11 Ref (d=30) | 2026-03-11 Rerun (d=30) | 2026-03-12 (d=100) | 2026-03-12 (d=400) |
|--------|---------|----------------------|------------------------|---------------------|---------------------|
| Unique node visits | ~5 | 19 | 2,766 | 19 | 19 |
| Total node visits | ~5 | 31 | 3,541 | 101 | 401 |
| Unique resource states | ~5 | 9 | 86 | 9 | 9 |
| Converged states | 0 | 0 | 0 | 0 | 0 |
| Max-depth aborted | 0 | 1 | 65 | 1 | 1 |

The reference run metrics are identical across all depths tested (19 unique nodes, 9 resource states). The rerun phase was not generated in this run, so the rich 2,766-node exploration from 2026-03-11 is not replicated here.

Note: The output file sizes are very large (63MB at depth 100, ~1GB at depth 400) despite only 19 unique nodes. This suggests the trace records are individually large (many objects in the state per step).

### Updated conclusions

The **P0 cross-reference inconsistency bug** remains confirmed. The `APIRevisionFetcher.Fetch` Manual policy path still performs no validation that the referenced CompositionRevision belongs to the referenced Composition. The cycling behavior at depth 100 and 400 matches exactly what was seen at depth 30 in the 2026-03-11 run. No new findings. The proposed fix (validate `crossplane.io/composition-name` label on the fetched revision) remains valid.
