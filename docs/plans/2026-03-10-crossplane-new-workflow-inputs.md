# New Crossplane Workflow Inputs for Bug Discovery

## Context

We found a nonconvergence bug (unconditional `Status().Update()` in `CompositionRevisionReconciler`) using the existing two-step workflows. The current `two-step-workflow.json` has two scenarios that exercise narrow flows (XR composition switching, Composition spec update). These are now dominated by the known update-cycle bug, limiting discovery of new issues. We need workflows that exercise different controller interaction patterns, leverage staleness perturbation, and test edge cases in revision selection, deletion, concurrent XR management, and cross-controller consistency.

## Available Controllers & Their Read/Write Patterns

| Controller | Reads | Writes | Key Stale Risk |
|---|---|---|---|
| `CompositionReconciler` | Get(Composition), List(CompositionRevision) | Create/Update(CompositionRevision) | Stale Composition hash skips needed revision creation |
| `CompositionRevisionReconciler` | Get(CompositionRevision), CheckCapabilities(FunctionRevision) | Status().Update(CompositionRevision) | Stale FunctionRevision → incorrect ValidPipeline |
| `CompositeReconciler` | Get(XR), Get(Composition), List(CompositionRevision), check ValidPipeline condition | Update(XR refs/status) | Stale revision list → wrong "latest"; stale ValidPipeline → uses unvalidated revision |

## Proposed Scenarios (7 total, new JSON file)

### 1. `crossplane-staleness/xr-created-before-revision-validated`
**Target bug**: Race between revision creation and pipeline validation. CompositeReconciler runs before CompositionRevisionReconciler has set `ValidPipeline=True` on the newly created revision.

- **Environment**: FunctionRevision + Composition only (no pre-existing revision or XR)
- **User input**: CREATE XWidget
- **Tuning**: `permuteControllers: [all three]`, `staleReads: {CompositeReconciler: [CompositionRevision]}`, `staleLookback: {CompositionRevision: 2}`
- **Why new**: Exercises the "bootstrap" path where XR, Composition, and revision are all being created/reconciled simultaneously. Existing workflows pre-seed revisions.

### 2. `crossplane-staleness/composition-update-races-xr-fetch`
**Target bug**: Stale Composition Get mixed with fresh revision List in `APIRevisionFetcher.Fetch`. The `LatestRevision()` call at `api.go:183` filters by `metav1.IsControlledBy` -- if Composition UID is stale, owner-reference matching may fail.

- **Environment**: FunctionRevision + Composition + CompositionRevision(rev-1) + XWidget (bound, Automatic policy)
- **User input**: UPDATE Composition (change `writeConnectionSecretsToNamespace`)
- **Tuning**: `permuteControllers: [CompositeReconciler, CompositionReconciler]`, `staleReads: {CompositeReconciler: [Composition, CompositionRevision]}`, `staleLookback: {Composition: 1, CompositionRevision: 2}`
- **Why new**: Tests what happens when an XR is mid-reconcile and its Composition changes underneath it. Existing workflow 1 switches the XR's compositionRef; this one changes the Composition itself.

### 3. `crossplane-deletion/xr-deleted-with-active-composition`
**Target bug**: Stale XR read during deletion. If CompositeReconciler reads pre-deletion XR, it proceeds with full reconcile (finalizer, revision selection, compose) on a doomed object.

- **Environment**: FunctionRevision + Composition + CompositionRevision(rev-1, validated) + XWidget (bound)
- **User input**: DELETE XWidget
- **Tuning**: `permuteControllers: [all three]`, `staleReads: {CompositeReconciler: [example.org/XWidget]}`, `staleLookback: {example.org/XWidget: 1}`
- **Why new**: No existing workflow tests deletion. Tests finalizer removal race and whether state converges after deletion.

### 4a. `crossplane-policy/manual-update-policy-composition-switch`
**Target bug**: Pure logic bug (no staleness). When `compositionUpdatePolicy=Manual`, `APIRevisionFetcher.Fetch` at `api.go:163` only does `Get(current revision)` without verifying the revision belongs to the referenced Composition. If user changes `compositionRef` but old `compositionRevisionRef` persists, the XR silently uses the wrong composition's pipeline.

- **Environment**: FunctionRevision + 2 Compositions (alpha, beta) + 2 CompositionRevisions (one per composition, both validated) + XWidget (bound to alpha, Manual policy)
- **User input**: UPDATE XWidget (switch compositionRef to beta, keep old revisionRef pointing to alpha's revision)
- **Tuning**: `permuteControllers: [CompositeReconciler]` -- no staleness, clean logic-only test
- **Why new**: All existing workflows use Automatic policy. Manual policy has a completely different code path in `Fetch` that never validates cross-reference consistency.

### 4b. `crossplane-policy/manual-update-policy-composition-switch-stale`
**Target bug**: Same as 4a but with staleness on CompositionRevision reads. After the user switches compositionRef, if CompositeReconciler reads a stale CompositionRevision that still reflects the old composition's state, it may compound the logic bug by persisting stale revision metadata on the XR.

- **Environment**: Same as 4a
- **User input**: Same as 4a
- **Tuning**: `permuteControllers: [CompositeReconciler, CompositionReconciler]`, `staleReads: {CompositeReconciler: [apiextensions.crossplane.io/CompositionRevision]}`, `staleLookback: {apiextensions.crossplane.io/CompositionRevision: 2}`
- **Why new**: Tests whether staleness amplifies the Manual policy logic bug into a more severe state corruption.

### 5. `crossplane-concurrency/two-xrs-shared-composition-update`
**Target bug**: Two XRs with Automatic policy independently call `Fetch`, both try to update their `compositionRevisionRef` to the latest revision. Under staleness, one XR may pin to the old revision while the other advances, causing permanent divergence.

- **Environment**: FunctionRevision + Composition + CompositionRevision(rev-1, validated) + 2 XWidgets (xr-one, xr-two, both bound to same composition, Automatic)
- **User input**: UPDATE Composition
- **Tuning**: `permuteControllers: [all three]`, `staleReads: {CompositeReconciler: [CompositionRevision]}`, `staleLookback: {CompositionRevision: 2}`
- **Why new**: First multi-XR scenario. Tests whether controller ordering causes asymmetric revision selection when multiple XRs share a composition.

### 6. `crossplane-staleness/function-capability-removed`
**Target bug**: Stale FunctionRevision in `CompositionRevisionReconciler`. `CheckCapabilities` lists FunctionRevisions to validate pipeline. If it reads a stale FunctionRevision (still has "composition" capability), it sets `ValidPipeline=True` even though the capability was removed. CompositeReconciler then trusts this stale validation.

- **Environment**: FunctionRevision (with capabilities) + Composition + CompositionRevision(rev-1, validated) + XWidget (bound)
- **User input**: UPDATE FunctionRevision (set capabilities to `[]`)
- **Tuning**: `permuteControllers: [CompositeReconciler, CompositionRevisionReconciler]`, `staleReads: {CompositionRevisionReconciler: [pkg.crossplane.io/FunctionRevision]}`, `staleLookback: {pkg.crossplane.io/FunctionRevision: 1}`
- **Why new**: First workflow to modify FunctionRevision. Exercises the cross-controller trust chain: RevisionReconciler certifies validity, CompositeReconciler trusts it.

### 7. `crossplane-deletion/composition-deleted-while-xr-bound`
**Target bug**: Orphaned `compositionRef` with no recovery path. CompositeReconciler reads stale (pre-deletion) Composition, proceeds to list revisions. But revisions may be GC'd via owner-reference cascade. `LatestRevision` returns nil, XR enters permanent error state.

- **Environment**: FunctionRevision + Composition + CompositionRevision(rev-1, validated) + XWidget (bound)
- **User input**: DELETE Composition
- **Tuning**: `permuteControllers: [CompositeReconciler, CompositionReconciler]`, `staleReads: {CompositeReconciler: [Composition]}`, `staleLookback: {Composition: 1}`
- **Why new**: Tests the transition from "composition exists" to "composition is NotFound" and whether the XR's error handling reaches a fixed point.

## Priority Ranking

| Priority | Scenario | Rationale |
|---|---|---|
| **P0** | 4a (Manual policy switch, clean) | Pure logic bug, no staleness needed, likely a real semantic error in Crossplane |
| **P0** | 4b (Manual policy switch, stale) | Tests if staleness amplifies the logic bug |
| **P0** | 1 (XR before validation) | Bootstrap race is a fundamental ordering dependency |
| **P1** | 6 (Function capability removed) | Novel input type, exercises trust chain between controllers |
| **P1** | 2 (Composition update races XR) | Targets the most complex read path (Fetch) |
| **P2** | 5 (Two XRs) | Concurrent XR divergence under staleness |
| **P2** | 3 (XR deletion) | Deletion race with stale pre-deletion read |
| **P2** | 7 (Composition deletion) | Orphan handling, may depend on GC modeling fidelity |

## Implementation

Workflow JSON file: `examples/crossplane/new-workflows.json` (8 scenarios total: 7 distinct + 4b staleness variant).

## Verification
1. `go build ./examples/crossplane/...` to ensure JSON parses correctly
2. Run: `cd examples/crossplane && go run . -input new-workflows.json` on individual scenarios
3. Check output for: unbounded state growth (nonconvergence), distinct-vs-total state divergence, error conditions that never resolve
