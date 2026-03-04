# Finding: Potential Crossplane Bug (`kamera-3d7`)

Date: 2026-02-26  
Scope: `CompositionRevisionReconciler` status update behavior in Crossplane v2.1.0

## Summary

Crossplane's `CompositionRevisionReconciler` unconditionally calls `Status().Update(...)` on the success path, even when no status fields changed.

This appears to be a real Crossplane bug (at least an efficiency/idempotency bug), not just a Kamera artifact:

- In Crossplane source, there is no "status changed?" guard before update.
- In Kubernetes API server semantics, no-op updates are expected to be ignored (no write, no new `resourceVersion`).
- In Kamera replay, every `Status().Update` call is currently modeled as an `UPDATE` effect, which amplifies this into a reconcile loop in exploration.

So the unconditional update call is likely a Crossplane issue, while the infinite loop manifestation in replay depth tests is primarily a Kamera modeling issue.

## Reproduction (Kamera Scenario)

Using `examples/crossplane/two-step-workflow.json`, with subtree completion disabled:

```bash
go run . \
  --inputs two-step-workflow.json \
  --parallel-processes \
  --parallel-child-index=1 \
  --perturb=false \
  --explore-config /tmp/crossplane-no-subtree.json \
  -depth 50
```

`/tmp/crossplane-no-subtree.json`:

```json
{
  "optimizations": {
    "subtreeCompletion": false
  }
}
```

Observed artifact:

- Dump: `/tmp/crossplane-child-dump/crossplane_default_composition_create_then_update_1.jsonl`
- Path length: `51` at depth cap `50` (non-converged looping path)

## Source Evidence

### 1. Unconditional status update in Crossplane

`revision/reconciler.go` success path:

- [reconciler.go](/Users/tgoodwin/tmp/gomodcache/github.com/crossplane/crossplane/v2@v2.1.0/internal/controller/apiextensions/revision/reconciler.go:169)
- [reconciler.go](/Users/tgoodwin/tmp/gomodcache/github.com/crossplane/crossplane/v2@v2.1.0/internal/controller/apiextensions/revision/reconciler.go:171)

Relevant flow:

1. `status.MarkConditions(xpv1.ReconcileSuccess(), v1.ValidPipeline())`
2. unconditional `return ..., r.client.Status().Update(ctx, rev)`

### 2. Condition updates are already idempotent

Crossplane runtime condition model does an equality check (ignoring transition time):

- [condition.go](/Users/tgoodwin/tmp/gomodcache/github.com/crossplane/crossplane-runtime/v2@v2.1.0/apis/common/condition.go:98)
- [condition.go](/Users/tgoodwin/tmp/gomodcache/github.com/crossplane/crossplane-runtime/v2@v2.1.0/apis/common/condition.go:170)

`MarkConditions` only propagates observed generation and then calls `SetConditions`:

- [manager.go](/Users/tgoodwin/tmp/gomodcache/github.com/crossplane/crossplane-runtime/v2@v2.1.0/pkg/conditions/manager.go:63)

This means repeated reconciles commonly have no status delta, yet still issue update requests.

### 3. Watch/ownership path that re-enqueues reconciles

Composition controller owns `CompositionRevision`:

- [reconciler.go](/Users/tgoodwin/tmp/gomodcache/github.com/crossplane/crossplane/v2@v2.1.0/internal/controller/apiextensions/composition/reconciler.go:75)

New revisions carry owner reference to the parent `Composition`:

- [revision.go](/Users/tgoodwin/tmp/gomodcache/github.com/crossplane/crossplane/v2@v2.1.0/internal/controller/apiextensions/composition/revision.go:55)

Revision controller has a primary watch on `CompositionRevision`:

- [reconciler.go](/Users/tgoodwin/tmp/gomodcache/github.com/crossplane/crossplane/v2@v2.1.0/internal/controller/apiextensions/revision/reconciler.go:66)

### 4. Kubernetes no-op update semantics

API server storage interface docs:

- If serialized output equals input, `GuaranteedUpdate` does not perform an update.
- [interfaces.go](/Users/tgoodwin/tmp/gomodcache/k8s.io/apiserver@v0.34.1/pkg/storage/interfaces.go:217)

Generic registry test explicitly asserts no-op update should be ignored and not written:

- [store_test.go](/Users/tgoodwin/tmp/gomodcache/k8s.io/apiserver@v0.34.1/pkg/registry/generic/registry/store_test.go:879)
- [store_test.go](/Users/tgoodwin/tmp/gomodcache/k8s.io/apiserver@v0.34.1/pkg/registry/generic/registry/store_test.go:898)

## Loop Path in the Dump

In the replay dump:

- Step index `3` (`CompositionRevisionReconciler`) is a real status transition (`status: {}` -> `Synced/ValidPipeline` set).
- Step indices `5, 7, 9, ...` are repeated `UPDATE` effects where `CompositionRevision` hash is unchanged before/after (`12572c...`), and delta payload is empty.
- Each such modeled `UPDATE` still enqueues:
  - `CompositionRevisionReconciler` (primary watch)
  - `CompositionReconciler` (owner/owns relationship)

This is the execution path that creates the non-converging alternation under current replay semantics.

## Assessment

### Why this is a valid Crossplane finding

Crossplane currently issues status update requests when no status mutation is needed. That is a genuine idempotency/performance smell and can create unnecessary API traffic and conflict surface.

### Why the observed infinite loop is stronger in Kamera

Kamera currently models every `Status().Update` call as a state-changing `UPDATE` effect:

- [client.go](/Users/tgoodwin/projects/kamera/pkg/replay/client.go:283)

And trigger propagation enqueues from every effect without a no-op content guard:

- [trigger.go](/Users/tgoodwin/projects/kamera/pkg/tracecheck/trigger.go:225)

Real API server behavior generally suppresses no-op writes, so the exact loop intensity in replay over-approximates real cluster behavior.

## Proposed Next Actions

1. Open upstream Crossplane issue: add guard to avoid `Status().Update` when conditions/status are unchanged.
2. Add Crossplane unit test for idempotent reconcile: second reconcile should not call status update (or should produce no persisted delta).
3. Add Kamera modeling improvement: treat no-op update calls as no effect when object hash does not change, aligning replay with API server semantics.

