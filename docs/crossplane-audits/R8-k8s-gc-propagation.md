# R-8: K8s GC propagationPolicy vs harness GC

**Status:** ✅ AUDITED with one significant fidelity gap. The harness GC is materially MORE eager than production K8s GC for the multi-owner case, and does not model `propagationPolicy` or `blockOwnerDeletion`. For the F3 retraction's purposes the gap may be benign, but it should be flagged.

**Threat addressed:** [CC-4](../upstream-updates/AUDIT-threats-to-validity.md#cc-4-ownerreference-auto-fixup), [F3-T2](../upstream-updates/AUDIT-threats-to-validity.md#f3-7222--already-retraction-candidate-threats-to-that-claim)

## Question

How does production K8s garbage collection behave (timing, propagationPolicy: Foreground/Background/Orphan), and does our `sleevectrl/pkg/controller/gc_controller.go` simulate it faithfully? In particular, does our GC controller cascade-delete things real K8s GC wouldn't?

## Research method

1. Read `sleevectrl/pkg/controller/gc_controller.go` end-to-end (135 lines).
2. WebFetched the K8s docs page on garbage collection (`https://kubernetes.io/docs/concepts/architecture/garbage-collection/`).
3. WebFetched the upstream `kubernetes/kubernetes` v1.30.0 garbage collector source (`pkg/controller/garbagecollector/garbagecollector.go`).
4. Compared `attemptToDeleteItem` semantics (multi-owner classification, propagationPolicy selection, `blockOwnerDeletion` handling) against our `Reconcile`.

## Findings

### Production K8s GC semantics (kubernetes v1.30.0)

| Behavior | Production GC |
|---|---|
| Multi-owner classification | `attemptToDeleteItem` partitions ownerReferences into `solid` (existing), `dangling` (missing), `waitingForDependentsDeletion`. **If any owner is solid, the dependent is NOT deleted** (log: `"item has at least one existing owner, will not garbage collect"`). |
| Propagation policy | `Foreground` if a `foregroundDeletion` finalizer is present; `Orphan` if an `orphan` finalizer is present; `Background` (default) otherwise. |
| `blockOwnerDeletion` | Read on the **owner-side** during foreground deletion (the GC waits for blocking dependents before deleting the owner). On the dependent-side it's not consulted at deletion time. |
| Foreground timing | Owner stays visible until **all dependents** with `blockOwnerDeletion=true` are gone. |
| Background timing | Owner deleted immediately; dependents cleaned **asynchronously** by the GC controller. Default. |
| Orphan | Owner deleted; dependents left in place (the `ownerReference` is removed from each dependent). |

### Harness GC semantics (`gc_controller.go`)

The harness GC is triggered after a REMOVE event and runs:

```go
// gc_controller.go:65-130 (paraphrased)
for _, kindInfo := range kindsToCheck {
    list := &unstructured.UnstructuredList{}
    list.SetAPIVersion(kindInfo.apiVersion); list.SetKind(kindInfo.listKind)
    r.Client.List(ctx, list, client.InNamespace(req.Namespace))
    for i := range list.Items {
        obj := &list.Items[i]
        ownerRefs := obj.GetOwnerReferences()
        if len(ownerRefs) == 0 { continue }
        if obj.GetDeletionTimestamp() != nil && !obj.GetDeletionTimestamp().IsZero() { continue }
        for _, ref := range ownerRefs {
            owner := &unstructured.Unstructured{}
            owner.SetAPIVersion(ref.APIVersion); owner.SetKind(ref.Kind)
            err := r.Client.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: ref.Name}, owner)
            if err == nil {
                if string(owner.GetUID()) != string(ref.UID) {
                    r.Client.Delete(ctx, obj); break // owner recreated, delete dependent
                }
                continue // owner exists with correct UID, OK
            }
            // Owner not found
            r.Client.Delete(ctx, obj); break // **delete on first dangling ref**
        }
    }
}
```

| Behavior | Harness GC | Match production? |
|---|---|---|
| Multi-owner classification | `break` on the **first** missing owner — deletes if ANY owner is missing | ❌ **No** — production requires ALL solid owners to be missing |
| Propagation policy | Implicit Background (single in-process loop) | ⚠️ Approximation — no Foreground/Orphan distinction |
| `blockOwnerDeletion` | Not consulted | ⚠️ Acceptable — production only consults this on the owner-side during Foreground |
| Recreated owner (UID mismatch) | Deletes dependent | ✅ Matches production behavior |
| Skip already-deleting | Yes (`if obj.GetDeletionTimestamp() != nil`) | ✅ Matches |
| Hardcoded kind list | Iterates a fixed list of 9 kinds (StatefulSet, Deployment, ReplicaSet, Pod, Service, ConfigMap, PVC, PDB, ServiceAccount) | ⚠️ Production iterates everything in the dependency graph; harness only sees these 9 kinds |

### Impact on F3

The F3 scenario involves a `Composition` being deleted while an XR remains. The composed resources (e.g., a `ConfigMap`) carry `ownerReferences` to the **XR** (the XWidget), not to the Composition itself. Production Crossplane does not put a Composition ownerRef on composed resources.

So the cascade-on-Composition-delete behavior of our GC depends on what owners the orphaned `CompositionRevision` carries. `CompositionRevision` is owned by the `Composition` (single ownerRef set by the real `composition.NewReconciler` we wire). When the `Composition` is deleted, our harness GC sees the revision has one ownerRef pointing at a missing Composition, and deletes the revision. This **matches** production for the single-owner case.

If a future scenario had a child with multiple ownerRefs (e.g., `ownerReferences=[ParentA, ParentB]` and only `ParentA` is deleted), our harness would incorrectly cascade-delete the child while production would keep it. **This gap doesn't fire on any current finding** but should be tracked.

### Impact on F6 orphan-persistence

The F6 orphan claim depends on whether a previously-composed `ConfigMap` survives after the function turns fatal. In F6:

- The `ConfigMap` carries an ownerReference to the **XR** (XWidget).
- The XR is NOT deleted in the F6 scenario.
- The function returns SEVERITY_FATAL and Crossplane skips GC (per R-3).

Our harness GC fires on REMOVE events. In F6, no REMOVE happens — neither the XR nor the Composition is deleted. So the harness GC never runs in F6. **No fidelity concern.**

If F6 were extended to a "delete-the-XR-while-fatal" subcase, our harness GC would correctly cascade-delete the orphan (matches production background deletion), and that would not invalidate the orphan-persistence claim because the claim is scoped to "while the XR exists."

## Source links

- Harness: `sleevectrl/pkg/controller/gc_controller.go:65-130`
- K8s docs: `https://kubernetes.io/docs/concepts/architecture/garbage-collection/`
- K8s GC controller: `https://github.com/kubernetes/kubernetes/blob/v1.30.0/pkg/controller/garbagecollector/garbagecollector.go` (`attemptToDeleteItem`, `classifyReferences`)

## Threat resolution

| Threat | Resolution |
|---|---|
| **CC-4**: harness GC respects `propagationPolicy` (Foreground / Background / Orphan) | NOT RESOLVED — harness implements only an approximation of Background. No Foreground or Orphan handling. **Fidelity gap acknowledged; does not affect F3/F6 in current scenarios.** |
| **CC-4**: harness GC cascades dependents that real K8s would orphan | PARTIAL — for single-owner deps, behavior matches Background. For multi-owner deps, harness over-deletes. **No current scenario uses multi-owner deps.** |
| **F3-T2**: harness GC closes the F3 error loop for spurious reasons | LOW RISK — `CompositionRevision` has a single owner (`Composition`). Both production K8s background GC and our harness GC will delete the revision on Composition delete. The retraction's mechanism (revision survives → permanent error loop) was independent of GC behavior and was driven by the prior DELETE-clobbering bug fixed in `e4daf33`. R-4 is still needed to confirm the new XWidget hashes don't encode a real `ReconcileError`. |

## What this means for F3 / F6 posting

For F6: no impact, harness GC doesn't run in F6 scenarios. Safe to post.

For F3: the GC fidelity gap doesn't affect the retraction claim because the relevant resource (`CompositionRevision`) has a single owner in the scenario. The retraction reasoning still depends on R-4 (trace audit of new XWidget hashes for `ReconcileError`).

## What's NOT addressed

- Whether the harness GC's hardcoded kind list (9 kinds) misses any kind that production scenarios would care about. For Crossplane scenarios, the relevant kinds are `ConfigMap`, `Secret` (not currently in the list — **gap**), and the various XR/composed-resource kinds (which are unstructured and would not be on this list either — **gap**). The harness GC currently relies on a per-scenario controller setup deciding which kinds to scan. **Recommend extending the kind list or making it configurable** if future scenarios use Secrets as composed resources.
- Multi-owner cascade behavior is silently wrong (over-eager). No current finding triggers this, but it should be tracked as a `// TODO` or deferred fix.
- Foreground deletion timing semantics are not modeled. Crossplane uses finalizers heavily; if a future scenario probes finalizer-based serialization, this gap would matter.
