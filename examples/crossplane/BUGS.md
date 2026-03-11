# Crossplane Bug Findings

## 1. CompositionRevision reconciler performs unconditional status writes

**Controller:** `revision/reconciler.go` (`CompositionRevisionReconciler`)

**Version:** crossplane v2.1.0

**Summary:**
The CompositionRevision reconciler calls `r.client.Status().Update(ctx, rev)` on every reconcile pass regardless of whether the status actually changed. When conditions are already set and equal, `SetConditions` correctly skips the in-memory replacement, but the unconditional `Status().Update()` still produces a write to the API server.

**Mechanism:**
1. CompositionRevisionReconciler reconciles a revision and calls `Status().Update()` (lines 164, 171).
2. The write triggers watch events for the CompositionRevision.
3. The CompositionRevision has an ownerReference to its parent Composition.
4. The Composition controller (which `Owns(&v1.CompositionRevision{})`) is re-enqueued via owner-reference fanout.
5. The Composition controller runs, finds nothing to do ("No new revision needed"), but the CompositionRevisionReconciler has also been re-enqueued (as primary for the CompositionRevision kind).
6. Cycle repeats indefinitely.

**In production:** controller-runtime's workqueue rate limiter dampens the cycle so it appears stable, but the reconcilers never reach a true fixed point. Under high load or API server latency this could waste significant resources.

**Idiomatic fix (in Crossplane):**
```go
original := rev.DeepCopy()
status.MarkConditions(xpv1.ReconcileSuccess(), v1.ValidPipeline())
if !equality.Semantic.DeepEqual(original.Status, rev.Status) {
    return reconcile.Result{}, r.client.Status().Update(ctx, rev)
}
return reconcile.Result{}, nil
```

**Discovered by:** Kamera trace exploration on the `composition-create-then-update` scenario. Observable as unbounded growth of Total States while Distinct States and Resource States remain constant.
