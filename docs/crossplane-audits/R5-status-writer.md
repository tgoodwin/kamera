# R-5: Kamera status subresource writer fidelity

**Status:** ✅ AUDITED — earlier triage note was either fixed or referred to a different gap; status subresource handling now correctly preserves `spec` and `metadata`.

**Threat addressed:** [CC-5](./cross-cutting-threats.md#cc-5-status-subresource-write-semantics), [CC-7](./cross-cutting-threats.md#cc-7-conditions-list-merge-semantics)

## Question

The earlier triage note `.yaks/crossplane-bug-triage/status-subresource-updates-treated-as-full-object-replacements/.context.md` reports: "applyEffects replaces entire object on status subresource update, causing XR to lose spec fields. Affects workflow 4b (manual-policy-switch-stale)." Does this gap still exist?

## Source

`pkg/replay/client.go:446-485`

```go
func (c *Client) Status() client.SubResourceWriter {
    return &subResourceClient{wrapped: c}
}

func (c *subResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
    preconditions := ExtractStatusUpdatePreconditions(opts)
    return c.wrapped.handleEffect(ctx, obj, event.UPDATE, &preconditions, &EffectOptions{Subresource: "status"})
}
```

`pkg/tracecheck/explore.go:1715-1718, 1767-1779`

The status flag is read in `applyEffects`:
```go
if effect.Subresource == "status" {
    mergedObj := mergeStatusSubresourceObject(oldObj, newObj, effect.OpType == event.PATCH || effect.OpType == event.APPLY)
}
```

`pkg/tracecheck/explore.go:1947-1972` — `mergeStatusSubresourceObject`:

```go
func mergeStatusSubresourceObject(oldObj, newObj *unstructured.Unstructured, preserveOnMissing bool) *unstructured.Unstructured {
    // ...
    mergedObj := oldObj.DeepCopy()
    sourceObj := newObj.DeepCopy()

    status, found, err := unstructured.NestedFieldNoCopy(sourceObj.Object, "status")
    // ...
    if !found {
        if preserveOnMissing { return mergedObj }
        delete(mergedObj.Object, "status")
        return mergedObj
    }

    mergedObj.Object["status"] = status
    return mergedObj
}
```

## Findings

✅ **`spec` and `metadata` are preserved** — `mergedObj := oldObj.DeepCopy()` keeps everything from the existing object except `status`.

✅ **Behavior matches production `Status().Update()`** — real Kubernetes also fully replaces the `status` block on UPDATE while preserving `spec`/`metadata`.

⚠️ **`status.conditions` is not merged by `type`** — when a controller writes a partial `status.conditions` list via `Status().Patch()` with strategic merge, the harness replaces the entire conditions list rather than merging by `type`. Real K8s SMP merges by `patchMergeKey: type`.

## Impact assessment

For F1, F5, F6 specifically:

- **Crossplane uses `Status().Update()`** in the relevant code paths (`reconciler.go:704, 754` and others). `Update` always replaces the entire status — production behavior matches our harness.
- **Crossplane sets conditions in memory before the Update** via `xpv1.SetConditions` which upserts by `type` *in the in-memory object*. The Update then writes the full updated condition list. The conditions-merge gap only matters for `Status().Patch()` callers with partial status — not relevant for these scenarios.

## Threat resolution

- **CC-5 status subresource write semantics:** RESOLVED. Spec/metadata preserved, status replaced — matches production for `Update`.
- **CC-7 conditions list merge:** PARTIALLY RESOLVED. Replace semantics work for the `Update` callers our scenarios use. A `Status().Patch()` with partial status would diverge, but Crossplane v2.2.0 doesn't do that in F1/F5/F6 paths.

## Conclusion

The earlier triage note's "loses spec fields" claim does not appear to apply to the current code. Either the issue was fixed or it referenced a different code path (the triage note specifically called out workflow 4b — `manual-policy-switch-stale` — and that scenario was re-run in SPRINT-0001 without producing spec-loss behavior in the trace).

No status-write fidelity gap that would invalidate F1/F5/F6 findings.

## What's NOT addressed

- The conditions-merge gap is real for `Status().Patch()` callers but doesn't affect Crossplane in v2.2.0's tested paths. If Crossplane adopts `Status().Patch()` with partial conditions in a future release, this gap would resurface.
