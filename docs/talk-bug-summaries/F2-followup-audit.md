# F2 follow-up audit: why the post-fix run still has 1 max-depth-aborted state

After applying PR #7283 to the composite + revision reconcilers, the F2 demo scenario at depth=50 still has 1 max-depth-aborted terminal out of 2 (down from 3 of 12 pre-fix). The user asked: *the F2 fix should let the system converge — what's still spinning?* This audit traces the remaining loop.

## TL;DR

**PR #7283 is incomplete.** It guards 1 of 13 `r.client.Status().Update()` call sites in `composite/reconciler.go` — the final happy-path return at line 877. The reconciler still issues 5+ status writes per invocation from the other 12 (currently unguarded) call sites in the reconcile body.

The harness path trace shows this directly: from step 8 onwards, the XR hash and ConfigMap hash are stable (`faf78f4` and `60f2920`), yet every CompositeReconciler invocation produces 5+ "UPDATE XWidget/example" effects plus an "APPLY ConfigMap" effect. The harness's RV-conflict-checking suppresses these as no-ops, so the state hash doesn't change — but the writes are still attempted, which is the very behavior F2 reports.

The harness reveals that the fix moved the needle (3 aborts → 1, 297 visits → 223) but did not close the gap.

## Evidence

### The reconciler has 13 Status().Update() call sites; PR #7283 guards 1

```
$ grep -nE "client\.Status\(\)\.Update" .../composite/reconciler.go
583, 599, 607, 618, 632, 649, 672, 693, 708, 758, 775, 791, 877
```

PR #7283's diff:

```go
+	xrBefore := xr.DeepCopyObject()
 ...
-	return result, errors.Wrap(r.client.Status().Update(updateCtx, xr), errUpdateStatus)
+	if !cmp.Equal(xr, xrBefore) {
+		return result, errors.Wrap(r.client.Status().Update(updateCtx, xr), errUpdateStatus)
+	}
+	return result, nil
```

The `cmp.Equal(xr, xrBefore)` guard is added only around line 877 — the final return on the happy path. All 12 other call sites (mostly in error or early-return branches) fire unconditionally.

### Path trace from the post-fix max-depth-aborted state

Path 0 of `aborted-1ds2dska` (43 paths, all hit depth 50):

| Step | Controller | XR hash | CM hash | Effects (truncated) |
|---|---|---|---|---|
| 0 | CompositeReconciler | `4812019` | — | UPDATE XWidget×2 |
| 1 | CompositionReconciler | — | — | CREATE CompositionRevision/widget-composition-4e01990 |
| 2 | CompositeReconciler | `212f73d` | — | UPDATE XWidget×2 |
| 4 | CompositionRevisionReconciler | — | — | UPDATE CompositionRevision (mark ValidPipeline=True) |
| 5 | CompositeReconciler | `8417824` | `709d71b` | UPDATE×2, APPLY XWidget, APPLY ConfigMap, ... |
| **8** | **CompositeReconciler** | **`faf78f4`** | **`60f2920`** | UPDATE×5, APPLY ConfigMap, ... |
| 9–50 | CompositeReconciler | `faf78f4` | `60f2920` | (same effects pattern, repeated) |

From step 8 onwards, the XR object hash never changes — meaning no write actually mutates the XR. **But each reconcile still attempts ~6 writes against it.** These writes are no-op from the API server's perspective (RV conflict checking returns 409 on identical content), but they're still controller calls to `client.Status().Update()`.

This matches the F2 bug exactly: the reconciler is doing *more writes than necessary in steady state*. PR #7283 took the worst offender (the unconditional final update) off the table; the rest remains.

### Depth budget is not the limit

Re-running at `-depth 200` produces the same outcome: 1 max-depth-aborted state. Total visits scaled with depth (223 → 373) but unique resource states stayed at 23. **The system genuinely does not converge** — the harness keeps scheduling CompositeReconciler from a stable state because the reconciler keeps emitting writes that the scheduler interprets as state-perturbing activity.

## What this means for the talk

This is actually the most interesting empirical result of the demo, not a footnote. Three angles worth raising:

1. **The harness finds an incomplete fix.** PR #7283 fixes one of thirteen call sites. The simulator detects that the others still produce write churn. This is exactly the kind of finding a state-space-aware harness should produce — the maintainer's PR review didn't surface this, the harness did.

2. **The fix-with-context vs fix-the-mechanism question.** PR #7283 is a *targeted* fix on the call site that surfaced empirically as the pain point. The harness suggests a *systematic* fix would refactor the reconciler to snapshot once, do all work, and only emit writes if the final state differs — i.e., apply the same `cmp.Equal` pattern globally rather than per-call-site.

3. **The simulator quantifies "almost converged."** Pre-fix: 12 distinct terminals, 3 max-depth aborts, 297 visits. Post-fix: 2 distinct terminals, 1 max-depth abort, 223 visits. The fix is real and measurable, but the state space isn't closed yet. That's a credible, calibrated finding to present to the maintainers.

## Suggested follow-up for the maintainers

A natural extension to PR #7283: apply the snapshot-and-cmp pattern to all 12 remaining call sites in `composite/reconciler.go`, or refactor the reconciler so all status writes flow through a single guarded path at the end. This would close the harness loop (zero max-depth aborts on the F2 scenario) and remove the residual write amplification.

Could be worth raising as a follow-up issue or a comment on PR #7283 itself, citing the harness output.
