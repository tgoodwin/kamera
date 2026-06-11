# F2 demo: harness convergence before vs after PR #7283

Demonstrates that PR [#7283](https://github.com/crossplane/crossplane/pull/7283) (the F2 fix) makes the kamera harness converge cleanly on a steady-state scenario that exhibits state-space pollution under the unfixed code.

## What the demo shows (clean version)

A minimal scenario that exercises **only the CompositionRevisionReconciler**: seeded with a FunctionRevision (capabilities=["composition"]) and a CompositionRevision (no `ValidPipeline` condition pre-set). No XR, no Composition. The reconciler's only job is to validate the function pipeline and mark `ValidPipeline=True`.

This isolates F2's fix surface to a single code path that uses **only `r.client.Status().Update(...)`** — no `client.Patch(client.Apply, ...)` SSA calls. The harness's SSA-Apply fidelity gap (see "Notes on harness fidelity" below) is sidestepped entirely.

| Metric | Pre-fix (v2.2.0) | Post-fix (PR #7283) | Δ |
|---|---|---|---|
| Unique node visits | 2 | 3 | +1 |
| **Total node visits** | **51** | **3** | **−94%** |
| Unique resource states | 2 | 2 | same |
| **Max-depth aborted states** | **1** | **0** | **fully eliminated** |
| **Converged states** | **0** | **1** | **converges** |
| Wall-clock | 0.22s | 0.03s | −86% |

The headline:

- **Pre-fix the harness never converges** — it exhausts depth 50 doing the same `Status().Update()` 25+ times over the same 2 distinct states. Same content, bumped resourceVersion, watch event, re-enqueue, repeat.
- **Post-fix the harness converges in 3 reconciles flat.** First reconcile establishes `ValidPipeline=True`; second reconcile sees no status change and returns without writing; the harness detects no further activity and terminates.

## Files

- **Scenario:** [`examples/crossplane/scenarios/workflow_f2_revision-only-convergence.json`](../../examples/crossplane/scenarios/workflow_f2_revision-only-convergence.json) — clean F2-demo scenario.
- **Harness binaries** (built locally, not committed):
  - `examples/crossplane/crossplane-prefix` — built against vanilla `github.com/crossplane/crossplane/v2 v2.2.0`.
  - `examples/crossplane/crossplane-postfix-revfix` — built with a `replace` directive pointing at a worktree of `crossplane/crossplane` containing PR #7283 cherry-picked onto `v2.2.0`.
- **Cherry-pick worktree:** `/tmp/crossplane-f2`, branch `f2-on-v2.2.0`, base `v2.2.0` + commit `70cd0e6cf` ("reconciler fix: only update status if it changed"). Cherry-pick had a trivial 6-line import-block conflict; resolved by keeping both `xpv1` (still needed at v2.2.0) and `cmp` (added by the PR).

## Reproduce

```bash
# 1. Set up the patched crossplane worktree
cd /Users/tgoodwin/projects/crossplane
git worktree add -b f2-on-v2.2.0 /tmp/crossplane-f2 v2.2.0
cd /tmp/crossplane-f2
git cherry-pick 70cd0e6cf
# (resolve trivial import-block conflict in composite/reconciler.go and
#  revision/reconciler.go, then `git add` and
#  `git -c core.editor=true cherry-pick --continue`)

# 2. Build pre-fix harness
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go build -o crossplane-prefix .

# 3. Add replace directive (in examples/crossplane/go.mod) right after the
#    existing kamera replace directive:
#       replace github.com/crossplane/crossplane/v2 => /tmp/crossplane-f2
go mod tidy

# 4. Build post-fix harness
go build -o crossplane-postfix .

# 5. Run pre-fix scenario
mkdir -p /tmp/f2-demo/rev-prefix
./crossplane-prefix -interactive=false -closed-loop=false -depth 50 \
  -inputs scenarios/workflow_f2_revision-only-convergence.json \
  -output /tmp/f2-demo/rev-prefix/ -log-level error

# 6. Run post-fix scenario
mkdir -p /tmp/f2-demo/rev-postfix
./crossplane-postfix -interactive=false -closed-loop=false -depth 50 \
  -inputs scenarios/workflow_f2_revision-only-convergence.json \
  -output /tmp/f2-demo/rev-postfix/ -log-level error

# 7. Compare
/Users/tgoodwin/projects/kamera/bin/kamera analyze campaign-metrics \
  /tmp/f2-demo/rev-prefix/f2_revision_only_convergence_0.jsonl
/Users/tgoodwin/projects/kamera/bin/kamera analyze campaign-metrics \
  /tmp/f2-demo/rev-postfix/f2_revision_only_convergence_0.jsonl
```

After the demo, restore the harness's `go.mod` (remove the `replace` directive) and `go mod tidy` to get back to a clean tree.

## Notes for the talk

- The cleanest message: **before the fix, the harness can't make this 2-object scenario terminate; after the fix, it terminates in 3 steps.**
- Pre-fix's 51 total visits across only 2 unique states is the F2 fingerprint exposed: the same `Status().Update()` is being attempted 25+ times with identical content, each one producing a watch event that re-enqueues the controller.
- This scenario exercises only the **CompositionRevisionReconciler** — both call sites in that reconciler are covered by PR #7283. So the convergence is entirely attributable to the fix.

## Notes on harness fidelity

An earlier draft of this demo used a CompositeReconciler-driven scenario (XR + Composition + composed ConfigMap). That scenario exposed a separate harness fidelity gap: the composer's `client.Patch(client.Apply, ...)` SSA calls (composition_functions.go:558, 596, 662) are mismodeled as whole-object replace rather than field-manager merge, so each reconcile strips `metadata.generation` and `spec.resourceRefs`, which the harness's auto-fixup re-adds, producing a perpetual loop independent of F2.

That fidelity gap is exactly the unimplemented "Fix 3" tracked under the C2/C4 negative-result work — extending the harness's status-write fidelity model (R-5) to cover SSA Apply field-manager semantics. For the talk, the right move is to use the revision-only scenario above (which doesn't hit the gap) and disclose the SSA-fidelity gap honestly as a known limitation. See [F2-followup-audit.md](./F2-followup-audit.md) for the full diagnosis of the SSA gap.

## Cost comparison: scenario redesign vs harness Fix 3

| Approach | Effort | Demo cleanliness | Other benefits |
|---|---|---|---|
| **CompositionRevision-only scenario** (this doc) | ~30 min | Clean: 0 → 1 converged state, 51 → 3 visits | Narrowly demos one of two reconcilers PR #7283 covers |
| Implement harness Fix 3 (SSA field-manager bookkeeping) | Days–weeks of pkg/tracecheck/ work | Full demo across all paths PR #7283 touches (composite + revision) | Unblocks C4 re-investigation; broadens R-5 fidelity claims; benefits every future Crossplane scenario |

For the upcoming talk: take the scenario-redesign route. The clean-convergence story is fully sufficient to demonstrate F2's fix and the kamera methodology. Fix 3 is a substantial harness investment that's better positioned as a "future work" beat than a blocker.

Should the talk audience want the composite-path demo, the natural follow-up is to invest in Fix 3 and re-run the originally-attempted scenario at [`workflow_f2_steady-state-convergence.json`](../../examples/crossplane/scenarios/workflow_f2_steady-state-convergence.json) — that scenario is preserved in the repo for that purpose.
