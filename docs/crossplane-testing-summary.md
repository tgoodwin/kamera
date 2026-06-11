# Crossplane Testing with Kamera — Presentation Summary

## Upstream issues filed against `crossplane/crossplane`

All issues filed 2026-03-19. Maintainer engagement primarily from `jbw976`
(Crossplane maintainer), who also invited a kamera presentation at an
upcoming Crossplane community meeting (issue #7224 thread, 2026-04-03).

| Issue | Maps to | State | Disposition |
|-------|---------|-------|-------------|
| [#7220](https://github.com/crossplane/crossplane/issues/7220) — Manual update policy: switching compositionRef silently composes with wrong Composition's revision | **F1 (P0)** | Open | No maintainer response yet. |
| [#7221](https://github.com/crossplane/crossplane/issues/7221) — CompositionRevisionReconciler calls `Status().Update()` unconditionally — infinite reconcile loop | **F2 (P3)** | Open, labeled `performance` | Accepted by maintainer ("Great find … definitely feels like something to fix"). Fix submitted as **[PR #7283](https://github.com/crossplane/crossplane/pull/7283)** (Open). |
| [#7222](https://github.com/crossplane/crossplane/issues/7222) — Deleting a Composition while XR is bound produces permanent ReconcileError | **F3 (P1)** | Open | No maintainer response yet. |
| [#7223](https://github.com/crossplane/crossplane/issues/7223) — Stale ValidPipeline + fatal-function orphaned resources | **F5 (P2) + F6 (P2)** | Open | **F6 fix proposal partially rejected.** Maintainer (jbw976, 2026-04-06): can't safely GC on mid-pipeline fatal because earlier steps' `desired` set is incomplete — a later step might have added resources still in use. F5 race acknowledged as real but lower severity. Earlier comment from `yugal07` (community contributor) proposed the F6 fix that was later refuted. |
| [#7224](https://github.com/crossplane/crossplane/issues/7224) — Claim lifecycle: deletion orphans XR; shared composition silent ownership theft | **C2 + C4 (both P2)** | **Closed (completed) by tgoodwin 2026-04-09 — both false positives** | C2 was a Kamera fidelity bug: external-user DELETE simulation overwrote the finalizer the ClaimReconciler had set, masking the real ordering. C4 was a Kamera SSA implementation bug: field-level merge semantics handled incorrectly (real `meta.AddControllerReference()` rejects cross-XR theft). Both ruled out after jbw976's pointers to the relevant code. |

**Net: 4 issues open + 1 PR open + 2 false positives** — out of 7 originally
flagged "confirmed bugs" in the analysis doc, **5 hold up** to maintainer
review (F1, F2, F3, F5, partial F6) and **2 don't** (C2, C4). F6 is
partially valid (the `Ready=True` + `Synced=False` confusion is real; the
GC-on-fatal fix is not safe).

## Crossplane harness setup

- **Target:** `github.com/crossplane/crossplane` v2.2.0 (upgraded from v2.1.0
  on 2026-03-17 for controller-runtime v0.23 compat).
- **Real controllers wired:** CompositionReconciler +
  CompositionRevisionReconciler + CompositeReconciler + ClaimReconciler +
  CleanupReconciler.
- **Stubbed:** FunctionRunner (`stubFunctionRunner` with named behaviors:
  default, fatal, different-resources, partial), FunctionRevision package
  manager, claim ConnectionPropagator, provider/managed resource controllers,
  dynamic watch engine for composed resources.
- **Scope:** ~25 scenarios across ordering, staleness, external events, fault
  injection.

## Confirmed bugs (7)

From `examples/BUG-FINDINGS.md` and `examples/crossplane/.agents/ANALYSIS.md`.

| ID | Severity | Description |
|----|----------|-------------|
| **F1** | **P0** | **Manual update policy fetches revision from wrong Composition.** `APIRevisionFetcher.Fetch` (`api.go:161-167`) does a bare `Get(currentRevision)` with zero validation that the revision belongs to the Composition referenced by `compositionRef`. Switching `compositionRef` alpha→beta while `compositionRevisionRef` still points to alpha-rev-1 silently composes resources from the wrong Composition. **No error raised.** Pure logic bug, no race or staleness needed. |
| **F2** | **P3** | **Unconditional `Status().Update()` in `revision/reconciler.go:164,171`** causes infinite reconcile cycling via owner-ref fanout to CompositionReconciler. All scenarios cycle (total/unique > 3). In production it's masked by workqueue rate limiting; in Kamera replay it blocked convergence-based analysis for the entire campaign. (This is the `kamera-3d7` finding from 2026-02-26.) |
| **F3** | **P1** | **Orphaned compositionRef after Composition deletion, permanent error loop.** Two ordering-dependent paths: CleanupReconciler-first → `Composition not found`; CompositeReconciler-first → `no compatible Compositions found`. Neither self-recovers. The code even has a TODO acknowledging the missing finalizer (`api.go:251-252`). |
| **F5** | **P2** | **Stale `ValidPipeline=True` allows composition with invalidated functions.** CompositeReconciler trusts a cached condition on CompositionRevision instead of independently verifying capabilities. Reproduces in 6/9 ordering categories. Eventually self-corrects but resources are written from invalid functions in the meantime. |
| **F6** | **P2** | **Function switch to SEVERITY_FATAL leaves orphans + stale Ready=True.** 7 distinct terminal states across 49 trials. In 86% of orderings the previously-composed ConfigMap persists (SEVERITY_FATAL returns before `GarbageCollectComposedResources`). XR shows confusing `Ready=True` + `Synced=False`. **Partially valid per maintainer (#7223):** the proposed GC-on-fatal fix is *not safe* (an unrun later pipeline step might add resources still in use), but the stale `Ready=True` in the error path is a real, separate inconsistency. |
| ~~**C2**~~ | ~~P2~~ | ~~Claim deletion orphans XR + composed resources.~~ **FALSE POSITIVE (#7224 closed 2026-04-09).** Kamera's external-user DELETE modeling overwrote the finalizer the ClaimReconciler had set; in real Crossplane the XR isn't created until *after* the finalizer is added (`claim/reconciler.go:440` then `Sync()` at `:467`), so a pre-finalizer delete leaves nothing to orphan. |
| ~~**C4**~~ | ~~P2~~ | ~~Two XRs silently steal ownership of the same composed resource.~~ **FALSE POSITIVE (#7224 closed 2026-04-09).** Kamera's SSA implementation handled field-level merge semantics incorrectly. Real `meta.AddControllerReference()` (`crossplane-runtime/pkg/meta/meta.go:160-162`) rejects cross-XR ownership conflicts. |

## Negative results / hypotheses ruled out internally

These showed up early as candidate bugs but the analysis docs explicitly
classify them as **not bugs** after deeper investigation. Useful framing for a
presentation about distinguishing real bugs from ordering noise.

- **CompositionRevision creation-vs-validation race** ("Finding 4"):
  CompositeReconciler can briefly select an unvalidated rev-2 and emit a
  transient "pipeline status unknown" error. Looked like a bug but **all
  orderings converge to identical final state** — the standard k8s
  retry-and-recover pattern. Refuted after running with all 3 controllers in
  `permuteControllers` and confirming 0 differing objects across terminal
  states.
- **two-xrs-shared-composition-update under stale reads:** hypothesized that
  one XR pins to rev-1 while the other advances to rev-2. **Refuted** —
  Kamera's staleness model is per-reconciler-ID, not per-invocation, so both
  XRs share the same view.
- **xr-and-composition-deleted-simultaneously:** hypothesized stuck-Terminating
  XR. **Refuted** — CompositeReconciler delete path doesn't fetch the
  Composition at all; XR deletes cleanly in every ordering.
- **two-xrs-deleted-simultaneously:** hypothesis untestable — both DELETEs fire
  at depth 0 before any composition runs, so the short-circuit hides the case.
  Would need a per-XR resource name stub to test properly.
- **manual-xr-switch-with-old-composition-deleted:** combined F1 + F3. **No new
  outcome** — produces the union of F1 and F3 behaviors, no new failure mode.
- **composition-update-with-capability-removal:** combined F4 + F5. The
  "rev-2 inherits ValidPipeline=True" hypothesis was **refuted** (new
  revisions are created with empty status). What manifests is just F5
  operating on rev-1.
- **Staleness perturbation contributes to F5:** **refuted** — corrected
  staleness window produces the same 3 outcome families as ordering-only. F5
  is purely ordering-dependent.

## Things to highlight for the presentation

1. **Methodology that worked despite cycling.** F2 broke convergence-based
   analysis for the whole campaign, but you got 7 bugs anyway via three
   workarounds: Monte Carlo aborted-state comparison, fixed-depth event
   injection at `userActionReadyDepths`, and divergence on non-cycling
   objects. Good war-story slide.
2. **Pure logic vs perturbation-discovered.** F1 and F2 don't need any
   perturbation — Kamera surfaced them by exhaustive exploration of even the
   reference run. F3/F5/F6/C2/C4 needed ordering and/or external events. Good
   story for "different perturbation dimensions catch different bug classes."
3. **F1 is the headline.** P0 silent data corruption, fully reproducible
   without races, with a clean explanation in 6 lines of Crossplane source.
4. **False positives are part of the workflow.** Finding 4 is the cleanest
   "looked like a bug, isn't one" — error visible in trace, but all orderings
   converge. Useful for showing you take refutation seriously and Kamera
   distinguishes ordering bugs from ordering noise.
5. **Harness fidelity caveats** (from triage notes in
   `.yaks/crossplane-bug-triage/`): status subresource updates were modeled as
   full-object replacements; missing watch mapping between
   Composition→XWidget required RequeueAfter polling; `disablePerturbations`
   was stripping staleness from rerun plans. Worth mentioning as honest
   fidelity caveats — they bound the scope of claims.
6. **The two false positives (C2, C4) are themselves a teaching moment.**
   Both were caused by Kamera-side simulation bugs (external-DELETE finalizer
   handling; SSA field-merge semantics), not by the controller logic. The
   maintainer pointed at exact lines (`claim/reconciler.go:440-468`,
   `meta.go:160-162`) and the divergence vanished. This is the right
   advertisement for the limits of the technique — and motivates the
   "harness fidelity hardening" work in `.yaks/`. It also shows the value of
   tight maintainer collaboration: jbw976's review caught what self-review
   didn't.

## Source pointers

- Bug summary table across all projects:
  `examples/BUG-FINDINGS.md`
- Detailed Crossplane analysis with traces and code refs:
  `examples/crossplane/.agents/ANALYSIS.md`
- F2 deep-dive (the `kamera-3d7` finding):
  `docs/findings/2026-02-26-kamera-3d7-crossplane-unconditional-status-update.md`
- Scenario JSON workflows:
  `examples/crossplane/scenarios/`
- Internal triage / fidelity issues:
  `.yaks/crossplane-bug-triage/`
