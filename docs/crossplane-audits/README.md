# Crossplane re-eval audits

Per-audit findings for SPRINT-0001's threats-to-validity exercise. Each audit checks one specific Kamera fidelity assumption against production behavior. Audits are referenced from [`../upstream-updates/AUDIT-threats-to-validity.md`](../upstream-updates/AUDIT-threats-to-validity.md), which is the master threat model.

## Tier 1 — pure code reads (complete)

| ID | Topic | Status | Net finding |
|---|---|---|---|
| [R-1](./R1-functionrevision-watch.md) | FunctionRevision watch wiring | ✅ AUDITED | Production wires; harness wires (with hardcoded single-revision target — bounded gap, doesn't affect current scenarios). |
| [R-2](./R2-scenario-fixtures.md) | Scenario fixtures and stub fidelity | ✅ AUDITED | F1 fixture matches production cluster-startup behavior; F6 fatal stub triggers the documented production code path. |
| [R-3](./R3-fatal-branch.md) | SEVERITY_FATAL early-return + stale Ready=True | ✅ AUDITED | Both F6 sub-claims source-grounded at exact line numbers in v2.2.0. |
| [R-4](./R4-f3-trace-audit.md) | F3 hypothesis-1 terminal-state XWidget condition trace audit | ✅ AUDITED | **F3 shifts, does not retract.** All 4 unique XWidget terminal hashes encode `Synced=False / ReconcileError` with `errSelectComp` "no compatible Compositions found"; hash drift is cosmetic (`generation`/`resourceVersion`). |
| [R-5](./R5-status-writer.md) | Kamera status subresource writer | ✅ AUDITED | Spec/metadata preserved across status writes; replace semantics match production for `Update` (the only path Crossplane uses in tested scenarios). |

## Tier 2 — web search and external research (complete)

| ID | Topic | Status | Net finding |
|---|---|---|---|
| [R-6](./R6-manual-policy-webhooks.md) | Crossplane manual-update-policy webhooks/defaulting | ✅ AUDITED | No XR mutating/defaulting/conversion webhook ships in v2.2.0; the only webhook is the unrelated `crossplane-no-usages` validator on DELETE. CC-8 closed. |
| [R-7](./R7-workqueue-semantics.md) | controller-runtime workqueue + watch ordering semantics | ✅ AUDITED | Workqueue de-dups per-key per-controller but does NOT serialize cross-controller. F5 race window is timing-feasible in production; harness explores valid orderings. |
| [R-8](./R8-k8s-gc-propagation.md) | K8s GC propagationPolicy vs harness GC | ✅ AUDITED with caveat | Harness GC approximates Background only; over-deletes for multi-owner deps. **No current finding triggers the gap** but tracked as deferred work. |
| [R-9](./R9-post-fatal-rereconcile.md) | Crossplane composition reconciler post-fatal re-reconcile behavior | ✅ AUDITED | Every retry retakes the SEVERITY_FATAL early-return; GC unreachable while function stays fatal. Orphan-persistence claim source-grounded when scoped to "while fatal." |
| [R-10](./R10-runfunction-protobuf.md) | gRPC RunFunctionResponse schema + function runtime contract | ✅ AUDITED | Stub adequately models the consumed fields for F1/F5/F6. Bare runner (no `FetchingFunctionRunner` wrap) is OK because no current scenario uses response `Requirements`. |

## Tier 3 — real-cluster experiments (executed 2026-04-29; highest confidence)

**Concrete kind-cluster plan:** [`kind-cluster-plan.md`](./kind-cluster-plan.md). Executed against `kind v0.30.0` + `kindest/node:v1.30.0` + Crossplane v2.2.0 (helm chart 2.2.0) + `function-patch-and-transform v0.10.0`.

| ID | Topic | Status | Net finding |
|---|---|---|---|
| [R-11](./R11-f1-real-cluster.md) | F1 reproduction on Crossplane v2.2.0 in kind | ✅ AUDITED | **F1 reproduces.** Patch `compositionRef alpha→beta` under Manual policy is silently accepted; `compositionRevisionRef` stays pinned to alpha-rev; XR shows `Synced=True / ReconcileSuccess` and composes with alpha-rev's content. No defaulting webhook. State stable for ≥60s. |
| [R-12](./R12-f5-real-cluster.md) | F5 reproduction on Crossplane v2.2.0 in kind | ⚠️ INCONCLUSIVE on real cluster | Approach A (direct `status.capabilities` patch) blocked: the package manager actively reconciles within seconds, so manual capability-stripping doesn't hold. Approach B (custom function package) deferred. F5 confidence remains HIGH from Tier 1/2 (R-1 + R-7). |
| [R-13](./R13-f6-real-cluster.md) | F6 reproduction on Crossplane v2.2.0 in kind | ✅ AUDITED | **F6 orphan-persistence reproduces.** alpha-output ConfigMap untouched for 3 min after switching XR to a fatal Composition; XR in `Synced=False / ReconcileError` loop with no recovery; `resourceRefs` still tracks orphan. **Stale-Ready partial:** the system-condition-skip mechanism (R-3) is confirmed (Ready persists unchanged across the error transition); a positive `Ready=True` reproduction needs a Ready-publishing composed resource (deferred). |
| [R-14](./R14-f3-real-cluster.md) | F3 reproduction on Crossplane v2.2.0 in kind | ✅ AUDITED | **F3 reproduces.** Composition deletes cleanly (no finalizer); CompositionRevision GC'd; XR enters permanent `Synced=False / ReconcileError` at T+90s with `errFetchComp` family ("cannot fetch Composition: cannot get CompositionRevision: ... not found") — the family the harness re-run did NOT surface. Composed dependent persists untouched (rv constant). 5 min observation, no recovery. |

## Findings synthesis after Tier 1 + Tier 2

### F1 (#7220) — confidence: HIGH
- Source code confirms the bug mechanism: `APIRevisionFetcher.Fetch` does `Get(currentRevision.Name)` with no validation under Manual policy.
- Harness fixture seeds the right initial state (R-2). No pre-baked CompositionRevisions with bad labels.
- Defaulting / mutating webhook threat closed by R-6: no XR-targeted webhook ships in v2.2.0.
- Remaining work: R-11 (real cluster) is the strongest possible audit but is not blocking.

### F5 (#7223 / F5) — confidence: HIGH
- Production wires the FunctionRevision → CompositionRevision watch (R-1). Our harness wires it.
- Workqueue does not serialize cross-controller (R-7); the race is timing-feasible in production. Our scheduler explores valid orderings.
- Remaining work: R-12 (real cluster timing) for quantitative race-probability evidence.

### F6 orphan-persistence (#7223 / F6) — confidence: HIGH (when scoped to "while fatal")
- Source code confirms: SEVERITY_FATAL early-return at `composition_functions.go:439` skips the GC call at line 538 (R-3). Our stub triggers the right branch (R-2).
- Per-reconcile orphan creation is source-grounded.
- R-9: every retry retakes the same fatal path; GC unreachable while function stays fatal. Once function is fixed, next successful reconcile WILL GC. Draft must scope to "while fatal" — and currently does.
- R-10: stub adequately models the response contract for the fatal path.
- Remaining work: R-13 (real cluster) for empirical confirmation.

### F6 stale-Ready-True (#7223 / F6) — confidence: HIGH
- Source code confirms (R-3): `reconciler.go:744` skips system conditions when iterating to mark unknown. `Ready` is a system condition. Stale `Ready=True` is therefore *guaranteed* on the error path, not race-dependent.
- Cleanest claim in the entire batch.

### F3 retraction (#7222) — **WITHDRAWN; F3 SHIFTS, does not retract.**
- R-4 (trace audit of hypothesis-1 terminal XWidget conditions) finds that **all 4 unique terminal hashes carry `Synced=False / ReconcileError` with the original `errSelectComp` "no compatible Compositions found" message**. The hash drift between baseline and re-run is cosmetic (`metadata.generation` / `resourceVersion` re-sequencing under the new GC reconciler), not a change in the underlying error condition.
- R-8 confirms the harness GC isn't doing cascades that would close the F3 loop for spurious reasons (the dependent has a single owner, behavior matches Background deletion).
- Net: the F3 pathology is real and reproduces in the hardened harness. The original `ConfigMap` divergence has collapsed (the dependent is no longer GC'd in any path within the depth budget), and one of the two original error orderings (`errFetchComp` "Composition not found") no longer surfaces — but the XR `Synced=False / ReconcileError` permanent loop is intact.
- Action required (not done by this audit): rewrite [`upstream-updates/7222-f3-composition-deletion.md`](../upstream-updates/7222-f3-composition-deletion.md) to drop the retraction framing and reposition as a "shifts" finding. R-14 (real-cluster F3 reproduction) becomes the highest-leverage remaining check.

## What this means for posting decisions

**Defensible to post now (after Tier 1 + Tier 2 + Tier 3):**
- **F1** — webhook fidelity threat closed by R-6; real-cluster reproduction confirmed by R-11 (cleanest of the four).
- **F5** — workqueue fidelity threat closed by R-7; real-cluster reproduction not pursued (R-12 inconclusive, Approach B deferred); confidence remains HIGH on Tier 1/2 grounds alone.
- **F6 orphan-persistence** — real-cluster reproduction confirmed by R-13 (3 min orphan persistence, no recovery while function unresolvable).
- **F6 stale-Ready-True** — R-3 source-grounding is the strongest evidence; R-13 confirms the underlying invariant (system conditions persist unchanged across error transition) but didn't directly produce a `Ready=True` value because the test composition didn't include a Ready-publishing resource. Posting unchanged.
- **F3 (#7222)** — real-cluster reproduction confirmed by R-14 (5 min permanent error loop, `errFetchComp` family — the family the hardened-harness re-run did NOT surface, suggesting Manual-policy and Automatic-policy paths exercise different error families and BOTH are real production pathologies).

**Tier 3 is now complete for the four issue drafts.** R-12 is the only inconclusive Tier 3 result; F5 confidence does not depend on it.

**Needs rewrite before posting:**
- F3 — R-4 closes the trace-level question against retraction. The 7222 draft must be rewritten as a "shifts" finding (same `errSelectComp` ReconcileError pathology in 4/4 unique terminals, narrower error-family coverage, ConfigMap divergence collapsed). R-14 (real-cluster reproduction) remains ideal but is no longer blocking; if anything it's the strongest remaining confirmation step.
