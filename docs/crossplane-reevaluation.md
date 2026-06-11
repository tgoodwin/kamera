# Crossplane re-evaluation hub

Single entry point for the SPRINT-0001 work re-evaluating five open Crossplane bug claims against a fidelity-hardened Kamera harness, plus the threats-to-validity audit work being done before the findings are posted upstream.

## Context

In March 2026, six Crossplane issues / one PR were filed against `crossplane/crossplane`:

| Issue | Bug | Initial state | Final state (post SPRINT-0001) |
|---|---|---|---|
| [#7220](https://github.com/crossplane/crossplane/issues/7220) | F1 — Manual update policy wrong revision | Open, no maintainer response | **still-reproduces** |
| [#7221](https://github.com/crossplane/crossplane/issues/7221) | F2 — Unconditional `Status().Update()` | Open, accepted | Fix in flight as PR #7283 |
| [#7222](https://github.com/crossplane/crossplane/issues/7222) | F3 — Composition deletion error loop | Open, no maintainer response | **shifts (still-reproduces, retraction refuted by R-4)** |
| [#7223](https://github.com/crossplane/crossplane/issues/7223) | F5 + F6 | Open, mixed maintainer response | F5 **shifts**; F6 split into two sub-claims |
| [#7224](https://github.com/crossplane/crossplane/issues/7224) | C2 + C4 | Closed by author 2026-04-09 | C2 fix validated; C4 still gated on Fix 3 |

The SPRINT-0001 re-runs were performed against a fidelity-hardened harness branch (`crossplane-reeval` @ `89acd8a`) that adds:
- Server-side resourceVersion conflict checking (PR #76 — `911b3bd`, `cb1c43e`, `7ba2045`, `1def992`)
- DELETE that preserves finalizers/spec/status + a `GarbageCollectorReconciler` (`e4daf33`, cherry-pick of `6cd7396`)
- Pre-merge fixes for the RV PR (`b12542d`, `89acd8a`)

Detailed branch composition: [reevaluation-plan.md §2](./crossplane-reevaluation-plan.md#2-fidelity-gap-fixes-since-7224-was-filed-2026-03-19).

## Where to find what

### Background and setup

- [`crossplane-testing-summary.md`](./crossplane-testing-summary.md) — original presentation summary; what was found before the re-evaluation. Useful for historical context.
- [`crossplane-reevaluation-plan.md`](./crossplane-reevaluation-plan.md) — the audit plan that drove SPRINT-0001. Lists every issue, every scenario JSON path on disk, and the recorded baseline hashes inlined per finding.
- [`findings/2026-02-26-kamera-3d7-crossplane-unconditional-status-update.md`](./findings/2026-02-26-kamera-3d7-crossplane-unconditional-status-update.md) — original F2 finding writeup.

### Sprint artifacts

- [`sprints/SPRINT-0001.md`](./sprints/SPRINT-0001.md) — the full sprint plan, merged from three independent drafter outputs. Phases 0–6 with checkboxes.
- [`sprints/SPRINT-0001-runlog.md`](./sprints/SPRINT-0001-runlog.md) — append-only run log with per-scenario commands, campaign metrics, terminal-state hashes, and provisional classifications.
- [`sprints/drafts/`](./sprints/drafts/) — the three independent sprint plan drafts and their cross-critiques. Historical scratch.

### Upstream-update drafts (one per finding)

These are review-ready drafts staged for tgoodwin to post manually. **None should be posted before the threats-to-validity audit is complete for that finding.**

- [`upstream-updates/7220-f1-manual-policy.md`](./upstream-updates/7220-f1-manual-policy.md) — F1 still-reproduces. Confidence: high after Tier 1 audit, pending CC-8 (defaulting webhook check).
- [`upstream-updates/7222-f3-composition-deletion.md`](./upstream-updates/7222-f3-composition-deletion.md) — F3 still-reproduces / shifts. Rewritten 2026-04-28 after R-4 trace audit refuted the original retraction framing. Confidence: HIGH after Tier 1 + Tier 2 + R-4. R-14 recommended for strongest framing, not blocking.
- [`upstream-updates/7223-f5-f6-reframe.md`](./upstream-updates/7223-f5-f6-reframe.md) — Reframes #7223 into F5 (still-reproduces / shifts), F6-orphan (shifts), and F6-stale-Ready (high-confidence after R-3). Withdraws the original GC-on-fatal proposal.
- [`upstream-updates/7224-c2-claim-deletion-validation-not-for-posting.md`](./upstream-updates/7224-c2-claim-deletion-validation-not-for-posting.md) — internal validation that `e4daf33` closes the C2 false positive. **Do not post; #7224 is closed.**

### Threats-to-validity audit

- [`upstream-updates/AUDIT-threats-to-validity.md`](./upstream-updates/AUDIT-threats-to-validity.md) — master threat model. Cross-cutting threats (CC-1 through CC-8), per-finding threat tables, research checklist (R-1 through R-17 in four tiers), decision rules for posting.
- [`crossplane-audits/`](./crossplane-audits/) — per-audit findings. One markdown per audit task. Index in [`crossplane-audits/README.md`](./crossplane-audits/README.md).

## Audit progress

| Tier | Status | What it gets you |
|---|---|---|
| Tier 1 — pure code reads | ✅ COMPLETE (R-1, R-2, R-3, R-5) | Source-grounded confirmation of F1, F5 watch wiring, F6 mechanism, and harness status-writer fidelity. |
| Tier 2 — web search / external research | ✅ COMPLETE (R-6 to R-10) | Confirms production webhook behavior, workqueue timing, GC propagation, post-fatal re-reconcile, and runtime contract. |
| Tier 3 — real-cluster experiments | ✅ COMPLETE 2026-04-29 (R-11 ✅, R-13 ✅, R-14 ✅; R-12 ⚠️ inconclusive — Approach B deferred) | F1, F3, F6-orphan reproduce on Crossplane v2.2.0 + kind v1.30. R-14 surfaced the `errFetchComp` family (Manual policy) that the harness re-run did not — both error families are real F3 pathologies. F5 not directly demonstrated (capability-stripping is reverted by package manager); confidence stands on Tier 1/2 grounds. |

## Posting readiness (after Tier 1 + Tier 2 + Tier 3)

| Finding | Confidence | Defensible to post now? |
|---|---|---|
| **F1** (#7220) | HIGH | Yes. Webhook threat closed by R-6; **R-11 reproduces on real cluster** (cleanest of the four — `Synced=True` with cross-referenced refs stable for ≥60s; alpha-rev content composed despite `compositionRef = beta`). |
| **F6 stale-Ready-True** (#7223) | HIGH | Yes. `reconciler.go:744` system-condition filter is unambiguous (R-3); R-13 confirms the underlying invariant on a real cluster. |
| **F6 orphan-persistence** (#7223) | HIGH (scoped to "while fatal") | Yes. R-3 + R-9 confirm; **R-13 reproduces on real cluster** (3 min orphan, `resourceRefs` still tracks orphan, no GC). |
| **F5** (#7223) | HIGH | Yes. Workqueue threat closed by R-7. R-12 inconclusive on real cluster (Approach A blocked, Approach B deferred); does not change posting posture. |
| **F3** (#7222) | HIGH | Yes. R-4 refuted the retraction; **R-14 reproduces on real cluster** (5 min permanent `errFetchComp` loop under Manual policy — the error family the harness re-run didn't surface, confirming the bug surface is broader than either alone shows). |

All four open-issue drafts now defensible at HIGH confidence after Tier 1 + Tier 2 + Tier 3 audits. Tier 3 added **direct real-cluster reproductions** for F1, F6-orphan, and F3 — eliminating simulation-fidelity threats for those three findings entirely. F5 stands on source + workqueue-semantics grounds.

## Memory and posting workflow

When you're ready to post:

1. Pick a finding.
2. Walk the [`crossplane-audits/README.md`](./crossplane-audits/README.md) status to confirm all threats relevant to that finding are at ✅ AUDITED or you've made an explicit "good enough" judgment.
3. Read the corresponding draft in `upstream-updates/`.
4. Edit if needed (the drafts are honest about what's verified vs caveated).
5. Post the "Suggested comment text" section to GitHub.
6. Mark the issue's row in this hub doc with the date and link to the comment.

## Quick links

- [Sprint plan](./sprints/SPRINT-0001.md) (what we did)
- [Run log](./sprints/SPRINT-0001-runlog.md) (raw evidence)
- [Audit hub](./crossplane-audits/README.md) (what we've checked)
- [Threats master](./upstream-updates/AUDIT-threats-to-validity.md) (what could still be wrong)
- [Per-finding drafts](./upstream-updates/) (what we'd post)
