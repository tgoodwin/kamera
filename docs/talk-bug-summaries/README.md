# Crossplane bug talk-prep summaries

Per-bug briefings for tgoodwin's talk to the Crossplane community. Each doc explains:

- What the bug is (mechanism + code locations + user impact).
- How Kamera surfaced it (scenario, harness exploration, observed terminal evidence).
- How we validated it was a real bug, not a harness artifact (Tier 1 source grounding, Tier 2 external research, Tier 3 real-cluster reproduction).

For negative results (C2, C4), the docs explain the original kamera finding, the fidelity issue that explained it as a false positive, and the harness fix (landed or planned).

The top-level index of all filed bugs with status verdicts and links to the original GitHub issues is at [`../crossplane-bugs-summary.md`](../crossplane-bugs-summary.md).

## Concepts primer

- [00 — Crossplane concepts primer](./00-crossplane-concepts.md) — start here if you're fuzzy on XR vs Composition vs CompositionRevision, `compositionRef` vs `compositionRevisionRef`, FunctionRevision capabilities, `ValidPipeline`, system conditions, etc. The per-bug docs reference these by name without re-explaining.

## Positive findings

| Doc | Bug | Status | Issue |
|---|---|---|---|
| [F1](./F1-manual-policy-wrong-revision.md) | Manual update policy uses wrong CompositionRevision | ✅ Reproduces (real cluster) | [#7220](https://github.com/crossplane/crossplane/issues/7220) |
| [F2](./F2-unconditional-status-update.md) | Unconditional `Status().Update()` on every reconcile | ✅ Fixed upstream (PR #7283) | [#7221](https://github.com/crossplane/crossplane/issues/7221) |
| [F3](./F3-composition-deletion-error-loop.md) | Composition deletion produces permanent error loop | 🔄 Reproduces (broader than originally framed) | [#7222](https://github.com/crossplane/crossplane/issues/7222) |
| [F5](./F5-stale-validpipeline-race.md) | Stale `ValidPipeline=True` race after function capability change | ✅ Reproduces (harness + source-grounded) | [#7223](https://github.com/crossplane/crossplane/issues/7223) |
| [F6-orphan](./F6-orphan-persistence-while-fatal.md) | Composed resources persist as orphans while function returns SEVERITY_FATAL | ✅ Reproduces (real cluster) | [#7223](https://github.com/crossplane/crossplane/issues/7223) |
| [F6-stale-Ready](./F6-stale-ready-true.md) | Stale `Ready=True` while `Synced=False` on error path | ✅ Reproduces (source-grounded; mechanism confirmed on cluster) | [#7223](https://github.com/crossplane/crossplane/issues/7223) |

## Negative results (Kamera fidelity issues)

| Doc | Original claim | Resolution |
|---|---|---|
| [C2](./C2-claim-deletion-false-positive.md) | Claim deletion orphans XR + composed resources | Harness DELETE-semantics fidelity gap; fixed by `e4daf33` |
| [C4](./C4-cross-xr-ownership-theft.md) | Cross-XR ownership theft of shared composed resource | Harness SSA field-manager fidelity gap; "Fix 3" planned, not yet implemented |
