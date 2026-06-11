# F1 — Manual update policy uses wrong CompositionRevision

**Issue:** [crossplane/crossplane#7220](https://github.com/crossplane/crossplane/issues/7220)
**Status:** ✅ Reproduces on real cluster (Crossplane v2.2.0). Confidence: HIGH.
**Audits:** [R-2](../crossplane-audits/R2-scenario-fixtures.md), [R-6](../crossplane-audits/R6-manual-policy-webhooks.md), [R-11](../crossplane-audits/R11-f1-real-cluster.md).

## TL;DR

Under `compositionUpdatePolicy: Manual`, an XR's `compositionRevisionRef` can point to a revision that does not belong to the Composition pointed at by `compositionRef`. Crossplane silently composes using the **pinned revision's** content, ignoring the new `compositionRef`. There is no validation, no warning, no error, and no webhook auto-correction.

## What's actually wrong

`internal/controller/apiextensions/composite/api.go` — `APIRevisionFetcher.Fetch`:

- Under **Automatic** policy (lines ~170-196 of `api.go`), Crossplane filters CompositionRevisions by the `crossplane.io/composition-name` label and selects the highest-revision match for the named Composition. The cross-reference is structurally impossible.
- Under **Manual** policy (lines 161-167), Crossplane does a bare `Get(currentRevision.Name)` on whatever name is in `compositionRevisionRef` and uses it. It does not check that the revision's `crossplane.io/composition-name` label matches the Composition referenced by `compositionRef`.

User impact: a user issues `kubectl patch xr/foo --type=merge -p '{"spec":{"compositionRef":{"name":"new-composition"}}}'`, expecting their XR to start using `new-composition`. Under Manual policy, the patch is accepted, but Crossplane keeps composing with whatever revision the XR was previously pinned to. The XR reports `Synced=True / ReconcileSuccess`. There is no surface signal that the user's intent has been ignored.

## How Kamera surfaced it

**Scenario:** [`workflow_crossplane-policy_manual-update-policy-composition-switch`](../../examples/crossplane/scenarios/workflow_crossplane-policy_manual-update-policy-composition-switch.json) — an XR is pinned to `widget-composition-alpha` revision 1 under Manual policy; an external user UPDATE flips `spec.compositionRef.name` to `widget-composition-beta` while leaving `spec.compositionRevisionRef.name` untouched.

Kamera ran the harness with the real Crossplane composite reconciler wired in, exploring valid orderings of controller invocations to depth 100. The dump for the primary run contains a converged terminal state where the XR carries:

- `compositionRef.name = "widget-composition-beta"`
- `compositionRevisionRef.name = "widget-composition-alpha-rev-1"`
- `Synced=True / ReconcileSuccess`

The reconciler proceeds through this cross-referenced state without raising an error, composing the alpha revision's `ConfigMap` content despite the new `compositionRef` pointing at beta. This is the F1 fingerprint: a state that should be rejected by validation but is silently accepted, plus the wrong content downstream.

## How we validated it

This claim is the cleanest in the batch — three independent confirmations.

### Tier 1 — source code grounding (R-2)

The harness fixture seeds an XR and two Compositions (alpha + beta); it does **not** pre-create CompositionRevisions with bad labels. The wired-from-real-source `composition.NewReconciler` creates revisions with production labels and ownerRefs. The bug mechanism — the bare `Get` in `APIRevisionFetcher.Fetch`'s Manual branch — is pure logic, source-grounded at exact line numbers in v2.2.0.

### Tier 2 — closing the "production webhook fixes it" threat (R-6)

The most plausible reason a harness-found cross-reference might not reproduce in production is that a defaulting / mutating / conversion webhook silently rewrites `compositionRevisionRef` to the matching revision before the reconciler ever sees it. R-6 audited the v2.2.0 release: there is **no** XR-targeted mutating, defaulting, or conversion webhook. The only webhook that ships is `crossplane-no-usages`, an unrelated validator on DELETE. Threat closed.

### Tier 3 — direct real-cluster reproduction (R-11)

Setup: kind v0.30.0 + kindest/node v1.30.0 + Crossplane v2.2.0 + function-patch-and-transform v0.10.0. Two Compositions (`widget-composition-alpha`, `widget-composition-beta`), each producing a distinctively-labeled ConfigMap. XR pinned to `widget-composition-alpha-a39f01a` under Manual policy.

Probe:
```bash
kubectl patch xwidget/example --type=merge -p \
  '{"spec":{"compositionRef":{"name":"widget-composition-beta"}}}'
```

Result over a 60-second observation window (sampled every 15s):

| T+ | compositionRef | compositionRevisionRef | Synced | composed ConfigMap |
|---|---|---|---|---|
| 0s | **beta** | **alpha-a39f01a** | True (ReconcileSuccess) | `alpha-output { source: alpha }` |
| 15s | beta | alpha-a39f01a | True | (unchanged) |
| 30s | beta | alpha-a39f01a | True | (unchanged) |
| 45s | beta | alpha-a39f01a | True | (unchanged) |
| 60s | **beta** | **alpha-a39f01a** | True (ReconcileSuccess) | `alpha-output { source: alpha }` |

The cross-referenced state is stable. `beta-output` is never created. Switching `compositionUpdatePolicy` to Automatic resolves the cross-reference within ~60s (controller catches up); the bug only persists under Manual policy.

Trace artifacts at [`../crossplane-audits/artifacts/tier3/r11/`](../crossplane-audits/artifacts/tier3/r11/) — the smoking-gun XR yaml is [`xr-after-patch-T+30s.yaml`](../crossplane-audits/artifacts/tier3/r11/xr-after-patch-T+30s.yaml).

## Suggested fix (per the original issue)

Mirror the Automatic-policy filter in the Manual-policy branch: before using a pinned `compositionRevisionRef`, verify the revision's `crossplane.io/composition-name` label matches the Composition referenced by `compositionRef`. If not, surface a clear error condition rather than silently using the pinned content.

## Talking points for the audience

- The bug is a five-line oversight in a single function — the asymmetry between Automatic and Manual paths is striking and very fixable.
- Manual policy is specifically the policy users opt into when they want stronger guarantees about which revision their XR uses. The lack of cross-reference validation undermines the contract.
- Kamera's contribution: the scenario exercises the user-action ordering directly. The bug is timing-independent (it's pure logic), but the harness made it concrete and reproducible without requiring a real cluster.
- The real-cluster reproduction (R-11) is identical to the harness prediction — same `Synced=True`, same composed content, same cross-reference persistence. F1 is the cleanest example of "harness predicts, cluster confirms."
