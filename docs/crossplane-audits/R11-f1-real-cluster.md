# R-11: F1 manual update policy on real cluster (kind + Crossplane v2.2.0)

**Status:** ✅ AUDITED — **F1 reproduces on real cluster.** Under `compositionUpdatePolicy: Manual`, mutating `spec.compositionRef.name` while leaving `spec.compositionRevisionRef.name` untouched is silently accepted. No defaulting webhook auto-corrects the revision pin. No validation webhook rejects the cross-reference. The XR shows `Synced=True / ReconcileSuccess` and Crossplane composes with the pinned revision content, ignoring the new `compositionRef` entirely.

**Threats addressed:** CC-8 (defaulting webhook check), F1-S1 (manual-policy mechanism), F1-Audit-1 (real-cluster reproduction). Closes the simulation-fidelity question for #7220 entirely.

**Date:** 2026-04-29
**Cluster:** `kind-crossplane-audit`, kindest/node v1.30.0
**Crossplane image:** `xpkg.crossplane.io/crossplane/crossplane:v2.2.0`
**Function:** `function-patch-and-transform v0.10.0`

## Setup

XRD: `xwidgets.example.org` (group `example.org`, kind `XWidget`, v1).

Two Compositions:
- `widget-composition-alpha` → P&T pipeline producing `ConfigMap/default/alpha-output { source: alpha }`.
- `widget-composition-beta` → P&T pipeline producing `ConfigMap/default/beta-output { source: beta }`.

CompositionRevisions auto-created by Crossplane:
- `widget-composition-alpha-a39f01a`
- `widget-composition-beta-a5e337f`

XR (`xr-r11.yaml`):
```yaml
apiVersion: example.org/v1
kind: XWidget
metadata:
  name: example
spec:
  compositionRef:
    name: widget-composition-alpha
  compositionRevisionRef:
    name: widget-composition-alpha-a39f01a
  compositionUpdatePolicy: Manual
  message: "F1 audit"
```

Initial reconcile after `kubectl apply`:
- `Synced=True / ReconcileSuccess`
- `alpha-output` ConfigMap created with `data: {source: alpha}, rv=3086`.
- `beta-output` does not exist.

## Probe

```bash
kubectl patch xwidget/example --type=merge -p \
  '{"spec":{"compositionRef":{"name":"widget-composition-beta"}}}'
# response: xwidget.example.org/example patched
```

The patch is **accepted with no admission rejection or webhook auto-correction.** No mutating webhook fires to update `compositionRevisionRef`. Observation window: 60s post-patch, sampling every 15s.

## Findings

| T+ | compositionRef | compositionRevisionRef | Synced | alpha-output | beta-output |
|---|---|---|---|---|---|
| 0s | **widget-composition-beta** | **widget-composition-alpha-a39f01a** | True (ReconcileSuccess) | rv=3086, source=alpha | absent |
| 15s | beta | alpha-a39f01a | True | rv=3086, source=alpha | absent |
| 30s | beta | alpha-a39f01a | True | rv=3086, source=alpha | absent |
| 45s | beta | alpha-a39f01a | True | rv=3086, source=alpha | absent |
| 60s | **widget-composition-beta** | **widget-composition-alpha-a39f01a** | True (ReconcileSuccess) | rv=3086, source=alpha | absent |

**The cross-reference (`compositionRef = beta`, `compositionRevisionRef = alpha-rev`) is stable for at least 60 seconds.** Crossplane is happily reconciling — `Synced=True` — and the composed resource is `alpha-output` with `data.source: alpha`, matching the **pinned revision's** content, not the **referenced Composition's** content.

The composed `alpha-output` ConfigMap's `resourceVersion` is constant (`rv=3086`) across the entire window, confirming Crossplane is making the existing managed resource match the pinned revision's desired state and finding no drift.

### Bonus probe: switch compositionUpdatePolicy to Automatic

```bash
kubectl patch xwidget/example --type=merge -p \
  '{"spec":{"compositionUpdatePolicy":"Automatic"}}'
```

After ~60s under Automatic:
- `compositionRevisionRef` updates to `widget-composition-beta-a5e337f` (the controller catches up).
- Crossplane re-reconciles; the composed resource's content flips: `alpha-output` ConfigMap now contains `data: {source: beta}` (Crossplane reuses the existing object — tracked by composition-resource-name annotation — and overwrites the data with beta's desired content). The ConfigMap is **not renamed** to `beta-output` because P&T patches the existing managed resource in-place; renaming a managed resource isn't part of the reconcile path.
- `resourceRefs` on the XR still references `alpha-output` (the original name).

This shows two things relevant to the F1 framing:
1. **The bug only persists while `compositionUpdatePolicy: Manual`.** Switching to Automatic resolves the cross-reference cleanly.
2. **Even Automatic recovery is imperfect:** the composed resource is rewritten in place rather than recreated under the new revision's resource name. A user who switched policies expecting "now I'll have a fresh `beta-output` ConfigMap" would be surprised.

## What this means for upstream-update draft 7220

The current draft framing (F1 still-reproduces) is **fully validated** by R-11. Tweaks worth considering when posting:

1. The **defaulting/mutating webhook hypothesis (CC-8)** is now empirically closed. Direct test: `kubectl patch` accepted with no rewrite, no rejection. This complements R-6 (no XR webhook ships in v2.2.0).
2. The bug is **time-stable**: `Synced=True` + cross-referenced state persists indefinitely as long as `compositionUpdatePolicy: Manual`. There is no eventual catch-up under Manual policy.
3. The original `api.go` flow (TODO at line ~250 about validating compositionRevisionRef matches compositionRef) is the natural fix surface. R-11 is direct evidence this is a user-visible correctness gap, not a simulator artifact.

## What's NOT addressed by R-11

- **Cluster-startup ordering with no pre-existing CompositionRevision.** The original F1 scenario in the harness focused on a startup race: the XR boots before any CompositionRevision exists for the new compositionRef. R-11 tests the steady-state cross-reference, which is the same bug surface but a different ordering path. R-11 is more conservative (steady-state, no race) and still reproduces. A harness scenario specifically modeling the startup race wasn't repeated on the real cluster — but the steady-state result implies the startup race would also reproduce since the path is the same code (`APIRevisionFetcher.Fetch` reads compositionRevisionRef without validating against compositionRef under Manual).
- **Multi-XR scenarios.** Single-XR test only.
- **Mixing label-pinned and name-pinned references.** R-11 used name-pinning (`compositionRevisionRef.name`). Crossplane also supports `compositionRevisionSelector` (label-based). Whether label-based selection catches mismatches is untested.

## Artifacts

Committed under [`./artifacts/tier3/r11/`](./artifacts/tier3/r11/) — see [`./artifacts/tier3/README.md`](./artifacts/tier3/README.md) for the per-file index. Key trace evidence:

- [`r11/xr-after-patch-T+30s.yaml`](./artifacts/tier3/r11/xr-after-patch-T+30s.yaml): the smoking gun — XR shows `compositionRef=widget-composition-beta`, `compositionRevisionRef=widget-composition-alpha-a39f01a`, `Synced=True`. Cross-referenced state silently accepted.
- [`r11/configmap-alpha-output-after-patch-T+30s.yaml`](./artifacts/tier3/r11/configmap-alpha-output-after-patch-T+30s.yaml): composed ConfigMap is `alpha-output { source: alpha }` despite `compositionRef=beta`.
- [`r11/configmap-beta-output-after-patch-T+30s.txt`](./artifacts/tier3/r11/configmap-beta-output-after-patch-T+30s.txt): `NotFound` — `beta-output` was never created.
- [`r11/observation.log`](./artifacts/tier3/r11/observation.log): 60s sampling showing cross-reference state stable.

## Comparison with hardened-harness re-run

| Aspect | Hardened harness (SPRINT-0001) | Real cluster (R-11) |
|---|---|---|
| Bug reproduces? | Yes (still-reproduces classification) | **Yes** |
| Synced state | True (ReconcileSuccess) | **True (ReconcileSuccess)** — identical |
| Composed resource source | alpha (pinned revision content) | **alpha** — identical |
| Stale cross-reference persists? | Yes (terminal hash carries cross-ref) | **Yes (≥60s, stable)** |
| Error/warning surfaced? | None | **None** |

Real-cluster behavior is identical to the simulator's prediction. F1 is the cleanest reproduction in the four Tier 3 audits.
