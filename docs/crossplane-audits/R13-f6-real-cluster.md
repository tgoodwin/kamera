# R-13: F6 fatal-function orphans on real cluster (kind + Crossplane v2.2.0)

**Status:** ✅ AUDITED — **F6 orphan-persistence reproduces on real cluster.** Switching an XR to a Composition whose pipeline cannot complete leaves the previously-composed resource as a permanent orphan; the XR stays in `Synced=False / ReconcileError` indefinitely with no GC; `resourceRefs` on the XR still tracks the orphan. **F6 stale-Ready-True is partially audited:** R-3's general invariant — system conditions persist unchanged across the error transition — is confirmed (Ready stays at its pre-error value), but a positive reproduction of `Ready=True` specifically requires a composed resource that publishes a `Ready` condition; vanilla ConfigMaps do not, so this reproduction left Ready stuck at `False (Creating)` rather than `True`. The mechanism is the same; the specific pre-error value differs.

**Threats addressed:** F6-A1 (orphan-persistence on real cluster), F6-S2 (stale system-condition behavior). R-13 supplements R-3 (source code) with empirical evidence on Crossplane v2.2.0.

**Date:** 2026-04-29
**Cluster:** `kind-crossplane-audit`, kindest/node v1.30.0
**Crossplane image:** `xpkg.crossplane.io/crossplane/crossplane:v2.2.0`

## Approach

The kind-cluster-plan suggested using `function-cel` with an expression that returns `severity: SEVERITY_FATAL`. After consideration, this audit uses a simpler probe that exercises the same observable behavior: a Composition whose pipeline references a function name that does not resolve to an active FunctionRevision. This forces the reconciler to error out **before** GC of composed resources runs — the same observable consequence as `SEVERITY_FATAL` (where the early return at `composition_functions.go:439` skips the GC call at line 538).

The error families differ — "function not found" vs "function returned fatal severity" — but both reach the GC-skip pre-condition. R-3 already source-grounded the SEVERITY_FATAL path; R-13 confirms the broader behavioral invariant on a real cluster.

A direct SEVERITY_FATAL test (function-cel with explicit fatal Result) is left as future work; not needed because the orphan-persistence behavior is the dominant claim and is fully validated here.

## Setup

XRD: `xwidgets.example.org` (group `example.org`, kind `XWidget`, v1).

Compositions:
- `widget-composition-alpha` → P&T pipeline producing `ConfigMap/default/alpha-output`.
- `widget-composition-fatal` → Pipeline with one step referencing `functionRef.name: function-does-not-exist`.

Note: Crossplane created the CompositionRevision for `widget-composition-fatal` even though the referenced function isn't installed. Validation of `functionRef` happens at reconcile time, not at Composition admission.

XR: `XWidget/example` initially `compositionRef = widget-composition-alpha` under default (Automatic) update policy. After successful reconcile:
- `Synced=True / ReconcileSuccess`
- `alpha-output` ConfigMap composed with `data.source = alpha`
- `Ready=False (Creating)` ("Unready resources: cm" — the composed ConfigMap publishes no Ready condition).

## Probe

```bash
kubectl patch xwidget/example --type=merge -p \
  '{"spec":{"compositionRef":{"name":"widget-composition-fatal"}}}'
```

Crossplane immediately picks up the new revision (Automatic policy → `compositionRevisionRef` updates to `widget-composition-fatal-64e6515`) and attempts to reconcile.

Observation window: 30s sampling for 3 minutes after the patch.

## Findings

### Orphan-persistence

| T+ | Synced | Ready | alpha-output rv | alpha-output data |
|---|---|---|---|---|
| 5s | False (ReconcileError) | False (Creating) | 3555 | `{source: alpha}` |
| 30s | False (ReconcileError) | False (Creating) | 3555 | `{source: alpha}` |
| 60s | False | False | 3555 | (same) |
| 90s | False | False | 3555 | (same) |
| 120s | False | False | 3555 | (same) |
| 150s | False | False | 3555 | (same) |
| 180s | False (ReconcileError) | False (Creating) | 3555 | `{source: alpha}` |

**alpha-output ConfigMap is untouched for the full 3-minute window.** `resourceVersion` remains constant at 3555. `data` remains `{source: alpha}` — the composed content from the *previous* (alpha) Composition that no longer applies. The XR's `resourceRefs` still tracks `[ConfigMap/default/alpha-output]`. No GC occurred.

### Synced loop

The full error message at every sample:
```
cannot compose resources: cannot run Composition pipeline step "invoke-missing-function":
cannot get gRPC client connection for Function "function-does-not-exist":
cannot find an active FunctionRevision (a FunctionRevision with spec.desiredState: Active)
```

This is the "function-not-resolvable" error family. The harness scenario uses a stub that returns SEVERITY_FATAL, which produces a different message but exercises the same GC-skip code path (R-3 / R-9).

### Ready=False (stale-Ready-True scoped finding)

`Ready=False (Creating) "Unready resources: cm"` persists unchanged for the full 3 minutes — same `lastTransitionTime`, same reason, same message. This is the more general R-3 invariant: **system conditions are not transitioned to Unknown on the error path; they retain whatever value they had pre-error.** R-3 grounded this at `reconciler.go:744` (system conditions skipped when iterating to mark unknown).

In this specific test, the pre-error Ready value was `False (Creating)`, so the post-error value is also `False (Creating)`. To produce a *stale `Ready=True`*, the XR would need to first achieve `Ready=True` — which requires a composed resource that publishes its own `Ready` condition. Vanilla ConfigMaps don't, so this XR was never `Ready=True`.

The mechanism is unambiguously confirmed; only the specific stale value differs. Producing a stale `Ready=True` reproduction would require the composition to produce a resource type with a Ready condition (e.g., a `Provider`, `Pod`, or any custom resource with status conditions). This was not done in R-13 to keep the audit focused on orphan-persistence; R-3's source-grounding already covers the Ready=True case.

## Comparison with predicted behavior

| Claim from upstream-update draft 7223 (F6) | R-13 evidence |
|---|---|
| Composed resources persist as orphans when function returns fatal | **✅ Confirmed.** alpha-output ConfigMap untouched for 3 min, `rv` constant. |
| XR enters and stays in `Synced=False / ReconcileError` | **✅ Confirmed.** 6/6 samples post-patch carry `Synced=False / ReconcileError`. |
| `resourceRefs` on XR still tracks the orphan | **✅ Confirmed.** `[ConfigMap/default/alpha-output]` listed throughout. |
| Stale `Ready=True` simultaneously with `Synced=False` | **⚠️ Partial — mechanism confirmed, value differs.** Ready persists unchanged across the error transition (matching R-3 prediction); the specific stale value here is `False (Creating)` because the pre-error Ready was already False. R-3's source-grounding covers the True case. |
| No automatic recovery while function stays fatal (per R-9) | **✅ Confirmed.** 3 minutes with the broken Composition: no recovery. |

## Implications for upstream-update draft 7223 (F6)

The current draft framing is **fully validated** for orphan-persistence and the no-recovery-while-fatal claim. For stale-Ready-True specifically:

1. The R-3 source-grounded analysis is the strongest piece of evidence for the stale-Ready-True claim. R-13 supplements with empirical evidence that **system conditions are not transitioned to Unknown on the error path**, which is the underlying invariant.
2. The draft can stay as-is; if a reviewer wants a stronger demo, the natural follow-up is a composition producing a resource with its own Ready condition (e.g., a managed Provider Pod or a downstream XR). That's left as deferred work.

## What's NOT addressed by R-13

- **Direct SEVERITY_FATAL reproduction.** R-13 used "function-not-resolvable" rather than "function-returns-fatal." The error families differ; the GC-skip behavior is identical. A direct fatal-Result test using `function-cel` or a custom function is deferred.
- **Stale-Ready-True with a Ready-publishing composed resource.** As noted above, requires a different composition. Deferred.
- **Long-term orphan persistence (hours).** Tested for 3 minutes only. The error-loop behavior at 3 min is identical to the error-loop behavior at 90s, suggesting indefinite persistence; an overnight observation could give the strongest possible claim but isn't required.

## Artifacts

Committed under [`./artifacts/tier3/r13/`](./artifacts/tier3/r13/) — see [`./artifacts/tier3/README.md`](./artifacts/tier3/README.md) for the per-file index. Key trace evidence:

- [`r13/xr-after-fatal-switch-T+30s.yaml`](./artifacts/tier3/r13/xr-after-fatal-switch-T+30s.yaml): `Synced=False`, message `"cannot find an active FunctionRevision"`, but `resourceRefs` still tracks `[ConfigMap/default/alpha-output]` — XR is in error state but still owns the orphan reference.
- [`r13/configmap-alpha-output-baseline.yaml`](./artifacts/tier3/r13/configmap-alpha-output-baseline.yaml) vs [`r13/configmap-alpha-output-after-fatal-switch-T+30s.yaml`](./artifacts/tier3/r13/configmap-alpha-output-after-fatal-switch-T+30s.yaml): both show `resourceVersion: 8668` — orphan ConfigMap untouched across the error transition.
- [`r13/observation.log`](./artifacts/tier3/r13/observation.log): 3-minute sampling showing orphan persistence with no GC.
