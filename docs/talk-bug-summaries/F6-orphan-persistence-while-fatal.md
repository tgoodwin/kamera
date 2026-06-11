# F6-orphan — Composed resources persist as orphans while function returns SEVERITY_FATAL

**Issue:** [crossplane/crossplane#7223](https://github.com/crossplane/crossplane/issues/7223) (F6 sub-claim)
**Status:** ✅ Reproduces on real cluster (when scoped to "while function stays fatal"). Confidence: HIGH.
**Audits:** [R-2](../crossplane-audits/R2-scenario-fixtures.md), [R-3](../crossplane-audits/R3-fatal-branch.md), [R-9](../crossplane-audits/R9-post-fatal-rereconcile.md), [R-10](../crossplane-audits/R10-runfunction-protobuf.md), [R-13](../crossplane-audits/R13-f6-real-cluster.md).

## TL;DR

When a Composition is switched to a function pipeline whose function returns `SEVERITY_FATAL` (or otherwise cannot complete its pipeline), the composite reconciler's early-return path skips the GC step that would have cleaned up resources composed by the previous Composition. Those resources persist as orphans for as long as the function stays fatal. Once the function is fixed, the next successful reconcile cleans them up — so the bug is correctly framed as orphan-persistence-**while-fatal**, not permanent-orphan.

## What's actually wrong

`internal/controller/apiextensions/composite/composition_functions.go` in v2.2.0:

- Line 439 — early return on `SEVERITY_FATAL` from any function in the pipeline. The reconcile aborts here with an error.
- Line 538 — `GarbageCollectComposedResources(...)`. The call that would clean up resources composed by the previous Composition that are no longer in the new pipeline's desired set.

When a function returns `SEVERITY_FATAL`, line 439 returns before line 538 runs. GC is unreachable from the fatal path.

User impact: imagine an XR previously bound to Composition A, which composed `ConfigMap/foo`. The user switches the XR to Composition B, whose pipeline references a misconfigured function that returns `SEVERITY_FATAL`. Result:
- Composition B doesn't compose anything (function fatal).
- `ConfigMap/foo` from Composition A is **not** cleaned up.
- The XR's `resourceRefs` still tracks `ConfigMap/foo`.
- The XR enters `Synced=False / ReconcileError` and stays there until the user fixes the function.

The orphan resource may quietly continue serving its old purpose (a stale LoadBalancer, an outdated ConfigMap, a leftover Database that's still receiving traffic). The user may not realize it persists.

## How Kamera surfaced it

**Scenario:** [`workflow_crossplane-function-failure_composition-switches-to-fatal.json`](../../examples/crossplane/scenarios/workflow_crossplane-function-failure_composition-switches-to-fatal.json) — an XR is bound to a Composition that composes a ConfigMap; the user patches the XR to bind to a different Composition whose function pipeline returns `SEVERITY_FATAL`.

The harness's function-runtime stub (`examples/crossplane/functions_stub.go`) implements the gRPC `RunFunction` contract and can be configured to return `Results[].Severity = SEVERITY_FATAL`. The harness explored 1,212 total node visits across 112 resource states with 12 max-depth-aborted terminals. Per-trial:

| Trial | ConfigMap in terminal? |
|---|---|
| 1, 2, 5, 6, 7, 8 | yes (orphan) |
| 3, 4, 9 | no (cleaned) |

Hardened: 6/9 (67%) orphan, 3/9 (33%) cleaned. Baseline (before fidelity fixes): 42/49 (86%) orphan, 7/49 (14%) cleaned. The orphan rate dropped meaningfully under hardening but remains the dominant outcome — the underlying Crossplane code path (early return before GC) is unchanged, so the bug-shape persists in the majority of orderings.

## How we validated it

### Tier 1 — source grounding (R-3)

R-3 traced the SEVERITY_FATAL path through the v2.2.0 source, line by line:
- The check is at `composition_functions.go:439`. It iterates over all `Results` from all functions in the pipeline; if any has `Severity == SEVERITY_FATAL`, the loop returns an error.
- The GC call is at `composition_functions.go:538`, after a series of validation steps that the early-return short-circuits.
- The early-return path is reached without ever computing the "previously-composed but no longer in desired set" delta.

This is mechanism-level proof that on the SEVERITY_FATAL path, GC of previously-composed resources cannot run.

### Tier 1 — fixture / stub fidelity (R-2)

R-2 audited the harness's fatal-stub. The stub returns the exact `RunFunctionResponse` shape Crossplane expects, including `Results[].Severity = SEVERITY_FATAL`. R-2 confirmed the stub triggers the documented production code path — even if production fatal functions populate `Desired` (they shouldn't, but the contract doesn't forbid it), the early-return at line 439 short-circuits before desired-state processing, so the stub's omission of `Desired` doesn't change behavior.

### Tier 2 — re-reconcile behavior (R-9)

The original report's framing implied "orphan persists until manually cleaned." R-9 audited Crossplane's post-fatal re-reconcile logic: every retry retakes the same fatal early-return; GC is unreachable while the function stays fatal. **Once the function is fixed, the next successful reconcile WILL run GC.** This narrows the claim to "orphan-persistence-while-fatal" rather than "permanent orphan." The current draft scopes correctly.

### Tier 2 — runtime contract (R-10)

R-10 audited the gRPC `RunFunctionRequest`/`Response` schema and confirmed the harness's stub adequately models the consumed fields for the fatal path. The bare-runner approach (no `FetchingFunctionRunner` wrap) is OK because no current scenario uses response `Requirements`.

### Tier 3 — direct real-cluster reproduction (R-13)

Setup: kind cluster as in R-11/R-14, with `widget-composition-alpha` (P&T pipeline producing `ConfigMap/default/alpha-output`) bound to `XWidget/example`. Crossplane created the CompositionRevision for `widget-composition-fatal` (which references `function-does-not-exist`) without rejecting it — capability validation happens at reconcile time, not at admission.

Probe:
```bash
kubectl patch xwidget/example --type=merge -p \
  '{"spec":{"compositionRef":{"name":"widget-composition-fatal"}}}'
```

3-minute observation, sampling every 30s:

| T+ | Synced | alpha-output rv | alpha-output data |
|---|---|---|---|
| 5s | False (ReconcileError) | 3555 | `{source: alpha}` |
| 30s | False | 3555 | (same) |
| 60s..180s | False | 3555 | `{source: alpha}` |

`alpha-output` ConfigMap is **untouched for the full window** (`resourceVersion` constant at 3555, `data` still `{source: alpha}` from the prior alpha Composition). The XR's `resourceRefs` still tracks `[ConfigMap/default/alpha-output]`. The XR enters and stays in `Synced=False / ReconcileError` with message:
```
cannot compose resources: cannot run Composition pipeline step "invoke-missing-function":
cannot get gRPC client connection for Function "function-does-not-exist":
cannot find an active FunctionRevision (a FunctionRevision with spec.desiredState: Active)
```

Note on error families: R-13 used a "function-not-resolvable" trigger rather than a SEVERITY_FATAL function. The error messages differ ("cannot find an active FunctionRevision" vs "function returned fatal severity") but both reach the same GC-skip code path — the composite reconciler can't complete the pipeline, so line 538 doesn't run. R-3's source-grounding covers the SEVERITY_FATAL case specifically; R-13 confirms the broader observable invariant on a real cluster.

A direct SEVERITY_FATAL test using a custom function (e.g., `function-cel` with explicit fatal Result) is left as deferred work; not required because the orphan-persistence behavior is the dominant claim.

Trace artifacts at [`../crossplane-audits/artifacts/tier3/r13/`](../crossplane-audits/artifacts/tier3/r13/). Smoking-gun XR yaml: [`xr-after-fatal-switch-T+30s.yaml`](../crossplane-audits/artifacts/tier3/r13/xr-after-fatal-switch-T+30s.yaml).

## Note on the suggested fix (withdrawn)

The original SPRINT-0001 report proposed "call `GarbageCollectComposedResources` before the SEVERITY_FATAL early-return." Maintainer review (jbw976) pointed out that `desired` is potentially incomplete at that point: if the fatal function is mid-pipeline, later steps may add resources still in use, and cleaning up "everything not in the partial-`desired` set" could delete in-use resources. **That argument holds and we're not contesting it.**

The orphan-persistence behavior is real, but the safe-fix surface is narrower than originally proposed. Possible directions (not pre-judged in our drafts):

- Only GC resources whose owner-ref or label is unambiguously this XR's previous-revision composition output. Doesn't help if `desired` is meant to be authoritative.
- Surface the orphan inventory in XR status so users know what to clean up manually.
- Document the orphan-on-fatal semantics so users aren't surprised.

The bug claim (the user-visible behavior) is decoupled from the fix proposal; we're leaving the latter open.

## Talking points for the audience

- The bug is two lines of code apart (439 vs 538) but those lines are gating behavior the user does not expect. R-3's source-grounding makes this very concrete.
- The "while fatal" scoping is important — the original framing implied a permanent orphan, but R-9 narrows it to "orphan as long as the function stays fatal." That's still bad (orphans can persist for hours/days if a deploy is broken), but it's not a forever-orphan and we shouldn't claim that.
- The hardened-harness orphan rate dropping from 86% to 67% is a good lesson: the harness wasn't "wrong" before, but its earlier DELETE/GC fidelity was crude enough that some additional orderings produced orphan terminals that real Crossplane wouldn't have produced. The hardened harness gives a tighter estimate while preserving the real bug.
- R-13's real-cluster reproduction is on a different error family (function-not-resolvable, not SEVERITY_FATAL). The GC-skip pre-condition is identical; if a maintainer wants the SEVERITY_FATAL flavor specifically, that's a deferred follow-up.
- The withdrawn fix proposal is worth raising honestly — the maintainer review surfaced a real safety issue with our naive proposal, and we should credit them for it.
