# R-2: Scenario fixtures and function stub fidelity

**Status:** ✅ AUDITED — fixtures and stub return shapes match production contract.

**Threat addressed:** [CC-1](./cross-cutting-threats.md#cc-1-function-runtime-stub-fidelity), [CC-2](./cross-cutting-threats.md#cc-2-compositionrevision-creation-and-labeling), [F1-A2, F6-A1, F6-A4](./per-finding-threats.md)

## Question

Do the F1 and F6 scenario fixtures seed objects in a way that matches what a real cluster would have at the equivalent point? Does our `stubFunctionRunner` return responses that trigger the same Crossplane code paths as a real function?

## F1 fixture (`workflow_crossplane-policy_manual-update-policy-composition-switch.json`)

Initial state seeds:
- XR `XWidget/default/example` with `compositionRef=widget-composition-alpha`, `compositionRevisionRef=widget-composition-alpha-rev-1`, `compositionUpdatePolicy=Manual`. Consistent — matches production.
- External UPDATE event: changes `compositionRef → widget-composition-beta` while leaving `compositionRevisionRef = widget-composition-alpha-rev-1`. The mismatched-refs state.

The CompositionRevisions for both `alpha-rev-1` and `beta-c34ead1` are created by the wired-from-real-source `CompositionReconciler` during exploration, not pre-seeded. This means the harness mirrors what a real cluster would do — Compositions exist first, revisions get created by the controller.

✅ **F1 fixture passes the audit.**

## F6 fatal stub (`functions_stub.go:53-64`)

```go
func fatalResponse() *fnv1.RunFunctionResponse {
    return &fnv1.RunFunctionResponse{
        Results: []*fnv1.Result{
            {
                Severity: fnv1.Severity_SEVERITY_FATAL,
                Message:  "simulated function failure",
            },
        },
    }
}
```

Production v2.2.0 `composition_functions.go:429-439` iterates `rsp.GetResults()` and the moment it sees `Severity_SEVERITY_FATAL`, it returns immediately:

```go
case fnv1.Severity_SEVERITY_FATAL:
    return CompositionResult{Events: events, Conditions: conditions}, errors.Errorf(errFmtFatalResult, fn.Step, rs.GetMessage())
```

Our stub returns exactly this shape. No `Desired` populated.

✅ **F6 stub triggers the production SEVERITY_FATAL branch correctly.**

⚠️ **Caveat (F6-T1):** real fatal functions could return BOTH `Desired.Resources` AND a fatal Result. Production line 397 (`d = rsp.GetDesired()`) captures the desired state BEFORE the fatal check at 429. But since the early-return at 439 fires before the desired-state-processing block at line 461, those captured resources never get used. Whether or not our stub populates `Desired`, the resulting code path is identical. Verified.

## CompositionRevision creation (CC-2)

The harness wires the *real* Crossplane `composition.NewReconciler` which creates revisions with the production code path:

`examples/crossplane/scenario.go:51-55`

```go
return composition.NewReconciler(
    mgr,
    composition.WithLogger(log),
    composition.WithRecorder(recorder),
)
```

Real `CompositionReconciler` sets the `crossplane.io/composition-name` label and `ownerReferences` automatically. No harness-specific seeding to inspect for label correctness — the production code does it.

✅ **CC-2 passes.** As long as the harness uses real Crossplane code (which it does), the labels and ownerRefs are production-faithful.

## Threat resolution

- **CC-1 function runtime stub fidelity:** RESOLVED for F6. Stub return shape triggers the documented production code path. For F1, the function stub isn't used (F1 doesn't depend on function execution).
- **CC-2 CompositionRevision creation:** RESOLVED. Real Crossplane code creates revisions; nothing in our harness pre-creates them with bad labels.
- **F1-A2 (external UPDATE equivalent to kubectl edit):** RESOLVED on the simulation side — the JSON fixture maps directly to a `client.Update` call on the XR. Whether real production has admission/defaulting that intercepts is a separate question (CC-8, R-6).
- **F6-A1 (SEVERITY_FATAL early-return reached):** RESOLVED.
- **F6-A4 (stub return shape matches real fatal):** RESOLVED — production code branches on `Severity` first; whatever else is in the response is irrelevant.

## What's NOT addressed

- Whether production has a *defaulting webhook* that auto-corrects mismatched refs in F1 (CC-8). That's R-6 (web search) or R-11 (real-cluster reproduction).
- Whether production has periodic resync that retries fatal functions and eventually GCs orphans (F6-T2). That's R-9 / R-13.
