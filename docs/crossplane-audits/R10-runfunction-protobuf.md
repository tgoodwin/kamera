# R-10: RunFunctionResponse protobuf schema + runtime contract

**Status:** ✅ AUDITED — for the F1, F5, F6 scenario set the stub adequately models the parts of the response that production actually consumes. Two known gaps (response `Requirements` for resource fetching, response `Conditions` for XR status) are noted but do not affect any current finding.

**Threat addressed:** [CC-1](../upstream-updates/AUDIT-threats-to-validity.md#cc-1-function-runtime-stub-fidelity)

## Question

Does our `stubFunctionRunner.RunFunction` adequately model the gRPC `fnv1.RunFunctionRequest`/`Response` contract? Specifically: are we missing fields that production functions use (TTL, conditions, capabilities, requirements) in a way that would change Crossplane's per-reconcile behavior?

## Research method

1. Read the protobuf schema at `proto/fn/v1/run_function.proto` (lines 121-159 for `RunFunctionResponse`).
2. Read each `rsp.Get*()` access in `internal/controller/apiextensions/composite/composition_functions.go`.
3. Read `internal/xfn/required_resources.go` for the `Requirements` consumer.
4. Read `internal/xfn/capabilities.go:96-105` for `SupportedCapabilities()`.
5. Compared production wiring (`cmd/crossplane/core/core.go:469-474`) against harness wiring (`examples/crossplane/scenario.go:68-79`).
6. Read each return shape in `examples/crossplane/functions_stub.go`.

## Findings

### Production reads these fields from `RunFunctionResponse`

`composition_functions.go:384-457` — the only places `rsp` is dereferenced in the composition path:

| Line | Access | Purpose |
|---|---|---|
| `392` | `rsp.GetMeta().GetTtl().AsDuration()` | Per-step TTL feeding into pipeline `result.TTL` |
| `397` | `rsp.GetDesired()` | Carry desired state forward to next step / final compose |
| `401` | `rsp.GetContext()` | Pipeline-step inter-step state |
| `403-425` | `rsp.GetConditions()` (and `c.GetType/Status/Reason/Message/Target`) | XR status conditions to apply |
| `429-457` | `rsp.GetResults()` (and `rs.GetSeverity/Message/Reason/Target`) | Events + the fatal short-circuit |

`required_resources.go:94` — `rsp.GetRequirements()` — used by `FetchingFunctionRunner` to fetch additional resources for the next iteration of a single function call (up to `MaxRequirementsIterations`).

### How production wires the runner

`cmd/crossplane/core/core.go:469-474`:

```go
// Middleware layering: We want Cache → FetchingFunctionRunner → gRPC.
runner = xfn.NewFetchingFunctionRunner(runner,
    xfn.NewExistingRequiredResourcesFetcher(cached),
    xfn.NewOpenAPIRequiredSchemasFetcher(oac))
```

So in production every function response goes through the `FetchingFunctionRunner` wrapper. The wrapper inspects `rsp.GetRequirements()` and re-runs the function up to `MaxRequirementsIterations` times until requirements stabilize.

### How the harness wires the runner

`examples/crossplane/scenario.go:68-79`:

```go
builder.WithReconciler("CompositeReconciler", func(c client.Client) tracecheck.Reconciler {
    runner := stubFunctionRunner{}
    composer := composite.NewFunctionComposer(c, c, runner)  // bare runner, NO FetchingFunctionRunner wrap
    return composite.NewReconciler(...)
})
```

The harness uses a **bare** `stubFunctionRunner` without the production `FetchingFunctionRunner` wrapper.

### Gap analysis: stub vs production-consumed fields

| Field | Stub returns? | Production reads? | Gap impact |
|---|---|---|---|
| `Meta.Ttl` | ❌ No (left zero) | Yes — feeds `result.TTL`, only matters with `EnableBetaRealtimeCompositions` (`reconciler.go:865-869`) | None — F1/F5/F6 don't enable realtime compositions; default `RequeueAfter: jitter(pollInterval)` (1m) applies regardless. |
| `Desired` | ✅ defaultResponse / partial / different — yes; fatalResponse — no | Yes — for compose | None for F1/F5/F6. Per R-3, on fatal the early-return at `composition_functions.go:439` discards `Desired` anyway. |
| `Context` | ❌ Always nil | Yes — but only matters across multi-step pipelines | None — F1/F5/F6 use single-step pipelines. |
| `Conditions` | ❌ Stub never sets | Yes — `composition_functions.go:403-425` would push these into XR status | **Potential gap** — but no F1/F5/F6 trace claim depends on function-supplied conditions. The F6 stale-Ready=True claim is about `Ready` (a system condition not supplied by functions), and is sourced from the system-condition filter at `reconciler.go:744`. |
| `Requirements` | ❌ Stub never sets | Yes — `FetchingFunctionRunner` would loop; bare runner ignores | None — harness uses bare runner; production with no requirements set behaves identically to the bare runner case. |
| `Results[].Severity` | ✅ fatalResponse returns SEVERITY_FATAL | Yes — `composition_functions.go:438-439` | Matches. (Verified in R-2 and R-3.) |
| `Results[].Message` | ✅ "simulated function failure" | Yes — included in error | Matches. |
| `Results[].Reason` / `Target` | ❌ Not set (defaults to empty) | Yes — but defaults to `reasonCompose` / `TARGET_UNSPECIFIED` if empty | None — defaults align. |

### Capability advertisement (request side)

`internal/xfn/capabilities.go:96-105` — production advertises:

```go
fnv1.Capability_CAPABILITY_CAPABILITIES,
fnv1.Capability_CAPABILITY_REQUIRED_RESOURCES,
fnv1.Capability_CAPABILITY_CREDENTIALS,
fnv1.Capability_CAPABILITY_CONDITIONS,
fnv1.Capability_CAPABILITY_REQUIRED_SCHEMAS,
```

`composition_functions.go:379` — production includes these in the `RequestMeta.Capabilities`. The harness does NOT bypass this — the bare runner's wrapper (`composite.NewFunctionComposer` → `composition_functions.go:323-458` pipeline loop) builds the same `fnreq.Meta = &fnv1.RequestMeta{Tag: Tag(fnreq), Capabilities: xfn.SupportedCapabilities()}` regardless of what wraps the runner. So capabilities ARE advertised in our request, even though our stub doesn't introspect them.

### Schema sanity-check

`proto/fn/v1/run_function.proto:121-159` defines:

```proto
message RunFunctionResponse {
  ResponseMeta meta = 1;
  State desired = 2;
  repeated Result results = 3;
  optional google.protobuf.Struct context = 4;
  Requirements requirements = 5;
  repeated Condition conditions = 6;
  optional google.protobuf.Struct output = 7;  // ignored for composition (line 158: "XRs will discard")
}
```

Our stub populates `Desired` and `Results`. The bare-runner wiring means `Requirements` is read by no one in our harness. The remaining fields default to zero values, which is a valid no-op response.

## Source links

- Schema: `proto/fn/v1/run_function.proto:121-159` (RunFunctionResponse)
- Production reads: `internal/controller/apiextensions/composite/composition_functions.go:384-457`
- `Requirements` reader: `internal/xfn/required_resources.go:69-152` (FetchingFunctionRunner)
- Capability list: `internal/xfn/capabilities.go:96-105`
- Production wiring: `cmd/crossplane/core/core.go:469-474`
- Harness wiring: `examples/crossplane/scenario.go:68-79`
- Stub: `examples/crossplane/functions_stub.go:26-65`

## Threat resolution

| Threat | Resolution |
|---|---|
| **CC-1**: stub returns wrong shape, fatal early-return doesn't fire | RESOLVED (also in R-2, R-3). Stub returns `Severity_SEVERITY_FATAL` matching `composition_functions.go:438`. |
| **CC-1**: stub doesn't advertise capabilities | RESOLVED — capability advertisement is in the **request** built by Crossplane (line 379), not in the response. Capabilities are correctly advertised regardless of the runner stub. |
| **CC-1**: stub missing `Requirements` would cause production to loop | NOT APPLICABLE — harness uses bare runner without `FetchingFunctionRunner` wrap. Production functions that don't need additional resources also leave `Requirements` empty; both production-with-empty-requirements and our bare-runner case skip the fetch loop. The harness simply does not exercise the fetch loop, which is fine for F1/F5/F6 (no function requires extra resources). |
| **CC-1**: stub missing function-supplied `Conditions` | LOW RISK — no F1/F5/F6 claim depends on function-supplied conditions. F6's stale-Ready claim is about a system condition, not a function-supplied one (R-3 already verified). |

## What this means for F1/F5/F6 posting

The function-runtime stub is fit-for-purpose for the three findings being posted. No additional fidelity blockers.

## What's NOT addressed

- The harness does not exercise the `FetchingFunctionRunner` middleware. If a future scenario required testing requirements-based resource fetching, we would need to either wrap the stub with `xfn.NewFetchingFunctionRunner` or extend the stub. **Track as deferred work.**
- The harness does not exercise function-supplied conditions (response `Conditions` field). If a future Crossplane scenario depends on conditions arriving from a function, the stub will need extending.
- The harness does not advertise per-revision function capabilities on `FunctionRevision` objects (these are package-level, not response-level). The single-FunctionRevision watch wiring in R-1 covers the package-level capability change scenario for F5.
- Caching middleware (`xfncached.NewFileBackedRunner`) is not modeled. Not needed for F1/F5/F6; would matter only for cache-related races.
