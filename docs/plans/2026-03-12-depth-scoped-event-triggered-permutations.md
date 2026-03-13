# Depth-Scoped and Event-Triggered Permutations

## JSON schema additions to `InputTuning`
```json
"permuteDepthRange": {"min": 1, "max": 5},
"permuteAfterEvent": {"opType": "CREATE", "kind": "apiextensions.crossplane.io/CompositionRevision"}
```
Both optional. When both set, AND logic (both must be satisfied). Omitting either = unconstrained.

## Files to modify

- **`pkg/coverage/types.go`**: Add `InputDepthRange`, `InputPermuteEvent` structs; add `PermuteDepthRange *InputDepthRange` and `PermuteAfterEvent *InputPermuteEvent` fields to `InputTuning`

- **`pkg/tracecheck/explore.go`**:
  - Add `PermuteDepthRange` and `PermuteEventTrigger` types
  - Add both as pointer fields on `PerturbationConfig` (line 52)
  - Modify `shouldExpandPendingOrder()` (line 1956): check depth range + `state.permuteTriggered`
  - Modify `initialStatesToEnqueue()` (line 1948): call `shouldExpandPendingOrder()` instead of inline check
  - In `materializeNextState()` (line 1425): propagate `permuteTriggered` from parent; check step effects against trigger condition
  - In `Clone()` (line 167): deep-copy new pointer fields

- **`pkg/tracecheck/state.go`**:
  - Add `permuteTriggered bool` field to `StateNode` (line 277)
  - `Clone()` (line 404): copy `permuteTriggered`
  - `serialize()` (line 428): append `|pt:1` when trigger is configured and active (for dedup differentiation)

- **`pkg/explore/tuning.go`**: Add translation blocks in `ApplyInputTuning()` after line 31 for both new fields

## New files
- **`pkg/tracecheck/permute_scope_test.go`**: Unit tests for depth range gating, event trigger activation/inheritance, combined AND logic, serialize differentiation

## Integration test
- Create `examples/crossplane/scenarios/workflow_crossplane-staleness_composition-update-races-xr-fetch-event-triggered.json` with `permuteAfterEvent` targeting `CREATE` of `CompositionRevision`
- Run and compare campaign-metrics against hypothesis-1 baseline

## Verify
```bash
go test ./pkg/tracecheck/... ./pkg/explore/... ./pkg/coverage/... && go build ./...
```
