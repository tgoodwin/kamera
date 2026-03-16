# Mid-Reconcile Fault Injection (CrashAt / RecoverAt)

## Motivation

Kamera currently treats each `Reconcile()` call as atomic — ordering permutation
only affects which controller runs NEXT, not what happens inside a controller.
This prevents testing intra-reconcile races where a controller crashes after
performing some but not all of its API writes.

Real-world examples:
- Disruption `StartCommand()` does: markDisrupted → createReplacementNodeClaims → MarkForDeletion. A crash after step 1 leaves nodes tainted but not replaced.
- Lifecycle `finalize()` does: deleteNodes → deleteCloudInstance → removeFinalizer. A crash after deleteNodes leaves the cloud instance running.
- Provisioner `CreateNodeClaims()` calls `Create()` for multiple NodeClaims via ParallelizeUntil. A crash after creating some but not all NodeClaims leaves partial state.

## Design

### Core concept: `CrashAfterEffect`

A new perturbation that terminates a reconciler's execution after it has produced
N write effects (CREATE, PATCH, DELETE) within a single `Reconcile()` call. The
remaining writes in that reconcile are discarded. The reconciler is then
re-enqueued after a configurable recovery delay.

### JSON schema additions to `InputTuning`

```json
"faultInjection": [
  {
    "reconciler": "disruption",
    "crashAfterEffect": 2,
    "recoverAtDepth": 10,
    "triggerOnce": true
  }
]
```

Fields:
- `reconciler`: ReconcilerID of the controller to crash
- `crashAfterEffect`: Stop the reconcile after this many write effects (CREATE/PATCH/DELETE). The Nth effect IS applied; effects N+1..end are discarded.
- `recoverAtDepth`: The depth at which the controller is re-enqueued with fresh state (simulating container restart). If omitted, the controller recovers on its next natural trigger (watch event or requeue).
- `triggerOnce`: If true, only crash once per trial. If false, crash every time the reconciler runs (useful for testing persistent failures).

### Semantics

**Crash behavior:**
1. The reconciler's `Reconcile()` is called normally
2. The effect recorder counts write effects (CREATE, PATCH, DELETE) during the reconcile
3. When the count reaches `crashAfterEffect`, the remaining effects are suppressed
4. The reconcile returns as if it completed normally (from kamera's perspective), but only the first N effects are materialized into the object store
5. Observations (GET, LIST) made before the crash point ARE recorded (they happened)
6. The reconciler IS NOT re-enqueued by the crash itself — it waits for `recoverAtDepth` or a natural trigger

**Recovery behavior:**
1. At `recoverAtDepth`, the reconciler is added to pending reconciles with source "Recovery"
2. The recovery reconcile gets fresh state — no staleness intervals apply for this one invocation (simulating a fresh List from the API server after restart)
3. After recovery, normal staleness rules resume
4. If `recoverAtDepth` is omitted, the reconciler recovers naturally (next watch event or RequeueAfter timer)

**Interaction with other perturbations:**
- Staleness intervals apply normally before and after the crash, except during the recovery reconcile
- Ordering permutation works as usual — the crashed reconciler is just not pending until recovery
- Multiple crash configs can target different reconcilers in the same trial

## Files to modify

### `pkg/coverage/types.go`

Add `InputFaultInjection` struct and field on `InputTuning`:

```go
type InputFaultInjection struct {
    Reconciler       string `json:"reconciler"`
    CrashAfterEffect int    `json:"crashAfterEffect"`
    RecoverAtDepth   int    `json:"recoverAtDepth,omitempty"`
    TriggerOnce      bool   `json:"triggerOnce,omitempty"`
}
```

Add to `InputTuning`:
```go
FaultInjection []InputFaultInjection `json:"faultInjection,omitempty"`
```

### `pkg/tracecheck/explore.go`

Add `FaultInjectionConfig` to `PerturbationConfig`:

```go
type FaultInjectionConfig struct {
    ReconcilerID     ReconcilerID
    CrashAfterEffect int
    RecoverAtDepth   int
    TriggerOnce      bool
}
```

Add `FaultInjection []FaultInjectionConfig` to `PerturbationConfig`.

**Effect interception point:** In the effect recording path (where `handleEffect` is
called for each write), add a counter per reconciler per reconcile invocation. When the
counter reaches `CrashAfterEffect`, set a flag that causes subsequent `handleEffect`
calls to be silently dropped for the remainder of that reconcile.

Key implementation location: `ReconcilerContainer.doReconcile()` or the effect context
manager. The crash interception must happen AFTER the Nth effect is applied to the
object store but BEFORE the (N+1)th effect is applied.

**Recovery scheduling:** In the main explore loop, after processing a step, check if
`state.depth >= recoverAtDepth` for any crashed reconciler. If so, add it to
`PendingReconciles` with `source: "Recovery"`.

### `pkg/tracecheck/state.go`

Add `crashedReconcilers map[ReconcilerID]bool` to `StateNode` to track which
reconcilers have been crashed (for `triggerOnce` semantics). Clone in `Clone()`.

### `pkg/explore/tuning.go`

Add translation from `InputFaultInjection` to `FaultInjectionConfig` in
`ApplyInputTuning()`.

### `examples/karpenter/scenario.go`

Add `FaultInjection` to `cloneCoverageInput()` so it's preserved across fuzzer variants.

## Implementation strategy

1. Add types to `coverage/types.go` and `tracecheck/explore.go`
2. Add effect counting + suppression in the effect recording path
3. Add recovery scheduling in the explore loop
4. Add state tracking for crashed reconcilers
5. Add tuning translation
6. Write unit tests in `pkg/tracecheck/fault_injection_test.go`

## Test plan

### Unit tests (`pkg/tracecheck/fault_injection_test.go`)

1. **CrashAfterEffect=1**: Reconciler produces 3 effects. Only the first is applied.
   Object store reflects effect 1, not effects 2-3.
2. **Recovery at depth**: Crashed reconciler re-enqueued at specified depth. Verify
   it appears in PendingReconciles with "Recovery" source.
3. **TriggerOnce**: Crash fires on first invocation, subsequent invocations proceed
   normally.
4. **No recovery depth**: Reconciler not re-enqueued by crash. Must wait for natural
   trigger.
5. **Multiple fault configs**: Two reconcilers with different crash points. Both
   crash independently.

### Integration tests (Karpenter scenarios)

1. **D9 revisited**: Crash `disruption` after `markDisrupted` (effect 2) but before
   `createReplacementNodeClaims`. Nodes are tainted but no replacement is created.
   Provisioner sees tainted nodes, pod is unschedulable, creates its own NodeClaim.
2. **D7 revisited**: Crash lifecycle controller after Node deletion (finalize stage 1)
   but before cloud instance deletion (stage 2). Cloud instance leaks.
3. **New scenario**: Crash provisioner after creating NodeClaim-1 but before creating
   NodeClaim-2 (in a batched 2-pod scenario). One pod gets a node, the other doesn't.

## Verify

```bash
go test ./pkg/tracecheck/... ./pkg/explore/... ./pkg/coverage/... && go build ./...
```
