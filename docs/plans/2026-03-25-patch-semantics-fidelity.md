# Patch Semantics Fidelity Plan

**Date:** 2026-03-25
**Status:** Draft

## Problem

Kamera's replay client and `applyEffects` engine diverge from real Kubernetes
API server semantics in several ways around Patch operations. These gaps can
cause Kamera to miss real bugs (false negatives) or allow controller behavior
that would fail on a real cluster.

## Audit Summary

| # | Gap | Where | Severity |
|---|-----|-------|----------|
| 1 | No resourceVersion conflict checking | `handleEffect`, `validateEffect` | **High** |
| 2 | Status subresource merge is flat field replacement | `mergeStatusSubresourceObject` | Medium |
| 3 | SSA field-manager conflicts not modeled | `validateEffect`, `applyEffects` | Medium |
| 4 | Patch type not preserved through effect pipeline | `Patch()` -> `handleEffect` | Low-Med |
| 5 | Label propagation on status subresource writes | `subResourceClient.Patch` | Low |

---

## Fix 1: ResourceVersion Conflict Checking (High Priority)

### Why it matters

Many controllers use optimistic concurrency: they read an object, modify it,
and write it back with the same `resourceVersion`. If another writer changed
the object in between, the API server returns HTTP 409 Conflict. Controllers
react to this (retry, requeue, give up) and that control flow matters for
staleness analysis.

Today, Kamera silently accepts every Update/Patch regardless of
resourceVersion. This means:
- A controller that would hit a conflict on a real cluster succeeds in Kamera
- Divergent control flow (conflict retry vs. success path) is invisible
- Potential double-write bugs are masked

### Approach

**Where:** `pkg/tracecheck/manager.go:validateEffect` and
`pkg/replay/client.go:handleEffect`.

The replay client already captures `PreconditionInfo` but the TODO at
`client.go:91` notes it is never validated. The `applyEffects` engine already
tracks object versions via content hashes in `ObjectVersions`.

1. **Extract resourceVersion from the object itself.** In Kubernetes, the
   resourceVersion for conflict checking comes from the object body (for
   Update) or is implicit in Strategic Merge Patch. The replay client receives
   the full object; the resourceVersion the controller set on it is the
   claimed version.

2. **Compare against current state version in `validateEffect`.** The
   `effectRKeys` map tracks existence. Extend it (or add a parallel map) to
   also track the current version hash for each resource key. On
   UPDATE/PATCH, compare the object's resourceVersion (or version hash) with
   the current state's version. If they diverge, return a 409 Conflict error.

3. **Scope:** Only enforce for UPDATE and PATCH (not APPLY, which has its own
   conflict model). SSA conflict detection is a separate fix (see Fix 3).

### Key design questions

- Kamera doesn't use real integer resourceVersions internally; it uses content
  hashes. The comparison should be: "does the object the controller is writing
  back carry the same version hash as what's currently in the state?" This
  requires either:
  - (a) Stamping the version hash onto objects when they are served from the
    cache frame (already happens via `copyInto`), or
  - (b) Tracking the version hash the controller *observed* (from its GET/LIST)
    and comparing that against the current state version at write time.

  Option (b) is more faithful. The `Observations` (reads) recorded for each
  reconcile already capture what the controller saw. At validation time, look
  up the last observed version of the target object from the reads list and
  compare against the current state.

### Files to modify

- `pkg/tracecheck/manager.go` -- `validateEffect`: add version comparison
  logic for UPDATE/PATCH ops
- `pkg/tracecheck/manager.go` -- effect state tracking: store current version
  hash per resource key alongside existence
- `pkg/replay/preconditions.go` -- potentially extract resourceVersion from
  object metadata and populate `PreconditionInfo.ResourceVersion`
- `pkg/tracecheck/explore.go` -- `applyEffects` already handles version
  tracking; may need to expose current-version lookup

### Testing

- Unit test: controller writes object with stale resourceVersion -> expect 409
- Unit test: controller writes object with current resourceVersion -> expect
  success
- Integration test: scenario where two controllers write to the same object
  in the same exploration step; second writer should see conflict

---

## Fix 2: Status Subresource Merge Fidelity (Medium Priority)

### Why it matters

`mergeStatusSubresourceObject` in `explore.go:1908-1933` does flat
top-level field replacement: it takes `newObj.status` and drops it into
`oldObj`. In Kubernetes:

- **Status Update** (`PUT /status`): replaces the entire status subresource
  but preserves spec/metadata from the stored version. Kamera handles this
  correctly.
- **Status Patch** (`PATCH /status` with SMP): performs a Strategic Merge Patch
  on the status field, which means nested fields are merged recursively, and
  list fields follow their merge strategy (e.g., `conditions` is merge-on-key
  by `type`).

Currently Kamera treats both the same way. The practical impact is:

- A Patch that sets `.status.conditions[0].status = "True"` would, on a real
  API server, merge into the existing conditions list by `type`. In Kamera,
  the entire `status` block from the controller's write replaces the old one.
- If a controller depends on previously-set status fields surviving a partial
  status patch, Kamera could produce a divergent state.

### Approach

The controller's write object (as recorded by `RecordEffect`) already contains
the *full* object as the controller constructed it, not the patch document.
This is because controller-runtime's `Status().Patch()` takes an `obj` and a
`patch` argument; the replay client captures `obj` (the desired state) rather
than the raw patch bytes.

This means the current flat-replacement is actually correct for the common
case: controllers typically read the object, modify status fields on the
in-memory copy, and write it back. The full status they write already includes
the merged result.

**The gap only materializes when:**
- A controller constructs a partial status object (not derived from a read)
  and uses merge patch
- Two controllers write to different status fields of the same object within
  the same exploration step

**Recommendation:** Document this as a known limitation rather than implement
full SMP. True SMP requires merge-strategy annotations from the CRD schema,
which Kamera does not currently load. The cost/benefit ratio is poor.

**If we do want to address it later:**
1. Load CRD structural schemas (OpenAPI v3) at harness init time
2. Use `k8s.io/apimachinery/pkg/util/strategicpatch` to perform the merge
3. Only needed for the `applyEffects` path, not the replay client

### Files

- `pkg/tracecheck/explore.go` -- add a comment documenting the limitation
- (Future) New file `pkg/tracecheck/smp.go` for schema-aware merge if needed

---

## Fix 3: SSA Field-Manager Conflict Detection (Medium Priority)

### Why it matters

Server-Side Apply tracks which field manager owns which fields. If two
managers try to set the same field without `force: true`, the API server
returns a 409 Conflict. Controllers using SSA depend on this for coordination.

Kamera currently treats APPLY as a simple upsert with no conflict checking.

### Approach

Full `managedFields` tracking is complex and likely overkill. A pragmatic
middle ground:

1. **Track field manager per APPLY effect.** Extend `EffectOptions` to carry
   the `FieldManager` string (already available in `client.PatchOptions` and
   `client.ApplyOptions`).

2. **Track field ownership at the top-level field granularity.** Maintain a
   map of `resourceKey -> fieldPath -> fieldManager`. When an APPLY effect
   arrives, check if any top-level fields it touches are owned by a different
   manager.

3. **If conflict detected and `Force` is not set, return 409.** If `Force` is
   set, transfer ownership.

This is coarser than real Kubernetes (which tracks ownership at the leaf-field
level) but catches the most common SSA conflict patterns.

### Key design questions

- **Granularity:** Top-level fields (spec, metadata.labels,
  metadata.annotations) vs. full leaf-field tracking. Top-level is simpler
  and catches most real conflicts. Start there.
- **Where to extract FieldManager:** `client.PatchOptions.FieldManager` for
  `Patch()` with `ApplyPatchType`, and `client.ApplyOptions.FieldManager` for
  the newer `Apply()` method.

### Files to modify

- `pkg/replay/preconditions.go` -- extract `FieldManager` and `Force` from
  PatchOptions/ApplyOptions into `PreconditionInfo`
- `pkg/replay/effects.go` -- add `FieldManager` and `Force` fields to
  `EffectOptions`
- `pkg/tracecheck/manager.go` -- `validateEffect`: add field-manager
  conflict check for APPLY ops
- `pkg/tracecheck/explore.go` -- `applyEffects` APPLY case: update
  field-ownership map after successful apply

### Testing

- Unit test: two different field managers APPLY to the same field -> 409
- Unit test: same field manager re-applies -> success
- Unit test: different field manager with `Force: true` -> success,
  ownership transfers

---

## Fix 4: Preserve Patch Type Through Effect Pipeline (Low-Medium)

### Why it matters

The replay client distinguishes `PATCH` vs `APPLY` in `event.OperationType`
but does not preserve the specific patch type (Strategic Merge Patch, JSON
Merge Patch, JSON Patch). This information is lost after `handleEffect`.

In practice this matters less than it sounds, because:
- The replay client captures the *result* object, not the patch document
- `applyEffects` works with full objects, not patch diffs
- The patch type only matters for how the API server merges, and Kamera
  replays effects, not merges

**Recommendation:** Low priority. Add a `PatchType` field to `EffectOptions`
for observability/debugging, but don't change merge behavior based on it.

### Files

- `pkg/replay/effects.go` -- add `PatchType types.PatchType` to
  `EffectOptions`
- `pkg/replay/client.go` -- populate `PatchType` in `Patch()` and
  `subResourceClient.Patch()`

---

## Fix 5: Label Propagation on Status Subresource (Low Priority)

### Why it matters

The real Kubernetes API server does not allow metadata changes through the
status subresource endpoint. `tag.LabelChange(obj)` and
`tracker.propagateLabels(obj)` are called on status subresource writes, which
could cause labels to appear that wouldn't exist on a real cluster.

However, these calls are part of Kamera's internal tracking mechanism (sleeve
labels for deterministic identity), not user-visible labels. This is an
internal bookkeeping concern, not a semantic fidelity issue.

**Recommendation:** No code change needed. Add a comment clarifying why label
propagation is intentional for internal tracking even on status writes.

---

## Implementation Order

```
Fix 1 (resourceVersion conflicts)    -- high value, moderate effort
  |
  v
Fix 3 (SSA field-manager conflicts)  -- medium value, moderate effort
  |                                      (shares infrastructure with Fix 1)
  v
Fix 4 (preserve patch type)          -- low effort, useful for debugging
  |
  v
Fix 2 (status SMP merge)             -- document limitation now,
                                        implement later if needed
Fix 5 (label propagation comment)    -- trivial
```

Fix 1 should come first because it addresses the most impactful false-negative
gap and the infrastructure it introduces (version tracking per resource key
in the validation path) is reused by Fix 3.

## Non-Goals

- **Full Strategic Merge Patch implementation.** Would require loading CRD
  OpenAPI schemas and using `k8s.io/apimachinery/pkg/util/strategicpatch`.
  High complexity, low marginal value given that Kamera records full objects
  rather than patch diffs.
- **Full leaf-level managedFields tracking.** Kubernetes tracks ownership at
  every leaf field. Coarse top-level tracking catches most real bugs at a
  fraction of the complexity.
- **Webhook mutation modeling.** Kamera does not model admission webhooks, so
  patch results cannot include webhook-injected fields. This is orthogonal to
  patch semantics.
