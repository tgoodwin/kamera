# Crossplane Scenario Analysis

This document presents evidence-grounded findings from Kamera's exploration of
Crossplane controller scenarios. Each finding clearly distinguishes **trace
evidence** (observed in dump files) from **code analysis** (inferred from source
code review).

**Crossplane version:** v2.2.0 (upgraded from v2.1.0 on 2026-03-17)
**Kamera branch:** `staleness-fixing`
**Default exploration depth:** 100
**Controllers wired:** CompositionReconciler, CompositionRevisionReconciler,
CompositeReconciler, ClaimReconciler

## Cycling and Analysis Methodology

**All scenarios cycle without converging** due to Finding 2 (unconditional
`Status().Update()` in CompositionRevisionReconciler). Every scenario reaches
max depth with `total/unique > 3`, meaning no scenario achieves a true fixed
point. Despite this, we successfully found 7 bugs using three workarounds:

### 1. Monte Carlo aborted-state comparison

Even though scenarios don't converge, the *aborted* terminal states at max
depth still have comparable per-object hashes. If 50 Monte Carlo trials all
abort at depth 100 but produce different object hashes, that's divergence —
the controllers reached different states depending on ordering. This works
because the cycling is isolated to CompositionRevisionReconciler's status
writes, while the divergent objects (ConfigMap ownership, XR resourceRefs,
Claim lifecycle state) stabilize early and remain stable through the cycling.

### 2. Fixed-depth external event injection

External events (DELETE claim, UPDATE composition) fire at a fixed depth via
`userActionReadyDepths` (e.g., depth 8) rather than waiting for convergence.
This sidesteps the cycling problem — we don't need convergence to inject the
perturbation.

### 3. Divergence on non-cycling objects

The cycling affects CompositionRevision status writes. Findings F6, C2, and C4
diverge on objects the cycling doesn't touch (ConfigMap content/ownership, XR
resourceRefs, Claim finalizer state). The cycling adds noise (more trace steps)
but doesn't mask the divergence signal.

**Limitation:** Convergence-based analysis would give cleaner results and might
reveal additional bugs masked by cycling noise. Fixing F2 in the harness (via
an idempotent-write filter) would unlock this for all existing scenarios.

## Campaign Metrics Summary

All runs at depth 100. "Cycling" = total/unique ratio > 3.

| Scenario | Phase | Unique Nodes | Total Visits | Resource States | Converged | Status |
|----------|-------|-------------|-------------|-----------------|-----------|--------|
| composition-deleted-while-xr-bound | reference | 15 | 101 | 6 | 0 | cycling (6.7x) |
| | rerun | 115 | 392 | 16 | 0 | cycling (3.4x) |
| xr-deleted-with-active-composition | reference | 15 | 101 | 6 | 0 | cycling (6.7x) |
| | rerun | 115 | 392 | 16 | 0 | cycling (3.4x) |
| composition-update-races-xr-fetch | reference | 15 | 201 | 6 | 0 | cycling (13.4x) |
| | rerun | 115 | 692 | 16 | 0 | cycling (6.0x) |
| function-capability-removed | reference | 10 | 101 | 5 | 0 | cycling (10.1x) |
| | rerun | 113 | 935 | 17 | 0 | cycling (8.3x) |
| xr-created-before-revision-validated | reference | 12 | 101 | 6 | 0 | cycling (8.4x) |
| manual-update-policy-composition-switch | reference | 19 | 101 | 9 | 0 | cycling (5.3x) |
| manual-update-policy-composition-switch-stale | combined | 1,895 | 2,028 | 86 | 0 | cycling (1.07x) |
| two-xrs-shared-composition-update | reference | 19 | 31 | 9 | 0 | cycling (1.6x) |

**Dump file locations:** `/tmp/depth100-*` (depth=100 runs), `/tmp/rerun-*` (earlier runs with rerun phases)

---

## Finding 1: Manual policy cross-reference inconsistency

**Severity: P0** — silent data corruption, no error visible to user

**Scenarios:**
- `workflow_crossplane-policy_manual-update-policy-composition-switch.json`
- `workflow_crossplane-policy_manual-update-policy-composition-switch-stale.json`

### Evidence (trace)

**Dump:** `/tmp/depth100-manual-policy-clean/*refere*.jsonl`

Reference trace, path 0, steps 0-4:

| Step | Controller | Effects |
|------|-----------|---------|
| 0 | CompositionReconciler | CREATE CompositionRevision/widget-composition-alpha-4e01990 |
| 1 | CompositionReconciler | CREATE CompositionRevision/widget-composition-beta-c34ead1 |
| 2 | CompositionRevisionReconciler | UPDATE CompositionRevision/widget-composition-alpha-rev-1 |
| 3 | CompositionRevisionReconciler | UPDATE CompositionRevision/widget-composition-beta-rev-1 |
| 4 | CompositeReconciler | 7 effects: UPDATE XWidget, APPLY ConfigMap/xr-config, etc. |

At step 4, the CompositeReconciler runs with `compositionRef` pointing to
`widget-composition-beta` but `compositionRevisionRef` pointing to
`widget-composition-alpha-rev-1`. It produces 7 write effects including
creating a ConfigMap — **it "successfully composed resources" using the wrong
composition's revision.** The XWidget state after step 4 has:

- `compositionRef.name: widget-composition-beta`
- `compositionRevisionRef.name: widget-composition-alpha-rev-1`
- `writeConnectionSecretToRef.namespace: default` (from alpha; beta specifies `crossplane-system`)

No error is raised. The reconciler reports success.

**Staleness adds minimal signal:** The stale variant (`rerun-manual-policy-stale`)
explores 1,895 unique nodes / 86 resource states vs the non-stale reference's 19/9.
The Manual policy path uses exact-name `Get` (not `List`), so stale reads don't
change which revision is fetched.

### Mechanism (code analysis)

`APIRevisionFetcher.Fetch` at `internal/controller/apiextensions/composite/api.go:161-167`:

```go
if current != nil && pol != nil && *pol == xpv1.UpdateManual {
    rev := &v1.CompositionRevision{}
    err := f.client.Get(ctx, types.NamespacedName{Name: current.Name}, rev)
    return rev, errors.Wrap(err, errGetCompositionRevision)
}
```

The Manual policy code path does a bare `Get(current revision)` with **zero
validation** that the revision belongs to the composition referenced by
`compositionRef`. The Automatic path (lines 170-196) properly filters revisions
by `crossplane.io/composition-name` label.

### Unverified hypotheses

None — this bug is fully demonstrated in the trace. No race conditions or
staleness required.

---

## Finding 2: Unconditional `Status().Update()` causes infinite reconcile cycling

**Severity: P3** — efficiency/liveness issue, masked by rate limiting in production

**Scenarios:** All 8 scenarios exhibit this pattern.

### Evidence (trace)

**Cycling signal across all scenarios:** Every reference run has
`total visits / unique nodes > 3`, with unique nodes plateauing as depth
increases. Example from composition-update-races-xr-fetch:

```
unique node visits:  15
total node visits:   201
ratio:               13.4x
```

15 unique states visited 201 times at depth 200 — pure cycling.

**Controller sequence in cycle** (composition-deleted, steps 6-15):
```
CompositeReconciler → CompositionReconciler → CompositionRevisionReconciler
→ CompositionRevisionReconciler → CompositeReconciler → CompositionReconciler
→ CompositionRevisionReconciler → CompositionRevisionReconciler → ...
```

The CompositionRevisionReconciler runs repeatedly, each time producing an
UPDATE effect on a CompositionRevision. Each UPDATE triggers the
CompositionReconciler (via ownerReference fanout) and the cycle continues.

### Mechanism (code analysis)

`revision/reconciler.go:164,171`: The CompositionRevisionReconciler calls
`r.client.Status().Update(ctx, rev)` on every reconcile pass regardless of
whether status actually changed. `SetConditions` correctly skips in-memory
replacement when conditions are equal, but the unconditional `Status().Update()`
still produces an API write → watch event → re-enqueue.

A second instance exists in `composite/reconciler.go:574` on the deletion path
(observed in `xr-deleted-with-active-composition`).

### Unverified hypotheses

- *Inferred from code:* Adding a `DeepEqual` check before `Status().Update()`
  would break the cycle and allow scenarios to converge. This has not been tested.

---

## Finding 3: Orphaned compositionRef with no recovery path

**Severity: P1** — permanent failure requiring manual intervention

**Scenario:** `workflow_crossplane-deletion_composition-deleted-while-xr-bound.json`

### Original run (no evidence — DELETE never fired)

The original workflow had no `userActionReadyDepths` configured. The DELETE
user action was scheduled to fire at convergence, but the reference run cycles
infinitely (Finding 2), so convergence was never reached and the deletion never
occurred.

**Dump:** `/tmp/depth100-composition-deleted/*refere*.jsonl`

- All 101 steps are `frameType: "explore"` — no user action frame exists
- No `DELETE` OpType appears in any effect across the entire trace
- The Composition remains present in every explored state
- Effect OpType counts: APPLY=2619, CREATE=68, UPDATE=7051, DELETE=0

### Re-run: Hypothesis 1 (2026-03-12) — DELETE forced at depth 0

**Hypothesis:** Adding `"userActionReadyDepths": {"0": 0}` forces the DELETE
at depth 0 (before convergence), exercising the orphaned-compositionRef scenario.

**Variant file:** `workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json`

**Dump:** `/tmp/finding3-hypothesis1/crossplane_deletion_composition_deleted_while_xr_bound_hypothesi_0.jsonl`

**Campaign metrics:**

| Metric | Value |
|--------|-------|
| Unique node visits | 44 |
| Total node visits | 324 |
| Cycling ratio | 7.4x |
| Unique resource states | 8 |
| Converged states | 0 |
| Aborted states | 3 |
| Max-depth aborted states | 3 |

**Effect OpType counts:** APPLY=192, DELETE=64, REMOVE=64, UPDATE=1343

The DELETE user action now fires. 64 DELETE + 64 REMOVE effects confirm the
Composition deletion is exercised across multiple paths.

### Evidence (trace-grounded)

**3 distinct aborted states with 2 differing objects and 2 identical:**

```
XWidget/default/example:
  State aborted-3pu4puz1: 93d750b   (states 0, 1)
  State aborted-zwbbv3wq: 9dc61c9   (state 2)

ConfigMap/default/xr-config:
  State aborted-3pu4puz1: 709d71b   (states 0, 1)
  State aborted-zwbbv3wq: (missing) (state 2)
```

**State 2 — CleanupReconciler-first ordering (65 paths):**

The ordering where CleanupReconciler runs before CompositeReconciler directly
demonstrates the orphaned-compositionRef bug:

| Step | Controller | Effects | Error |
|------|-----------|---------|-------|
| 0 | External User | DELETE Composition | — |
| 1 | CleanupReconciler | REMOVE Composition | — |
| 2 | CompositeReconciler | (none) | `cannot fetch Composition: cannot get Composition: Composition.apiextensions.crossplane.io "widget-composition-ephemeral" not found` |
| 3 | CompositionReconciler | (none) | — |
| 4 | CompositionRevisionReconciler | UPDATE revision | — |
| 5 | CompositeReconciler | (none) | same "not found" error |
| ... | (cycling) | | same "not found" error repeats every CompositeReconciler step |

Every CompositeReconciler invocation in state 2 produces the identical error.
The `compositionRef` still points to `widget-composition-ephemeral`, which no
longer exists, and the reconciler has no code path to clear it. This is a
permanent error loop with no self-recovery.

**States 0, 1 — CompositeReconciler-first ordering (64 + 127 paths):**

When the CompositeReconciler runs before CleanupReconciler, it initially
**succeeds** (the Composition still exists with a deletionTimestamp):

| Step | Controller | Effects | Error |
|------|-----------|---------|-------|
| 0 | External User | DELETE Composition | — |
| 1 | CompositeReconciler | 7 effects (UPDATE XWidget x4, APPLY ConfigMap, APPLY XWidget x2) | — |
| 2 | CompositionReconciler | (none) | — |
| 3 | CompositionRevisionReconciler | UPDATE revision | — |
| 4 | CleanupReconciler | REMOVE Composition | — |
| 5 | CompositeReconciler | (none) | `cannot select Composition: no compatible Compositions found` |

After step 4 removes the Composition, all subsequent CompositeReconciler
invocations fail with "no compatible Compositions found". This different error
indicates the `compositionRef` was cleared (possibly by the successful
composition at step 1 writing the XR), forcing `SelectComposition` to re-run
and List Compositions — finding none.

**Ordering-dependent divergence confirmed:** The two ordering families produce
different error messages, different XWidget states, and different resource
inventories (ConfigMap present vs missing).

### Mechanism (code analysis)

The two error paths correspond to different code locations:

1. **State 2 path** (`errFetchComp`): `api.go:173-176` — `Fetch` calls
   `client.Get(compositionRef)`, gets `NotFound`, wraps as `errGetComposition`.
   The `compositionRef` remains set. The reconciler sets `ReconcileError` and
   returns. No code path clears `compositionRef`.

2. **States 0/1 path** (`errSelectComp`): After the successful composition at
   step 1 writes the XR, a subsequent reconcile finds the Composition gone.
   The `compositionRef` was cleared during the successful composition, so
   `SelectComposition` (`api.go:250-289`) runs, Lists all Compositions, finds
   zero matches.

In both cases, `api.go:251-252` contains the acknowledgment:
`"need to block the deletion of composition via finalizer once it's selected"`

### Unverified hypotheses

- *Inferred from code:* Clearing `compositionRef` on `NotFound` would allow
  re-selection of an alternative Composition (if one exists). Not tested.
- *Inferred from code:* Adding a finalizer on the Composition when selected
  (as the TODO suggests) would prevent deletion while the XR is bound. Not tested.
- The cycling (Finding 2) prevents convergence analysis; with the cycling fix,
  the aborted states might converge to distinct steady states that would make
  the divergence even clearer.

### Production impact

Both error paths result in a **permanent reconcile error loop** for the XR.
In production:
- controller-runtime's exponential backoff dampens the error rate but never
  resolves it
- The XR remains in a `ReconcileError` state indefinitely
- User-visible symptom: `kubectl describe` shows persistent `ReconcileError`
  condition with "not found" or "no compatible Compositions" message
- Manual intervention required: user must either re-create the deleted Composition
  or manually clear/update the XR's `compositionRef` and `compositionRevisionRef`
- Blast radius: affects every XR bound to the deleted Composition

---

## Observation: CompositionRevision creation vs validation ordering

**Not a bug** — all orderings converge to the same final state. Transient error
in some orderings is self-resolving via retry. Retained as an observation because
it demonstrates Kamera's ability to distinguish ordering-dependent bugs (divergent
outcomes) from ordering-dependent noise (transient errors that self-correct).

**Scenarios:**
- `workflow_crossplane-staleness_composition-update-races-xr-fetch.json` (primary)
- `workflow_crossplane-staleness_xr-created-before-revision-validated.json` (bootstrap variant)
- `workflow_crossplane-concurrency_two-xrs-shared-composition-update.json` (multi-XR variant)

### Evidence (trace)

**Dump:** `/tmp/depth100-composition-update-races/*refere*.jsonl`

Reference trace, path 0:

| Step | Controller | Effects |
|------|-----------|---------|
| 0 | CompositionReconciler | CREATE CompositionRevision/widget-composition-4e01990 |
| 1 | CompositionRevisionReconciler | UPDATE CompositionRevision/widget-composition-rev-1 |
| 2 | CompositeReconciler | (no effects) |

At step 0, a new revision (rev-2) is created. At step 1, the
CompositionRevisionReconciler validates the **old** revision (`widget-composition-rev-1`),
not the new one (`widget-composition-4e01990`). At step 2, the CompositeReconciler
runs, selects the unvalidated rev-2, and produces no write effects — an error
case.

**The error ordering is observable.** The "wrong" ordering
(CompositionReconciler → CompositeReconciler before CompositionRevisionReconciler
validates rev-2) is directly demonstrated in the trace.

**Rerun (115 unique nodes, 692 total):** All 3 terminal states are max-depth
aborts. No path converges. The cycling pattern prevents reaching a state where
rev-2 is validated and then used successfully.

### Mechanism (code analysis)

`APIRevisionFetcher.Fetch` (api.go:170-196) selects the latest revision by
revision number via `LatestRevision()` regardless of `ValidPipeline` status.
The pipeline check happens AFTER selection at `reconciler.go:631-646`.

### Why this is not a bug

Re-investigation (2026-03-12) with all 3 controllers in `permuteControllers`
confirmed that **both orderings converge to the same final state** (0 differing
objects across 3 terminal states). The error ordering (CompositeReconciler
selects unvalidated rev-2) is transient: the same path shows CompositeReconciler
succeeding at step 6 after CompositionRevisionReconciler validates rev-2.

This is the expected Kubernetes controller pattern — transient errors during
convergence are normal and handled by retry/backoff. A genuine ordering-dependent
bug requires divergent converged states (as in Findings 1, 3, and 5).

### Re-run: Finding 4 verification (2026-03-12)

**What changed:** Two hypothesis variants were created to address configuration
gaps in the original scenario:

1. The original `permuteControllers` excluded `CompositionRevisionReconciler`,
   preventing exploration of orderings where validation precedes selection.
2. The user action (UPDATE Composition) never fired because there was no
   `userActionReadyDepths` and the reference run never converged.

**Variant files:**
- Hypothesis 1: `workflow_crossplane-staleness_composition-update-races-xr-fetch-hypothesis-1.json`
  — all 3 controllers permuted, `userActionReadyDepths: {"0": 0}`, no staleness
- Hypothesis 2: `workflow_crossplane-staleness_composition-update-races-xr-fetch-hypothesis-2.json`
  — all 3 controllers permuted, no `userActionReadyDepths`, no staleness

**Campaign metrics comparison:**

| Variant | Unique Nodes | Total Visits | Resource States | Converged | Aborted | Cycling |
|---------|-------------|-------------|-----------------|-----------|---------|---------|
| Original reference | 15 | 201 | 6 | 0 | 1 | 13.4x |
| Original rerun | 115 | 692 | 16 | 0 | 3 | 6.0x |
| Hypothesis 1 (user action + 3 controllers) | 123 | 698 | 17 | 0 | 3 | 5.7x |
| Hypothesis 2 (3 controllers, no user action) | 115 | 692 | 16 | 0 | 3 | 6.0x |

**Dump locations:**
- `/tmp/finding4-hypothesis1/` (hypothesis 1)
- `/tmp/finding4-hypothesis2/` (hypothesis 2)

#### Key finding: BOTH orderings confirmed in trace

The hypothesis 1 run (3 controllers permuted, user action at depth 0) produces
3 terminal states with 0 differing objects — all orderings converge to the same
final state. The diff tool reports 6 identical objects across all 3 states.

**Two distinct initial orderings explored (1,019 total paths):**

| Ordering (steps 1-3 after user action) | Paths | Outcome |
|----------------------------------------|-------|---------|
| CompositeReconciler -> CompositionReconciler -> CompositionRevisionReconciler | 680 | SUCCESS at step 1 (uses existing validated rev-1) |
| CompositionReconciler -> CompositeReconciler -> CompositionRevisionReconciler | 339 | ERROR at step 2, then SUCCESS at step 6 |

**Error ordering (trace-grounded, hypothesis 1, state 1, path 0):**

| Step | Controller | Effects | Outcome |
|------|-----------|---------|---------|
| 0 | External User | UPDATE Composition | `writeConnectionSecretsToNamespace: crossplane-system` |
| 1 | CompositionReconciler | CREATE CompositionRevision/widget-composition-c34ead1 | Creates unvalidated rev-2 |
| 2 | CompositeReconciler | (none) | ERROR: "selected CompositionRevision widget-composition-c34ead1 does not have a valid function pipeline: pipeline status unknown" |
| 3 | CompositionRevisionReconciler | UPDATE widget-composition-rev-1 | Validates rev-1 (not rev-2) |
| 4-5 | CompositionReconciler, CompositionRevisionReconciler | UPDATE widget-composition-c34ead1 | Validates rev-2 |
| 6 | CompositeReconciler | 8 effects | SUCCESS — rev-2 now validated |

**Success ordering (trace-grounded, hypothesis 1, state 0, path 0):**

| Step | Controller | Effects | Outcome |
|------|-----------|---------|---------|
| 0 | External User | UPDATE Composition | `writeConnectionSecretsToNamespace: crossplane-system` |
| 1 | CompositeReconciler | 7 effects | SUCCESS — uses existing validated rev-1 |
| 2 | CompositionReconciler | CREATE CompositionRevision/widget-composition-c34ead1 | Creates rev-2 |
| 3 | CompositionRevisionReconciler | UPDATE widget-composition-rev-1 | Validates rev-1 |
| 5-6 | CompositionRevisionReconciler, CompositeReconciler | 9 effects | Validates rev-2, then CompositeReconciler switches to it |

**The ordering `CompositionReconciler -> CompositionRevisionReconciler -> CompositeReconciler`
(where rev-2 is validated BEFORE CompositeReconciler selects it) was NOT explored
as an initial ordering.** This is because CompositionRevisionReconciler's first
invocation always validates rev-1 (the existing revision), not rev-2 (newly
created). Rev-2 is validated on CompositionRevisionReconciler's second invocation.
The "perfect" success ordering requires two CompositionRevisionReconciler runs
before CompositeReconciler, which does occur later in the trace (step 6 succeeds
in all paths).

#### User action analysis

Hypothesis 2 (no `userActionReadyDepths`) produces identical metrics to the
original rerun (115 unique nodes, 692 total, 3 states, 0 differing objects).
The user action UPDATE never fires in hypothesis 2 because the run cycles without
converging. However, this does NOT matter for this finding — the race occurs from
the initial environment because CompositionReconciler creates rev-2 on its first
reconcile of the existing Composition.

Hypothesis 1 (user action at depth 0) produces slightly more exploration (123
unique nodes vs 115) because the UPDATE changes the Composition spec (different
`writeConnectionSecretsToNamespace`), creating a different revision hash
(`c34ead1` vs `4e01990`). The race manifests identically.

#### Staleness analysis

Both hypothesis variants remove staleness entirely. The original scenario
configured `staleReads` for CompositeReconciler on Composition and
CompositionRevision with lookback 1 and 2 respectively. Removing staleness
does not reduce the number of distinct orderings explored. This confirms the
bug is **purely ordering-dependent** — staleness adds exploration breadth
(more paths through the DAG) but no new outcome categories, consistent with
Finding 5's conclusion.

#### Previous findings status

| Finding | Status |
|---------|--------|
| Error ordering exists (CompositeReconciler selects unvalidated rev-2) | **CONFIRMED** — 339 paths in hypothesis 1 |
| Success ordering exists (CompositeReconciler uses validated rev-1, then switches to validated rev-2) | **CONFIRMED** — 680 paths in hypothesis 1. Previously unverified. |
| Both orderings converge to the same final state | **CONFIRMED** — 0 differing objects across 3 terminal states |
| Error is transient (self-resolving) | **CONFIRMED** — error at step 2, success at step 6 in the same path |
| Staleness contributes to the bug | **REFUTED** — removing staleness produces identical outcome categories |
| CompositionRevisionReconciler needed in permuteControllers | **CONFIRMED** — adding it was essential for exploring the success ordering. Without it, only the error ordering was visible in the reference run. |

---

## Finding 5: Stale ValidPipeline enables composition with invalidated functions

**Severity: P2** — data integrity risk, eventually self-correcting

**Scenario:** `workflow_crossplane-staleness_function-capability-removed.json`

### Evidence (trace)

**Dump:** `/tmp/depth100-function-capability/*rerun*.jsonl`

The rerun phase explores 113 unique nodes / 17 resource states across 9 terminal
branches (all max-depth aborted).

**Partial evidence from earlier run (depth 10, `/tmp/rerun-function-capability/`):**
The earlier analysis reported that in 4 of 23 terminal states, the
CompositeReconciler "successfully composed resources" after the capability was
removed. The step pattern cited was:

```
Step 0: External User removes composition capability
Step 1: CompositeReconciler reads XWidget, selects revision (ValidPipeline=True)
Step 2: CompositeReconciler composes resources (creates ConfigMap, updates XWidget)
Step 3+: CompositionRevisionReconciler detects MissingCapabilities
```

### Mechanism (code analysis)

The CompositeReconciler checks `rev.GetCondition(v1.TypeValidPipeline)` at
`reconciler.go:631` — a cached condition set by the CompositionRevisionReconciler.
It does NOT independently verify function capabilities. When a FunctionRevision's
capabilities change, there is a window where the stale `ValidPipeline=True`
condition allows composition with the invalidated function.

### Unverified hypotheses

- *Inferred from code:* Adding a generation/hash check to the `ValidPipeline`
  condition or having the CompositeReconciler independently verify capabilities
  would close the window. Not tested.

### Re-run: Finding 5 verification (2026-03-12)

**What changed:** The staleness infrastructure was refactored to support
`stalenessIntervals` (KindSequence-based windows) alongside the legacy
`staleReads`/`staleLookback` format. This re-run tests both formats and verifies
the earlier "4 of 23 states" claim.

**Variant files used:**
- Original: `workflow_crossplane-staleness_function-capability-removed.json` (legacy `staleReads` format)
- Interval-based: `interval_function-capability-removed.json` (`stalenessIntervals` format, staleAt=1 catchUpAt=4)
- Hypothesis-1: `workflow_crossplane-staleness_function-capability-removed-hypothesis-1.json` (corrected window staleAt=4 catchUpAt=8)

**Campaign metrics comparison:**

| Variant | Phase | Unique Nodes | Total Visits | Resource States | Converged | Aborted |
|---------|-------|-------------|-------------|-----------------|-----------|---------|
| Original (previous) | reference | 10 | 101 | 5 | 0 | 1 |
| Original (previous) | rerun | 113 | 935 | 17 | 0 | 9 |
| Legacy staleReads (re-run) | rerun | 113 | 935 | 17 | 0 | 9 |
| Interval staleAt=1,catchUpAt=4 (re-run) | reference | 10 | 101 | 5 | 0 | 1 |
| Interval staleAt=1,catchUpAt=4 (re-run) | rerun | 113 | 935 | 17 | 0 | 9 |

Both the legacy `staleReads` format and the new `stalenessIntervals` format
produce **identical results** (same node counts, state hashes, and terminal
state distribution). The formats are functionally equivalent for this scenario.

**Dump locations:**
- `/tmp/finding5-rerun/` (interval variant)
- `/tmp/finding5-rerun-old-format/` (legacy variant)

#### Key finding: staleness interval is ineffective

The configured staleness interval (`staleAt=1, catchUpAt=4` for
`pkg.crossplane.io/FunctionRevision`) has **no effect** on this scenario. The
initial FunctionRevision KindSequence is 4, which means the staleness window
`[1, 4)` is already expired before the rerun begins. The
CompositionRevisionReconciler always sees the current FunctionRevision state.

The bug demonstrated is purely **ordering-dependent**, not staleness-dependent:
when CompositeReconciler runs before CompositionRevisionReconciler has a chance
to invalidate the pipeline, it successfully composes with the stale
`ValidPipeline=True` condition from the initial environment state.

#### Verification of the "4 of 23 states" claim: PARTIALLY CONFIRMED

The earlier claim was "4 of 23 terminal states showed successful composition
after capability removal." The re-run at depth 100 shows **6 of 9 terminal
states** with successful composition. The discrepancy (4/23 vs 6/9) is due to
different exploration depth (10 vs 100), but the core finding holds: a
significant fraction of orderings allow successful composition after capability
removal.

**3 distinct outcome categories across 9 terminal states (1,074 total paths):**

| Category | States | Paths | XWidget Hash | ConfigMap | Pattern |
|----------|--------|-------|-------------|-----------|---------|
| A (error only) | 0, 1, 2 | 432 | 579a4db | (missing) | CompositionRevisionReconciler before CompositeReconciler |
| B (2 compositions) | 3, 4, 5 | 355 | 5333e65 | 60f2920 | CompositeReconciler runs twice before others |
| C (1 composition) | 6, 7, 8 | 287 | 93d750b | 709d71b | CompositeReconciler runs once before others |

**Category A (error-only, correct behavior):**
```
Step 0: External User removes composition capability (UPDATE FunctionRevision)
Step 1: CompositionRevisionReconciler -> UPDATE CompositionRevision (sets ValidPipeline=False)
Step 2: CompositeReconciler -> (no effects) ERROR: "missing required capabilities: composition"
```

**Category B (2 successful compositions, bug demonstrated):**
```
Step 0: External User removes composition capability
Step 1: CompositeReconciler -> 7 effects (UPDATE XWidget x4, APPLY ConfigMap, APPLY XWidget x2)
        "Successfully composed resources" — reads stale ValidPipeline=True
Step 2: CompositeReconciler -> 9 effects (more writes)
Step 3: CompositionReconciler -> CREATE new CompositionRevision
Step 8+: CompositeReconciler -> ERROR: "missing required capabilities: composition"
```

**Category C (1 successful composition, bug demonstrated):**
```
Step 0: External User removes composition capability
Step 1: CompositeReconciler -> 7 effects (UPDATE XWidget, APPLY ConfigMap, APPLY XWidget)
Step 2: CompositionReconciler -> CREATE new CompositionRevision
Step 6+: CompositeReconciler -> ERROR: "missing required capabilities: composition"
```

**Data integrity impact (trace-grounded):**
- Categories B and C: XWidget status shows `Synced=True`, `Ready=True`,
  `phase=Composed` — the reconciler reports success
- ConfigMap created with `data.message: "hello from kamera"` (from the now-invalid function)
- Category A: XWidget retains original spec, no status conditions, no ConfigMap

#### Hypothesis-1: corrected staleness window (staleAt=4, catchUpAt=8)

**Hypothesis:** Setting the staleness window to cover the actual FunctionRevision
transition (KindSeq 4->5) would keep CompositionRevisionReconciler frozen on the
old FunctionRevision (with capabilities), potentially delaying pipeline
invalidation and widening the composition window.

**Previous result (2026-03-12): BLOCKED** by missing `event.APPLY` handling in
`replayEventSequenceToState` (`noncausal_rollup.go:56`).

**Fix applied:** Added `event.APPLY` to the `CREATE/UPDATE/PATCH` case in both
`noncausal_rollup.go:56` and `causal_rollup.go:115`.

**Re-run result (2026-03-12, post-fix):**

**Dump:** `/tmp/finding5-hypothesis1/`

| Phase | Unique Nodes | Total Visits | Resource States | Converged | Aborted |
|-------|-------------|-------------|-----------------|-----------|---------|
| Reference | 35 | 42 | 13 | 3 | 0 |
| Staleness (staleAt=4, catchUpAt=8) | 113 | 935 | 17 | 0 | 9 (max-depth) |

**Reference run now converges** (0 aborted states, 35 unique / 42 total — low
cycling). 3 distinct converged states with the same hash families as the
staleness run:

| Outcome | Reference States | Staleness States | XWidget | ConfigMap |
|---------|-----------------|-----------------|---------|-----------|
| Error only (correct) | state-2 | aborted-14ammt3o | 579a4db | (missing) |
| 1 composition (bug) | state-0 | aborted-2q7fodp5, aborted-whct6mzg | 93d750b | 709d71b |
| 2 compositions (bug) | state-1 | aborted-1ky7yvk4, aborted-2nucwn44 | 5333e65 | 60f2920 |

**Conclusion: staleness adds exploration breadth but no new outcome families.**
The corrected staleness interval (`staleAt=4, catchUpAt=8, lag=-1`) produces
the same 3 outcome categories as the reference (ordering-only) run. The staleness
run explores 113 unique nodes vs the reference's 35, but maps to identical hash
families. This confirms the bug is **purely ordering-dependent**: the
`ValidPipeline=True` condition from the initial environment state is the stale
data, and no staleness injection on FunctionRevision changes the outcome because
the CompositeReconciler doesn't read FunctionRevision directly — it reads the
CompositionRevision's cached condition.

#### Previous findings status

| Finding | Status |
|---------|--------|
| Composition after capability removal exists | **CONFIRMED** — 6/9 states in depth-100 re-run |
| Ordering-dependent divergence | **CONFIRMED** — 3 distinct outcome categories, confirmed in both reference and staleness runs |
| Staleness perturbation contributes to the bug | **REFUTED** — corrected window [4,8) produces same 3 outcome families as ordering-only; bug is ordering-dependent |
| ValidPipeline=True is stale | **CONFIRMED** — it is stale relative to the FunctionRevision change, but this is an ordering issue (the condition was never updated by CompositionRevisionReconciler), not a staleness-interval issue |
| APPLY panic in noncausal_rollup.go | **FIXED** — added `event.APPLY` to both `noncausal_rollup.go` and `causal_rollup.go` switch statements |

---

## Scenarios with no new bug found

### two-xrs-shared-composition-update

**Scenario:** `workflow_crossplane-concurrency_two-xrs-shared-composition-update.json`

**Hypothesis tested:** Under stale reads, one XR pins to rev-1 while the other
advances to rev-2, causing permanent divergence.

**Result:** Cannot manifest under the current Kamera staleness model. Staleness
is applied per-reconciler-ID, not per-reconcile-invocation. Both XRs always see
the same stale/fresh view of CompositionRevisions within a given branch. The
rerun explores 19 unique nodes / 9 resource states with zero convergence (cycling).

The creation-vs-validation race (Finding 4) is also visible here but produces
no new evidence beyond what composition-update-races-xr-fetch provides.

### xr-created-before-revision-validated

This is a bootstrap variant of Finding 4 (creation vs validation race). The
scenario starts with no CompositionRevision, requiring all three controllers to
complete in sequence. The reference explores 12 unique nodes / 6 resource states,
all cycling. All explored orderings produce errors.

*Inferred from code:* Only the ordering CompositionReconciler →
CompositionRevisionReconciler → CompositeReconciler would succeed. This was
never observed in any trace.

### xr-and-composition-deleted-simultaneously (2026-03-13)

**Scenario:** `workflow_crossplane-concurrent-deletion_xr-and-composition-deleted-simultaneously.json`

**Hypothesis tested:** Two concurrent DELETEs (XR + Composition at the same
time). The XR's deletion finalizer needs the Composition to understand which
composed resources to clean up. If CleanupReconciler removes the Composition
before the XR finalizer runs, the XR may be stuck in Terminating permanently.

**Result:** Hypothesis not confirmed. The CompositeReconciler deletion path
does not fetch or inspect the Composition — it only removes the XR's finalizer
and updates status. The XR deletes cleanly in all controller orderings. No
ordering-dependent divergence was observed.

**Metrics:** Reference: 12 unique / 101 total (8.4x cycling). Exploration
(permuted controllers): 43 unique / 139 total (3.2x cycling). Single terminal
state across all paths, 0 differing objects.

**Do not re-explore:** The XR delete path's independence from the Composition
is architectural (composite/reconciler.go deletion short-circuit). Concurrent
deletion of XR + Composition cannot produce a stuck-Terminating XR via this
mechanism. Further scenario variants along this axis are unlikely to reveal
new bugs unless the delete path is substantially changed.

**Observed side-effect:** After both objects are removed, the orphaned
CompositionRevision (ownerRef pointing to the deleted Composition) cycles
indefinitely due to Finding 2. In production, Kubernetes GC would
cascade-delete the CompositionRevision, ending the cycle. The model does not
simulate GC.

### two-xrs-deleted-simultaneously (2026-03-13)

**Scenario:** `workflow_crossplane-concurrent-deletion_two-xrs-deleted-simultaneously.json`

**Hypothesis tested:** Two concurrent DELETEs of both XRs sharing a
Composition. One XR's cleanup may delete composed resources the other XR's
finalizer still expects, leaving one XR stuck in Terminating.

**Result:** Hypothesis not testable with this scenario design. Both DELETEs
fire at depth 0 via `userActionReadyDepths`, before any composition has run.
The CompositeReconciler delete path (`meta.WasDeleted(xr)=true` on first
reconcile) short-circuits without entering the compose phase, so no composed
resources are ever created. Both XRs delete cleanly in all orderings.

**Metrics:** Reference: 16 unique / 101 total (6.3x cycling). Exploration
(permuted controllers): 302 unique / 499 total (1.65x cycling). 2 terminal
states, 0 differing objects.

**Do not re-explore in this form.** To test the actual hypothesis (shared
composed resource conflict at deletion time), a different scenario is needed:
start with both XRs in a composed/ready state (composed resources already in
environment), then delete both XRs simultaneously. The kamera-stub returns a
static ConfigMap name (`xr-config`) for all XRs, so both XRs' composed
resources would be the same object — a prerequisite for the conflict. However,
this also means only one XR can successfully compose at a time
(`AddControllerReference` would fail for the second XR), making the scenario
degenerate. A stub that generates per-XR resource names would be needed to
test this properly.

### manual-xr-switch-with-old-composition-deleted (2026-03-13)

**Scenario:** `workflow_crossplane-concurrent-deletion_manual-xr-switch-with-old-composition-deleted.json`

**Hypothesis tested:** Combines Finding 1 (Manual policy mismatched refs) with
Finding 3 (deleted Composition orphaning). User simultaneously UPDATEs the XR
to switch compositionRef=beta (leaving compositionRevisionRef=alpha-rev-1) AND
DELETEs widget-composition-alpha. The Manual policy code will Get(alpha-rev-1)
— which belongs to the now-deleted Composition — while compositionRef points to
beta. Different orderings may produce 3+ divergent terminal states.

**Result:** Finding 1 is confirmed operative — the Manual policy fetches
alpha-rev-1 regardless of compositionRef=beta, and alpha-rev-1 persists
(orphaned, no GC in model) indefinitely. However, adding the concurrent DELETE
of alpha does not produce new divergent terminal states. The exploration run
(770 unique nodes / 1,761 total, 12 terminal states) shows **0 differing
objects** across all 12 states — all orderings cycle to the same object content.

The concurrent DELETE confirms the orphaned-revision scenario from Finding 3
is reachable via the Manual policy path, but does not compound into a distinct
new failure mode beyond what Findings 1 and 3 individually document.

**Metrics:** Reference: 20 unique / 101 total (5.0x). Exploration: 770 unique /
1,761 total (2.3x). 12 aborted states, 0 differing objects.

**Do not re-explore:** The interaction of Manual policy + concurrent Composition
deletion produces no outcome category beyond what Findings 1 and 3 cover
individually. In production (with GC), alpha-rev-1 would be cascade-deleted
after alpha is removed, causing the Manual policy `Get(alpha-rev-1)` to return
NotFound — at which point the XR enters Finding 3's permanent error loop.
That behavior is already documented under Finding 3.

### composition-update-with-capability-removal (2026-03-13)

**Scenario:** `workflow_crossplane-concurrent-staleness_composition-update-with-capability-removal.json`

**Hypothesis tested:** Combines Finding 4 (unvalidated rev-2 race) with
Finding 5 (stale ValidPipeline). User simultaneously UPDATEs the Composition
(triggers rev-2 creation) AND removes the composition capability from the
FunctionRevision. Rev-2 is born already-invalid; CompositeReconciler may select
it before CompositionRevisionReconciler marks it ValidPipeline=False — and
since rev-2 was never valid, this is a stricter variant of Finding 5 where the
stale condition was never correct for that revision.

**Result:** The "inherited ValidPipeline=True on rev-2" hypothesis was
**refuted**. New CompositionRevisions are created with zero status conditions
(not inherited from prior revisions). Rev-2 never has ValidPipeline=True;
any CompositeReconciler attempt to use rev-2 correctly errors with "pipeline
status unknown."

The bug that manifests is **Finding 5 operating on rev-1**: before
CompositionRevisionReconciler has processed the capability removal, rev-1
still carries `ValidPipeline=True`. If CompositeReconciler runs first, it
selects rev-1 (rev-2 may not exist yet at step 2) and composes resources using
the stale condition. This is identical to Finding 5's mechanism.

**Metrics:** Reference: 11 unique / 101 total (9.2x). Exploration (permuted
controllers): 129 unique / 1,030 total (8.0x). 10 aborted states, 5 distinct
terminal state hashes.

**State categorization (trace-grounded):**

| State | Terminal ID | Compositions | First controller after user action |
|-------|-------------|-------------|-------------------------------------|
| 0, 1 | aborted-2pgzvfjj | 1 (bug) | CompositeReconciler |
| 2 | aborted-2u7kzlp9 | 0 (correct) | CompositionRevisionReconciler |
| 3 | aborted-3d1891je | 1 (bug) | CompositeReconciler |
| 4, 5 | aborted-3swcrta5 | 0 (correct) | CompositionRevisionReconciler |
| 6 | aborted-3swcrta5 | 0 (correct) | CompositionReconciler |
| 7, 8, 9 | aborted-4e6yru0h | 2 (bug) | CompositeReconciler |

6 of 10 states show buggy composition. The first controller to run after the
user action fully determines the outcome: CompositeReconciler-first → bug;
CompositionRevisionReconciler-first or CompositionReconciler-first → correct.

**Do not re-explore as a new finding.** This scenario confirms Finding 5
applies when a Composition UPDATE coincides with a capability removal, but
produces no new outcome categories. Any further investigation of the
ValidPipeline race should be tracked under Finding 5.

---

## Function Failure Exploration (2026-03-17)

**Harness change:** `stubFunctionRunner` now supports multiple function behaviors
keyed by function name: `kamera-stub` (default), `kamera-stub-fatal`
(SEVERITY_FATAL), `kamera-stub-different-resources` (Secret instead of ConfigMap),
`kamera-stub-partial` (partial readiness). Scenarios switch function behavior by
updating the Composition's pipeline to reference a different function name.

**Crossplane upgraded from v2.1.0 to v2.2.0** to resolve controller-runtime
v0.23.0 compatibility (required by kamera root module).

### Finding 6: Composition function switch leaves orphaned resources (DIVERGENCE)

**Severity: P2** — orphaned resources + ordering-dependent XR status

**Scenario:** `workflow_crossplane-function-failure_composition-switches-to-fatal.json`

**Hypothesis:** After a successful composition (ConfigMap created), updating the
Composition to reference a fatal function leaves the ConfigMap orphaned because
SEVERITY_FATAL returns before composed resource GC runs.

**Result: 7 distinct terminal states across 49 Monte Carlo trials.**

| Category | XWidget hash | ConfigMap | Ready | Trials | Fraction |
|----------|-------------|-----------|-------|--------|----------|
| A (composed then failed) | 88115b88 | present (orphan) | True | 17 | 35% |
| B (composed then failed, different status) | 47f80aa2 | present (orphan) | True | 17 | 35% |
| C (composed then failed, variant) | 841a3d02 | present (orphan) | True | 6 | 12% |
| D (never composed) | 58034af3 | missing | — | 3 | 6% |
| E (never composed, variant) | dd2a95e9 | missing | — | 3 | 6% |
| F (composed then failed, variant) | 45dbfa98 | present (orphan) | True | 2 | 4% |
| G (never composed, variant) | e34d4646 | missing | — | 1 | 2% |

**Key divergence:** ConfigMap present in 42/49 trials (86%), missing in 7/49 (14%).
XWidget has 6 distinct hashes across 7 states.

**ConfigMap-present states (86%):**
```
compositionRevisionRef: widget-composition-0905a1f (fatal revision)
resourceRefs: [{ConfigMap/xr-config}]
Synced=False reason=ReconcileError
Ready=True reason=Available
```
The initial composition succeeded (ConfigMap created, Ready=True), then the
function switched to fatal. SEVERITY_FATAL returns before GC, so the ConfigMap
persists as an orphan. The XR shows `Ready=True` (stale from prior success) but
`Synced=False` (current reconcile failed).

**ConfigMap-missing states (14%):**
```
compositionRevisionRef: widget-composition-0905a1f (fatal revision)
resourceRefs: None
Synced=False reason=ReconcileError
(no Ready condition)
```
The Composition was updated before CompositeReconciler ran, so the function was
already fatal on the first compose attempt. No resources were ever created.

**Mechanism:** The CompositeReconciler's error path (`composition_functions.go:404`)
returns immediately on SEVERITY_FATAL without calling
`GarbageCollectComposedResources`. Previously-composed resources referenced in
`spec.resourceRefs` remain in the API server indefinitely. The XR's `Ready=True`
condition from the prior successful composition is never cleared because the
error path only sets `Synced=False`.

**Production impact:** Users see `Ready=True` + `Synced=False` — a confusing
mixed signal. The orphaned ConfigMap persists until the function is fixed and a
successful reconcile runs GC. If the function is permanently broken, the ConfigMap
leaks forever.

### F7: Resource switch — GC works correctly (CLEAN)

**Scenario:** `workflow_crossplane-function-failure_composition-switches-resources.json`

**Hypothesis:** Updating the Composition to reference a function that returns
Secret instead of ConfigMap may leave the old ConfigMap orphaned if GC doesn't
handle cross-resource-type transitions.

**Result: 1 terminal state across 49 trials. No divergence.**

All trials end with Secret present and ConfigMap deleted. GC correctly identifies
the ConfigMap as no longer in the `desired` set and removes it. Resource state
exploration varied (9-13 states) confirming different orderings were tested.

### F8: Function flap recovery (CLEAN)

**Scenario:** `workflow_crossplane-function-failure_function-flap-fatal-recovery.json`

**Hypothesis:** Function flaps normal → fatal → normal. After recovery, the final
state may differ from a never-failed scenario depending on ordering around the
fault window.

**Result: 1 terminal state across 49 trials. No divergence.**

All trials recover to identical state with ConfigMap present. The transient fatal
window does not produce permanent divergence. Resource state exploration varied
(12-16 states) confirming different orderings were tested.

---

## Claim Lifecycle Exploration (2026-03-17)

**Harness change:** ClaimReconciler wired into harness with deterministic name
generator (`my-widget-xr` suffix). No-op ConnectionPropagator (secret
propagation not tested). Watches XR changes via `claimRef` field mapper.

### C1: Claim-XR Binding Race (CLEAN)

**Scenario:** `workflow_crossplane-claim_claim-xr-binding-race.json`

**Hypothesis:** ClaimReconciler and CompositeReconciler both write to the XR
object. Different orderings of the 4 controllers (Claim, Composite, Composition,
CompositionRevision) may produce different XR states.

**Result: 1 terminal state across 98 trials. No divergence.**

All trials converge to identical state: Claim bound to XR, XR composed with
ConfigMap. The claim binding + composition lifecycle is ordering-robust when
4 controllers are permuted.

### C2: Claim Deletion During Composition — DIVERGENCE FOUND

**Severity: P2** — orphaned XR + composed resources after claim deletion

**Scenario:** `workflow_crossplane-claim_claim-deleted-during-composition.json`

**Hypothesis:** Deleting a Claim after composition has started may leave the XR
and composed resources orphaned, depending on whether the ClaimReconciler's
finalizer runs before or after the Claim is removed.

**Result: 2 distinct terminal states across 98 Monte Carlo trials.**

| Category | XR | ConfigMap | Trials | Fraction |
|----------|----|-----------|--------|----------|
| Orphaned (XR survives) | present | present | 96 | 98% |
| Full cleanup | missing | missing | 2 | 2% |

In both states, the WidgetClaim is successfully deleted. The divergence is
whether the XR and its composed resources survive:

- **98% of orderings**: XR and ConfigMap persist as orphans. The Claim is deleted
  before the ClaimReconciler can add its finalizer and process the deletion
  cascade. Without the finalizer, the XR is never deleted.
- **2% of orderings**: The ClaimReconciler runs before the Claim is fully removed,
  adds the finalizer, processes the deletion (deletes XR), and the CompositeReconciler
  cleans up composed resources.

**Production impact:** In production, controller-runtime's finalizer handling is
slightly different (the DELETE sets `deletionTimestamp` but doesn't remove the
object until all finalizers clear). However, this finding highlights a genuine
ordering sensitivity in the claim deletion lifecycle — if the ClaimReconciler
doesn't reconcile the Claim between the user DELETE and the finalizer being added,
the XR can be orphaned.

### C3: ClaimReconciler Crash Mid-Sync (CLEAN)

**Scenario:** `workflow_crossplane-claim_claim-crash-mid-sync.json`

**Hypothesis:** Crashing the ClaimReconciler after its first write effect (XR
created but Claim's `resourceRef` not yet updated) could cause the Claim to
create a second XR on the next reconcile, orphaning the first.

**Result: 1 terminal state across 98 trials. No divergence.**

The deterministic name generator means the re-created XR gets the same name
(`my-widget-xr`), so the name collision prevents orphan creation. The crash
recovery is clean — the ClaimReconciler re-reconciles, finds the existing XR
by name, and binds to it.

### C4: Two Claims Shared Composition — Composed Resource Ownership Race

**Scenario:** `workflow_crossplane-claim_two-claims-shared-composition.json`

**Result: 2 terminal states across 98 trials.**

| Category | ConfigMap owner | Trials | Fraction |
|----------|----------------|--------|----------|
| A | `widget-beta-xr` | 54 | 55% |
| B | `widget-alpha-xr` | 44 | 45% |

Both Claims, both XRs, and all other objects are identical across all trials.
The divergence is solely in the ConfigMap's `ownerReferences` and
`crossplane.io/composite` label — whichever XR's CompositeReconciler writes
last wins ownership.

**Severity: P2** — silent ownership theft leads to data loss on XR deletion.

The CompositeReconciler silently overwrites the ConfigMap's `ownerReferences`
without detecting that it's already owned by a different XR. If the "winning"
XR is deleted, Kubernetes GC cascade-deletes the ConfigMap, and the "losing"
XR's `resourceRefs` now point to a missing resource. The reconciler should
detect cross-XR ownership conflicts and error rather than silently stealing.

### C5: Manual Policy Composition Switch via Claim (CLEAN)

**Scenario:** `workflow_crossplane-claim_manual-policy-composition-switch.json`

**Hypothesis:** Claim with `compositionUpdatePolicy: Manual` switches
`compositionRef` from alpha to beta while keeping `compositionRevisionRef`
pointing to alpha-rev-1. The F1 bug (wrong revision fetched) should compound
through the Claim syncer.

**Result: 1 terminal state across 98 trials. No divergence.**

The Manual policy path converges despite the mismatched refs. The XR hash
matches the baseline C1 scenario, suggesting the Claim syncer's field
propagation normalizes the state regardless of ordering.

---

## Unexplored Areas (2026-03-17)

### Priority 2: Wire ClaimReconciler

Claims are the user-facing interface to XRs. The Claim → XR binding lifecycle
adds a new interaction surface with the already-wired CompositeReconciler:

- **Claim-XR ownership race**: ClaimReconciler creates XR, CompositeReconciler
  starts composing it, ClaimReconciler updates binding status — all writing to
  the same XR object
- **Claim deletion while XR is mid-compose**: ClaimReconciler removes finalizer
  vs CompositeReconciler mid-compose
- **Manual policy + Claim**: Does the F1 bug (wrong revision fetched) compound
  when claim-level compositionRef disagrees with XR-level?

### Priority 3: Multiple Compositions competing

Current scenarios use 1-2 Compositions. Explore:

- **3+ Compositions with overlapping labels**: Is `SelectComposition` deterministic
  when multiple Compositions have equal scores?
- **Composition created while another is being deleted**: Race between
  CleanupReconciler removing old and CompositionReconciler creating revision for new.

### Priority 4: Wire UsageReconciler

Usage protection prevents deletion of in-use resources:

- **Delete XR while Usage protection active**: UsageReconciler blocks deletion,
  CompositeReconciler tries to clean up — ordering-dependent outcome?
- **Usage removed concurrent with XR deletion**: Race between protection removal
  and delete cascade.

---

## Artifact Index

| Scenario | Workflow JSON | Old Report (stale) | Dump Location |
|----------|--------------|-------------------|---------------|
| composition-deleted-while-xr-bound | [workflow JSON](../scenarios/workflow_crossplane-deletion_composition-deleted-while-xr-bound.json) | [old report](../scenarios/composition-deleted-while-xr-bound.md) | `/tmp/depth100-composition-deleted/` |
| composition-deleted-while-xr-bound (hypothesis-1) | [hypothesis JSON](../scenarios/workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json) | — | `/tmp/finding3-hypothesis1/` |
| xr-deleted-with-active-composition | [workflow JSON](../scenarios/workflow_crossplane-deletion_xr-deleted-with-active-composition.json) | [old report](../scenarios/xr-deleted-with-active-composition.md) | `/tmp/depth100-xr-deleted/` |
| composition-update-races-xr-fetch | [workflow JSON](../scenarios/workflow_crossplane-staleness_composition-update-races-xr-fetch.json) | [old report](../scenarios/composition-update-races-xr-fetch.md) | `/tmp/depth100-composition-update-races/` |
| composition-update-races-xr-fetch (hypothesis-1) | [hypothesis JSON](../scenarios/workflow_crossplane-staleness_composition-update-races-xr-fetch-hypothesis-1.json) | — | `/tmp/finding4-hypothesis1/` |
| composition-update-races-xr-fetch (hypothesis-2) | [hypothesis JSON](../scenarios/workflow_crossplane-staleness_composition-update-races-xr-fetch-hypothesis-2.json) | — | `/tmp/finding4-hypothesis2/` |
| function-capability-removed | [workflow JSON](../scenarios/workflow_crossplane-staleness_function-capability-removed.json) | [old report](../scenarios/function-capability-removed.md) | `/tmp/depth100-function-capability/` |
| function-capability-removed (interval) | [interval JSON](../scenarios/interval_function-capability-removed.json) | — | `/tmp/finding5-rerun/` |
| function-capability-removed (hypothesis-1) | [hypothesis JSON](../scenarios/workflow_crossplane-staleness_function-capability-removed-hypothesis-1.json) | — | (panics — see Finding 5 re-run) |
| xr-created-before-revision-validated | [workflow JSON](../scenarios/workflow_crossplane-staleness_xr-created-before-revision-validated.json) | [old report](../scenarios/xr-created-before-revision-validated.md) | `/tmp/depth100-xr-before-validation/` |
| manual-update-policy-composition-switch | [workflow JSON](../scenarios/workflow_crossplane-policy_manual-update-policy-composition-switch.json) | [old report](../scenarios/manual-update-policy-composition-switch.md) | `/tmp/depth100-manual-policy-clean/` |
| manual-update-policy-composition-switch-stale | [workflow JSON](../scenarios/workflow_crossplane-policy_manual-update-policy-composition-switch-stale.json) | [old report](../scenarios/manual-update-policy-composition-switch-stale.md) | `/tmp/rerun-manual-policy-stale/` |
| two-xrs-shared-composition-update | [workflow JSON](../scenarios/workflow_crossplane-concurrency_two-xrs-shared-composition-update.json) | [old report](../scenarios/two-xrs-shared-composition-update.md) | `/tmp/rerun-two-xrs-shared/` |
| xr-and-composition-deleted-simultaneously | [workflow JSON](../scenarios/workflow_crossplane-concurrent-deletion_xr-and-composition-deleted-simultaneously.json) | — | `/tmp/xr-comp-simultaneous-ref/`, `/tmp/xr-comp-simultaneous-explore/` |
| two-xrs-deleted-simultaneously | [workflow JSON](../scenarios/workflow_crossplane-concurrent-deletion_two-xrs-deleted-simultaneously.json) | — | `/tmp/two-xrs-simultaneous-ref/`, `/tmp/two-xrs-simultaneous-explore/` |
| manual-xr-switch-with-old-composition-deleted | [workflow JSON](../scenarios/workflow_crossplane-concurrent-deletion_manual-xr-switch-with-old-composition-deleted.json) | — | `/tmp/manual-xr-switch-ref/`, `/tmp/manual-xr-switch-explore/` |
| composition-update-with-capability-removal | [workflow JSON](../scenarios/workflow_crossplane-concurrent-staleness_composition-update-with-capability-removal.json) | — | `/tmp/comp-update-cap-removal-ref/`, `/tmp/comp-update-cap-removal-explore/` |
