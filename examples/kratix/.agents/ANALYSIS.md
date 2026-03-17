# Kratix Scenario Analysis

## Controller Architecture

Controllers registered in the kamera harness:

| ID | Trigger | Role |
|----|---------|------|
| `PromiseController` | Promise | Manages Promise lifecycle, creates CRDs + PromiseRevisions |
| `PromiseRevisionController` | PromiseRevision | Tracks latest revision, unsets previous |
| `DynamicResourceRequestController/<name>` | Custom CRD (per Promise) | Processes resource requests, creates Work + ResourceBinding |
| `WorkController` | Work | Schedules work to Destinations via WorkPlacements |
| `WorkPlacementController` | WorkPlacement | Writes workloads to state stores (S3/Git) |

### Controller Subsystems

**Promise lifecycle** (PromiseController → PromiseRevisionController → DynamicResourceRequestController):
- Well-designed for ordering and crash resilience (K2-K4 all negative)
- Single quality issue: redundant timestamp-driven status writes (K1)

**Work scheduling** (WorkController → WorkPlacementController):
- Ordering-dependent status divergence (K5, K8-K10 same root cause)
- Fault injection divergence (K6)
- External event divergence (K7)
- Scales with Work count (K11)

---

## Confirmed Bugs

### K1: Redundant status writes due to fresh timestamps (P3)

**Evidence:** Existing trace at `res.json/kratix_default_easyapp_create_then_update_image_0.jsonl`
**Severity: P3** — Convergence quality issue, not a correctness bug.

The `DynamicResourceRequestController` produces 7 consecutive UPDATE effects on the same
EasyApp resource before converging. Each UPDATE is triggered by fresh `metav1.NewTime(time.Now())`
timestamps in the condition-setting helpers (`lib/resourceutil/util.go`). Even when the
logical condition hasn't changed, the new timestamp causes a status write, which triggers
a watch event, which re-enqueues the reconcile.

The controller DOES converge (the 8th reconcile produces no effects), but the 7 redundant
status writes waste API server resources and generate unnecessary watch events. In a cluster
with many resource requests, this multiplies linearly.

**Root cause:** `MarkConfigureWorkflowAsRunning()`, `MarkReconciledPending()`, and similar
helpers in `lib/resourceutil/util.go` always set `LastTransitionTime: metav1.NewTime(time.Now())`
regardless of whether the condition actually transitioned.

---

### K5: CONFIRMED — Work/WorkPlacement ordering-dependent status divergence

**Scenario file:** `examples/kratix/scenarios/k5_work-status-toctou.json`
**Evidence:** `examples/kratix/.agents/evidence/k5_work-status-toctou/`
**Severity: P2** — Controller ordering produces 4 distinct final states.

With `permuteControllers: ["WorkController", "WorkPlacementController"]`, different orderings
produce 4 distinct final states across 688 MC trials:

| State hash | Trials | Work effects | WP effects |
|---|---|---|---|
| `2v2t8ac5` (majority) | 449 (65%) | 2 UPDATEs | 1 UPDATE |
| `3jhyw4bi` | 120 (17%) | 0 | 1 UPDATE |
| `2uphpjk1` | 63 (9%) | 1 UPDATE | 1 UPDATE |
| `39rnc9sv` | 56 (8%) | 1 UPDATE | 0 |

The Work object produces 3 status variants and WorkPlacement produces 2. In state `3jhyw4bi`,
Work never updates its status at all — missing finalizers or conditions. In state `39rnc9sv`,
WorkPlacement never updates — status conditions (WriteSucceeded, Ready) are never set.

**Root cause:** The Work and WorkPlacement controllers' status update logic is
ordering-sensitive. When WorkController runs first, it sets finalizers and status correctly.
When WorkPlacementController runs first, Work may miss its initial status setup. The
controllers don't have sufficient idempotency guards for their status writes.

#### Reproduction

```bash
cd examples/kratix
go build -o kratix .
mkdir -p /tmp/k5-repro
./kratix --inputs scenarios/k5_work-status-toctou.json \
  --output /tmp/k5-repro --interactive=false --timeout 60s
```

Count distinct final states:
```bash
cd /tmp/k5-repro
for f in *.jsonl; do
  jq -r '.states[0].paths[0][-1].contentsHashAfter // "?"' "$f" 2>/dev/null
done | sort | uniq -c | sort -rn
```

Expected: 4 distinct states. ~35% of trials diverge from majority.

---

### K6: CONFIRMED — WorkController crash leaves Work uninitialized

**Scenario file:** `examples/kratix/scenarios/k6_scheduler-create-status-race.json`
**Severity: P2** — Fault injection produces 2 distinct final states.

With `faultInjection: [{reconciler: "WorkController", crashAfterEffect: 1, triggerOnce: true}]`,
the WorkController crashes after its first write. 25% of trials (30/120) show the Work
object never getting its status updated after the crash — the recovery reconcile doesn't
produce effects in some staleness configurations.

The minority state has a different Work hash — the Work's status conditions are incomplete
because the crash prevented the initial setup and the recovery didn't fully compensate.

#### Reproduction

```bash
mkdir -p /tmp/k6-repro
./kratix --inputs scenarios/k6_scheduler-create-status-race.json \
  --output /tmp/k6-repro --interactive=false --timeout 60s
```

---

### K7: CONFIRMED — Destination deletion produces inconsistent final state

**Scenario file:** `examples/kratix/scenarios/k7_destination-deletion-during-placement.json`
**Severity: P2** — External event produces 4 distinct final states.

Destination deletion via external event (`opType: DELETE, source: UserAction`) at
`readyDepth=3` produces 4 distinct final states across 92 trials:

| State | Trials | Destination present? |
|---|---|---|
| `2jzc708s` | 39 (42%) | No |
| `34gpsw5q` | 29 (32%) | No |
| `wvmv7xg5` | 18 (20%) | **Yes** |
| `zaqpkpv7` | 6 (7%) | No |

In 20% of trials, the Destination persists in the final state despite being deleted — the
deletion event is processed differently depending on when the WorkPlacement controller reads
the Destination relative to the deletion. The Work and WorkPlacement status conditions also
vary across states.

#### Reproduction

```bash
mkdir -p /tmp/k7-repro
./kratix --inputs scenarios/k7_destination-deletion-during-placement.json \
  --output /tmp/k7-repro --interactive=false --timeout 60s
```

---

### K11: CONFIRMED — Multiple Works amplify ordering divergence

**Scenario file:** `examples/kratix/scenarios/k11_multiple-works-same-destination.json`
**Severity: P2** — 5 distinct final states with 2 Work objects.

Two Work objects competing for the same Destination produce 5 distinct final states (up
from 4 with single Work in K5). The divergence compounds combinatorially: work1 has 3
status variants, work2 has 2, and the combinations produce more total states.

This confirms K5's root cause scales linearly with Work count — each Work independently
varies its status conditions based on ordering, and the combinations grow.

---

## Negative Results

### K2: PromiseRevision latest-label race (NEGATIVE)

**Scenario file:** `examples/kratix/scenarios/k2_promise-revision-latest-race.json`

Tested with ordering permutation AND fault injection (`crashAfterEffect=1` on
PromiseRevisionController). The `UnsetPreviousLatestRevision` pattern self-heals in all
orderings and crash points. Both revisions converge to correct latest state. The Promise
lifecycle versioning is well-designed.

### K3: Stale ResourceBinding version after Promise upgrade (NEGATIVE)

**Scenario file:** `examples/kratix/scenarios/k3_stale-resource-binding-version.json`

After fixing the harness gap (DynamicResourceRequestController frame context issue),
10/10 trials converge to the same state. ResourceBinding version handling is correct
under ordering permutation.

### K4: DynamicResourceRequestController crash (NEGATIVE — self-healing)

**Scenario file:** `examples/kratix/scenarios/k4_dynamic-request-crash-mid-reconcile.json`

5/5 trials converge to the same state with fault injection (`crashAfterEffect=1`). The
controller handles partial writes gracefully — the recovery reconcile completes normally.

### K8-K10: Same 4-state divergence across perturbation dimensions

K8 (staleness on WorkPlacement reads), K9 (staleness on Destination reads), and K10
(WorkPlacement crash after 1 write) all produce the exact same 4 distinct final states as
K5. The staleness and fault injection don't produce NEW divergent states — they just shift
the distribution across the same 4 ordering-dependent variants.

This confirms the K5 root cause is purely in the status update logic, not in stale data
or crash recovery.

---

## Area Coverage Assessment

### Promise lifecycle subsystem: EXHAUSTED

Tested with ordering (K2), fault injection (K2 crash, K4), staleness (K3 implicit),
and external events (K3). All negative. The Promise/PromiseRevision/DynamicResourceRequest
chain is well-designed for consistency.

### Work scheduling subsystem: SATURATED for this scenario shape

K5 identified the root cause (ordering-dependent status). K6, K7, K8-K10, K11 all hit
the same root cause from different angles. New bugs would require:
- Testing with the real state store writer (currently a noop/fake)
- Testing Destination scheduling with multiple Destinations
- Testing Work deletion/cleanup (finalizer interactions)

### Harness gap fixed

The DynamicResourceRequestController frame context issue was caused by the factory function
returning a shared placeholder pointer across forks. Each fork overwrote the placeholder
with a new replay client, but the previous fork's reconciler container still used the
shared pointer. Fix: return a fresh pointer per factory invocation.

---

## Summary

| Bug | Description | Confirmed | Severity |
|-----|-------------|-----------|----------|
| **K1** | Redundant timestamp-driven status writes | Yes (trace) | P3 |
| **K5** | Work/WorkPlacement ordering-dependent status | Yes (4 states) | P2 |
| **K6** | WorkController crash leaves Work uninitialized | Yes (2 states) | P2 |
| **K7** | Destination deletion inconsistent final state | Yes (4 states) | P2 |
| **K11** | Multiple Works amplify ordering divergence | Yes (5 states) | P2 |
