# Kamera Bug Findings Summary

Bugs found across Kubernetes controller projects using kamera's four perturbation
dimensions: ordering, staleness, external events, and fault injection.

Each project's detailed analysis is in `examples/<project>/.agents/ANALYSIS.md`.

## Overview

| Project | Confirmed Bugs | Severity Distribution | Status |
|---------|---------------|----------------------|--------|
| **Karpenter** | 12 | 7×P1, 4×P2, 1×P3 | Exhausted (provisioning + disruption + lifecycle) |
| **Crossplane** | 7 | 1×P0, 1×P1, 4×P2, 1×P3 | Explored (25 scenarios, cycling blocks full convergence analysis) |
| **Kratix** | 6 | 5×P2, 1×P3 | Explored (Promise lifecycle clean, Work scheduling + Health monitoring) |
| **KRO** | 2 | 2×P1 | Explored (Instance Controller crash recovery + deletion) |
| **KCP** | 6 | 1×P1, 5×P2 | Explored (workspace init + API binding + endpoint discovery) |
| **Cluster API** | 0 | — | Explored (11 scenarios, 0 divergence — robust) |
| **Knative Serving** | 1 | 1×P2 | Explored (ordering cycling blocks convergence analysis for staleness/events) |

**Total confirmed bugs: 34** across 6 projects (+ 1 clean project).

---

## Karpenter (12 bugs)

Source: `sigs.k8s.io/karpenter` (AWS node autoscaler)
Analysis: `examples/karpenter/.agents/ANALYSIS.md`

| ID | Description | Severity | Perturbation |
|----|-------------|----------|-------------|
| D1 | Batching bypasses `nodes` limit (100%) | P1 | Ordering |
| D2 | Sequential off-by-one in `ExceededBy` (40%) | P1 | External event + ordering |
| D5 | Custom resource limit silently ignored (100%) | P1 | Pure logic |
| D6 | Multi-NodePool spillover failure (100%) | P1 | Pure logic |
| D10 | NodeClass readiness TOCTOU (18%) | P2 | External event + ordering |
| D12 | Emptiness disruption deletes node with active workload (12%) | P1 | External event + staleness + ordering |
| D13 | Premature consolidatable → create-delete cycle (100%) | P1 | Ordering |
| D14 | Disruption budget violated across reconcile cycles (100%) | P2 | Ordering |
| D16 | Node hydration ordering-dependent labels (29%) | P3 | Ordering |
| D19 | Disruption + provisioner race violates `nodes` limit (57%) | P2 | External event + ordering |
| D21 | Liveness timeout race causes create-delete churn (33%) | P2 | Ordering |
| D23 | Provisioner places pod on ghost node (stale taint) (100%) | P2 | Ordering + stale state |

**Key finding:** The `nodes` resource is not tracked by the scheduler's internal
accounting (`filterByRemainingResources`, `subtractMax`). This single root cause
produced 5 distinct bugs (D1, D2, D5, D6, D19).

**Crash resilience finding:** Karpenter's mutual finalization design
(`nodeclaim.lifecycle` + `node.termination`) is crash-resilient — fault injection
on the finalization path self-heals in all tested configurations.

---

## Crossplane (7 bugs)

Source: `github.com/crossplane/crossplane` v2.2.0
Analysis: `examples/crossplane/.agents/ANALYSIS.md`
Harness: CompositionReconciler + CompositionRevisionReconciler + CompositeReconciler +
CleanupReconciler (real Crossplane code).

| ID | Description | Severity | Perturbation |
|----|-------------|----------|-------------|
| F1 | Manual update policy fetches revision from wrong Composition — silent data corruption | P0 | Pure logic |
| F2 | Unconditional `Status().Update()` causes infinite reconcile cycling (all scenarios) | P3 | Pure logic |
| F3 | Orphaned compositionRef after Composition deletion — permanent error loop | P1 | External event + ordering |
| F5 | Stale ValidPipeline condition allows composition with invalidated functions (6/9 orderings) | P2 | Ordering |
| F6 | Function switch to fatal leaves orphaned resources + stale Ready=True (7 states, 49 trials) | P2 | External event + ordering |
| C2 | Claim deletion orphans XR + composed resources (2 states, 98 trials, 98% orphan rate) | P2 | External event + ordering |
| C4 | Two XRs silently steal ownership of same composed resource (2 states, 98 trials) | P2 | Ordering |

**F1 detail:** `APIRevisionFetcher.Fetch` in Manual policy mode does a bare
`Get(current revision name)` with zero validation that the revision belongs to
the Composition referenced by `compositionRef`. Switching `compositionRef` from
alpha to beta while `compositionRevisionRef` still points to alpha-rev-1
silently composes resources using the wrong Composition's revision. No error raised.

**F3 detail:** Deleting a Composition while an XR is bound produces 2 ordering-
dependent error paths: "Composition not found" (CleanupReconciler-first) vs
"no compatible Compositions" (CompositeReconciler-first). Both are permanent
error loops with no self-recovery. Different orderings also diverge on whether
composed resources (ConfigMap) survive.

**F6 detail:** Updating a Composition's pipeline from a working function to a
SEVERITY_FATAL function produces 7 distinct terminal states. In 86% of orderings,
the ConfigMap from the prior successful composition persists as an orphan (never
cleaned up because SEVERITY_FATAL returns before GC). The XR shows a confusing
`Ready=True` + `Synced=False` status. In 14% of orderings, the CompositeReconciler
never composed before the switch, so no orphan exists. Two clean scenarios
(resource switch, flap recovery) confirmed GC works correctly when the function
changes output, and that transient failures recover.

**Note:** All scenarios cycle due to F2 (unconditional status write), which
blocks convergence-based analysis. One additional observation (CompositionRevision
creation vs validation ordering) was investigated across 3 scenarios and confirmed
as **not a bug** — the transient error self-resolves via retry in all orderings.
Six further scenarios tested combinations of Findings 1/3/5 but produced no new
outcome categories.

---

## Kratix (6 bugs)

Source: `github.com/syntasso/kratix`
Analysis: `examples/kratix/.agents/ANALYSIS.md`

| ID | Description | Severity | Perturbation |
|----|-------------|----------|-------------|
| K1 | Redundant timestamp-driven status writes | P3 | Pure logic |
| K5 | Work/WorkPlacement ordering-dependent status (4 states, 35%) | P2 | Ordering |
| K6 | WorkController crash leaves Work uninitialized (2 states, 25%) | P2 | Fault injection |
| K7 | Destination deletion inconsistent final state (4 states, 58%) | P2 | External event |
| K11 | Multiple Works amplify ordering divergence (5 states, 37%) | P2 | Ordering |
| K13 | Multiple HealthRecords ordering-dependent healthStatus (2 states, 35%) | P2 | Staleness + ordering |

**Key finding:** The Work scheduling subsystem has one systemic ordering bug
(status condition setting) that manifests across all perturbation dimensions.
K5-K10 all hit the same 4 states. The Promise lifecycle subsystem is clean.

---

## KRO (2 bugs)

Source: `github.com/kubernetes-sigs/kro`
Analysis: `examples/kro/.agents/ANALYSIS.md`
Harness: Both real KRO controllers (RGD + Instance) wired in via adapters.

| ID | Description | Severity | Perturbation |
|----|-------------|----------|-------------|
| K2b | Instance Controller crash mid-apply: Service never created (15+ states, 2201 runs) | P1 | Fault injection + ordering |
| K6 | Instance Controller crash during deletion: orphaned children, CRD incorrectly deleted (6 states, 304 runs) | P1 | Fault injection + ordering |

**K2b detail:** A single mid-reconcile crash (`triggerOnce`) during the Instance
Controller's applyset Apply loop permanently prevents Service creation. Across
2201 runs: 1439 end with only Ingress, 406 with Deployment+Ingress (no Service),
196 with no children at all. The applyset parallel Apply at
`applyset.go:297-305` combined with the parent ApplySet metadata patch at
`resources.go:54` creates a vulnerability window where partial child applies
produce inconsistent ApplySet state that prevents correct recovery.

**K6 detail:** Crashing the Instance Controller after the 1st write during
Application deletion produces 6 distinct final states. In 26 runs (9%) the
Application survives deletion entirely; in 2 runs all 9 objects (App +
Deployment + Service + Ingress + ReplicaSet + Pod + Endpoints + CRD + RGD)
survive; in 7 runs the CRD is incorrectly deleted despite
`allowCRDDeletion=false`.

**Clean scenarios:** Ordering (K1), ingress toggle (K4), RGD fault injection
(K5), and rapid spec changes (K7) showed no divergence.

---

## KCP (6 bugs)

Source: `github.com/kcp-dev/kcp` (multi-tenant Kubernetes control plane)
Analysis: `examples/kcp/kamera/ANALYSIS.md`
Harness: 9 real KCP controllers wired via `//go:linkname` (workspace init +
API binding lifecycle + endpoint discovery). Uses fake KCP clientset, fake
apiextensions clientset, and fake k8s clientset with typed bookmark watch
reactors for informer sync.

| ID | Description | Severity | Perturbation |
|----|-------------|----------|-------------|
| KCP4 | Late APIExport: endpoint URLs unpopulated (2 states) | P2 | External event + ordering |
| KCP5 | WorkspaceType change: condition divergence on LogicalCluster (2 states) | P2 | External event + ordering |
| KCP7 | APIExport deletion: 10 distinct final states, 4/9 objects diverge (30% APIExport survives deletion) | P1 | External event + ordering |
| KCP8 | Partition deletion: 4 endpoint configurations (4 states) | P2 | External event + ordering |
| KCP17 | Pure ordering divergence: apibinding reconciler triggers condition write conflict (2 states) | P2 | Ordering |
| KCP18b | Crash recovery divergence: apibinding reconciler crash leaves inconsistent LogicalCluster (2 states) | P2 | Fault injection + ordering |

**Key finding:** Ordering alone is clean with 7 controllers (KCP1-3, KCP10b).
Adding the 8th controller (apibinding reconciler) breaks ordering robustness —
KCP17 is a pure ordering bug invisible with fewer controllers. This demonstrates
that wiring more controllers reveals bugs that can't be found with fewer.

**Three distinct root causes:**
1. **Endpoint condition chain race** (KCP4, KCP7, KCP8, KCP13): The primary/
   secondary endpoint slice controllers have no synchronization beyond watches.
   External events (deletion, late arrival) leave inconsistent condition/URL state.
2. **Concurrent condition write conflict** (KCP5, KCP14, KCP17): Multiple
   controllers write conditions to LogicalCluster. The winner is ordering-
   dependent, especially when the apibinding reconciler resets binding state.
3. **Crash recovery divergence** (KCP18b): The apibinding reconciler's multi-write
   reconcile (CRD creation + status commit) is crash-vulnerable. A mid-reconcile
   crash leaves partial state that produces ordering-dependent recovery outcomes.

**Most severe:** KCP7 (APIExport deletion) produces 10 distinct final states
affecting 4/9 objects, including the APIExport surviving its own deletion in
30% of orderings. Requires manual intervention — the system does not self-heal.

**Clean scenarios:** Single consumer (KCP1/KCP1b), multi-consumer (KCP2),
multiple default bindings (KCP3), consumer deletion (KCP10b).

**Exploration scope:** 20 scenarios, 9 of ~50 controllers wired across 3 regions
(workspace init, API binding lifecycle, endpoint discovery). Staleness injection
not available (custom strategy bypasses replay client). See `LANDSCAPE.md` for
the full controller map and exploration progress.

---

## Knative Serving (1 bug)

Source: `knative.dev/serving` v0.46.5
Analysis: `examples/knative-serving/.agents/ANALYSIS.md`
Harness: ServiceReconciler + ConfigurationReconciler + RevisionReconciler +
RouteReconciler + KPA + ServerlessServiceReconciler (all real Knative code),
plus IngressStatusStub and RevisionDigestStub.

| ID | Description | Severity | Perturbation |
|----|-------------|----------|-------------|
| N1 | Ordering-dependent state divergence: 4+ distinct final states (50 trials, 30% non-reference) | P2 | Ordering |

**N1 detail:** When controller execution order is permuted (50 MC trials),
the system converges to at least 4 distinct final states. The reference state
appears in 70% of orderings; the remaining 30% settle into 3 alternative
states. The divergence is traced to the ServiceReconciler's status propagation
chain: it reads Configuration/Route status and propagates it to the Service.
When controller ordering changes which runs first, the Service status
propagation captures different intermediate states, leading to different
terminal conditions.

In production, this means the final Service status (conditions, latestReady,
URL) can differ depending on which controller happened to process events
first — a non-deterministic outcome for the same input.

**Clean scenarios:** Multi-action scenarios (image update, deletion, rapid
updates) have not yet been tested with ordering perturbation due to path
depth/timeout constraints. Staleness, external events, and fault injection
remain to be explored.

---

## Cluster API (0 bugs — clean)

Source: `sigs.k8s.io/cluster-api` main (post-v1.12, pre-v1.13)
Analysis: `examples/cluster-api/.agents/ANALYSIS.md`
Harness: MachineDeploymentReconciler + MachineSetReconciler + MachineReconciler +
MachineHealthCheckReconciler + provider stubs (InfraMachine, BootstrapConfig).

**Result: zero divergence across 11 scenarios and all 4 perturbation dimensions.**

Scenarios tested: ownership handoff races (D3), rolling updates (D2, D6),
concurrent SSA vs strategic merge patch (D7), crash mid-create (D1), deletion
cascade (D5), delete-during-update (D8), scale-down (D9), MHC condition
contention (D10), MHC stale reads (D11), MHC during rolling update (D12).

**Why CAPI is robust:**
- **Condition ownership protocol** (`patch.WithOwnedConditions`) prevents lost
  updates when 3 controllers write conditions to the same Machine object
- **SSA field management** prevents metadata conflicts between MachineSet and
  Machine controller
- **Finalizer-based deletion** ensures cascade ordering is deterministic
- **ManagedFields migration** settles correctly, not causing ongoing churn

**Kamera platform finding:** This evaluation drove 5 platform bug fixes
(K-CAPI-1 through K-CAPI-5) improving simulation fidelity for SSA, JSON
patches, multi-write reconciles, and no-op detection.

---

## Methodology

Bugs are found by running Monte Carlo exploration with kamera and checking for
**divergent final state hashes** across trials. Multiple distinct hashes indicate
ordering-dependent behavior — the controllers produce different outcomes depending
on which runs first.

Each bug is confirmed with trace evidence showing the exact controller ordering
that triggers the divergence, with reproduction steps and persisted trace files.

Perturbation dimensions used:
- **Ordering** (`permuteControllers`): which controller runs next
- **Staleness** (`stalenessIntervals`): informer cache lag simulation
- **External events** (`externalInputs`): infrastructure/user state changes mid-execution
- **Fault injection** (`faultInjection`): mid-reconcile controller crash
