# Kamera Bug Findings Summary

Bugs found across Kubernetes controller projects using kamera's four perturbation
dimensions: ordering, staleness, external events, and fault injection.

Each project's detailed analysis is in `examples/<project>/.agents/ANALYSIS.md`.

## Overview

| Project | Confirmed Bugs | Severity Distribution | Status |
|---------|---------------|----------------------|--------|
| **Karpenter** | 12 | 7×P1, 4×P2, 1×P3 | Exhausted (provisioning + disruption + lifecycle) |
| **Crossplane** | 9 | See crossplane ANALYSIS.md | Partially explored (cycling blocks convergence analysis) |
| **Kratix** | 6 | 5×P2, 1×P3 | Explored (Promise lifecycle clean, Work scheduling + Health monitoring) |
| **KRO** | 2 | 2×P1 | Explored (Instance Controller crash recovery + deletion) |
| **Knative Serving** | — | — | Not yet explored |
| **Cluster API** | — | — | Not yet explored |

**Total confirmed bugs: 29** across 4 projects.

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

## Crossplane (9 findings)

Source: `github.com/crossplane/crossplane` v2.1.0
Analysis: `examples/crossplane/.agents/ANALYSIS.md`
Bugs: `examples/crossplane/BUGS.md`

Key findings include:
- Unconditional `Status().Update()` in CompositionRevisionReconciler creates
  infinite reconcile loops (all 8 scenarios cycle without converging)
- Ordering-dependent divergence in composition update races
- Stale ValidPipeline condition after FunctionRevision capability changes
- Error ordering where CompositeReconciler selects unvalidated revision

**Note:** All scenarios cycle due to the unconditional status write bug, which
blocks convergence-based analysis. Findings are based on partial-trace and
mid-trace observations.

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
