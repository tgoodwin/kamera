# Non-Determinism Analysis Summary

## Executive Summary

**Finding 1**: The Knative KPA (Pod Autoscaler) reconciler exhibits non-deterministic behavior, suspected due to Go map iteration order. This non-determinism is **contained within Knative code**, not in the kamera/tracecheck exploration framework.

**Finding 2** (January 2026): The **EndpointsController is missing a Watch registration for Pods** in kamera. This causes the final Endpoints state to depend on whether EndpointsController is coincidentally still in the pending queue when Pods become Ready. The fix is to add `.Watches("Pod", mapPodToServices)` to mirror real Kubernetes behavior (beads task kamera-53o).

**Impact**: Different exploration runs can discover different numbers of converged states (2 vs 3), potentially missing valid final cluster configurations. Additionally, without the EndpointsController fix, some converged states have empty Endpoints while others have populated Endpoints based purely on reconciler timing.

---

## Configuration Used

The diagnostic trials used config file `ablation/configs/study-1-both.json` with:
- `subtreeCompletion: true`
- `completedPathDedup: true`

For new verification runs, see `analysis/nondeterminism-verification/`:
- `explore-config-original.json` - **all optimizations enabled**
- `explore-config-no-opt.json` - no optimizations
- `run-trials.sh` - script to run multiple trials with dump output

**Note**: The default configuration for the knative-serving example is **no optimizations** (`builder.WithoutOptimizations()` in `scenario.go`).

---

## Evidence

### 1. KPA Triggering is Deterministic

At depth 47 across all three trials:
- **Identical pending list** (same MD5 hash)
- **Identical contentsHash** (resource state): `3iuhvw1w`
- **Identical position** in pending queue: KPA at index 5
- **Identical exploration metrics**: 120 distinct states, 34 resource states

**Conclusion**: Kamera triggers KPA at exactly the same points with exactly the same input state.

### 2. KPA Output is Non-Deterministic

Despite identical inputs, KPA produces different outputs:

| Trial | KPA Effects at Depth 47 |
|-------|------------------------|
| 1 | 2 effects (ServerlessService + PodAutoscaler) |
| 2 | 2 effects (ServerlessService + PodAutoscaler) |
| 3 | **1 effect** (PodAutoscaler only) |

### 3. Effect Distribution Across Trials

KPA effect counts throughout exploration:

| Trial | 2-effect runs | 1-effect runs |
|-------|---------------|---------------|
| 1 | 12 | 11 |
| 2 | 12 | 11 |
| 3 | **10** | **14** |

Trial 3 has more 1-effect executions, leading to fewer triggered reconciles and fewer branches explored.

### 4. Impact on Converged States

| Trial | Converged States | UniqueResourceStates | EarlyConvergence |
|-------|-----------------|---------------------|------------------|
| 1 | 3 | 80 | 6 |
| 2 | 3 | 82 | 6 |
| 3 | **2** | **73** | **3** |

**Converged state hashes found:**
- All trials: `2r21c27a`, `pdu8xy8e`
- Only trials 1 & 2: `1i908egc` ← **Trial 3 missed this state**

---

## Root Cause

The KPA reconciler in `knative.dev/serving/pkg/reconciler/autoscaling/kpa` likely iterates over an internal map when deciding which resources need updates. Go's random hash seed (different per process) causes different iteration orders, which affects:

1. Whether to update ServerlessService
2. Whether to update PodAutoscaler
3. Or both

This is a common Go pitfall where map iteration order affects program logic.

---

## Diagnostic Data Location

Previous diagnostic runs are stored in:
```
/Users/tgoodwin/projects/kamera/analysis/nondeterminism-diagnostics/
├── trial-1/
│   ├── diagnostics.log      # EFFECTS_ORDER, PENDING_LIST diagnostics
│   ├── full-output.log      # Complete exploration output
│   ├── reconcile-steps.log  # Reconcile step tracking
│   └── stats.json           # Final statistics
├── trial-2/
└── trial-3/
```

---

## Key Log Patterns to Search

### Find KPA effect variations:
```bash
grep "KPA" diagnostics.log | grep "EFFECTS_ORDER" | grep "numEffects"
```

### Find convergence points:
```bash
grep "arrived at converged state" full-output.log
```

### Compare pending lists at specific depths:
```bash
grep "PENDING_LIST.*\"depth\": 47" diagnostics.log
```

---

## Implications

1. **Kamera is working correctly** - it deterministically triggers reconcilers and records their effects
2. **Knative has internal non-determinism** - the KPA reconciler's behavior varies based on Go's map seed
3. **Exploration coverage varies** - some runs may miss valid converged states
4. **Multiple runs recommended** - to ensure complete coverage of possible final states

---

## Detailed Dump Analysis (January 2026)

### Analysis of `trials-study1-both/trial-1/dump.jsonl`

This trial found **2 converged states** (`pdu8xy8e` and `15wjkorq`) from the same starting state. Analysis of the execution paths reveals:

### 1. Divergence Point Identified

**Step 6** is where the paths diverge. Both paths:
- Start from identical `stateBefore` (Configuration, Revision, Route, Service - no PA)
- Execute the same controller: **RevisionReconciler**
- But produce different outputs

### 2. Effect Comparison at Divergence

| Path | Effects | Objects Created/Updated |
|------|---------|------------------------|
| Path 0 | 4 | CREATE Deployment, CREATE ImageCache, **CREATE PA**, UPDATE Revision |
| Path 1 | 3 | CREATE Deployment, CREATE ImageCache, UPDATE Revision |

**Path 1 is missing the PodAutoscaler creation.**

### 3. Revision Status Condition Differences

| Path | Conditions Set |
|------|----------------|
| Path 0 | Active, ContainerHealthy, Ready, ResourcesAvailable (4 conditions) |
| Path 1 | ContainerHealthy, Ready, ResourcesAvailable (3 conditions) |

The `Active` condition is **only set when `PropagateAutoscalerStatus` is called** from `reconcilePA`. Since Path 1 doesn't create the PA, this function is not reached.

### 4. Cascade Effect

After the divergence at step 6, subsequent steps continue to diverge:

| Step | Controller | States Match |
|------|-----------|-------------|
| 7 | RouteReconciler | No |
| 8 | ServiceReconciler | No |
| 9 | ConfigurationReconciler | No |
| 10 | RevisionDigestStub | No |

### 5. Pending Reconciler Differences

After step 6:
- **Path 0 pending (7 reconcilers)**: includes `KPA: kamera-test`
- **Path 1 pending (6 reconcilers)**: missing KPA

The PA creation in Path 0 triggers the KPA reconciler to be added to pending queue.

### Hypothesis: Informer Cache Timing Issue

The `reconcilePA` function in the RevisionReconciler uses a **deployment lister** to fetch the deployment:

```go
func (c *Reconciler) reconcilePA(ctx context.Context, rev *v1.Revision) error {
    deployment, err := c.deploymentLister.Deployments(ns).Get(deploymentName)
    if err != nil {
        return err  // Returns early if deployment not found
    }
    // ... creates PA, calls PropagateAutoscalerStatus
}
```

The deployment was just created by `reconcileDeployment` in the same reconcile cycle. If the deployment lister (backed by informer cache) doesn't see the newly created deployment, `reconcilePA` returns early without:
1. Creating the PA
2. Calling `PropagateAutoscalerStatus` (which sets the `Active` condition)

This would explain why Path 1 has:
- No PA created
- No `Active` condition set
- Fewer pending reconcilers (KPA not triggered)

### Complete Causal Chain: Initial Race → Final Divergence

The analysis reveals a complete causal chain from the initial "read your writes" race to the final divergent state:

#### Stage 1: Initial Divergence (Step 6)
**Trigger**: Informer cache timing nondeterminism in RevisionReconciler's `reconcilePA`

| Path | PA Created? | Revision Conditions |
|------|-------------|---------------------|
| Path 0 | Yes | Active, ContainerHealthy, Ready, ResourcesAvailable |
| Path 1 | No | ContainerHealthy, Ready, ResourcesAvailable |

#### Stage 2: Reconciler Ordering Divergence (Steps 6-12)
The PA creation difference causes different reconciler scheduling:

| Step | Path 0 | Path 1 |
|------|--------|--------|
| 12 | KPA (triggered by PA) | RevisionReconciler (re-runs, creates PA) |

Path 1 eventually creates the PA at step 12, but by different reconciler ordering.

#### Stage 3: Revision Conditions Eventually Converge
Both paths eventually reach the same Revision conditions:
- Active: False (NoTraffic)
- ContainerHealthy: True
- Ready: True
- ResourcesAvailable: True

#### Stage 4: EndpointsController Timing Divergence
The different reconciler ordering affects when EndpointsController runs relative to Pod becoming Ready:

| Path | EC Creates Endpoints | Pod Ready | EC Updates Endpoints |
|------|---------------------|-----------|---------------------|
| Path 0 | Step 25 | Step 38 | Never (EC not in queue) |
| Path 1 | Step 31 | Step 36 | Step 40 (EC still in queue) |

**Key difference**: In Path 1, EndpointsController remains in the pending queue when the Pod becomes Ready, so it runs again and populates the Endpoints with pod addresses.

#### Stage 5: Final Divergent State
The only difference between the two converged states is the **Endpoints** object:

| State | Endpoints Content |
|-------|------------------|
| State 0 | Empty (no subsets) |
| State 1 | Has pod address (10.0.0.1) |

#### Summary of Causal Chain

```
Initial Race: RevisionReconciler can't read deployment it just wrote
    ↓
Path Split: PA created vs not created at step 6
    ↓
Different Reconciler Ordering: KPA triggered vs not triggered
    ↓
EndpointsController Timing: In/out of queue when Pod Ready
    ↓
Final Divergence: Endpoints empty vs populated
```

### Root Cause: EndpointsController Not Triggered by Pod Changes

**Key Finding (January 2026)**: The EndpointsController timing divergence is caused by a missing Watch registration in kamera's exploration framework.

#### The Problem

In `pkg/tracecheck/explorebuilder.go`, EndpointsController is registered as:
```go
b.WithReconciler("EndpointsController", ...).For("Service")
b.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "Pod"}, "EndpointsController")
```

However, `WithResourceDepGK` only declares dependencies for **stuck detection**, NOT for triggering. Looking at `trigger.go:getTriggered()`, reconcilers are only triggered via:
1. `tm.owners` - primary reconcilers from `.For(kind)`
2. `tm.watchers` - explicit Watch registrations
3. Owner references on changed objects

The `tm.deps` map (populated by `WithResourceDepGK`) is NOT used for triggering.

#### Real Kubernetes Behavior

In real Kubernetes, the EndpointsController uses:
```go
Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.mapPodToServices))
```

This maps Pod changes → Services in the same namespace → triggers reconciliation. When a Pod becomes Ready, all Services with matching selectors are enqueued.

#### Kamera Behavior

In kamera, there is no such Watch registration. EndpointsController is only triggered when:
- Services change (it's the primary for Service)
- NOT when Pods change

So whether EndpointsController runs after Pod becomes Ready is purely coincidental based on:
- Whether it's still in the pending queue from an earlier trigger
- Which depends on the reconciler ordering that diverged at step 6

#### Fix Required

Add a Watch registration for EndpointsController on Pods:
```go
b.WithReconciler("EndpointsController", func(c client.Client) Reconciler {
    return &controller.EndpointsReconciler{...}
}).For("Service").Watches("Pod", mapPodToServices)
```

Where `mapPodToServices` enqueues all Services in the Pod's namespace.

### Analysis Script

A Python script for analyzing dump files is available at:
```
/Users/tgoodwin/projects/kamera/analysis/analyze_divergence.py
```

Usage:
```bash
python3 analysis/analyze_divergence.py <path-to-dump.jsonl>
```

---

## Next Steps for Manual Investigation

1. ~~Run with `-dump-output` to capture full resource state at convergence~~ ✓ Done
2. ~~Compare the converged states to understand their differences~~ ✓ Analyzed
3. ~~Trace into Knative code to identify the specific source of non-determinism~~ ✓ Traced
4. ~~Investigate why EndpointsController doesn't trigger on Pod Ready~~ ✓ Found root cause
5. **[P0] Fix EndpointsController triggering**: Add `.Watches("Pod", mapPodToServices)` to EndpointsController registration in `explorebuilder.go` (see beads task kamera-53o)
6. **Investigate informer cache timing**: Why does the deployment lister sometimes not see the newly created deployment? (beads task kamera-z38)
7. **Add instrumentation**: Log when `reconcilePA` fails to find the deployment
8. **Consider fix for informer timing**: Either ensure informer caches are updated before subsequent phases, or have `reconcilePA` use the client directly instead of the lister
