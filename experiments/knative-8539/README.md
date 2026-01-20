# Knative Issue #8539 Reproduction

Reproducing a historical race condition in Knative Serving using Kamera's DFS exploration.

## The Bug

**Issue**: https://github.com/knative/serving/issues/8539

When creating Knative Services with `--min-scale 0`, some Revisions become permanently stuck at `Ready=Unknown` even though the underlying deployment succeeded.

**Root Cause**: A race condition between the PodAutoscaler (PA) controller and Revision controller:
1. PA spins up pod, goes `Active=True`
2. No traffic → PA scales to zero → `Active=False` (NoTraffic)
3. If Revision controller reconciles after step 2, it never observed `Active=True`
4. Revision stays stuck at `Ready=Unknown`

**The Fix** (in production Knative): Changed from checking `IsActive()` (ephemeral) to `IsScaleTargetInitialized()` (sticky/level-triggered) - allowing Revision to become Ready if PA was *ever* initialized, not just *currently* active.

## Reproduction Setup

### Modified Gomodcache
Location: `~/tmp/gomodcache-knative-8539`

Modified file: `knative.dev/serving@v0.46.5/pkg/apis/serving/v1/revision_lifecycle.go`

### Changes Made

In `PropagateAutoscalerStatus()`, we replaced the fixed code with buggy code that couples Revision Ready purely to PA Active:

**Original (fixed):**
```go
if ps.IsScaleTargetInitialized() && !resUnavailable {
    rs.MarkResourcesAvailableTrue()
    rs.MarkContainerHealthyTrue()
}
```

**Buggy (our change):**
```go
if ps.IsActive() {
    rs.MarkResourcesAvailableTrue()
    rs.MarkContainerHealthyTrue()
} else {
    // PA not active - override to keep Ready=Unknown
    rs.MarkResourcesAvailableUnknown(ReasonDeploying, "PodAutoscaler is not active")
    rs.MarkContainerHealthyUnknown(ReasonDeploying, "PodAutoscaler is not active")
}
```

### Why the else branch?

Initial attempt only changed the conditional from `IsScaleTargetInitialized()` to `IsActive()`. This was insufficient because:

1. `PropagateDeploymentStatus()` runs *before* `PropagateAutoscalerStatus()` and can set `ResourcesAvailable=True` based on Deployment status
2. `reconcileDeployment()` can set `ContainerHealthy=True` when `ReadyReplicas > 0`

These alternate paths allowed Revision to become Ready regardless of PA Active status. The `else` branch overrides whatever was set previously, making Ready purely dependent on PA Active.

### Exploration Config

Also enabled `PermuteOrder()` on KPA reconciler in `scenario.go` to ensure DFS explores orderings where KPA runs before RevisionReconciler.

## Running the Experiment

```bash
./experiments/knative-8539/run-8539-bug.sh
```

## Expected Outcome

Kamera's DFS should produce divergent terminal states:
1. **Happy path**: RevisionReconciler reconciles when PA `Active=True` → Revision `Ready=True`
2. **Bug path**: RevisionReconciler reconciles when PA `Active=False` → Revision `Ready=Unknown` (stuck)
