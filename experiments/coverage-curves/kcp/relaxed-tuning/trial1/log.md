# KCP4 Late-APIExport Relaxed Perturbation Tuning Log

**Start time:** 2026-03-28T08:40:40-07:00
**Scenario:** kcp4_late-apiexport
**Goal:** Find minimal perturbation config producing 2+ converged states

## Bug Surface Analysis

The scenario models a late-arriving APIExport in the KCP workspace initialization flow:
- Consumer LogicalCluster is in `Initializing` phase with `system:apibindings` initializer
- APIExport "widgets" arrives as an external input at a configurable depth
- Key race: `APIBinderInitializerController` (creates APIBinding, removes initializer from LC) vs `DefaultAPIBindingLifecycleController` (maintains APIBindings, sets WorkspaceAPIBindingsReconciled condition) both react to the APIExport arrival
- `APIExportEndpointSliceController` validates the APIExport reference and may produce different status depending on timing

Original config: 6 controllers, maxDepth=60, userActionReadyDepths 0:5

## Iterations


### Iteration 1 (v1) - 2026-03-28T08:43:00
- **Config:** 6 controllers (LC, ABInit, DABLC, ABASync, AEES, AEESURLs), maxDepth=30, export@depth3
- **Result:** 0 converged, 96 aborted (all hit maxDepth)
- **Wall time:** ~13 min
- **Why:** Depth 30 not enough for 6 controllers to converge with error-retry cycles before export arrives

### Iteration 2 (v2) - 2026-03-28T08:54:00
- **Config:** 3 controllers (ABInit, DABLC, AEES), maxDepth=30, export@depth3
- **Result:** 0 converged, 21 aborted
- **Wall time:** ~2 min
- **Why:** Still insufficient depth for convergence

### Iteration 3 (v3) - 2026-03-28T08:58:00 -- BUG REPRODUCED
- **Config:** 2 controllers (APIExportEndpointSliceController, APIExportEndpointSliceURLsController), maxDepth=50, export@depth2
- **Result:** 2 converged states, 0 aborted
- **Total states:** 321 distinct, 57 resource states
- **Wall time:** ~32 seconds
- **Bug:** APIExportEndpointSlice diverges:
  - State 0: stuck with APIExportValid=False/APIExportNotFound (export processed before arrival)
  - State 1: correct - APIExportValid=True, PartitionValid=True, URL populated

Now minimizing from this baseline...


### Iteration 4 (v4) - 2026-03-28T09:00:00
- **Config:** 6 controllers, maxDepth=50, depthRange 3-8, export@depth3
- **Result:** Still running after 10+ min at 4000+ states
- **Why:** Too many controllers make state space explode

### Iteration 5 (v5) - 2026-03-28T09:03:00
- **Config:** 2 controllers (AEES, AEESURLs), maxDepth=35, export@depth2
- **Result:** 0 converged, 3 aborted
- **Why:** Depth 35 too shallow

### Iteration 6 (v6) - 2026-03-28T09:03:30
- **Config:** 2 controllers, maxDepth=25, export@depth2
- **Result:** 0 converged, 6 aborted

### Iteration 7 (v7) - 2026-03-28T09:03:45
- **Config:** 2 controllers, maxDepth=20, export@depth2
- **Result:** 0 converged, 11 aborted

### Iteration 8 (v8) - 2026-03-28T09:04:00 -- BUG REPRODUCED (minimized!)
- **Config:** 1 controller (APIExportEndpointSliceController), maxDepth=50, export@depth2
- **Result:** 2 converged, 0 aborted
- **Total states:** 145 unique, 48 resource states
- **Wall time:** ~14 seconds

### Iteration 9 (v9) - 2026-03-28T09:06:00
- **Config:** 1 controller, maxDepth=40, export@depth2
- **Result:** 0 converged, 2 aborted

### Iteration 10 (v10) - 2026-03-28T09:06:30
- **Config:** 1 controller, maxDepth=45, export@depth2
- **Result:** 2 converged, 0 aborted (145 unique, 48 resource, ~14s)

### Iteration 11 (v11) - 2026-03-28T09:08:00
- **Config:** 1 controller, maxDepth=42, export@depth2
- **Result:** 0 converged, 2 aborted

### Iteration 12 (v12) - 2026-03-28T09:08:30
- **Config:** 1 controller, maxDepth=43, export@depth2
- **Result:** 0 converged, 2 aborted

### Iteration 13 (v13) - 2026-03-28T09:09:00
- **Config:** 1 controller, maxDepth=45, export@depth1
- **Result:** 1 converged, 0 aborted (only 1 state = no divergence)
- **Why:** Export arrives too early, controller doesn't process endpoint slice before export arrives

### Iteration 14 (v14) - 2026-03-28T09:10:00 -- MINIMUM DEPTH FOUND
- **Config:** 1 controller (APIExportEndpointSliceController), maxDepth=44, export@depth2
- **Result:** 2 converged, 0 aborted
- **Total states:** 145 unique, 48 resource states
- **Wall time:** ~14 seconds
- **Minimum depth:** 44 (43 fails, 44 succeeds)

### Iteration 15 (v15) - 2026-03-28T09:10:30
- **Config:** 1 controller, maxDepth=40, export@depth3
- **Result:** 0 converged, 2 aborted

### Iteration 16 (v16) - 2026-03-28T09:11:00
- **Config:** 1 controller, maxDepth=44, depthRange 0-3, export@depth2
- **Result:** 2 converged, 0 aborted (145 unique, 48 resource, ~14s)
- **Note:** depthRange didn't change anything - same state count as v14

## Summary

### Optimal Configuration (v14)
- **Controllers permuted:** 1 (APIExportEndpointSliceController only)
- **maxDepth:** 44
- **userActionReadyDepths:** {"0": 2}
- **State count:** 145 unique nodes, 48 resource states
- **Wall time:** ~14 seconds
- **Converged states:** 2

### Bug Description
The APIExportEndpointSlice diverges between two orderings:
- **State A (stuck/buggy):** APIExportEndpointSlice has `APIExportValid=False` with reason `APIExportNotFound`. The endpoint slice controller processes the slice before the APIExport arrives and returns nil (no retry), so it never recovers.
- **State B (correct):** APIExportEndpointSlice has `APIExportValid=True`, `PartitionValid=True`, and a populated endpoint URL.

The root cause: the APIExportEndpointSliceController's reconcile function returns `nil` when the APIExport is NotFound (line 76 of reconcile.go), which means no re-queue. If the controller runs before the APIExport is created, it gets stuck permanently.

### Experiment Metrics
- **Total iterations:** 16
- **Total wall time:** ~30 minutes (2026-03-28T08:40:40 to ~2026-03-28T09:12:00)
- **First reproduction:** Iteration 3 (v3), after ~18 minutes
- **Minimum config found:** Iteration 14 (v14), after ~30 minutes

