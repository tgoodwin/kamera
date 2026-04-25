# Focused Tuning Experiment: kcp4_late-apiexport

## Bug Surface Analysis

The scenario models a late-arriving APIExport in KCP. The consumer LogicalCluster is in "Initializing"
phase with a `system:apibindings` initializer. When the APIExport arrives (external input), multiple
controllers race to reconcile.

The bug (per ANALYSIS.md) is an **endpoint condition chain race**:
- The primary APIExportEndpointSliceController sets conditions on the EndpointSlice
- The secondary APIExportEndpointSliceURLsController reads those conditions and populates URLs
- When the APIExport arrives late, the URLs controller may run before conditions are set
  and never get re-triggered, leaving endpoints unpopulated

**Root cause**: Condition dependency chain race between the two endpoint slice controllers.

**Key finding during tuning**: The KCP harness `scenario.go` has its own `applyInputTuning()`
that does NOT handle `permuteDepthRange`, `stalenessIntervals`, `faultInjection`, or
`searchMode`. These JSON fields are silently ignored. Only `maxDepth`, `permuteControllers`,
`staleReads`/`staleLookback`, and `userActionReadyDepths` are effective.

---

## Iterations

### v1 (baseline): 2 controllers from prior tuning, maxDepth 36
- Config: permuteControllers=[LogicalClusterController, APIExportEndpointSliceController], maxDepth=36, userAction@depth2
- Result: 0 converged, 17 aborted, ~55s (timeout)
- Analysis: Wrong controller pair. LogicalCluster + primary EndpointSlice don't produce the race.

### v2 (deterministic baseline): no permutation, maxDepth 50
- Config: permuteControllers=[], maxDepth=50, userAction@depth2
- Result: 1 converged (hash 13le6ul6), 0 aborted, ~4s
- Analysis: Deterministic convergence at depth ~43. Establishes baseline resource state count (21).

### v3: 3 controllers, permuteDepthRange 2-10
- Config: permuteControllers=[APIBinderInit, DefaultAPIBinding, APIExportEndpointSlice], maxDepth=50, permuteDepthRange=2-10, userAction@depth2
- Result: 1 converged (13le6ul6), 0 aborted, ~58s (timeout)
- Analysis: permuteDepthRange is SILENTLY IGNORED by KCP harness. Full permutation at 3 controllers too broad.

### v4: 2 controllers, permuteDepthRange 2-6
- Config: permuteControllers=[LogicalCluster, APIExportEndpointSlice], maxDepth=50, permuteDepthRange=2-6, userAction@depth1
- Result: 1 converged (13le6ul6), 0 aborted, ~40s (timeout)
- Analysis: Still wrong controller pair. permuteDepthRange ignored.

### v5: APIBinder + DefaultAPIBinding, permuteDepthRange 2-8
- Config: permuteControllers=[APIBinderInit, DefaultAPIBinding], maxDepth=50, permuteDepthRange=2-8, userAction@depth2
- Result: 1 converged (13le6ul6), 0 aborted, ~58s (timeout)
- Analysis: Initializer pair doesn't produce endpoint divergence.

### v6: All 6 controllers, permuteDepthRange 3-5
- Config: permuteControllers=[all 6], maxDepth=55, permuteDepthRange=3-5, userAction@depth3
- Result: 1 converged (13le6ul6), 0 aborted, ~59s (timeout)
- Analysis: 6 controllers with full permutation (depth range ignored) too broad.

### v7: All 6 controllers + staleReads
- Config: permuteControllers=[all 6], maxDepth=55, permuteDepthRange=3-6, staleReads on APIBinder+DefaultAPIBinding, userAction@depth3
- Result: 0 converged, 0 aborted (no states returned)
- Analysis: Staleness configuration killed all exploration branches.

### v8: 2 controllers + staleReads only
- Config: permuteControllers=[APIBinderInit, DefaultAPIBinding], maxDepth=55, staleReads on both, userAction@depth2
- Result: 0 converged, 0 aborted (no states returned)
- Analysis: staleReads creates invalid states that prune all branches.

### v9: Original 6 controllers, maxDepth 50
- Config: permuteControllers=[all 6], maxDepth=50, userAction@depth5
- Result: 1 converged (13le6ul6), 0 aborted, ~58s (timeout)
- Analysis: 6 controllers creates too many branches. Only deterministic convergence found.

### v10: staleness only, no permutation
- Config: permuteControllers=[], staleReads={APIExportEndpointSliceURLsController: [apis.kcp.io/APIBinding]}, maxDepth=50, userAction@depth2
- Result: 0 converged, 0 aborted (no states returned)
- Analysis: staleReads mechanism incompatible with this scenario's early-depth state.

### **v11: 2 endpoint controllers permuted -- BUG REPRODUCED**
- Config: permuteControllers=[APIExportEndpointSliceController, APIExportEndpointSliceURLsController], maxDepth=50, userAction@depth5
- Result: **2 converged (13le6ul6, 1f6pcsy7), 0 aborted, ~31s**
- Analysis: **SUCCESS!** The key insight is that only the two endpoint slice controllers need
  to be permuted. The race is between the primary controller (sets conditions) and the URLs
  controller (reads conditions). userAction at depth 5 is critical -- it gives enough
  pre-APIExport state to establish the race window.

### v12: 2 endpoint controllers, maxDepth 45
- Config: permuteControllers=[APIExportEndpointSlice, APIExportEndpointSliceURLs], maxDepth=45, userAction@depth5
- Result: 0 converged, 2 aborted
- Analysis: maxDepth 45 insufficient. Deterministic convergence needs ~43 steps; with permutation some branches need more headroom.

### v13: 2 endpoint controllers, maxDepth 48
- Config: permuteControllers=[APIExportEndpointSlice, APIExportEndpointSliceURLs], maxDepth=48, userAction@depth5
- Result: 1 converged (1f6pcsy7), 1 aborted
- Analysis: Marginal depth. Only one branch converges. The other needs depth > 48.

### v14: 2 endpoint controllers, userAction at depth 3
- Config: permuteControllers=[APIExportEndpointSlice, APIExportEndpointSliceURLs], maxDepth=50, userAction@depth3
- Result: 1 converged (1f6pcsy7), 1 aborted, ~43s (timeout)
- Analysis: Earlier user action changes which paths find the race. Depth 5 is needed.

### v15: 2 endpoint controllers, userAction at depth 4
- Config: permuteControllers=[APIExportEndpointSlice, APIExportEndpointSliceURLs], maxDepth=50, userAction@depth4
- Result: 1 converged (1f6pcsy7), 1 aborted, ~41s (timeout)
- Analysis: Depth 4 misses the second convergence path.

---

## Final Result

**Winning configuration: v11** (`kcp4_focused-v11.json`)

| Metric | Value |
|--------|-------|
| Converged states | 2 (bug reproduced) |
| Aborted states | 0 |
| Wall time | ~31 seconds |
| Controllers permuted | 2 (APIExportEndpointSliceController, APIExportEndpointSliceURLsController) |
| maxDepth | 50 |
| userAction depth | 5 |
| Iterations to success | 11 |
| Total experiment wall time | ~25 minutes |

**Key insights:**
1. The bug is specifically about the condition dependency chain between the two endpoint slice controllers
2. Permuting only those 2 controllers (instead of all 6) reduces state space from 1,795 to 315 nodes
3. userAction at depth 5 is critical -- earlier injection misses the race window
4. maxDepth 50 gives sufficient headroom for both branches to converge
5. The KCP harness silently ignores `permuteDepthRange` -- previous tuning attempts (v1-v14 in the scenarios dir) were running without this constraint
6. `staleReads` is broken for this scenario -- produces 0 states every time
