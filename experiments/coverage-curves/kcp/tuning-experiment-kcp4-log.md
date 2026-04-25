# KCP4 Perturbation Tuning Experiment Log

**Scenario**: kcp4_late-apiexport.json
**Goal**: Reproduce the 2-converged-states bug with minimal perturbation scope and wall time
**Baseline**: 6 controllers, maxDepth 60, 9216 total states, ~34 min, 2 converged states

## Key Facts
- Divergence at step 1: LogicalClusterController vs APIExportEndpointSliceController
- Shortest converged path: 41 steps (state 1), 46 steps (state 0)
- External input "apiexport-arrives" fires at depth 5
- Converged state hashes: 13le6ul6 and 1f6pcsy7

---

## Milestones

- **FIRST reproduction**: v1 at 21:36:54 (~1 min 5 sec after experiment start)
- **Best reproduction**: v7 -- 2 controllers, maxDepth 45, userActionReadyDepths=2, 4 end-states (2 converged, 2 aborted), ~58 sec wall
- **Minimum viable maxDepth**: 38 (v12), below 36 fails
- **Total experiment wall time**: ~25 minutes (21:35 to 22:00)

---

## Results Summary

| Variant | Controllers | maxDepth | userActionDepth | permuteDepthRange | Env Trimmed | Converged | Aborted | Total States | Wall (sec) | Bug? |
|---------|-------------|----------|-----------------|-------------------|-------------|-----------|---------|--------------|------------|------|
| baseline | 6 | 60 | 5 | - | No | 2 | - | 9216 | ~2040 | YES |
| v1 | 2 | 45 | 5 | - | No | 2 | 6 | 662 | ~65 | YES |
| v2 | 2 | 30 | 5 | - | No | 0 | 17 | 452 | ~45 | NO |
| v3 | 2 | 45 | 5 | {0,8} | No | 2 | 6 | 662 | ~65 | YES |
| v4 | 2 | 42 | 5 | - | No | 2 | 12 | 668 | ~66 | YES |
| v5 | 2 | 45 | 5 | - | YES (no Shard/Partition/Slice) | 1 | 3 | ~400 | ~30 | NO |
| v6 | 2 | 45 | 5 | {0,6} | No | 2 | 6 | 662 | ~65 | YES |
| v7 | 2 | 45 | 2 | - | No | 2 | 2 | 584 | ~58 | YES |
| v8 | 2 | 45 | 3 | - | No | 2 | 2 | 584 | ~72 | YES |
| v9 | 2 | 35 | 2 | - | No | 0 | 21 | 573 | ~57 | NO |
| v10 | 2 | 45 | 1 | - | No | 1 | 2 | 425 | ~42 | NO |
| v11 | 2 | 40 | 2 | - | No | 2 | 6 | 584 | ~57 | YES |
| v12 | 2 | 38 | 2 | - | No | 2 | 12 | 594 | ~58 | YES |
| v13 | 2 | 36 | 2 | - | No | 0 | 17 | 579 | ~57 | NO |
| v14 | 2 | 40 | 2 | {0,2} | No | 2 | 6 | 584 | ~58 | YES |

---

## Iterations

### v1 -- Minimal 2-controller, maxDepth 45
- **Timestamp**: 2026-03-26T21:35:49 to 21:36:54
- **Changes**: Reduced from 6 to 2 controllers (LogicalClusterController, APIExportEndpointSliceController). maxDepth 45 (down from 60). Kept userActionReadyDepths=5, full environment.
- **File**: `scenarios/kcp4_tuning-v1.json`
- **Result**: 662 total states, 8 end-states (2 converged, 6 aborted), ~65 sec wall
- **Bug reproduced**: YES -- first reproduction!

### v2 -- maxDepth 30 (too aggressive)
- **Timestamp**: 2026-03-26T21:37:58 to 21:38:43
- **Changes**: Same as v1 but maxDepth=30.
- **File**: `scenarios/kcp4_tuning-v2.json`
- **Result**: 452 total states, 17 end-states (0 converged, 17 aborted), ~45 sec wall
- **Bug reproduced**: NO -- paths too short to converge

### v3 -- permuteDepthRange {0,8}
- **Timestamp**: 2026-03-26T21:39:29 to 21:40:34
- **Changes**: Same as v1 with permuteDepthRange {min:0, max:8}.
- **File**: `scenarios/kcp4_tuning-v3.json`
- **Result**: 662 total states, 8 end-states (2 converged, 6 aborted), ~65 sec wall
- **Bug reproduced**: YES -- identical to v1 (depth range doesn't affect 2-controller scenarios much)

### v4 -- maxDepth 42
- **Timestamp**: 2026-03-26T21:40:35 to 21:41:41
- **Changes**: Same as v1 but maxDepth=42 (just above shortest path of 41).
- **File**: `scenarios/kcp4_tuning-v4.json`
- **Result**: 668 total states, 14 end-states (2 converged, 12 aborted), ~66 sec wall
- **Bug reproduced**: YES

### v5 -- Trimmed environment (no Shard, Partition, EndpointSlice)
- **Timestamp**: 2026-03-26T21:42:39 to 21:43:09
- **Changes**: Removed Shard, Partition, and APIExportEndpointSlice objects from environment. Kept 2 controllers, maxDepth 45, userActionReadyDepths=5.
- **File**: `scenarios/kcp4_tuning-v5.json`
- **Result**: ~400 total states, 4 end-states (1 converged, 3 aborted), ~30 sec wall
- **Bug reproduced**: NO -- removing environment objects killed one divergent path. Different converged hash (3mtv2slq).

### v6 -- permuteDepthRange {0,6}
- **Timestamp**: 2026-03-26T21:43:10 to 21:44:15
- **Changes**: Same as v1 with permuteDepthRange {min:0, max:6}.
- **File**: `scenarios/kcp4_tuning-v6.json`
- **Result**: 662 total states, 8 end-states (2 converged, 6 aborted), ~65 sec wall
- **Bug reproduced**: YES

### v7 -- Earlier external input (userActionReadyDepths=2) **BEST**
- **Timestamp**: 2026-03-26T21:45:03 to 21:46:01
- **Changes**: Moved external input from depth 5 to depth 2. maxDepth 45, 2 controllers.
- **File**: `scenarios/kcp4_tuning-v7.json`
- **Result**: 584 total states, 4 end-states (2 converged, 2 aborted), ~58 sec wall
- **Bug reproduced**: YES -- cleanest result (fewest aborted paths)

### v8 -- userActionReadyDepths=3
- **Timestamp**: 2026-03-26T21:46:02 to 21:47:14
- **Changes**: External input at depth 3 instead of 5.
- **File**: `scenarios/kcp4_tuning-v8.json`
- **Result**: 584 total states, 4 end-states (2 converged, 2 aborted), ~72 sec wall
- **Bug reproduced**: YES

### v9 -- maxDepth=35 with early input (too short)
- **Timestamp**: 2026-03-26T21:48:08 to 21:49:05
- **Changes**: userActionReadyDepths=2, maxDepth=35.
- **File**: `scenarios/kcp4_tuning-v9.json`
- **Result**: 573 total states, 21 end-states (0 converged, 21 aborted), ~57 sec wall
- **Bug reproduced**: NO -- maxDepth too short even with earlier input

### v10 -- userActionReadyDepths=1 (too early)
- **Timestamp**: 2026-03-26T21:49:07 to 21:49:49
- **Changes**: External input at depth 1 (immediately after first step).
- **File**: `scenarios/kcp4_tuning-v10.json`
- **Result**: 425 total states, 3 end-states (1 converged, 2 aborted), ~42 sec wall
- **Bug reproduced**: NO -- input too early, only 1 converged state

### v11 -- maxDepth=40, early input
- **Timestamp**: 2026-03-26T21:50:38 to 21:51:35
- **Changes**: userActionReadyDepths=2, maxDepth=40.
- **File**: `scenarios/kcp4_tuning-v11.json`
- **Result**: 584 total states, 8 end-states (2 converged, 6 aborted), ~57 sec wall
- **Bug reproduced**: YES

### v12 -- maxDepth=38, early input (minimum viable depth)
- **Timestamp**: 2026-03-26T21:51:37 to 21:52:35
- **Changes**: userActionReadyDepths=2, maxDepth=38.
- **File**: `scenarios/kcp4_tuning-v12.json`
- **Result**: 594 total states, 14 end-states (2 converged, 12 aborted), ~58 sec wall
- **Bug reproduced**: YES -- minimum maxDepth that works with userActionReadyDepths=2

### v13 -- maxDepth=36 (below minimum)
- **Timestamp**: 2026-03-26T21:53:23 to 21:54:20
- **Changes**: userActionReadyDepths=2, maxDepth=36.
- **File**: `scenarios/kcp4_tuning-v13.json`
- **Result**: 579 total states, 17 end-states (0 converged, 17 aborted), ~57 sec wall
- **Bug reproduced**: NO

### v14 -- permuteDepthRange {0,2} (narrowest window)
- **Timestamp**: 2026-03-26T21:54:22 to 21:55:20
- **Changes**: userActionReadyDepths=2, maxDepth=40, permuteDepthRange {min:0, max:2}.
- **File**: `scenarios/kcp4_tuning-v14.json`
- **Result**: 584 total states, 8 end-states (2 converged, 6 aborted), ~58 sec wall
- **Bug reproduced**: YES -- confirms divergence is at step 0-1 only

---

## Analysis

### What matters for reproducing this bug:
1. **LogicalClusterController and APIExportEndpointSliceController** must be permuted (2 of 6 original controllers)
2. **All environment objects required**: Shard, Partition, and APIExportEndpointSlice cannot be removed (v5 proved this)
3. **External input timing**: userActionReadyDepths=2 is optimal. 1 is too early (doesn't allow divergence), 5 (original) works but slower
4. **Minimum maxDepth**: 38 (with userActionReadyDepths=2). Below 36 all paths abort before converging

### Wall time breakdown:
- Go compilation overhead: ~35 sec (can be eliminated with precompiled binary)
- Exploration time: ~23 sec for the best variant (v7)
- Total with compilation: ~58 sec
- Total without compilation: ~23 sec (estimated)

### Reduction from baseline:
- Controllers: 6 -> 2 (67% reduction)
- maxDepth: 60 -> 38-45 (37-25% reduction)
- Total states explored: 9216 -> ~584 (94% reduction)
- Wall time: ~34 min -> ~58 sec (97% reduction)
- End states: many -> 4 (2 converged + 2 aborted) in v7

### Recommended configuration (v7):
```json
{
  "maxDepth": 45,
  "permuteControllers": [
    "LogicalClusterController",
    "APIExportEndpointSliceController"
  ],
  "userActionReadyDepths": {"0": 2}
}
```

For tightest possible config (v12):
```json
{
  "maxDepth": 38,
  "permuteControllers": [
    "LogicalClusterController",
    "APIExportEndpointSliceController"
  ],
  "userActionReadyDepths": {"0": 2}
}
```
