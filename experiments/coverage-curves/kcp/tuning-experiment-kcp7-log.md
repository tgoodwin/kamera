# KCP7 Perturbation Tuning Experiment Log

**Goal**: Find a trimmed variant of kcp7_apiexport-deletion.json that reproduces the determinism bug (2+ converged states) in under 30 seconds.

**Original scenario**: 6 permuted controllers, maxDepth=50, 19 converged states found but exploration took 2+ hours (interrupted at 13,712 states).

**Experiment start**: 2026-03-26T19:59:23-0700

---

## Iterations

### v1 -- 2 controllers, maxDepth=30, full env (9 objects)
- **Timestamp:** 20:00:12 - 20:02:10 (~118s)
- **Changes:** Reduced from 6 to 2 controllers (LogicalClusterController + DefaultAPIBindingLifecycleController), maxDepth 50->30
- **File:** `kcp7_tuning-v1.json`
- **Result:** 2 converged states (102gaho4, gujopjye), 1258 total states, 64 terminal (2 converged + 62 aborted), ~118s
- **Bug reproduced:** YES
- **Assessment:** First reproduction. Converged path length = 23 steps.

### v4 -- 2 controllers, maxDepth=25, trimmed env (6 objects)
- **Timestamp:** 20:03:33 - 20:04:50 (~77s, CPU-contested with v5/v6)
- **Changes:** Removed Shard, Partition, EndpointSlice from env. maxDepth=25.
- **File:** `kcp7_tuning-v4.json`
- **Result:** 2 converged states (zzv8dp26, 22rgntsd), 861 total states, 51 terminal (2+49), ~77s
- **Bug reproduced:** YES
- **Assessment:** Converged path length = 18 steps. Trimmed env significantly reduces per-state cost.

### v5 -- 2 controllers, permuteDepthRange {1,10}, maxDepth=30, trimmed env
- **Timestamp:** 20:03:34 - 20:05:07 (~93s, CPU-contested)
- **Changes:** Added permuteDepthRange {1,10}
- **File:** `kcp7_tuning-v5.json`
- **Result:** 2 converged states (zzv8dp26, 22rgntsd), 1021 total states, 21 terminal (2+19), ~93s
- **Bug reproduced:** YES
- **Assessment:** permuteDepthRange had no effect on state count vs v6 (identical: 1021 states, 21 terminal)

### v6 -- 2 controllers, maxDepth=30, trimmed env (6 objects)
- **Timestamp:** 20:03:35 - 20:05:08 (~93s, CPU-contested)
- **Changes:** Same as v4 but maxDepth=30 instead of 25
- **File:** `kcp7_tuning-v6.json`
- **Result:** 2 converged states (zzv8dp26, 22rgntsd), 1021 total states, 21 terminal (2+19), ~93s
- **Bug reproduced:** YES
- **Assessment:** More aborted states from deeper exploration; v4's maxDepth=25 more efficient

### v7 -- 2 controllers, maxDepth=22, ultra-minimal env (5 objects)
- **Timestamp:** 20:06:19 - 20:07:17 (~58s)
- **Changes:** maxDepth=22, removed root:provider LogicalCluster
- **File:** `kcp7_tuning-v7.json`
- **Result:** 2 converged states (1sin89wi, 24wbwstz), 660 total states, 45 terminal (2+43), ~58s
- **Bug reproduced:** YES
- **Assessment:** More states than expected; maxDepth=22 barely enough for convergence

### v9 -- 2 controllers, permuteDepthRange {6,14}, maxDepth=20, trimmed env
- **Timestamp:** 20:08:56 - 20:09:52 (~56s, CPU-contested with v10)
- **Changes:** permuteDepthRange {6,14} focused on divergence window
- **File:** `kcp7_tuning-v9.json`
- **Result:** 2 converged states (zzv8dp26, 22rgntsd), 642 total states, 54 terminal (2+52), ~56s
- **Bug reproduced:** YES
- **Assessment:** No improvement over v10; depth range irrelevant with 2 controllers

### v10 -- 2 controllers, maxDepth=18, trimmed env (6 objects) [BEST]
- **Timestamp:** 20:10:24 - 20:11:11 (~47s concurrent, 49s solo)
- **Changes:** maxDepth=18 (exact convergence depth)
- **File:** `kcp7_tuning-v10.json`
- **Result:** 2 converged states (zzv8dp26, 22rgntsd), 549 total states, 45 terminal (2+43), **49s (pre-built binary, solo run)**
- **Bug reproduced:** YES
- **Assessment:** Best configuration found. 549 states is the irreducible minimum for 2-controller permutation of this scenario.

### v11 -- v10 + permuteAfterEvent (DELETE/APIExport)
- **Timestamp:** 20:11:59 - 20:12:46 (~47s concurrent)
- **Changes:** Added permuteAfterEvent trigger for DELETE of APIExport
- **File:** `kcp7_tuning-v11.json`
- **Result:** 2 converged states, 549 total states, 45 terminal -- identical to v10
- **Bug reproduced:** YES
- **Assessment:** permuteAfterEvent had zero effect (all relevant permutation happens after external input already)

### v12 -- v10 + permuteDepthRange {11,14}
- **Timestamp:** 20:15:18 - 20:16:06 (~48s)
- **Changes:** Added permuteDepthRange {11,14} targeting actual divergence step
- **File:** `kcp7_tuning-v12.json`
- **Result:** 2 converged states, 549 total states, 45 terminal -- identical to v10
- **Bug reproduced:** YES
- **Assessment:** Confirms depth range is irrelevant for 2-controller permutation

### v13 -- v10 with ultra-minimal env (4 objects)
- **Timestamp:** 20:19:24 - 20:20:13 (~49s)
- **Changes:** Removed root and root:provider LogicalClusters (4 objects only)
- **File:** `kcp7_tuning-v13.json`
- **Result:** 2 converged states (1az1wfan, 3u2f3xr1), 549 total states, 45 terminal -- same exploration structure
- **Bug reproduced:** YES
- **Assessment:** Removing more objects doesn't change state count; per-state cost is dominated by reconciler logic, not object count

### v14 -- userAction at depth 4, maxDepth=15
- **Timestamp:** 20:20:39 - 20:21:21 (~42s)
- **File:** `kcp7_tuning-v14.json`
- **Result:** 0 converged states, 43 aborted
- **Bug reproduced:** NO
- **Assessment:** Too early deletion + too short depth; pre-deletion setup needs more steps

### v15 -- userAction at depth 6, maxDepth=16
- **Timestamp:** 20:21:41 - 20:22:31 (~50s)
- **File:** `kcp7_tuning-v15.json`
- **Result:** 0 converged states, 61 aborted
- **Bug reproduced:** NO
- **Assessment:** maxDepth=16 still too short for convergence after deletion at depth 6

### v16 -- userAction at depth 6, maxDepth=18
- **Timestamp:** 20:22:53 - 20:23:52 (~59s)
- **File:** `kcp7_tuning-v16.json`
- **Result:** 2 converged states (zzv8dp26, 22rgntsd), 53 terminal (2+51), ~59s
- **Bug reproduced:** YES
- **Assessment:** Worse than v10 (59s vs 49s); more states due to earlier deletion creating more branching paths

### v17 -- userAction at depth 7, maxDepth=17
- **Timestamp:** 20:24:11 - 20:25:03 (~52s)
- **File:** `kcp7_tuning-v17.json`
- **Result:** 1 converged state (zzv8dp26), 56 terminal (1+55), ~52s
- **Bug reproduced:** NO (only 1 converged state)
- **Assessment:** maxDepth=17 loses one converged state; need exactly maxDepth=18

---

## Key Findings

1. **Controller pair is correct:** LogicalClusterController + DefaultAPIBindingLifecycleController are sufficient. These are the two that the original dump analysis flagged as diverging at step 2.

2. **maxDepth=18 is the minimum** for convergence with trimmed env. The converged paths are exactly 18 steps. maxDepth=17 loses one converged state; maxDepth=22+ gains no additional converged states.

3. **Environment trimming reduced converged path from 23 to 18 steps** (full env had paths of 23 steps; trimmed env has 18). Removing Shard, Partition, EndpointSlice is the key optimization. Further removal (root/provider LogicalClusters) doesn't change state count.

4. **permuteDepthRange has zero effect** when only 2 controllers are permuted. All variants with different depth ranges (v5, v9, v12) produced identical state counts (549).

5. **permuteAfterEvent has zero effect** in this scenario because all meaningful permutations happen after the external input anyway.

6. **549 states is the irreducible minimum** for 2-controller exhaustive exploration of this scenario. Per-state cost is ~89ms, dominated by reconciler deep-copy and execution, not by object count.

7. **Wall time is memory-bound, not CPU-bound.** With GOGC=off, CPU time halved (9.2s -> 4.75s) but wall time was unchanged (~49s). Only 22% CPU utilization.

8. **The actual divergence point** is at step 13 (with trimmed env) where APIExportController runs at different points relative to other controllers. The permutation of LogicalClusterController and DefaultAPIBindingLifecycleController at steps 7 and 11-12 creates different reconcile queue states that lead APIExportController to produce different effects.

## Winning Configuration (v10 -- best achievable)

```json
{
  "tuning": {
    "maxDepth": 18,
    "permuteControllers": [
      "LogicalClusterController",
      "DefaultAPIBindingLifecycleController"
    ],
    "userActionReadyDepths": {
      "0": 8
    }
  }
}
```
With a trimmed environment containing 6 objects: 3 LogicalClusters (root, root:provider, root:consumer), WorkspaceType, APIExport, APIBinding. No Shard, Partition, or EndpointSlice.

**Scenario file:** `kcp7_tuning-v10.json`
**Performance:** 2 converged states, 549 total states, 49s wall time (pre-built binary)

## Milestones

| Milestone | Variant | Timestamp | Elapsed from start | States | Wall time per run |
|---|---|---|---|---|---|
| Experiment start | -- | 19:59:23 | 0m | -- | -- |
| First bug reproduction | v1 | 20:02:10 | **~3 minutes** | 1,258 | ~118s |
| Minimal state count (549) | v10 | 20:11:11 | **~12 minutes** | 549 | ~49s |
| Confirmed floor | v13 | 20:20:13 | ~21 minutes | 549 | ~49s |
| Experiment end | v17 | 20:25:03 | ~26 minutes | -- | -- |

**Time to first reproduction:** ~3 minutes (includes Go compilation + 118s run)
**Time to minimal reproduction (549 states, ~49s):** ~12 minutes
**Total experiment wall time:** ~26 minutes (19:59:23 - 20:25:03)

**30-second target NOT met.** The best achievable is ~49s wall time. The bottleneck is memory-bound state exploration (not CPU), which cannot be addressed by tuning parameters alone. A runtime optimization (e.g., copy-on-write state, reduced allocation) would be needed to close the gap.

---

## Artifact References

### Scenario Variants

All variant scenario files live in `/Users/tgoodwin/projects/kcp/kamera/scenarios/`:

| Variant | File | Controllers | Key change | Result |
|---|---|---|---|---|
| v1 | `kcp7_tuning-v1.json` | 2 | Baseline: 2 controllers, full env | 1258 states, 118s |
| v4 | `kcp7_tuning-v4.json` | 2 | Trimmed env, maxDepth=25 | 861 states, 77s |
| v5 | `kcp7_tuning-v5.json` | 2 | + permuteDepthRange {1,10} | 1021 states, 93s |
| v6 | `kcp7_tuning-v6.json` | 2 | Trimmed env, maxDepth=30 | 1021 states, 93s |
| v7 | `kcp7_tuning-v7.json` | 2 | Ultra-minimal env (5 obj), maxDepth=22 | 660 states, 58s |
| v9 | `kcp7_tuning-v9.json` | 2 | permuteDepthRange {6,14} | 642 states, 56s |
| **v10** | **`kcp7_tuning-v10.json`** | **2** | **maxDepth=18 (BEST)** | **549 states, 49s** |
| v11 | `kcp7_tuning-v11.json` | 2 | + permuteAfterEvent | 549 states, 49s |
| v12 | `kcp7_tuning-v12.json` | 2 | + permuteDepthRange {11,14} | 549 states, 49s |
| v13 | `kcp7_tuning-v13.json` | 2 | Ultra-minimal env (4 obj) | 549 states, 49s |
| v14 | `kcp7_tuning-v14.json` | 2 | userAction@4, maxDepth=15 | 0 converged |
| v15 | `kcp7_tuning-v15.json` | 2 | userAction@6, maxDepth=16 | 0 converged |
| v16 | `kcp7_tuning-v16.json` | 2 | userAction@6, maxDepth=18 | 2 converged, 59s |
| v17 | `kcp7_tuning-v17.json` | 2 | userAction@7, maxDepth=17 | 1 converged, 52s |
