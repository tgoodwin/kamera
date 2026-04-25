# KCP17 Perturbation Tuning Experiment

**Start time:** 2026-03-26T19:26:30-0700

**Goal:** Achieve 2+ converged states in under 30 seconds by tuning the perturbation parameters for kcp17.

**Base scenario:** `kcp17_ordering-8ctrl.json` (8 controllers, maxDepth=50)

**Known bug characteristics:**
- Two converged state paths diverge at step 21
- State 0 picks LogicalClusterCleanupController, State 1 picks APIBindingReconcilerController
- Shortest converged path: 33 steps
- Both paths share identical first 20 steps

---

## Iterations

### v1 — 4 controllers, maxDepth=36
- **Timestamp:** 19:27:46 - 19:29:42
- **Changes:** Reduced from 8 to 4 controllers (LogicalClusterController, APIBindingReconcilerController, APIBinderInitializerController, DefaultAPIBindingLifecycleController), maxDepth 50->36
- **Result:** 2 converged states (2famtzb3, 1i5we6fu), 1182 total states, ~116s (interrupted at timeout)
- **Bug reproduced:** YES
- **Assessment:** Too slow; still too many states from 4-controller permutation

### v2 — 4 controllers, maxDepth=36, permuteDepthRange 18-25
- **Timestamp:** 19:30:40 - 19:32:39
- **Changes:** Added permuteDepthRange {min:18, max:25} to focus permutation around divergence
- **Result:** 2 converged states, 1213 total states, ~118s (interrupted)
- **Bug reproduced:** YES
- **Assessment:** permuteDepthRange didn't help with 4 controllers; state explosion too high

### v3 — 2 controllers (LogicalClusterController + APIBindingReconcilerController), permuteDepthRange 18-25
- **Timestamp:** 19:33:06 - 19:34:20
- **Changes:** Reduced to 2 controllers, but used LogicalClusterController (not the correct diverging controller)
- **Result:** 2 converged states, 723 total states, ~74s
- **Bug reproduced:** YES
- **Assessment:** Significant improvement (723 vs 1200 states) but wrong controller; ~74s

### v4 — 2 correct controllers (LogicalClusterCleanupController + APIBindingReconcilerController), permuteDepthRange 18-25
- **Timestamp:** 19:35:10 - 19:35:43
- **Changes:** Switched to LogicalClusterCleanupController (the actual diverging controller from bug analysis)
- **Result:** 2 converged states (1i5we6fu, 2famtzb3), 334 total states, ~33s
- **Bug reproduced:** YES
- **Assessment:** 334 states is much better; ~33s is close to target but not under 30s

### v5 — Same as v4 but tighter depth range 19-23
- **Timestamp:** 19:36:10 - 19:36:44
- **Changes:** Tightened permuteDepthRange from {18,25} to {19,23}
- **Result:** 2 converged states, 334 total states, ~34s
- **Bug reproduced:** YES
- **Assessment:** No change in state count; depth range tightening has no effect

### v6 — maxDepth=34 (tighter), depth range 19-23
- **Timestamp:** 19:39:05 - 19:39:39
- **Changes:** maxDepth 36->34
- **Result:** 2 converged states, 334 total states, ~34s
- **Bug reproduced:** YES
- **Assessment:** maxDepth reduction doesn't help; all paths already converge before depth 34

### v7 — permuteDepthRange 20-22 (minimal range)
- **Timestamp:** 19:40:05 - 19:40:38
- **Changes:** Tightened depth range to just 20-22
- **Result:** 2 converged states, 334 total states, ~33s
- **Bug reproduced:** YES
- **Assessment:** Confirms depth range irrelevant; branching happens at exactly one step

### v8 — Monte Carlo mode, 20 trials
- **Timestamp:** 19:42:03 - 19:42:36
- **Changes:** Added search.mode="monte_carlo" with 20 trials
- **Result:** 2 converged states, 334 total states, ~33s
- **Bug reproduced:** YES
- **Assessment:** MC sampling doesn't reduce state space with only 2 controllers

### v9 — maxDepth=33 (exact shortest path)
- **Timestamp:** 19:43:03 - 19:43:36
- **Changes:** maxDepth 34->33
- **Result:** 2 converged states, 334 total states, ~33s
- **Bug reproduced:** YES
- **Assessment:** No change; 334 states is the irreducible minimum for this controller set + environment

### v10 — Monte Carlo 5 trials
- **Timestamp:** 19:44:12 - 19:44:45
- **Changes:** MC trials reduced from 20 to 5
- **Result:** 2 converged states, 334 total states, ~33s
- **Bug reproduced:** YES
- **Assessment:** MC with fewer trials still explores full space for 2-controller case

### v11 — Minimal environment (removed Shard, Partition, EndpointSlice) [SUCCESS]
- **Timestamp:** 19:45:20 - 19:45:47 (confirmed 27.2s on second run)
- **Changes:** Removed Shard, Partition, and APIExportEndpointSlice objects from environment state
- **Result:** 2 converged states (1qip67x0, 3rh6yxoz), 275 total states, **27.2s**
- **Bug reproduced:** YES (different hashes due to fewer objects in final state, but same divergence pattern)
- **Assessment:** SUCCESS - under 30 seconds

### v12 — Minimal env, no permuteDepthRange (simplest winning config)
- **Timestamp:** 19:47:30 - 19:47:57
- **Changes:** Removed permuteDepthRange entirely (confirmed it has no effect)
- **Result:** 2 converged states (1qip67x0, 3rh6yxoz), 275 total states, **27.5s**
- **Bug reproduced:** YES
- **Assessment:** SUCCESS - confirms depth range is unnecessary

### v13 — Even smaller env (removed root:provider LogicalCluster too)
- **Timestamp:** 19:48:15 - 19:48:42
- **Changes:** Also removed root:provider LogicalCluster (5 objects total)
- **Result:** 2 converged states (3r84wniq, ij3cisr2), 275 total states, **27.5s**
- **Bug reproduced:** YES
- **Assessment:** Same state count; removing more objects doesn't help further. Floor is ~275 states / ~27s.

---

## Key Findings

1. **Controller selection is critical:** Switching from LogicalClusterController to LogicalClusterCleanupController (the actual diverging controller) cut states from 723 to 334.

2. **permuteDepthRange has zero effect** when the branching happens at a single deterministic depth point. All variants from v4-v10 produced exactly 334 total states regardless of depth range settings.

3. **Monte Carlo mode is equivalent to exhaustive search** with only 2 permuted controllers.

4. **Environment size is the key lever for per-step cost.** Removing 3 non-essential objects (Shard, Partition, EndpointSlice) reduced states from 334 to 275 AND reduced per-step cost, bringing total wall time from ~33s to ~27s.

5. **maxDepth has no effect** once set above the convergence depth (~33 steps).

## Winning Configuration (v12 -- simplest)

```json
{
  "tuning": {
    "maxDepth": 34,
    "permuteControllers": [
      "LogicalClusterCleanupController",
      "APIBindingReconcilerController"
    ]
  }
}
```
With a minimal environment containing only: 3 LogicalClusters, WorkspaceType, APIExport, APIBinding (no Shard, Partition, or EndpointSlice).

**Scenario file:** `kcp17_tuning-v12.json`
**Performance:** 2 converged states, 275 total states, 27.5s wall time

## Milestones

| Milestone | Variant | Timestamp | Elapsed from start | States | Wall time per run |
|---|---|---|---|---|---|
| Experiment start | -- | 19:26:30 | 0m | -- | -- |
| First bug reproduction | v1 | 19:29:42 | **~3 minutes** | 1,182 | ~116s |
| Minimal state count (275) | v11 | 19:45:47 | **~19 minutes** | 275 | ~27s |
| Simplest winning config | v12 | 19:47:57 | ~21 minutes | 275 | ~27.5s |
| Experiment end | v13 | 19:48:42 | ~22 minutes | 275 | ~27.5s |

**Time to first reproduction:** ~3 minutes (includes Go compilation + 116s run)
**Time to minimal reproduction (275 states, <30s):** ~19 minutes
**Total experiment wall time:** ~22 minutes (19:26:30 - 19:48:42)

---

## Artifact References

### Scenario Variants

All variant scenario files live in the KCP harness directory (symlinked from `examples/kcp`):

| Variant | File | Controllers | Key change |
|---|---|---|---|
| v1 | `/Users/tgoodwin/projects/kcp/kamera/scenarios/kcp17_tuning-v1.json` | 4 | Reduced from 8 to 4 controllers |
| v2 | `.../kcp17_tuning-v2.json` | 4 | Added permuteDepthRange 18-25 |
| v3 | `.../kcp17_tuning-v3.json` | 2 | LogicalClusterController + APIBindingReconcilerController |
| v4 | `.../kcp17_tuning-v4.json` | 2 | **LogicalClusterCleanupController** + APIBindingReconcilerController |
| v5-v10 | `.../kcp17_tuning-v5.json` through `v10` | 2 | Depth range / maxDepth / MC variations |
| v11 | `.../kcp17_tuning-v11.json` | 2 | Minimal environment (removed Shard, Partition, EndpointSlice) |
| **v12** | **`.../kcp17_tuning-v12.json`** | **2** | **Winning config: minimal env, no depth range** |
| v13 | `.../kcp17_tuning-v13.json` | 2 | Further env reduction (removed provider LogicalCluster) |

### Run Dumps and Logs

All run artifacts are saved in `experiments/coverage-curves/kcp/tuning-runs/`:

- `kcp17-tuning-v{N}-dump.json` -- exploration dump (converged states, paths, stats)
- `kcp17-tuning-v{N}-log.txt` -- simulator log output (coverage counters per step)

### Original Exhaustive Run

- Log: `experiments/coverage-curves/kcp/kcp-k17-log.txt` (15,298 steps, interrupted at ~2hr)
- Dump: `experiments/coverage-curves/kcp/kcp-k17-dump.json` (2 converged states from partial exploration)
