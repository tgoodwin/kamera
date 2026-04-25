# Coverage Curve & Agent Tuning Experiments

**Date:** 2026-03-26

## Coverage Curve Data

Ran existing scenarios for KRO and KCP to capture per-step coverage counters (Total States, Unique States, Unique Resource States) from simulator logs. Plotting script: `scripts/plot_coverage.py`.

### Results

| Scenario | Total | Unique S | Unique R | R/S | Pruning (S/Total) | Wall time | Converged | Bug? |
|---|---|---|---|---|---|---|---|---|
| KRO k1 ordering | 1,264 | 1,070 | 57 | 5.3% | 84.7% | ~2s | 1 | no |
| KRO k3 staleness | 3,662 | 1,256 | 72 | 5.7% | 34.3% | ~4s | 2 | yes |
| KCP kcp1 init-ordering | 1,214 | 1,186 | 64 | 5.4% | 97.7% | ~2m | 1 | no |
| KCP kcp1b all-ordering | 6,265 | 5,940 | 193 | 3.3% | 94.8% | ~10m | 1 | no |
| KCP kcp2 multi-consumer | 5,813 | 5,526 | 200 | 3.6% | 95.1% | ~9m | 1 | no |
| KCP kcp4 late-APIExport | 9,216 | 8,930 | 254 | 2.8% | 96.9% | ~34m | 2 | yes |
| KCP kcp7 APIExport-deletion | 13,712 | 13,342 | 355 | 2.7% | 97.3% | ~117m* | 19 | yes |
| KCP kcp17 ordering-8ctrl | 15,298 | 14,292 | 416 | 2.9% | 93.4% | ~117m* | 2 | yes |

*interrupted at ~2hr mark; DFS did not complete

### Key Observations

1. **R/S ratio is consistently low (2.7-5.7%)**: Most unique states S differ only in Q or L (pending reconciliations and cache states), not in R (actual resource contents). The interleaving and cache permutations produce many distinct execution states but few distinct resource outcomes.

2. **Pruning is highly effective for ordering**: KCP ordering scenarios achieve 93-97% unique/total ratios. Staleness perturbation (KRO k3) is much less efficient at 34%.

3. **KCP controllers are ~100x slower per step than KRO**: KRO completes 3,662 steps in 4 seconds; KCP takes ~2 hours for 15,298 steps. Per-step cost dominates wall time.

4. **Exhaustive exploration hits scalability limits**: kcp7 and kcp17 were interrupted after 2 hours with the DFS incomplete. The `ContentsHash()` serialization cost grows with state size, causing individual steps to take minutes in the tail of the DFS.

### Data Files

- `kro/kro-k1-log.txt`, `kro/kro-k3-log.txt` -- KRO scenario logs
- `kcp/kcp-k1-log.txt` through `kcp/kcp-k17-log.txt` -- KCP scenario logs
- `kcp/kcp-k4-dump.json`, `kcp/kcp-k7-dump.json`, `kcp/kcp-k17-dump.json` -- exploration dumps for bug-finding scenarios

## Agent Tuning Experiments

**Question:** How long does an autonomous agent take to minimize a bug-reproducing scenario's perturbation scope?

### Setup

- **Starting point:** kcp17_ordering-8ctrl.json (8 controllers permuted, exhaustive DFS took 2+ hours incomplete)
- **Known bug:** 2 converged states, paths diverge at step 21 between LogicalClusterCleanupController and APIBindingReconcilerController
- **Method:** Launch an autonomous agent that iteratively creates scenario variants, runs them, observes results, and adjusts. No human intervention during iteration.

### Result

| Metric | Exhaustive (original) | Agent-tuned (v12) |
|---|---|---|
| Controllers permuted | 8 | 2 |
| Environment objects | 9 | 6 |
| Total states explored | 15,298 (incomplete) | 275 |
| Converged states | 2 | 2 |
| Wall time | ~2 hours (incomplete) | 27.5 seconds |
| Bug reproduced | yes | yes |

**Time to first bug reproduction: ~3 minutes** (v1, 4 controllers, 1,182 states, 116s run)
**Time to minimal reproduction: ~19 minutes** (v11, 2 controllers, 275 states, 27s run)
**Total experiment time: ~22 minutes, 13 iterations**

### What the Agent Learned

1. **Controller selection is critical**: Identifying the two controllers involved in the divergence (LogicalClusterCleanupController + APIBindingReconcilerController) was the biggest lever, cutting states from 1,182 to 334.

2. **permuteDepthRange had zero effect**: The branching happens at a single deterministic point; constraining the depth window doesn't change the exploration.

3. **Environment minimization helped**: Removing 3 non-essential objects (Shard, Partition, EndpointSlice) cut states from 334 to 275 and wall time from ~33s to ~27s.

4. **maxDepth and Monte Carlo mode were irrelevant** once the controller set was minimal.

### Winning Configuration

```json
"tuning": {
  "maxDepth": 34,
  "permuteControllers": [
    "LogicalClusterCleanupController",
    "APIBindingReconcilerController"
  ]
}
```

### Detailed Log

See `kcp/tuning-experiment-log.md` for per-iteration timestamps, configs, and results.

---

## Agent Tuning Experiment (KCP7)

**Question:** Same as above, applied to a different (more severe) bug.

### Setup

- **Starting point:** kcp7_apiexport-deletion.json (6 controllers permuted, exhaustive DFS took 2+ hours incomplete, 19 converged states)
- **Known bug:** 19 converged states (determinism violation), paths diverge at step 2 between LogicalClusterController and DefaultAPIBindingLifecycleController. Has an external input (APIExport deletion) firing at depth 8.
- **Method:** Same autonomous agent approach, no human intervention.

### Result

| Metric | Exhaustive (original) | Agent-tuned (v10) |
|---|---|---|
| Controllers permuted | 6 | 2 |
| Environment objects | 9 | 6 |
| Total states explored | 13,712 (incomplete) | 549 |
| Converged states | 19 | 2 |
| Wall time | ~2 hours (incomplete) | 49 seconds |
| Bug reproduced | yes | yes |

**Time to first bug reproduction: ~3 minutes** (v1, 2 controllers, 1,258 states, 118s run)
**Time to minimal reproduction: ~12 minutes** (v10, 2 controllers, 549 states, 49s run)
**Total experiment time: ~26 minutes, 17 iterations**

Note: The 30-second target was not met for kcp7 (best: 49s). The bottleneck is memory-bound state exploration, not CPU. The per-step cost (~89ms) is dominated by reconciler deep-copy and execution. The agent identified this as a runtime optimization issue, not a tuning issue.

### What the Agent Learned

1. **549 states is the irreducible floor** for 2-controller exhaustive exploration of this scenario
2. **maxDepth=18 is the exact minimum** for convergence (17 loses a converged state)
3. **Environment trimming shortened converged paths from 23 to 18 steps** (the key optimization)
4. **permuteDepthRange and permuteAfterEvent had zero effect** (same as kcp17)

### Detailed Log

See `kcp/tuning-experiment-kcp7-log.md` for per-iteration timestamps, configs, and results.

---

---

## Agent Tuning Experiment (KCP4)

**Question:** Same as above, applied to a bug whose exhaustive run DID complete (34 min).

### Setup

- **Starting point:** kcp4_late-apiexport.json (6 controllers permuted, exhaustive DFS completed in ~34 min, 9,216 states, 2 converged states)
- **Known bug:** 2 converged states, paths diverge at step 1 between LogicalClusterController and APIExportEndpointSliceController. Has an external input (late APIExport arrival) firing at depth 5.
- **Method:** Same autonomous agent approach, no human intervention.

### Result

| Metric | Exhaustive (original) | Agent-tuned (v7) |
|---|---|---|
| Controllers permuted | 6 | 2 |
| Environment objects | 7 | 7 (all required) |
| Total states explored | 9,216 | 584 |
| Converged states | 2 | 2 |
| Wall time | ~34 min | ~58 seconds |
| Bug reproduced | yes | yes |

**Time to first bug reproduction: ~1 minute** (v1, 2 controllers, 2 converged states)
**Time to minimal reproduction: ~10 minutes** (v7, 584 states, 58s run)
**Total experiment time: ~26 minutes, 14 iterations**

### What the Agent Learned

1. **Only 2 of 6 controllers needed**: LogicalClusterController + APIExportEndpointSliceController
2. **All 7 environment objects are required**: Unlike kcp17/kcp7, removing objects kills a divergent path
3. **External input timing matters**: Moving from depth 5 to depth 2 reduced aborted paths; depth 1 is too early
4. **permuteDepthRange {0, 2} works**: Confirms divergence is in steps 0-1 only

### Detailed Log

See `kcp/tuning-experiment-kcp4-log.md` for per-iteration timestamps, configs, and results.

---

## Cross-Experiment Comparison

| | KCP4 | KCP17 | KCP7 |
|---|---|---|---|
| Original bug severity | 2 converged states | 2 converged states | 19 converged states |
| Original controllers permuted | 6 | 8 | 6 |
| Exhaustive wall time | ~34 min (complete) | ~2 hrs (incomplete) | ~2 hrs (incomplete) |
| Time to first reproduction | **~1 min** | ~3 min | ~3 min |
| Time to minimal reproduction | ~10 min | ~19 min | ~12 min |
| Total tuning time | ~26 min (14 iter) | ~22 min (13 iter) | ~26 min (17 iter) |
| Minimal states | 584 | 275 | 549 |
| Minimal wall time | 58s | 27.5s | 49s |
| Speedup vs exhaustive | 35x | >260x | >100x |

All three experiments reproduced the bug on the **first or second attempt** (1-3 minutes). Minimal reproductions were found in **10-19 minutes**. The tuning process consistently takes **under 30 minutes** for KCP scenarios, regardless of the original exhaustive exploration time.

Key pattern: the agent always starts by identifying the 2 controllers involved in the divergence (from the dump analysis), which immediately produces a massive reduction. The remaining iterations optimize secondary factors (environment size, maxDepth, input timing).

---

## Artifact References

### Coverage Curve Data
- KRO logs: `kro/kro-k1-log.txt`, `kro/kro-k3-log.txt`
- KCP logs: `kcp/kcp-k1-log.txt` through `kcp/kcp-k17-log.txt`
- KCP dumps: `kcp/kcp-k4-dump.json`, `kcp/kcp-k7-dump.json`, `kcp/kcp-k17-dump.json`
- Plotting script: `scripts/plot_coverage.py`

### KCP17 Tuning Experiment
- Experiment log: `kcp/tuning-experiment-log.md`
- Original scenario: `/Users/tgoodwin/projects/kcp/kamera/scenarios/kcp17_ordering-8ctrl.json`
- Winning variant: `/Users/tgoodwin/projects/kcp/kamera/scenarios/kcp17_tuning-v12.json`
- All variants: `/Users/tgoodwin/projects/kcp/kamera/scenarios/kcp17_tuning-v*.json`
- Run dumps and logs: `kcp/tuning-runs/kcp17-tuning-v{N}-dump.json` and `kcp17-tuning-v{N}-log.txt`

### KCP7 Tuning Experiment
- Experiment log: `kcp/tuning-experiment-kcp7-log.md`
- Original scenario: `/Users/tgoodwin/projects/kcp/kamera/scenarios/kcp7_apiexport-deletion.json`
- Winning variant: `/Users/tgoodwin/projects/kcp/kamera/scenarios/kcp7_tuning-v10.json`
- All variants: `/Users/tgoodwin/projects/kcp/kamera/scenarios/kcp7_tuning-v*.json`
- Run dumps and logs: `kcp/tuning-runs/kcp7-tuning-v{N}-dump.json` and `kcp7-tuning-v{N}-log.txt`

### KCP4 Tuning Experiment
- Experiment log: `kcp/tuning-experiment-kcp4-log.md`
- Original scenario: `/Users/tgoodwin/projects/kcp/kamera/scenarios/kcp4_late-apiexport.json`
- Winning variant: `/Users/tgoodwin/projects/kcp/kamera/scenarios/kcp4_tuning-v7.json`
- All variants: `/Users/tgoodwin/projects/kcp/kamera/scenarios/kcp4_tuning-v*.json`
- Run dumps and logs: `kcp/tuning-runs/kcp4-tuning-v{N}-dump.json` and `kcp4-tuning-v{N}-log.txt`
