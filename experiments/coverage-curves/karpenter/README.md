# Karpenter D12: Exhaustive vs Agent Tuning

## Bug Summary

D12 ("Emptiness disruption deletes node with active workload") is a TOCTOU race
in Karpenter's disruption controller. The disruption controller evaluates whether
a node is empty via a field-selector pod query. If it reads a stale view that
misses a recently-bound pod, it incorrectly marks the node as empty and deletes
the NodeClaim, destroying the node the pod was just scheduled to.

D12 triggers via three perturbation dimensions: ordering, staleness, and external
events (pod binding).

## Exhaustive Configuration

The exhaustive scenario (`d12_exhaustive.json`) is a cross-product of two
dimensions that the closed-loop pipeline cannot auto-vary together:

1. **User action ready depth** (0 to 119): the depth at which the pod binding
   external event fires. This controls when the kube-scheduler assigns the
   workload pod to the node relative to the disruption controller's emptiness
   check.

2. **Staleness intervals**: auto-derived per variant by the closed-loop pipeline
   from the reference trace. For each (controller, kind) read pair observed in
   the reference, the pipeline enumerates every (staleAt, catchUpAt) window and
   runs a single MC trial with that interval plus ordering permutation.

This produces 120 scenario variants (one per action depth). Each variant runs:
- 10 Monte Carlo reference trials (ordering permutation, no staleness)
- 1 rerun trial (ordering permutation from observed controllers)
- ~304 staleness interval trials (auto-derived, one per interval)

All 15 harness controllers are permuted. Monte Carlo is required because
Karpenter's `state.Cluster` singleton is stateful and does not support DFS
backtracking.

### Why Monte Carlo instead of DFS

Karpenter controllers share a singleton in-memory `state.Cluster` struct that
accumulates state across reconcile calls. The DFS explorer forks state by
snapshotting the replay client's object store, but cannot snapshot the
`state.Cluster` interior. The `OnFork`/`OnCrash` hooks reset it, but this means
DFS branches observe a clean cluster rather than the accumulated state from
prior steps. Monte Carlo avoids this by running each trial as an independent
single path from the initial state.

### Why sweep user action depth

The pod binding is an external event (simulating the kube-scheduler). Its timing
relative to the disruption controller's emptiness check determines whether the
race window is open. A hand-tuned scenario places it at a specific depth; the
exhaustive approach tries every depth so no timing window is missed.

## Running

```bash
cd examples/karpenter
GOARCH=arm64 go build -o /tmp/karpenter-harness .
/tmp/karpenter-harness \
  --inputs ../../experiments/coverage-curves/karpenter/d12_exhaustive.json \
  --output ../../experiments/coverage-curves/karpenter/exhaustive-output \
  --interactive=false \
  --timeout 7200s \
  --fuzz-cases 0 \
  --metrics-only-staleness \
  --parallel-processes
```

### Flags

- `--metrics-only-staleness`: Skip full JSONL dumps for staleness interval
  phases. Instead, record lightweight CSV metrics (duration, state counts,
  terminal hash). This eliminates the ~700KB-per-trial disk I/O bottleneck.
- `--parallel-processes`: Run each (variant, trial) pair as a separate child
  process using the pre-built binary. Required for Karpenter because the
  in-process runner is limited to `MaxParallel=1` (singleton state constraint).

## Output

Two types of output in `exhaustive-output/`:

1. **JSONL dump files** (reference + rerun phases): Full trace data with
   `campaignMetrics`, execution paths, and state hashes.
2. **CSV metric files** (`staleness_metrics_child_*.csv`): One per child
   process, containing lightweight per-trial metrics for staleness intervals.

### CSV schema

```
scenario_name, phase_name, duration_ns, total_states, resource_states,
terminal_hash, converged, reconciler, kind, stale_at, catch_up_at
```

### Plotting

```bash
python3 experiments/coverage-curves/karpenter/plot_d12_exhaustive.py \
  experiments/coverage-curves/karpenter/exhaustive-output
```

## Experiment Results (2026-03-29)

### Run 1: 15 controllers, 2h timeout

All 15 harness controllers permuted. Each child's staleness sweep generated
~719 intervals. None of 10 children completed within 2 hours. Each child
accumulated ~1 minute of CPU time in 2 hours of wall time, indicating the
per-trial exploration (with 15 controllers permuted) is extremely slow.

### Run 2: 7 controllers (D12-relevant), 2h timeout

Reduced to the 7 controllers from the original tuned D12 scenario. Each child's
staleness sweep is smaller but still took >2 hours. Children accumulated ~1-2
minutes of CPU time. No children completed their staleness sweep.

### Analysis

The exhaustive sweep is compute-bound, not I/O-bound. The `--metrics-only-staleness`
flag eliminates disk I/O but doesn't help because the bottleneck is the sequential
staleness exploration within each child process. Key numbers:

- ~300-700 staleness intervals auto-derived per variant (depends on controller count)
- Each interval = 1 Monte Carlo path to depth ~70-120
- Each path takes several seconds of wall time
- 10 MC trials per variant means the total per variant is 300-700 intervals per trial
- With 10 parallel children, throughput is ~1 variant per 2-3 hours

**For a full 120-variant sweep: ~240-360 hours.**

### Next steps

Options to make this tractable:
1. **Reduce trial count** to 1 MC trial per variant (not 10)
2. **Reduce depth** to find the minimal convergence depth
3. **Reduce action depth range** from 0-119 to the interesting range (e.g., 5-40)
4. **Run on a beefier machine** with more cores for parallel children
5. **Let the agent tune it** (which is the whole point of the experiment)

## Performance Notes

### Bottleneck analysis

The primary bottleneck is the sequential staleness sweep within each child
process. The simclock package uses global mutable state, preventing concurrent
in-process explorations. Each child must process its staleness intervals one at
a time. The `--metrics-only-staleness` flag eliminates the secondary bottleneck
(disk I/O) but does not parallelize the exploration itself.

With 7 controllers permuted, each MC path visits ~70-120 states and takes
several seconds. The reference + rerun phases complete quickly (~20s each), but
the staleness sweep dominates wall time.
