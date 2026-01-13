# Kamera Ablation Study: Knative

Run settings:
- flags: `-depth 100 -timeout 60m -interactive=false -emit-stats -log-level info`

## Primer

*What's a "Cluster State" ?* -- all of the resources in the cluster

*What's a "Node" ?* -- a cluster state + the set of pending reconciles that still need to execute.

## Workload
Reconciling the creation of one Knative service. It gets created and then scaled down due to the absense of traffic.

All experiments found the same result (1 converged state) which hashes to the same value.

## Optimization Descriptions

- `orderingPruning`: when multiple reconciles are pending, explore each “first reconcile” choice once per content state; avoid re-expanding equivalent reorderings when the same content state is re-encountered.
- `onlyPermuteTriggered`: restrict order permutation to reconcilers triggered by the last step; when off, any eligible pending reconcile can be moved first.
- `completedPathDedup`: de-duplicate exploration of identical state+history paths once the full path has already been explored.
- `memoization`: cache reconcile results for a given (cluster state, reconciler, request); if we've seen the (cluster state, reconciler, request) before, return the cached result instead of executing the reconcile code.
- `earlyConvergence`: if all pending reconciles are known no-ops for the current objects and a converged outcome for those objects has already been seen, skip executing the known no-ops.
- `subtreeCompletion`: track node expansions (branching points) and mark when the subtree beneath a branching point has been fully searched. then, skip re-exploring a state (ands its descendants) if we've already explored the full subtree beneath it.

## Optimization Configs

| Experiment |  orderingPruning | onlyPermuteTriggered | completedPathDedup | memoization | earlyConvergence | subtreeCompletion |
| --- | --- | --- | --- | --- | --- | --- |
| all | on | on | on | on | on | on |
| no-subtree | on | on | on | on | on | off |
| no-early-convergence | on | on | on | on | off | off |
| no-memoization | on | on | on | off | on | off |
| no-path-dedup | on | on | off | off | off | off |
| nothing | off | on | off | off | off | off |
| permute-all-pending | on | off | on | on | on | on |


## Results
| Experiment | Time | Total Nodes | Unique Nodes | Unique Cluster States | Already Seen Nodes |
| --- | --- | --- | --- | --- | --- |
| all | 37.71545275s | 668 | 661 | 77 | 7 |
| no-subtree | 54.469500917s | 1105 | 661 | 77 | 444 |
| no-early-convergence | 1m2.55785525s | 1223 | 681 | 77 | 542 |
| no-memoization | 3m13.605744792s | 3774 | 661 | 77 | 3113 |
| no-path-dedup | 3m48.969075791s | 4516 | 681 | 77 | 3835 |
| nothing | 1h (TIMEOUT) | 71748 | 613 | 74 | 71135 |
| permute-all-pending | TBD | TBD | TBD | TBD | TBD |


## Optimization Utilization (# usages)
| Experiment | orderingPruning branch skips | orderingPruning no-op skips | completedPathDedup skips | cachePrediction skips | earlyConvergence skips | subtreeCompletion skips |
| --- | --- | --- | --- | --- | --- | --- |
| all | 24 | 0 | 0 | 34 | 4 | 66 |
| no-subtree | 407 | 0 | 0 | 88 | 19 | 0 |
| no-early-convergence | 476 | 0 | 3 | 90 | 0 | 0 |
| no-memoization | 3163 | 0 | 0 | 0 | 107 | 0 |
| no-path-dedup | 3675 | 0 | 0 | 0 | 0 | 0 |
| nothing | 0 | 0 | 0 | 0 | 0 | 0 |
| permute-all-pending | TBD | TBD | TBD | TBD | TBD | TBD |
