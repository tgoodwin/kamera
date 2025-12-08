## Context
Deep DFS runs spend disproportionate time in churny subtrees where permutations repeatedly converge or get pruned as duplicate mutation histories. When this happens, upper portions of the explore tree receive less coverage, and timeouts expire before we exercise alternative orderings that might reveal different converged states.

## Goal
Introduce a subtree-aware circuit breaker that stops exploration beneath the youngest branching ancestor once we see enough convergences or duplicate-path skips under that anchor. The breaker should be keyed to the specific branch that spawned the permutations (order-sensitive), so pruning is local: repeated churn under a deep branch should not prevent exploring siblings or higher ancestors.

## Plan
- Track a divergence ancestry on each `StateNode`, pushing a new anchor (order-sensitive hash of the branching node) whenever we expand multiple pending reconciles or generate multiple stale views.
- Accumulate counters per anchor for both convergences and mutation-history skip events; when a node converges or we skip enqueueing a child as a duplicate, increment counts for all anchors in its ancestry.
- At pop time, check the youngest anchor’s counters against a configurable threshold; if exceeded, short-circuit exploring that subtree and move on to other parts of the state space.
