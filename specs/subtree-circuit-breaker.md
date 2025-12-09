## Context
Deep DFS runs spend disproportionate time in churny subtrees where permutations repeatedly converge or get pruned as duplicate mutation histories. When this happens, upper portions of the explore tree receive less coverage, and timeouts expire before we exercise alternative orderings that might reveal different converged states.

## Goal
Introduce a subtree-aware circuit breaker that stops exploration beneath the youngest branching ancestor once we see enough convergences or duplicate-path skips under that anchor. The breaker should be keyed to the specific branch that spawned the permutations (order-sensitive), so pruning is local: repeated churn under a deep branch should not prevent exploring siblings or higher ancestors.

## Plan
- Track a divergence ancestry on each `StateNode`, pushing a new anchor (order-sensitive hash of the branching node) whenever we expand multiple pending reconciles or generate multiple stale views.
- Accumulate counters per anchor for both convergences and mutation-history skip events; when a node converges or we skip enqueueing a child as a duplicate, increment counts for all anchors in its ancestry.
- At pop time, check the youngest anchor’s counters against a configurable threshold; if exceeded, short-circuit exploring that subtree and move on to other parts of the state space.


## notes
dec 7th

Key Findings
The Core Problem:
~65% of reconcile work is wasted (626 skipped paths out of 968 reconcile steps)
The waste comes from exploring no-op ordering permutations
Different orderings of [X (mutates), Y (no-op)] all produce the same output state with the same "effective history" (only mutations count)
Root Cause:
The ExecutionHistory.UniqueKey() filters out no-ops before generating the signature. So paths that differ only in no-op orderings are detected as duplicates - but only AFTER running the expensive reconcile.
Depth Distribution:
Most work (768 visits) happens at depths 70-79
Average 4-5 pending reconciles at each step
Queue stays large (~300+) throughout
What We Added
Depth distribution tracking - see where exploration time is spent
Early skip for already-seen order-sensitive states - avoids re-processing
ObjectsHash() helper - for potential future caching optimization
Potential Optimizations (not implemented)
Track no-op reconcilers: When a reconcile produces no changes, remember it. Avoid expanding orderings that only differ by known no-ops.
Eager history signature check: Before running reconcile, compute what the output history signature would be (given current effective history + whether this reconcile will mutate). Skip if already seen.
Limit ordering expansion: Instead of expanding all N orderings, only expand orderings that differ in mutating reconcilers.
Shall I pursue any of these optimizations, or would you like to explore the codebase in a different direction?