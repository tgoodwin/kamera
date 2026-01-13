# Ordering Expansion Strategy

## Overview
The explorer searches a DFS state space of reconciler executions. When multiple
reconciles are pending, the system can choose which one runs first. That choice
can produce order-dependent behavior, so the explorer must branch to cover
those alternatives without exploding the search space.

## Strategy
We use a targeted expansion strategy:
- When a reconcile step changes state, we compute `triggeredByStep`.
- We only permute the order of reconcilers that were triggered by that state
  change.
- The set of reconcilers eligible for permutation is further gated by
  `PermuteOrder` (a per-reconciler config map).

This keeps the branching factor manageable while still probing likely sources
of order sensitivity.

## Assumptions
This strategy assumes:
- Order-dependent behavior arises primarily among reconcilers that react to the
  same state change (i.e., they are triggered together).
- Permuting reconcilers that were not triggered by the most recent change does
  not reveal new outcomes worth the added combinatorial cost.

These are pragmatic, not universal, assumptions. If full soundness is required
for all possible orderings, the explorer must enumerate all permutations of the
pending list, which can be expensive.

## Ordering Pruning Key
Ordering pruning skips re-expanding a state if it has already expanded the same
set of permutations. Because expansion depends on `triggeredByStep`, the
pruning key must include the *permutable triggered set*, not just the state
hash.

Current key:
- `NodeHash` (objects + pending list, order-insensitive)
- plus a `permuteSignature` derived from `triggeredByStep` filtered by
  `PermuteOrder`, treated as a set of reconciler IDs.

If expansion ever changes to enumerate all pending permutations, the key could
be simplified to `NodeHash` alone.

## Tradeoffs
Pros:
- Keeps branching factor tractable.
- Targets the most likely order-dependent interleavings.

Cons:
- Unsound if order-dependent behavior requires reordering of reconcilers that
  were not triggered by the most recent state change.
- Ordering pruning can also skip orderings whose first reconcile is a known
  no-op. This assumes a no-op-first ordering cannot influence later outcomes.
  That is a heuristic and may drop states when combined with triggered-only
  expansion.

## Notes
- `triggeredByStep` is derived from the effects of the most recent reconcile.
- `PermuteOrder` is the user-controlled knob for which reconcilers participate
  in permutation.
- `disableNoOpOrderingSkip` disables the no-op-first ordering shortcut while
  keeping ordering pruning enabled.
