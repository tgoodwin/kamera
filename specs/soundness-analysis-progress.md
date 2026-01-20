# Soundness Analysis Progress

## 01/07/26 Summary of Findings

1) **Ordering pruning was unsound due to triggered-only expansion.**
- The explorer only permutes reconcilers that were `triggeredByStep`.
- Ordering pruning originally keyed only on `NodeHash`, so it skipped expansion
  even when `triggeredByStep` differed.
- Logs confirmed the same `contentsHash` appeared with multiple distinct
  `triggeredByStep` sets.
- Fix implemented: ordering-prune key now includes a `permuteSignature`
  (set of permutable reconcilers from `triggeredByStep`).

2) **Logical-state mismatch persisted after the above fix.**
- With the `permuteSignature` fix, opt1–opt5 still reported 73 logical states
  while opt6 (ordering pruning off) reported 77.
- So another pruning effect was still dropping states.

3) **No-op ordering skip is likely unsound under the current model.**
- `wasNoOp()` only checks for zero object writes + no error.
- A no-op reconcile still removes itself from pending, and can requeue or enqueue
  async requests.
- Because ordering expansion is order-sensitive, skipping a “no-op-first”
  ordering can drop reachable pending orders and their subtrees.
- Added config flag `disableNoOpOrderingSkip` to isolate this behavior.

4) **Pending order itself matters for pruning soundness.**
- `NodeHash` is order-insensitive; but order expansion preserves the relative
  order of remaining pending reconciles.
- Test-only env flag `KAMERA_ORDER_PRUNE_ORDER_HASH=1` switches ordering-prune
  key to use `OrderHash` instead of `NodeHash`.
- With this flag ON and no-op skip disabled, opt4 reached 77 logical states,
  matching opt6.

5) **Disabling no-op skip + order-sensitive pruning can blow up opt1.**
- opt1 with `disableNoOpOrderingSkip=true` and order-sensitive pruning expands
  drastically (thousands of distinct states), so runtime balloons.

## Current Status
- Ordering-prune key fix (permuteSignature) is correct and necessary.
- The remaining 73 vs 77 gap is tied to no-op ordering skip and/or order-
  insensitive pruning of pending order.
- Order-sensitive pruning fixes the gap in opt4 when no-op skip is disabled.

## Files and Flags Introduced
- New config flag: `disableNoOpOrderingSkip` (optimization toggle).
- New spec: `specs/ordering-expansion-optimization.md`.
- Test-only env flag: `KAMERA_ORDER_PRUNE_ORDER_HASH=1` to use OrderHash
  as the ordering-prune key.
- Added `Logical States` to the “Taking reconcile step” log line.

## 01/12/26

- triggered-only expansion is not complete. We made this behavior toggleable via the `onlyPermutetriggered` flag, and when running with it on vs off (e.g. run with `ablation/config/knative-1-all.json` vs `knative-opt-5-all.json`, we find 80 vs 73 logical states.
- there may be a class of optimizations that are *not complete* but still useful. maybe this is one of them?
- still, would be good to re-verify why `onlyPermuteTriggered` is not complete.



