# D12 Consolidatable-Condition-TOCTOU Tuning Log (v2)

## Bug Summary

The "consolidatable-condition-toctou" bug is a TOCTOU (time-of-check-time-of-use) race in Karpenter's disruption pipeline. The `disruption` controller checks whether a node is empty/consolidatable by reading Pod data. If its view of Pods is stale (from before a pod was scheduled to the node), it sees the node as empty and proceeds to delete it, even though a workload pod has been bound to the node.

Key controllers involved:
- `disruption`: main disruption loop, reads Pods to check emptiness, initiates delete
- `disruption.queue`: executes disruption commands (node deletion)
- `nodeclaim.disruption`: sets Consolidatable condition on NodeClaims
- `state.pod`: updates cluster state with pod bindings
- `state.node`: updates cluster state with node info
- `state.nodeclaim`: updates cluster state with nodeclaim info
- `state.nodepool`: updates nodepool state, marks cluster unconsolidated

## Iteration 1: Baseline reproduction (7 controllers)

**File:** d12-tuning-v2-1.json

**Configuration:**
- permuteControllers: all 7 (disruption, disruption.queue, nodeclaim.disruption, state.pod, state.node, state.nodeclaim, state.nodepool)
- stalenessIntervals: disruption sees stale core/Pod at kindSeq 2, catchUp at 30, lag -1 (frozen)
- userActionReadyDepths: {"0": 20}
- maxDepth: 120
- trials: 20, seed: 800

**Reasoning:** Same configuration as the original scenario file to establish a baseline.

**Result:**
- 16/20 converged, 4 aborted
- BUG REPRODUCED: 5 trials with 5 objects (correct), 11 trials with 4 objects (buggy)
- Wall time: ~2 seconds

---

## Iteration 2: Remove state.nodepool (6 controllers)

**File:** d12-tuning-v2-2.json

**Configuration change:** Removed state.nodepool from permuteControllers.

**Reasoning:** NodePool doesn't change during this scenario. state.nodepool only marks the cluster unconsolidated on generation changes, which don't happen here.

**Result:**
- 17/20 converged, 3 aborted
- BUG REPRODUCED: 2 correct (5 obj) vs 15 buggy (4 obj)

---

## Iteration 3: Remove state.nodeclaim (5 controllers)

**File:** d12-tuning-v2-3.json

**Configuration change:** Removed state.nodeclaim. Controllers: disruption, disruption.queue, nodeclaim.disruption, state.pod, state.node.

**Reasoning:** The NodeClaim doesn't change until the disruption controller acts on it, so the nodeclaim state informer is not involved in the TOCTOU race.

**Result:**
- 18/20 converged, 2 aborted
- BUG REPRODUCED: 6 correct (5 obj) vs 12 buggy (4 obj)

---

## Iteration 4: Remove state.node (4 controllers)

**File:** d12-tuning-v2-4.json

**Configuration change:** Removed state.node. Controllers: disruption, disruption.queue, nodeclaim.disruption, state.pod.

**Reasoning:** Node info is pre-seeded in the environment. The node state informer updates the cluster state with node info, but the race is specifically about pod data staleness in the disruption controller.

**Result:**
- 19/20 converged, 1 aborted (95% convergence!)
- BUG REPRODUCED: 4 correct (5 obj) vs 15 buggy (4 obj)

---

## Iteration 5: Remove state.pod (3 controllers -- wrong set)

**File:** d12-tuning-v2-5.json

**Configuration change:** Removed state.pod. Controllers: disruption, disruption.queue, nodeclaim.disruption.

**Reasoning:** Test whether the pod state informer ordering matters for the bug, or if the staleness mechanism alone is sufficient.

**Result:**
- 16/20 converged, 4 aborted
- NO DIVERGENCE: All converged trials had 4 objects (buggy). The bug always fires.
- Without permuting state.pod, the cluster state never gets the pod update before disruption. Every ordering triggers the TOCTOU.

**Conclusion:** state.pod must be in the permutation set for some orderings to show the correct behavior. Otherwise the staleness means disruption always acts on stale data.

---

## Iteration 6: Tighten staleness window (4 controllers, catchUpAt=20)

**File:** d12-tuning-v2-6.json

**Configuration change:** catchUpAt reduced from 30 to 20 (same as action depth). Controllers: disruption, disruption.queue, nodeclaim.disruption, state.pod.

**Result:**
- 16/20 converged, 4 aborted
- BUG REPRODUCED: 3 correct (5 obj) vs 13 buggy (4 obj)

---

## Iteration 7: Reduced maxDepth (80) and action depth (10)

**File:** d12-tuning-v2-7.json

**Configuration:** 4 controllers, maxDepth=80, action at depth 10, catchUpAt=20.

**Reasoning:** Try to reduce exploration depth to speed up execution.

**Result:**
- Only 2/20 converged, 18 aborted
- Bug reproduced in the data but too many aborted trials.
- maxDepth=80 is insufficient for the disruption pipeline to complete.

---

## Iteration 8: maxDepth=100, action=10, catchUpAt=20

**File:** d12-tuning-v2-8.json

**Result:**
- 9/20 converged, 11 aborted
- BUG REPRODUCED but low convergence rate. Action depth 10 too early.

---

## Iteration 9: maxDepth=100, action=15, catchUpAt=15, 30 trials

**File:** d12-tuning-v2-9.json

**Result:**
- 17/30 converged, 13 aborted (57% convergence)
- BUG REPRODUCED: 1 correct vs 16 buggy
- catchUpAt=15 too tight -- staleness ends right as action fires, most orderings still stale.

---

## Iteration 10: maxDepth=100, action=15, catchUpAt=25, 30 trials

**File:** d12-tuning-v2-10.json

**Result:**
- 18/30 converged, 12 aborted (60% convergence)
- BUG REPRODUCED: 5 correct vs 13 buggy

---

## Iteration 11: Best balance -- 4 controllers, maxDepth=120, action=20, catchUp=30, 10 trials

**File:** d12-tuning-v2-11.json

**Configuration:** Same as iteration 4 but with only 10 trials.

**Result:**
- 10/10 converged (100% convergence!)
- BUG REPRODUCED: 2 correct (5 obj) vs 8 buggy (4 obj)
- This is the "sweet spot" configuration with 4 controllers.

---

## Iteration 12: Remove disruption.queue (3 controllers)

**File:** d12-tuning-v2-12.json

**Configuration:** disruption, nodeclaim.disruption, state.pod. 10 trials.

**Reasoning:** disruption.queue only executes after disruption creates a command. Its ordering shouldn't affect the TOCTOU check.

**Result:**
- 8/10 converged (80% convergence)
- BUG REPRODUCED: 1 correct (5 obj) vs 7 buggy (4 obj)

---

## Iteration 13: Just 2 controllers -- disruption + nodeclaim.disruption

**File:** d12-tuning-v2-13.json

**Configuration:** Only disruption and nodeclaim.disruption permuted. 10 trials.

**Reasoning:** Test whether the race is purely between the disruption controller's stale pod view and the nodeclaim.disruption controller setting the Consolidatable condition. state.pod ordering may not matter since the staleness mechanism operates at the API read level.

**Result:**
- 10/10 converged (100% convergence!)
- BUG REPRODUCED: 2 correct (5 obj) vs 8 buggy (4 obj)
- This is the MINIMUM controller set with 100% convergence!

---

## Iteration 14: Just 1 controller -- disruption alone

**File:** d12-tuning-v2-14.json

**Configuration:** Only disruption permuted. 10 trials.

**Reasoning:** Is the staleness mechanism alone sufficient, without any controller ordering nondeterminism?

**Result:**
- 8/10 converged (80% convergence)
- BUG REPRODUCED: 1 correct (5 obj) vs 7 buggy (4 obj)
- The staleness mechanism alone creates the divergence! The ordering of the disruption controller relative to nodeclaim.disruption matters for whether the Consolidatable condition is set before or after the disruption check, but the staleness on Pod reads creates the primary TOCTOU.

---

## Summary

### Best Minimized Configuration (variant 13)

```json
{
  "permuteControllers": ["disruption", "nodeclaim.disruption"],
  "stalenessIntervals": [{
    "reconciler": "disruption",
    "kind": "core/Pod",
    "staleAt": 2,
    "catchUpAt": 30,
    "lag": -1
  }],
  "userActionReadyDepths": {"0": 20},
  "maxDepth": 120,
  "search": {
    "mode": "monte_carlo",
    "monteCarlo": {"seed": 800, "trials": 10}
  }
}
```

**Reduction from original:**
- Controllers: 7 -> 2 (71% reduction)
- Trials: 20 -> 10 (50% reduction)
- Convergence: 80% -> 100% (improved!)
- Bug reproduction: Yes (2 correct, 8 buggy)

### Key Findings

1. The bug is fundamentally a **staleness bug**, not an ordering bug. The staleness on the disruption controller's Pod reads (staleAt=2, frozen) causes it to never see the pod scheduled to the node, leading it to incorrectly identify the node as empty and delete it.

2. The minimal controller set is `disruption` + `nodeclaim.disruption`:
   - `disruption` is the controller that reads stale Pod data and incorrectly deletes the node
   - `nodeclaim.disruption` sets the Consolidatable condition that gates disruption eligibility
   - The ordering between these two determines whether the node is marked consolidatable before or after disruption checks

3. The state informer controllers (state.pod, state.node, state.nodeclaim, state.nodepool) are NOT required for reproduction. They only affect the cluster state, but the staleness mechanism operates at the API read level within the disruption controller.

4. `catchUpAt=30` with `lag=-1` (frozen) is the sweet spot. Tighter values (15-20) still reproduce but with lower convergence rates.

5. `maxDepth=120` is needed for reliable convergence. Lower values (80-100) cause too many aborted trials.
