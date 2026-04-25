# KCP Bug Summaries (KCP4, KCP7, KCP17)

These are the three bugs used in the agent tuning experiments.

## KCP4: Late APIExport (P2, 2 converged states)

**Perturbation:** External event + ordering

**Setup:** An APIExport arrives (CREATE) after controllers have already started processing workspace initialization. Six controllers are permuted.

**Root cause:** The URLs controller depends on the primary EndpointSlice controller setting conditions first. If the URLs controller runs *before* the primary controller sets its conditions, it sees no work to do and never gets re-triggered.

**Consequence:** Endpoint URLs are unpopulated in one ordering but populated in another. Two final states, one with missing URLs.

**Diverging object:** `APIExportEndpointSlice/root:provider/widgets`
- State 0: URLs controller populates endpoints (correct)
- State 1: Only primary controller runs, no URLs populated

**Severity:** P2 (transient, self-heals on next reconcile cycle)

## KCP7: APIExport Deletion (P1, 19 converged states)

**Perturbation:** External event + ordering

**Setup:** An APIExport is deleted (DELETE at depth 8) while bindings are active. Six controllers are permuted. Full happy-path environment with 9 objects.

**Root cause:** Six controllers race to process the deletion. Some orderings allow a controller to re-create or prevent the deletion entirely. Each controller reads different state depending on whether the export was still present when it ran.

**Consequence:** 10 distinct final states originally documented (19 found in our extended run). 4 of 9 objects diverge:
1. **APIExport survives its own deletion** in ~30% of orderings (a controller re-creates or blocks deletion)
2. **APIBinding** ends up in 3 distinct states (stale annotations, different conditions)
3. **LogicalCluster** conditions diverge (same pattern as KCP17)
4. **EndpointSlice** diverges (same pattern as KCP4)

**Diverging objects:**
- `APIExport/root:provider/widgets` -- 7 states deleted, 3 states survive
- `APIBinding/root:consumer/widgets-7fajt` -- 3 distinct final hashes
- `APIExportEndpointSlice/root:provider/widgets` -- 2 distinct final hashes
- `LogicalCluster/root:consumer/cluster` -- 2 distinct final hashes

**Severity:** P1 (requires manual intervention, system does not self-heal to a consistent state)

## KCP17: Pure Ordering Divergence (P2, 2 converged states)

**Perturbation:** Ordering only (no external event)

**Setup:** Standard workspace initialization with 8 controllers permuted. No external events. This is the first pure ordering bug found in KCP; with 7 controllers all scenarios were clean.

**Root cause:** The APIBinding reconciler resets a binding's `InitialBindingCompleted` condition to False (because a `resource-bindings` annotation is missing). This triggers both the APIBinderInitializer and the DefaultAPIBindingLifecycle controller to re-reconcile the LogicalCluster. Both controllers write conditions to the same LogicalCluster object, and whichever runs last wins.

**Consequence:** Two final states with different LogicalCluster conditions. The winner of the condition write is ordering-dependent.

**Diverging object:** `LogicalCluster/root:consumer/cluster`
- State 0: last write by APIBinderInitializerController (step 22)
- State 1: last write by DefaultAPIBindingLifecycleController (step 7)

**Severity:** P2 (non-deterministic LogicalCluster conditions during workspace initialization; conditions oscillate depending on which controller reconciles last)

**Notable:** This bug is only visible when the 8th controller (APIBindingReconciler) is included in the permutation set. It was invisible with 7 controllers.
