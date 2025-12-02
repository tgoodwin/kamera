# Simulating Knative’s Autoscaler Ticker in Deterministic Exploration

## Context
In real Knative, the KPA controller constructs a `MultiScaler` that runs an async `time.NewTicker(2s)` goroutine (`runScalerTicker`). Each tick runs `autoscaler.Scale`, updates the Decider’s status (DesiredScale, ExcessBurstCapacity), and calls `Inform` to enqueue the PA for another reconcile.

In our explorer, `KnativeStrategy.ReconcileAtState` builds a fresh controller per reconcile, and the controller’s context is canceled immediately afterward. With determinized time (depth-aware; no wall clock between reconciles), the `time.NewTicker` goroutine never fires before the context is canceled. Result: `decider.Status.DesiredScale` never updates from its initial value (-1→0), so KPA sees `want=0` and marks the PA/Revision `TimedOut` (“The target could not be activated.”).

## What we observed
- Using determinized caches (`~/tmp/gocache`, `~/tmp/gomodcache`), the converged PA shows `Active=False/Ready=False` with `TimedOut` and `desiredScale=0`.
- Adding logs inside the fake UniScaler showed `Scale` never runs in determinized runs: the ticker goroutine doesn’t tick before the controller is torn down.
- Timestamps in conditions are zeroed (1970), consistent with frozen wall-clock time.
- Route/Service remain Ready=Unknown (IngressNotConfigured), but that’s a separate wiring issue.

## Why the ticker is missing in simulation
- `MultiScaler.runScalerTicker` uses `time.NewTicker`. In the harness we recreate KPA per reconcile, so the ticker goroutine dies when the reconcile finishes. With no wall-clock advancement between reconciles, the ticker never fires even once.
- In real Knative the ticker is long-lived and drives enqueues via the `Watch` callback; reconciliation does not block on the ticker.

## Possible paths forward
1) **Deterministic tick provider**: Patch determinization (or the harness) to replace `time.NewTicker` with a hook that calls `tickScaler` synchronously. Options:
   - Call `tickScaler` once per reconcile (after Decider create/update) so Decider.Status is populated before KPA uses it.
   - Tie ticks to simulated depth/time: e.g., one tick per reconcile depth step, or per Decider creation.
   - Keep `Inform` semantics so enqueues still happen, but driven deterministically.

2) **Seed Decider status**: In determinized mode, set `decider.Status.DesiredScale` to initialScale (>=1) when creating/updating the Decider, bypassing the need for ticks during activation. Less faithful to enqueue semantics, but minimal.

3) **Long-lived controller per branch**: Retain KPA controller across steps so its ticker can tick while simulated time advances. More intrusive to the strategy/lifecycle but closer to real behavior.

## Next experiments
- Log `decider.Status.DesiredScale` before `c.scaler.scale` in KPA to verify it stays 0/-1 in determinized runs.
- Inject a deterministic tick in `MultiScaler.createScaler` (conditional on determinized mode) and rerun to see if `TimedOut` disappears.
- Ensure any change still triggers `Inform`/enqueue so we model the background resync behavior.

## Requirements for a clock-aware fix (activation + scale-to-zero)
- Replace `time.NewTicker` with a deterministic tick provider tied to simulated time; each tick must still call `tickScaler` and `Inform` so KPA enqueues PAs.
- Plumb a shared simulated `Now()` into KPA’s time checks (`handleScaleToZero`, `ActiveFor`, `InactiveFor`, `CanFailActivation`, condition timestamps) so activation/timeout/scale-to-zero honor simulated elapsed time.
- Advance simulated time per reconcile/depth step (e.g., fixed delta per reconcile) so both activation progress and eventual scale-down can be modeled.
- Preserve enqueue semantics: when ticks fire, add a pending KPA reconcile (mirroring `Inform`/watch in real Knative) into the explorer’s pending set.
- Automate via determinize: rewrite `time.NewTicker`/`time.Now()` in autoscaler/KPA to the simulated clock/tick provider, keeping behavior consistent without manual guards.

## Translating `Inform` → pending reconciles
- Capture enqueues: KPA registers a watcher (`MultiScaler.Watch`) that normally calls `controller.EnqueueKey`. In the fake workqueue, record enqueued keys (thread-safe slice).
- After reconcile completes, strategy can read enqueued keys and append `PendingReconcile{ReconcilerID: "KPA", Request: ns/name}` to the state’s pending list, mirroring background enqueue behavior.
- Alternative: treat Decider status updates as “changes” that trigger KPA via dependency graph, but direct enqueue capture is simpler and closer to real behavior.
- Decider is not a K8s resource (in-memory only), so we won’t see it in state snapshots; the enqueue side-effects are the observable signal to feed into Kamera’s pending reconciles.
- Harness idea: expose an explorer “enqueue pending reconcile” hook; wire `MultiScaler.Watch` to that hook so `Inform` directly produces pending KPA reconciles without relying on a real workqueue.

## Goal: simulating scaledown
Objective: get PA/Revision to scale to zero after simulated time elapses, with minimal moving parts.

Steps:
- Add a sim clock/ticker in determinize for autoscaler/KPA: swap `time.Now` and `time.NewTicker` in those packages for `simclock.Now()`/`simclock.NewTicker()`.
- Drive `simclock` forward by 1s per DFS step; on each step, fire any tickers whose interval has elapsed. No goroutines.
- Keep a long-lived `MultiScaler` per explore branch; set `tickProvider` to the sim ticker so `runScalerTicker` uses logical ticks.
- Wire `MultiScaler.Watch` to the explorer’s pending reconcile queue (skip the real workqueue) so each tick enqueues the PA key.
- Let KPA’s existing `handleScaleToZero`/`ActiveFor` logic read `simclock.Now()`: once stable window + grace pass and desiredScale=0, the next tick enqueues KPA and that reconcile scales the deployment to zero.
