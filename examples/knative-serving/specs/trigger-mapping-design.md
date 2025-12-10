# Trigger Mapping Design: Faithfully Modeling Controller-Runtime Watches

## Problem Statement

Our `TriggerManager` in `pkg/tracecheck/trigger.go` needs to model how Kubernetes controllers are triggered when resources change. We attempted to fix a triggering issue by adding subscriber-based triggering via `WithResourceDep`, but this fix relied on **unstable "same name" assumptions** that don't reflect how controller-runtime actually works.

### Background: The Original Bug

The Knative simulation had an issue where Route wasn't being triggered when Configuration/Revision became Ready, causing the top-level Service to never reach Ready state.

### The Flawed Fix

We added subscriber triggering that used **identity mapping**: when Configuration "demo" changes, trigger RouteReconciler for "demo". This appeared to work because Knative objects happen to share names (Service "demo" → Configuration "demo" → Route "demo").

**But this is wrong.** Identity mapping in controller-runtime means:
- "When Configuration X changes, enqueue Configuration X for reconciliation"
- This only makes sense if **the controller reconciles Configurations**

Route controller doesn't reconcile Configurations - it reconciles Routes. So using identity mapping to trigger RouteReconciler when Configuration changes is semantically incorrect. It only worked by coincidence because Route and Configuration share the name "demo".

### Why "Same Name" Is Not a Valid Strategy

Consider what identity mapping actually means:

```go
// This controller reconciles Deployments
ctrl.NewControllerManagedBy(mgr).
    For(&Deployment{}).  // When Deployment X changes → enqueue Deployment X
    Build(r)
```

If we tried to use identity mapping for cross-resource watches:
```go
// WRONG: Route controller watching Configuration with identity mapping
// When Configuration "demo" changes → enqueue... Configuration "demo"?
// But Route controller reconciles Routes, not Configurations!
```

The "same name coincidence" made our broken implementation appear to work, but:
1. It's not how controller-runtime works
2. It fails when names don't match (Revision "kamera-test" vs Route "demo")
3. It's not semantically correct even when names do match

## How Controller-Runtime Actually Works

### The Three Trigger Patterns

Controller-runtime supports exactly three ways to trigger reconciliation:

#### 1. Primary (`.For()`) - Identity Mapping
```go
For(&Deployment{})  // Uses EnqueueRequestForObject
```
- When Deployment X changes → enqueue Deployment X
- **Only for the primary resource this controller manages**
- The enqueued object IS the changed object

#### 2. Owned (`.Owns()`) - Owner Reference Mapping
```go
Owns(&ReplicaSet{})  // Uses EnqueueRequestForOwner{OwnerType: &Deployment{}}
```
- When ReplicaSet X changes → look at X's ownerReferences → find Deployment owner → enqueue that Deployment
- **For resources this controller creates/owns**
- Uses ownerRef to map child → parent

#### 3. Watched (`.Watches()`) - Custom Mapping
```go
Watches(&ConfigMap{}, handler.EnqueueRequestsFromMapFunc(mapFunc))
```
- When ConfigMap X changes → call mapFunc(X) → enqueue whatever mapFunc returns
- **For arbitrary cross-resource dependencies**
- Requires explicit mapping logic

### Key Insight: No "Same Name" Pattern

Controller-runtime does NOT have a "same name" mapping mode. The patterns are:
- **Identity**: Enqueue the literal changed object (for primary resources)
- **Owner**: Follow ownerRef to find what to enqueue (for owned resources)
- **Custom**: Explicit mapping function (for everything else)

## How Knative's Tracker Implements Custom Mapping

Knative uses a **tracker** to implement the custom mapping pattern dynamically:

```go
// During Route "demo" reconcile:
c.tracker.TrackReference(tracker.Reference{
    Kind: "Configuration",
    Name: "demo",
}, route)  // Registers: "Route demo is tracking Configuration demo"

// When Configuration "demo" changes:
// Tracker callback fires
// Looks up: "Who's tracking Configuration demo?" → Route "demo"
// Enqueues: Route "demo"
```

This is NOT "same name" mapping - it's explicit registration. Route "demo" explicitly registers that it's tracking Configuration "demo". The names matching is incidental to the explicit tracking relationship.

For Revision tracking:
```go
// Route "demo" tracks Revision "kamera-test"
c.tracker.TrackReference(tracker.Reference{
    Kind: "Revision",
    Name: "kamera-test",
}, route)  // Registers: "Route demo is tracking Revision kamera-test"

// When Revision "kamera-test" changes:
// Tracker looks up: "Who's tracking this?" → Route "demo"
// Enqueues: Route "demo" (NOT "kamera-test"!)
```

## Current State of trigger.go

### What Works Correctly

1. **Primary triggers** (`.For()` semantics): When object changes, trigger its primary reconciler with object's name ✓
2. **Owner triggers** (`.Owns()` semantics): When object changes, look at ownerRefs, trigger owner's reconciler with owner's name ✓

### What's Broken

3. **Subscriber triggers** (`WithResourceDep`): Uses identity mapping (changed object's name = reconcile request name), which:
   - Is semantically incorrect (identity means enqueue the changed object, not a different object with the same name)
   - Only works by coincidence when names match
   - Fails when names don't match

### Current Workaround

We have deduplication logic at lines 208-229 that skips subscriber triggers if the subscriber is also an owner. This patches over some symptoms but doesn't fix the underlying semantic incorrectness.

## Proposed Solution

### Option A: Owner-Based Mapping Only (Simpler)

Remove "same name" subscriber triggering entirely. Only support:
1. **Primary triggers** - object changes → its reconciler
2. **Owner triggers** - child changes → owner's reconciler

For cross-resource watches that aren't owner-based (like Route → Configuration), rely on ownership chains:
- Configuration changes → Service reconciler triggered (Service owns Configuration)
- Service reconciler can then update/trigger Route as needed

**Pros**: Simple, follows controller-runtime strictly
**Cons**: May require restructuring how some controllers propagate state

### Option B: Owner-Based Mapping for WithResourceDep (Recommended)

Keep `WithResourceDep` but change its semantics to use **owner-based mapping**:

```go
// "When Revision changes, find its Configuration owner, trigger RouteReconciler for that Configuration"
builder.WithResourceDep("Revision", OwnerKind("Configuration"), "RouteReconciler")
```

This maps the changed object to a related object via ownership, which is semantically correct and doesn't rely on naming coincidences.

### Option C: Implement Tracker Pattern (Most Flexible)

Implement a lightweight tracker where reconcilers can register explicit tracking relationships:

```go
// During reconcile, register what we're tracking
tracker.Track(configRef, routeRef)

// When Configuration changes, tracker knows to enqueue the Route that registered tracking
```

**Pros**: Most faithful to Knative
**Cons**: More complex, requires reconciler cooperation

## Recommended Implementation Plan

### Phase 1: Remove Broken Subscriber Triggering

1. Remove the identity-based subscriber triggering from `getTriggered()`
2. Remove the deduplication hack at lines 208-229
3. Keep `WithResourceDep` but only use it for staleness calculation (its original purpose)

### Phase 2: Add Owner-Based Watch Mapping

1. Add new API for owner-mapped watches:
   ```go
   // When Revision changes, map via owner to Configuration, trigger these reconcilers
   builder.WatchesViaOwner("Revision", "Configuration", "RouteReconciler")
   ```

2. Implement owner lookup in `getTriggered()`:
   ```go
   // For owner-mapped watches:
   // 1. Object X changes
   // 2. Look up X's ownerRef of specified kind
   // 3. Enqueue reconciler for owner's namespace/name
   ```

### Phase 3 (Optional): Tracker Pattern

If needed for more complex scenarios, implement dynamic tracker registration.

## Test Cases

1. **Primary trigger**: Deployment changes → DeploymentReconciler triggered for that Deployment
2. **Owner trigger**: ReplicaSet changes → DeploymentReconciler triggered for owning Deployment
3. **Owner-mapped watch**: Revision "kamera-test" (owned by Configuration "demo") changes → RouteReconciler triggered for "demo"
4. **No false triggers**: Configuration "demo" changes should NOT trigger RouteReconciler for "demo" via identity mapping (that's semantically wrong)

## Files to Modify

- `pkg/tracecheck/trigger.go` - Remove identity subscriber triggering, add owner-mapped watches
- `pkg/tracecheck/explorebuilder.go` - Add `WatchesViaOwner()` API
- `pkg/tracecheck/trigger_test.go` - Update tests for new semantics
- `examples/knative-serving/scenario.go` - Update to use owner-mapped watches

## Summary

**The core insight**: We should not support "same name" as a triggering strategy because:
1. It's not how controller-runtime works
2. Identity mapping means "enqueue the changed object", not "enqueue a different object with the same name"
3. Cross-resource watches in controller-runtime use either owner references or explicit custom mapping

**The path forward**: Follow controller-runtime's model strictly with primary, owner, and (optionally) tracker-based triggers. No "same name" coincidences.

## References

- Controller-runtime EventHandler: `sigs.k8s.io/controller-runtime/pkg/handler`
- Knative tracker: `knative.dev/pkg/tracker`
- Current trigger.go implementation: `pkg/tracecheck/trigger.go:190-237`
