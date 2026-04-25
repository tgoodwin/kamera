# KRO K2b Perturbation Tuning Experiment Log

**Start time:** 2026-03-28T16:51:12-07:00
**End time:** 2026-03-28T16:55:29-07:00
**Total wall time:** ~4 minutes 17 seconds
**Total iterations:** 10
**Goal:** Find the most focused perturbation configuration that reproduces the KRO Application child-resource bug.

## Summary

The bug was reproduced on the very first attempt and then minimized across 9 more iterations.

**Optimal configuration (v9):**
- permuteControllers: [ApplicationController] (1 controller)
- faultInjection: ApplicationController crashAfterEffect=1, triggerOnce=true
- maxDepth: 4
- Result: Reference 6 objects vs Rerun 3 objects, 6 unique nodes each

**Root cause:** When the ApplicationController crashes after its 1st write effect (the SSA patch that adds the finalizer and management labels), the patch replaces the Application's entire content with just metadata (finalizer, labels). The `spec` field is lost. On re-reconcile, the controller reads back the spec-less Application and fails with `includeWhen "schema.spec.ingress.enabled": no such key: spec`, permanently unable to create children.

---

## Iteration 1: v1 -- ApplicationController crash after 2nd effect, 2 controllers

**Start:** 2026-03-28T16:51:12-07:00
**Config:**
- permuteControllers: [RGDController, ApplicationController]
- faultInjection: ApplicationController crashAfterEffect=2, triggerOnce=true
- maxDepth: 50

**Rationale:** The ApplicationController write sequence is: (1) ensureManaged (finalizer+labels), (2) patchInstanceWithApplySetMetadata, (3) Apply child resources (Deployment, Service, Ingress), (4) updateStatus. Crashing after the 2nd write means ApplySet metadata is set but no children are created. On re-reconcile, the controller may see stale state and skip children. Only 2 KRO controllers permuted keeps the state space tight.

**End:** 2026-03-28T16:51:40-07:00
**Wall time:** ~28s
**Result:** BUG REPRODUCED
- Reference phase: 9 objects [Application, CRD, Deployment, Endpoints, Ingress, Pod, ReplicaSet, RGD, Service]
- Rerun phase: 3 objects [Application, CRD, RGD] -- ALL children missing
- Reference: 34 unique nodes, 17 unique resource states
- Rerun: 8 unique nodes, 7 unique resource states (stuck in error loop at max depth)
- Error: `includeWhen "schema.spec.ingress.enabled": no such key: spec`

## Iteration 2: v2 -- ApplicationController crash after 1st effect, 2 controllers

**Start:** 2026-03-28T16:51:40-07:00
**Config:**
- permuteControllers: [RGDController, ApplicationController]
- faultInjection: ApplicationController crashAfterEffect=1, triggerOnce=true
- maxDepth: 50

**Rationale:** Try even tighter crash point (after 1st effect = just the finalizer patch).

**End:** 2026-03-28T16:52:31-07:00
**Wall time:** ~51s
**Result:** BUG REPRODUCED
- Reference: 9 objects, 34 unique nodes, 17 resource states
- Rerun: 3 objects, 8 unique nodes, 7 resource states
- Same error pattern as v1

## Iteration 3: v3 -- Same as v2 but maxDepth=20

**Start:** 2026-03-28T16:52:31-07:00
**Config:**
- permuteControllers: [RGDController, ApplicationController]
- faultInjection: ApplicationController crashAfterEffect=1, triggerOnce=true
- maxDepth: 20

**Rationale:** Reduce depth to minimize state space exploration.

**End:** 2026-03-28T16:53:04-07:00
**Wall time:** ~33s
**Result:** BUG REPRODUCED
- Reference: 9 objects, 21 unique nodes, 13 resource states
- Rerun: 3 objects, 8 unique nodes, 7 resource states

## Iteration 4: v4 -- maxDepth=10

**Start:** 2026-03-28T16:53:04-07:00
**Config:**
- permuteControllers: [RGDController, ApplicationController]
- faultInjection: ApplicationController crashAfterEffect=1, triggerOnce=true
- maxDepth: 10

**End:** 2026-03-28T16:53:28-07:00
**Wall time:** ~24s
**Result:** BUG REPRODUCED
- Reference: 8 objects (missing Pod), 11 unique nodes, 9 resource states
- Rerun: 3 objects, 8 unique nodes, 7 resource states

## Iteration 5: v5 -- maxDepth=8, only ApplicationController permuted

**Start:** 2026-03-28T16:53:28-07:00
**Config:**
- permuteControllers: [ApplicationController]
- faultInjection: ApplicationController crashAfterEffect=1, triggerOnce=true
- maxDepth: 8

**Rationale:** Remove RGDController from permutation (it's deterministic). Reduce to 1 permuted controller.

**End:** 2026-03-28T16:53:49-07:00
**Wall time:** ~21s
**Result:** BUG REPRODUCED
- Reference: 8 objects, 9 unique nodes, 8 resource states
- Rerun: 3 objects, 8 unique nodes, 7 resource states

## Iteration 6: v6 -- maxDepth=7

**Start:** 2026-03-28T16:53:49-07:00
**Config:**
- permuteControllers: [ApplicationController]
- faultInjection: ApplicationController crashAfterEffect=1, triggerOnce=true
- maxDepth: 7

**End:** 2026-03-28T16:54:06-07:00
**Wall time:** ~17s
**Result:** BUG REPRODUCED
- Reference: 7 objects, 8 unique nodes, 7 resource states
- Rerun: 3 objects, 8 unique nodes, 7 resource states

## Iteration 7: v7 -- maxDepth=6

**Start:** 2026-03-28T16:54:06-07:00
**Config:**
- permuteControllers: [ApplicationController]
- faultInjection: ApplicationController crashAfterEffect=1, triggerOnce=true
- maxDepth: 6

**End:** 2026-03-28T16:54:32-07:00
**Wall time:** ~26s
**Result:** BUG REPRODUCED
- Reference: 6 objects [Application, CRD, Deployment, Ingress, RGD, Service], 7 unique nodes, 6 resource states
- Rerun: 3 objects [Application, CRD, RGD], 7 unique nodes, 6 resource states

## Iteration 8: v8 -- maxDepth=5

**Start:** 2026-03-28T16:54:32-07:00
**Config:**
- permuteControllers: [ApplicationController]
- faultInjection: ApplicationController crashAfterEffect=1, triggerOnce=true
- maxDepth: 5

**End:** 2026-03-28T16:54:48-07:00
**Wall time:** ~16s
**Result:** BUG REPRODUCED
- Reference: 6 objects, 6 unique nodes, 5 resource states
- Rerun: 3 objects, 6 unique nodes, 5 resource states

## Iteration 9: v9 -- maxDepth=4 (MINIMUM REPRODUCING DEPTH)

**Start:** 2026-03-28T16:54:48-07:00
**Config:**
- permuteControllers: [ApplicationController]
- faultInjection: ApplicationController crashAfterEffect=1, triggerOnce=true
- maxDepth: 4

**End:** 2026-03-28T16:55:15-07:00
**Wall time:** ~27s
**Result:** BUG REPRODUCED
- Reference: 6 objects [Application, CRD, Deployment, Ingress, RGD, Service], 6 unique nodes, 5 resource states
- Rerun: 3 objects [Application, CRD, RGD], 6 unique nodes, 5 resource states

## Iteration 10: v10 -- maxDepth=3 (TOO SHALLOW)

**Start:** 2026-03-28T16:55:15-07:00
**Config:**
- permuteControllers: [ApplicationController]
- faultInjection: ApplicationController crashAfterEffect=1, triggerOnce=true
- maxDepth: 3

**End:** 2026-03-28T16:55:29-07:00
**Wall time:** ~14s
**Result:** NO BUG -- too shallow
- Reference: 2 objects [CRD, RGD], 4 unique nodes
- Rerun: 2 objects [CRD, RGD], 4 unique nodes
- The Application CREATE user action is never injected (requires depth >= 4)

---

## Minimization Summary

| Version | Controllers | Crash Point | Max Depth | Ref Objects | Rerun Objects | Bug? |
|---------|------------|-------------|-----------|-------------|---------------|------|
| v1      | 2 (RGD+App) | after 2    | 50        | 9           | 3             | YES  |
| v2      | 2 (RGD+App) | after 1    | 50        | 9           | 3             | YES  |
| v3      | 2 (RGD+App) | after 1    | 20        | 9           | 3             | YES  |
| v4      | 2 (RGD+App) | after 1    | 10        | 8           | 3             | YES  |
| v5      | 1 (App)     | after 1    | 8         | 8           | 3             | YES  |
| v6      | 1 (App)     | after 1    | 7         | 7           | 3             | YES  |
| v7      | 1 (App)     | after 1    | 6         | 6           | 3             | YES  |
| v8      | 1 (App)     | after 1    | 5         | 6           | 3             | YES  |
| v9      | 1 (App)     | after 1    | 4         | 6           | 3             | YES  |
| v10     | 1 (App)     | after 1    | 3         | 2           | 2             | NO   |

The original exhaustive scenario used 7 permuted controllers, crash points 1-7 for both RGD and Application controllers, and maxDepth=80. The minimized configuration uses 1 permuted controller, crash point 1, and maxDepth=4.
