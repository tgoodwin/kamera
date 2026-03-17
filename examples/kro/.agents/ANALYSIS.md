# KRO Harness Analysis

## Phase 0: Controller Surface Map

### Controllers Under Test

| Controller | Source | Fidelity | Key Writes |
|-----------|--------|----------|------------|
| ResourceGraphDefinitionReconciler | `pkg/controller/resourcegraphdefinition/` | **Real** | CRD Ensure, Status Patch, Finalizer Patch |
| Instance Controller | `pkg/controller/instance/` | **Real** | SSA Apply (Deployment, Service, Ingress), Status Apply, Finalizer/Label Apply |
| Deployment Controller | kamera built-in | Simulation | ReplicaSet Create, Deployment Status Update |
| ReplicaSet Controller | kamera built-in | Simulation | Pod Create, RS Status Update |
| Pod Lifecycle | kamera built-in | Simulation | Pod Status Update (Pending→Running) |
| Service/Endpoints | kamera built-in | Simulation | Endpoints Update |

### Surgical KRO Changes (5 commits)

All changes are non-behavioral and suitable for upstream contribution:

1. **`NewBuilderFromResolver`** — alternate constructor accepting pre-built SchemaResolver + RESTMapper
2. **`DynamicControllerRegistrar`** — interface extraction (Register/Deregister) for the RGD controller
3. **Nil schemaResolver support** — uses schemaless parsing for all resources
4. **Nil/empty status schema** — early return when status schema is absent
5. **`skipCELValidation`** — skip CEL type checking when no OpenAPI schemas available

### Harness Architecture

```
┌─────────────────────────────────────────────────┐
│                  kamera explorer                │
│  ┌──────────────┐    ┌────────────────────────┐ │
│  │ RGD Reconciler│    │  Instance Controller   │ │
│  │ (real KRO)   │    │  (real KRO)            │ │
│  └──────┬───────┘    └──────────┬─────────────┘ │
│         │                       │               │
│  ┌──────▼───────────────────────▼─────────────┐ │
│  │         replayClientSet adapter            │ │
│  │  ┌─────────────┐  ┌────────────────────┐   │ │
│  │  │ dynamic.If  │  │ CRDInterface       │   │ │
│  │  │ adapter     │  │ adapter            │   │ │
│  │  └──────┬──────┘  └────────┬───────────┘   │ │
│  └─────────┼──────────────────┼───────────────┘ │
│            │                  │                 │
│  ┌─────────▼──────────────────▼───────────────┐ │
│  │        kamera replay client.Client         │ │
│  └────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
```

Key adapters:
- **replayDynamicClient**: Bridges `dynamic.Interface` → `client.Client` with GVR→GVK resolution
- **replayClientSet**: Implements `kroclient.SetInterface` for KRO's client abstraction
- **replayCRDClient**: CRD Ensure/Delete/Get via replay client
- **stubDynamicControllerRegistrar**: No-op Register/Deregister (kamera handles watches natively)
- **adaptInstanceController**: Translates KRO's `requeue.RequeueNeededAfter`/`RequeueNeeded`/`NoRequeue` error types to `reconcile.Result`

### Known Limitations

1. **No API server defaulting**: CRD defaults (e.g., `image: "nginx"`) are not applied automatically. Application instances must include all fields explicitly.
2. **SSA Apply semantics**: The replay client records Apply as a write effect but doesn't fully materialize server-side merge. The dynamic adapter re-reads after Apply to get the merged state.
3. **CEL type checking disabled**: Graph building uses schemaless parsing. CEL expressions are extracted and dependency graphs built correctly, but expression types are not validated against OpenAPI schemas.
4. **DynamicController stub**: The RGD controller's microcontroller registration (Register/Deregister) is a no-op. In production, this wires up child resource watches.

---

## Phase 1: Scenario Results

### K1: Ordering — RGD vs Instance Controller

**Hypothesis**: Controller execution order between RGD and Instance controllers affects final state.

**Scenario**: `scenarios/k1_ordering-rgd-instance.json`
- Environment: RGD seeded with Application definition (3 resources: Deployment, Service, Ingress)
- External input: CREATE Application instance with `ingress.enabled=true`
- Perturbation: `permuteControllers: ["ResourceGraphDefinitionController", "ApplicationController"]`
- Depth: 40

**Result**: **No divergence detected.**
- Reference run: 1 converged state, hash `1bqct03q`
- Permuted run: 4 converged states, all hash `1bqct03q`
- All orderings converge to the same final state

**Interpretation**: The dependency chain (RGD must create CRD before Instance Controller can proceed) naturally enforces ordering. The Instance Controller correctly handles the case where the CRD doesn't exist yet (returns requeue).

---

## Approach B: Full-Fidelity Fallback

If deeper bugs are suspected in the RGD controller or graph building:

1. Add OpenAPI schema resolution by embedding schemas for core types (Deployment, Service, Ingress) or connecting to a test API server
2. Enable CEL type checking (`skipCELValidation = false`)
3. Provide real `DynamicController` with fake metadata client for microcontroller registration testing

This would unlock testing of:
- Schema validation edge cases
- CEL expression type mismatches
- Microcontroller watch registration races

---

## Harness Fixes Applied

### SSA Apply Idempotency (kamera core)

**Problem**: Controllers that unconditionally re-apply the same desired state via SSA Apply created infinite reconcile loops. Each Apply was recorded as a write effect, triggering re-enqueue.

**Fix**: In `applyEffects`, when processing an APPLY effect on an existing object, check if the spec changed. If spec is unchanged, skip the effect (don't record state change, don't trigger re-enqueue). This mirrors real API server behavior where idempotent SSA Apply returns the same resource version.

### Apply Re-read (kro harness)

**Problem**: After SSA Apply with a partial patch (e.g., only metadata fields), the returned object was missing `spec` because the replay client doesn't materialize merged state.

**Fix**: Dynamic adapter re-reads the full object via Get after every Apply, returning the complete merged state to the controller.

---

## Area Coverage Assessment

| Area | Coverage | Notes |
|------|----------|-------|
| RGD → CRD creation | Tested | Real controller, graph building with schemaless parsing |
| RGD → Status update | Tested | Real controller updates topological order, resources info |
| Instance → Child SSA Apply | Tested | Real controller creates Deployment, Service, Ingress |
| Instance → includeWhen | Tested | Real CEL evaluation of `schema.spec.ingress.enabled` |
| Instance → Status update | Tested | Real controller applies status with conditions |
| Instance → Finalizer/Labels | Tested | Real controller applies managed finalizer and KRO labels |
| Ordering: RGD vs Instance | Tested | No divergence (K1) |
| Staleness perturbation | Not yet tested | Next scenario batch |
| External events (scale up/down) | Not yet tested | Next scenario batch |
| Deletion flow | Not yet tested | Requires DELETE external input |
