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

### K2: Fault Injection — Instance Controller Crash Mid-Apply (CONFIRMED DIVERGENCE)

**Hypothesis**: Crashing the Instance Controller after its 2nd or 3rd write effect (mid-apply of child resources) produces inconsistent final states.

**Scenario**: `scenarios/k2_fault-instance-mid-apply.json`
- Environment: Default RGD + Application instance with `ingress.enabled=true`
- Perturbation: `faultInjection` on ApplicationController after 2nd/3rd write effect
- Additional: ordering perturbation + staleness intervals auto-generated

**Result**: **12 distinct final state hashes across 628 runs.**

| Hash | Count | Notes |
|------|-------|-------|
| `kkkokprn` | 385 | Most common final state |
| `1t34gt65` | 102 | Second most common |
| `dmkpu179` | 44 | |
| `vnj9i4lh` | 28 | |
| `cp0t27s6` | 28 | |
| `8serm9k0` | 12 | |
| `2zlgm5tu` | 12 | |
| `l8zhp9ml` | 7 | |
| `axezj6ks` | 7 | |
| `1lzb3ys6` | 2 | |
| `5n70ee14` | 1 | Rare |
| `303e07hx` | 1 | Rare |

**Root cause analysis**: The Instance Controller's applyset applies child resources (Deployment, Service, Ingress) and then updates parent ApplySet metadata and status. When the controller crashes mid-apply:
1. Some children are applied, others are not
2. The parent's ApplySet annotations may reflect an incomplete set
3. On retry, the controller re-reads the instance and re-computes desired state
4. But the ApplySet metadata mismatch can cause the prune logic to behave differently
5. The combination of which children exist + which ApplySet labels are present + the controller's conflict detection logic produces multiple distinct final states

**Vulnerability window**: Between the per-child `Apply()` calls (applyset.go line 393) and the parent metadata patch (resources.go line 54). The parallel apply means different subsets of children may be applied before crash.

**Evidence**: `evidence/k2_fault_instance_crash_after_2nd_write_reference_0.jsonl`, `evidence/k2_fault_instance_crash_after_2nd_write_rerun_0.jsonl`

---

### K5: Fault Injection — RGD Controller Crash

**Hypothesis**: Crashing the RGD controller after CRD creation but before status update produces inconsistent state.

**Scenario**: `scenarios/k5_fault-rgd-mid-reconcile.json`
- Perturbation: `faultInjection` on RGD controller after 1st/2nd write

**Result**: **No divergence.** All 4 runs converge to hash `3pjf6e5w`.

**Interpretation**: The RGD controller's write sequence is inherently idempotent. CRD Ensure is idempotent (create-or-update), status patch is idempotent, and the retry path correctly re-executes the full sequence.

---

### K3: Staleness — Spec Update During Reconcile

**Hypothesis**: Instance Controller reading stale Application during replicas scale-up produces intermediate states.

**Scenario**: `scenarios/k3_staleness-spec-update.json`
- Staleness intervals on ApplicationController for kro.run/Application

**Result**: Only reference runs produced (staleness intervals may need tuning). Both references converge to `1bqct03q`. **Needs re-run with corrected staleness config.**

---

### K4: Ingress Toggle

**Hypothesis**: Toggling `ingress.enabled` with staleness produces dangling Ingress resources.

**Scenario**: `scenarios/k4_toggle-ingress.json`

**Result**: Only reference runs produced. True→false converges to `1bqct03q`, false→true to `heyrko8l`. These are expected different final states (one has Ingress, one doesn't). **Needs re-run with staleness intervals that produce permuted runs.**

---

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
