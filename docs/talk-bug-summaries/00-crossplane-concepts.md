# Crossplane concepts primer (talk-prep)

A walkthrough of the Crossplane concepts that show up in the SPRINT-0001 bugs. Aimed at someone who knows Kubernetes well but is fuzzy on Crossplane-specific terminology like XR, CompositionRevision, `compositionRef`, etc. Read this top-to-bottom; later sections build on earlier ones.

The companion talk-prep docs ([F1](./F1-manual-policy-wrong-revision.md) through [C4](./C4-cross-xr-ownership-theft.md)) reference these concepts by name without re-explaining them.

## 1. The mental model: Crossplane as a control plane for user-defined abstractions

Crossplane lets a platform team define **abstractions** over their infrastructure (databases, networks, queues, whole apps), and lets app teams **request** those abstractions declaratively. Under the hood, Crossplane translates the user's request into the actual managed resources (RDS instances, S3 buckets, GCP projects) by running a programmable pipeline.

The trick: Crossplane re-uses the Kubernetes API model the whole way down. Abstractions are CRDs. Requests are custom resources. The translation logic is itself a controller. Everything is declarative state in etcd and reconciled.

So when the talk references "an XR" or "a Composition," those are Kubernetes objects with `kind`, `apiVersion`, `metadata`, `spec`, `status` — same as anything else.

## 2. The user-facing object hierarchy

```
       (Platform team defines)              (App team uses)
        ┌───────────────────┐              ┌─────────────┐
        │       XRD         │ ◄──── kind   │    Claim    │ (namespaced)
        │ (CompositeResource│              └──────┬──────┘
        │    Definition)    │                     │ owns
        └─────────┬─────────┘                     ▼
                  │ spawns                 ┌─────────────┐
                  ▼                        │     XR      │ (cluster-scoped)
        ┌───────────────────┐              └──────┬──────┘
        │   Composition     │ ◄─────refs──────────┘
        └─────────┬─────────┘
                  │ snapshots into
                  ▼
        ┌───────────────────┐
        │CompositionRevision│ ← what the XR actually composes against
        └───────────────────┘
```

Three layers worth distinguishing:

### XRD (CompositeResourceDefinition)
**What it is:** the platform team's schema definition. "Here is a new Crossplane API kind called `XWidget` with the following OpenAPI schema." Roughly the Crossplane analogue of a CRD-of-CRDs.

**Why it matters:** when you `kubectl apply` an XRD, Crossplane creates two CRDs from it: one for the **XR** kind and one for the **Claim** kind. Those are what users interact with.

In the SPRINT-0001 scenarios, the XRD is `xwidgets.example.org` defining `kind: XWidget`. Tier 3 reproductions use this same XRD.

### Claim (`kind: Widget`)
**What it is:** namespaced user request. "Give me a Widget in namespace foo." Optional — platform teams can choose not to expose Claims at all.

**Why it matters:** Claims are what application teams typically interact with. Each Claim has a corresponding **XR** that does the actual work. The Claim is essentially a scoped, namespaced facade.

**SPRINT-0001 link:** [C2](./C2-claim-deletion-false-positive.md) is about the Claim → XR cascade-deletion path.

### XR (CompositeResource, `kind: XWidget`)
**What it is:** cluster-scoped composite resource. The thing Crossplane actually reconciles. Either created directly (no Claim) or spawned by a ClaimReconciler when a Claim is created.

**Why it matters:** the XR is the central object in every SPRINT-0001 bug. Its `spec.compositionRef` and `spec.compositionRevisionRef` determine *what* it composes; its `status.conditions` carry `Synced` and `Ready`; its `status.resourceRefs` lists the composed dependents.

A typical XR's spec/status looks like:
```yaml
apiVersion: example.org/v1
kind: XWidget
metadata:
  name: example
spec:
  message: "hello"
  compositionRef:
    name: widget-composition-alpha
  compositionRevisionRef:
    name: widget-composition-alpha-a39f01a
  compositionUpdatePolicy: Manual
status:
  conditions:
  - type: Synced
    status: "True"
    reason: ReconcileSuccess
  - type: Ready
    status: "False"
    reason: Creating
  resourceRefs:
  - apiVersion: v1
    kind: ConfigMap
    name: alpha-output
    namespace: default
```

## 3. Composition and CompositionRevision

This is the layer most relevant to F1 / F3, and it's the part most people get fuzzy on.

### Composition
**What it is:** mutable cluster-scoped object describing *how* an XR of a given kind should be composed. Holds the function pipeline definition.

**Mental model:** "the recipe." When the platform team edits a Composition, they're editing the recipe.

A Composition is mutable. Editing it in place (`kubectl edit composition foo`) silently changes the recipe for every XR currently bound to it.

### CompositionRevision
**What it is:** immutable snapshot of a Composition spec at a point in time. Crossplane creates one automatically every time the Composition's spec changes meaningfully.

**Mental model:** "a frozen version of the recipe." Like a Deployment's ReplicaSets — the parent (Composition) is the user-edited surface; the children (CompositionRevisions) are the historical record that controllers actually reconcile against.

**Naming convention:** `<composition-name>-<spec-hash>`, e.g. `widget-composition-alpha-a39f01a`. The hash suffix is a deterministic function of the Composition's spec; **byte-identical specs produce byte-identical revision names**. This is occasionally relevant — see the F3 doc's "bonus probe" footnote.

CompositionRevisions are owned by their Composition via `ownerReferences`, so when a Composition is deleted, its revisions get garbage-collected by Kubernetes.

### Why both exist
The split lets the platform team edit the Composition without immediately affecting bound XRs. The XR can pin to a specific revision (immutable), while the platform team iterates on the parent Composition. New revisions get created; old XRs keep using their pinned revision until updated.

## 4. `compositionRef` vs `compositionRevisionRef` vs `compositionUpdatePolicy`

Three fields on `XR.spec`. They look similar but mean different things.

| Field | What it points to | Who can edit it | Mental model |
|---|---|---|---|
| `compositionRef` | The Composition (mutable parent) by name | User intent: "this XR should use Composition X." | "Which recipe?" |
| `compositionRevisionRef` | A specific CompositionRevision (immutable snapshot) by name | Either the user OR the controller (depending on policy) | "Which frozen version of that recipe?" |
| `compositionUpdatePolicy` | enum: `Automatic` (default) or `Manual` | User | "Who decides when to upgrade to a newer revision?" |

The interaction:

- Under **`Automatic`** (default): the controller continuously updates `compositionRevisionRef` to point at the *latest* revision of the Composition referenced by `compositionRef`. The user sets `compositionRef`; the controller manages `compositionRevisionRef` for them.
- Under **`Manual`**: the user controls `compositionRevisionRef` themselves. The controller does not update it. Users opt into Manual when they want stability — "I've tested with revision X, I don't want to auto-upgrade."

What the **composite reconciler actually composes against** is `compositionRevisionRef`. `compositionRef` is in some sense advisory — it's a name for the Composition family, but the immutable revision is what determines the pipeline that runs.

This is the gap [F1](./F1-manual-policy-wrong-revision.md) exploits. Under Manual policy, the user can patch `compositionRef = beta` while leaving `compositionRevisionRef = alpha-rev-1`. Crossplane composes against alpha-rev-1 (the pinned revision) and reports `Synced=True`. The user thinks they switched to beta; they didn't. There's no validation that the pinned revision actually belongs to the named Composition.

## 5. Functions, FunctionRevisions, capabilities

Compositions don't directly produce composed resources — they invoke a **pipeline of functions** that compute the desired composed resources. (The legacy "patch and transform" approach has been generalized: P&T is now itself a function called `function-patch-and-transform`.)

### Function (and the pipeline)
**What it is:** an out-of-process gRPC service that takes `RunFunctionRequest` (containing the XR + observed state + previous step's output) and returns `RunFunctionResponse` (containing desired state, results, possibly fatal severity).

A Composition's `spec.pipeline` is a list of named steps. Each step references a Function by name and passes its input. The output of one step is the input to the next. The final output is the set of desired composed resources.

```yaml
apiVersion: apiextensions.crossplane.io/v1
kind: Composition
metadata:
  name: widget-composition-alpha
spec:
  mode: Pipeline
  pipeline:
  - step: render
    functionRef:
      name: function-patch-and-transform
    input:
      apiVersion: pt.fn.crossplane.io/v1beta1
      kind: Resources
      resources:
      - name: cm
        base:
          apiVersion: v1
          kind: ConfigMap
          metadata:
            name: alpha-output
          data:
            source: alpha
```

### Function and FunctionRevision
Same parent/child pattern as Composition: a `Function` is the mutable parent (essentially "I want this OCI package installed"), and `FunctionRevision` is an immutable snapshot of an installed version.

The active FunctionRevision (`spec.desiredState: Active`) is the one Crossplane invokes. When the package manager pulls a new version, a new FunctionRevision becomes active.

### Capabilities
Each FunctionRevision advertises **capabilities** at `status.capabilities`. The capability that matters for the SPRINT-0001 bugs is `composition` — meaning "this function may be invoked as part of a Composition pipeline."

Capabilities live at `status.capabilities` (set by the package manager controller from the package's `crossplane.yaml` metadata), not at `spec.capabilities`. This matters for [R-12](../crossplane-audits/R12-f5-real-cluster.md) — a manual `kubectl patch --subresource=status` to fake capability removal is reverted by the package manager controller within seconds.

### ValidPipeline condition
The CompositionRevision controller validates that every function referenced by the pipeline advertises `composition` capability. The result is published as a condition on the CompositionRevision:

```yaml
status:
  conditions:
  - type: ValidPipeline
    status: "True"
    reason: ValidPipeline
```

The composite reconciler checks `ValidPipeline=True` on the CompositionRevision before invoking the function pipeline. This is a precondition that says "this pipeline has been validated as runnable."

[F5](./F5-stale-validpipeline-race.md) is exactly the race window where `ValidPipeline=True` is stale: the FunctionRevision controller has updated capabilities, but the CompositionRevision controller hasn't re-validated yet. During that window, the composite reconciler proceeds and runs a pipeline that should have been rejected.

## 6. The composite reconciler lifecycle (simplified)

Per reconcile of an XR:
1. **Fetch the CompositionRevision.** `APIRevisionFetcher.Fetch` resolves `compositionRevisionRef` (under Manual) or selects the latest matching revision (under Automatic).
2. **Validate `ValidPipeline=True`** on the CompositionRevision.
3. **Run the function pipeline** — invoke each step's Function via gRPC, threading the previous step's output forward.
4. **Compute desired state** from the pipeline output.
5. **Apply / update / delete composed resources** to bring observed state to desired state. This is where the GC step at [`composition_functions.go:538`](https://github.com/crossplane/crossplane/blob/v2.2.0/internal/controller/apiextensions/composite/composition_functions.go) lives — it cleans up resources that were composed previously but are no longer in the new desired set.
6. **Update XR status conditions** — `Synced=True` on success, `Synced=False` with a reason on failure.

Several of the SPRINT-0001 bugs are about specific points in this flow:
- F1: step 1 misbehaves under Manual policy (no cross-reference validation).
- F3: step 1 fails permanently after the Composition is deleted (revision GC'd).
- F5: step 2 returns stale True due to the cross-controller race.
- F6-orphan: step 3 returns SEVERITY_FATAL, short-circuiting before step 5's GC runs.
- F6-stale-Ready: step 6 sets `Synced=False` but doesn't touch system conditions like `Ready`.

## 7. Status conditions: Synced, Ready, and "system" vs not

Crossplane XRs publish two primary conditions:

- **`Synced`** — "the reconciler successfully ran to completion." `Synced=True` means the controller reconciled without error. `Synced=False` means an error occurred.
- **`Ready`** — "the composed resources are ready." Aggregated from the readiness of the composed dependents.

Both are considered **system conditions** in the Crossplane source (`crossplane-runtime`'s `xpv1.IsSystemConditionType` returns true for these). System conditions get **special treatment** in the reconciler:

- They are *not* iterated over when the error path tries to mark conditions Unknown.
- This is intentional for `Synced` (it's set explicitly elsewhere as `False`).
- It is **unintentional** for `Ready`, which keeps whatever value it had pre-error.

That's the [F6-stale-Ready](./F6-stale-ready-true.md) bug: `Ready` retains its prior value across the error transition, producing the contradictory `Synced=False + Ready=True` user-visible state.

## 8. Composed resources and ownership

When a function pipeline says "the desired state includes a `ConfigMap/foo`," the composite reconciler creates that ConfigMap and:
- Sets the XR as a controller via `metadata.ownerReferences[].controller=true`.
- Tracks the resource in the XR's `status.resourceRefs`.
- Tags the resource with a `crossplane.io/composition-resource-name` annotation so the reconciler can correlate it with the pipeline's named output.

This means composed resources are owned by the XR. When the XR is deleted, Kubernetes' garbage collector cascade-deletes the composed resources via owner references (Background propagation by default).

This ownership model is relevant for several bugs:
- **F3:** when the Composition is deleted, composed resources do **not** get cleaned up automatically (the XR is still alive, holding the owner references). They become orphans not because of GC failure, but because Crossplane has no semantic that says "Composition deletion → cascade-delete composed resources."
- **F6-orphan:** when the function returns SEVERITY_FATAL, the GC step that would normally remove "no longer in desired set" resources never runs. The orphans persist with their old owner references intact.
- **C4:** when two XRs try to compose the same target resource, real K8s SSA rejects the second `controllerRef=true` write via field-manager conflict detection. The harness's earlier SSA implementation didn't model this.

## 9. Finalizers and DELETE semantics

Standard Kubernetes pattern:
- A controller sets a `metadata.finalizers` entry on an object before it does anything substantive.
- On DELETE, the API server marks `metadata.deletionTimestamp` but does **not** remove the object — the finalizer blocks removal.
- The controller observes the deletion-timestamp-with-finalizer state, runs its cleanup logic, then patches off its finalizer.
- Once all finalizers are gone, the API server removes the object.

The Crossplane ClaimReconciler uses this pattern to ensure XR cleanup before Claim deletion: it sets a finalizer on the Claim, observes the deletion timestamp, deletes the bound XR, waits for the XR to be gone, then removes its finalizer.

Real K8s DELETE is implemented as a **patch** of `metadata.deletionTimestamp` — it preserves `finalizers`, `spec`, `status`, etc. The Kamera harness's earlier `Client.Delete` implemented DELETE as a wholesale replace, **clobbering finalizers**. That's the [C2](./C2-claim-deletion-false-positive.md) negative result.

## 10. Putting it all together: which concept maps to which bug

| Concept | Bugs it appears in |
|---|---|
| `compositionRef` vs `compositionRevisionRef` mismatch under Manual policy | [F1](./F1-manual-policy-wrong-revision.md) |
| `Status().Update()` write semantics on the XR | [F2](./F2-unconditional-status-update.md) |
| Composition deletion → CompositionRevision GC (no finalizer guard) | [F3](./F3-composition-deletion-error-loop.md) |
| FunctionRevision capabilities + CompositionRevision `ValidPipeline` | [F5](./F5-stale-validpipeline-race.md) |
| Function pipeline SEVERITY_FATAL early-return + composed-resource GC | [F6-orphan](./F6-orphan-persistence-while-fatal.md) |
| System conditions skipped on the error path (`Ready`) | [F6-stale-Ready](./F6-stale-ready-true.md) |
| Claim → XR cascade via finalizers | [C2](./C2-claim-deletion-false-positive.md) (negative result) |
| Cross-XR ownership conflict on shared composed resource via SSA | [C4](./C4-cross-xr-ownership-theft.md) (negative result) |

## 11. Quick glossary (one-line each)

- **XRD:** schema-of-schemas; defines an XR kind and (optionally) a Claim kind.
- **Claim:** namespaced user-facing facade; spawns a cluster-scoped XR.
- **XR (CompositeResource):** the cluster-scoped object the composite reconciler reconciles.
- **Composition:** mutable cluster-scoped object holding the function-pipeline recipe.
- **CompositionRevision:** immutable snapshot of a Composition spec; deterministic name = `<composition>-<spec-hash>`.
- **`compositionRef`:** XR field naming the Composition family.
- **`compositionRevisionRef`:** XR field naming the specific revision the controller composes against.
- **`compositionUpdatePolicy`:** `Automatic` (controller manages `compositionRevisionRef`) or `Manual` (user manages it).
- **Function:** out-of-process gRPC service invoked as a Composition pipeline step.
- **FunctionRevision:** immutable snapshot of an installed function package version; advertises capabilities at `status.capabilities`.
- **Capability:** declared property of a function (e.g., `composition` means "may be invoked in a Composition pipeline").
- **`ValidPipeline` condition:** condition on a CompositionRevision saying "all referenced functions advertise the right capabilities."
- **SEVERITY_FATAL:** result severity from a function indicating the pipeline cannot complete.
- **Composed resource (a.k.a. dependent):** a resource the XR composed; owned by the XR via `ownerReferences`.
- **`Synced` condition:** "did the reconciler complete without error?" Set explicitly per reconcile.
- **`Ready` condition:** aggregated readiness of composed dependents. **System condition** — special-cased on the error path (this is the F6-stale-Ready bug).
