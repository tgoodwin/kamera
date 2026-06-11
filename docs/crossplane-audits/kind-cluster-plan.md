# Real-cluster audit plan (kind + Crossplane v2.2.0)

**Status:** plan only; not executed yet.
**Audits covered:** R-11 (F1), R-12 (F5), R-13 (F6), R-14 (F3).
**Goal:** smoking-gun evidence — for each finding still on the table, reproduce the bug (or fail to reproduce it) on a real Crossplane v2.2.0 cluster. One real-cluster reproduction eliminates ~all simulation-fidelity threats for that finding.

## Why this matters

Tier 1 (code reads) confirmed that the bug mechanisms are source-grounded in v2.2.0. Tier 2 (web research, in flight) will confirm the workqueue + GC + webhook properties. But the most defensible posture going into the Crossplane community meeting is "I reproduced this on a real cluster and here's the recording."

The one thing simulation-fidelity arguments can't survive is `kubectl get xwidget -o yaml` showing the bug.

## Total time estimate

- **Phase 0 (setup):** 60–90 min if you've never deployed Crossplane before; 20–30 min if you have.
- **R-11 (F1):** 30–45 min including write-up.
- **R-12 (F5):** 60–90 min — hardest, requires capability manipulation.
- **R-13 (F6):** 45–60 min.
- **R-14 (F3):** 30–45 min.
- **Total:** ~4–6 hours active time. F5 is the long pole.

## Phase 0 — shared setup

### Prerequisites

- `kind` (you said you have this).
- `kubectl` (any 1.27+).
- `helm` 3.x.
- `docker` running.
- A scratch directory for manifests: `mkdir -p ~/crossplane-audit && cd ~/crossplane-audit`.

### Step 0.1: spin up kind cluster

```bash
cat <<EOF > kind-config.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
EOF

kind create cluster --name crossplane-audit --image kindest/node:v1.30.0 --config kind-config.yaml
kubectl cluster-info --context kind-crossplane-audit
```

### Step 0.2: install Crossplane v2.2.0

```bash
helm repo add crossplane-stable https://charts.crossplane.io/stable
helm repo update

helm install crossplane crossplane-stable/crossplane \
  --namespace crossplane-system \
  --create-namespace \
  --version 2.2.0 \
  --wait

kubectl get pods -n crossplane-system
# expect: crossplane-* and crossplane-rbac-manager-* both Running
```

Verify the version:

```bash
kubectl get deployment -n crossplane-system crossplane -o jsonpath='{.spec.template.spec.containers[0].image}'
# expect: xpkg.upbound.io/crossplane/crossplane:v2.2.0 (or similar)
```

### Step 0.3: create a synthetic XRD that matches our harness scenario

We want `XWidget` to mirror what the Kamera harness uses. Save as `xrd.yaml`:

```yaml
apiVersion: apiextensions.crossplane.io/v1
kind: CompositeResourceDefinition
metadata:
  name: xwidgets.example.org
spec:
  group: example.org
  names:
    kind: XWidget
    plural: xwidgets
  versions:
    - name: v1
      served: true
      referenceable: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                message:
                  type: string
                  default: "hello from real cluster"
            status:
              type: object
              properties:
                phase:
                  type: string
```

```bash
kubectl apply -f xrd.yaml
kubectl wait --for=condition=Established xrd/xwidgets.example.org --timeout=60s
```

### Step 0.4: install function-patch-and-transform (provides the `composition` capability)

```bash
cat <<EOF | kubectl apply -f -
apiVersion: pkg.crossplane.io/v1
kind: Function
metadata:
  name: function-patch-and-transform
spec:
  package: xpkg.upbound.io/crossplane-contrib/function-patch-and-transform:v0.10.0
EOF

kubectl wait --for=condition=Healthy function/function-patch-and-transform --timeout=120s
kubectl get functionrevision -l pkg.crossplane.io/package=function-patch-and-transform
```

### Step 0.5: observation tooling

Install `stern` for live multi-pod log tailing (optional but very useful for R-12):

```bash
brew install stern  # or: curl -L .../stern && chmod +x
```

A useful watch for the audits:

```bash
# In a separate terminal during each test:
kubectl get xwidget,configmap,secret,composition,compositionrevision -A --watch
```

---

## R-11 — F1 manual update policy on real cluster

**Hypothesis:** changing `compositionRef` from alpha to beta while leaving `compositionRevisionRef = alpha-rev-1` causes Crossplane to compose using alpha's revision content despite `compositionRef = beta`. No error.

**Falsification:** if a defaulting webhook auto-corrects `compositionRevisionRef` when `compositionRef` changes, F1 doesn't reproduce on a real cluster, and the Kamera trace was simulating a state that production never permits.

### Manifests

`composition-alpha.yaml`:

```yaml
apiVersion: apiextensions.crossplane.io/v1
kind: Composition
metadata:
  name: widget-composition-alpha
spec:
  compositeTypeRef:
    apiVersion: example.org/v1
    kind: XWidget
  mode: Pipeline
  pipeline:
    - step: produce-configmap
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
                namespace: default
              data:
                source: alpha
```

`composition-beta.yaml`:

```yaml
apiVersion: apiextensions.crossplane.io/v1
kind: Composition
metadata:
  name: widget-composition-beta
spec:
  compositeTypeRef:
    apiVersion: example.org/v1
    kind: XWidget
  mode: Pipeline
  pipeline:
    - step: produce-configmap
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
                name: beta-output
                namespace: default
              data:
                source: beta
```

`xr-initial.yaml`:

```yaml
apiVersion: example.org/v1
kind: XWidget
metadata:
  name: example
spec:
  compositionRef:
    name: widget-composition-alpha
  compositionRevisionRef:
    name: widget-composition-alpha-1   # will need to look up actual revision name
  compositionUpdatePolicy: Manual
  message: "F1 audit"
```

### Recipe

```bash
# 1. Apply both Compositions, wait for revisions
kubectl apply -f composition-alpha.yaml -f composition-beta.yaml
sleep 5
kubectl get compositionrevision

# Expected output: two revisions, one per Composition. Note the actual names
# (likely widget-composition-alpha-<hash> and widget-composition-beta-<hash>).
ALPHA_REV=$(kubectl get compositionrevision -l crossplane.io/composition-name=widget-composition-alpha -o jsonpath='{.items[0].metadata.name}')
BETA_REV=$(kubectl get compositionrevision -l crossplane.io/composition-name=widget-composition-beta -o jsonpath='{.items[0].metadata.name}')
echo "ALPHA_REV=$ALPHA_REV BETA_REV=$BETA_REV"

# 2. Edit xr-initial.yaml to use $ALPHA_REV as compositionRevisionRef, then apply
sed -i.bak "s/widget-composition-alpha-1/$ALPHA_REV/" xr-initial.yaml
kubectl apply -f xr-initial.yaml

# 3. Wait for composed resource (alpha-output ConfigMap)
kubectl wait --for=condition=Synced xwidget/example --timeout=60s
kubectl get configmap -A | grep -E 'alpha-output|beta-output'
# expect: alpha-output present, beta-output absent.

# 4. THE CRUCIAL STEP: change compositionRef but keep compositionRevisionRef
kubectl patch xwidget/example --type=merge -p \
  '{"spec":{"compositionRef":{"name":"widget-composition-beta"}}}'

# 5. Wait a beat, then observe
sleep 10
kubectl get xwidget/example -o jsonpath='{.spec.compositionRef.name}{"\n"}{.spec.compositionRevisionRef.name}{"\n"}'
kubectl get xwidget/example -o yaml | grep -A20 "status:"
kubectl get configmap -A | grep -E 'alpha-output|beta-output'
```

### Observation criteria

| Outcome | Interpretation |
|---|---|
| `compositionRef = beta` and `compositionRevisionRef = $ALPHA_REV` and `alpha-output` ConfigMap still present | **F1 reproduces.** Bug confirmed on real cluster. Strong evidence to post. |
| `compositionRef = beta` and `compositionRevisionRef` was rewritten to `$BETA_REV` automatically | Defaulting/webhook caught it. F1 doesn't reproduce in production. The Kamera report was a simulation artifact. **Retract #7220.** |
| `compositionRef = beta`, `compositionRevisionRef = $ALPHA_REV`, but `Synced=False` with explicit error | F1 reproduces but Crossplane catches it. Less severe than original report; revise upstream draft. |
| XR rejected by API server admission | Webhook is doing strict validation. F1 doesn't reproduce. **Retract #7220.** |

### Cleanup

```bash
kubectl delete xwidget/example
kubectl delete composition widget-composition-alpha widget-composition-beta
kubectl delete configmap -A -l app.kubernetes.io/managed-by=crossplane
```

---

## R-12 — F5 stale `ValidPipeline` race on real cluster

**Hypothesis:** removing `composition` capability from a function while a Composition uses it creates a window where `CompositeReconciler` reads stale `ValidPipeline=True` and composes with the invalidated function.

**Challenge:** capabilities are baked into FunctionRevision objects via the function's package metadata. The clean way to "remove" a capability is to publish a new function version without it. The fast way for testing is to `kubectl edit` the FunctionRevision spec directly (if it's mutable).

### Approach A — quick test via direct edit (recommended first attempt)

```bash
# 1. Find an active FunctionRevision
FR=$(kubectl get functionrevision -l pkg.crossplane.io/package=function-patch-and-transform \
  -o jsonpath='{.items[0].metadata.name}')
echo "FunctionRevision: $FR"

# 2. Inspect current capabilities
kubectl get functionrevision/$FR -o jsonpath='{.spec.capabilities}{"\n"}'
# expect: ["composition"] or similar

# 3. Try to patch capabilities to empty (this may be rejected — that's information too)
kubectl patch functionrevision/$FR --type=json \
  -p='[{"op":"replace","path":"/spec/capabilities","value":[]}]'
# If rejected: "FunctionRevision spec is immutable." — proceed to Approach B.
# If accepted: continue.
```

### Approach B — re-deploy function without composition capability

This is more involved and requires building a function package. **Skip on first pass; only do this if Approach A's information isn't sufficient.** Out of scope for this initial plan.

### Recipe (assuming Approach A succeeds)

```bash
# Setup: apply a Composition + XR using function-patch-and-transform
kubectl apply -f composition-alpha.yaml    # from R-11
kubectl apply -f xr-initial.yaml           # from R-11; revert to alpha
kubectl wait --for=condition=Synced xwidget/example --timeout=60s

# Capture timestamp T0
T0=$(date -u +%s)

# Strip composition capability (Approach A above)
kubectl patch functionrevision/$FR --type=json \
  -p='[{"op":"replace","path":"/spec/capabilities","value":[]}]'

# Tail Crossplane logs to see the race in real time:
stern -n crossplane-system crossplane > /tmp/r12-crossplane.log &

# Watch for any new compositions in the next 60 seconds
for i in {1..12}; do
  echo "=== T+$((i*5))s ==="
  kubectl get configmap alpha-output -o jsonpath='{.metadata.resourceVersion}' 2>/dev/null
  kubectl get xwidget/example -o jsonpath='{.status.conditions[?(@.type=="Synced")].status}' 2>/dev/null
  echo
  sleep 5
done

kill %1 2>/dev/null
```

### Observation criteria

| Outcome | Interpretation |
|---|---|
| ConfigMap `resourceVersion` increments AFTER `T0` (within seconds) → `Synced=False` shortly after | F5 race likely fired: a compose happened during the stale window before invalidation. **Strong evidence.** |
| ConfigMap `resourceVersion` stable at T0 value → `Synced=False` immediately | `CompositionRevisionReconciler` won the race. F5 doesn't reproduce on this cluster (might still happen under different timing). Less strong; consider re-running 5+ times to estimate frequency. |
| FunctionRevision patch rejected | Need Approach B. Document the rejection and either skip R-12 or invest in building a function package. |

### Note on retry

F5 is a race — a single negative result doesn't disprove it. Run 5–10 trials. If even one shows the post-T0 compose, F5 is real.

### Cleanup

Same as R-11 plus:
```bash
# Restore the capability if you patched it
kubectl patch functionrevision/$FR --type=json \
  -p='[{"op":"replace","path":"/spec/capabilities","value":["composition"]}]'
```

---

## R-13 — F6 fatal-function orphans on real cluster

**Hypothesis:** when a Composition switches to use a function that returns SEVERITY_FATAL, previously-composed resources persist as orphans, and the XR shows confusing `Synced=False` + `Ready=True`.

**Challenge:** we need a function that returns SEVERITY_FATAL. Easiest options:
1. Use `function-cel` with an expression that always errors.
2. Use `function-go-templating` with a template that always errors.
3. Build a custom 20-line function (overkill for this audit).

We'll use option 1.

### Setup

```bash
# Install function-cel
cat <<EOF | kubectl apply -f -
apiVersion: pkg.crossplane.io/v1
kind: Function
metadata:
  name: function-cel
spec:
  package: xpkg.upbound.io/crossplane-contrib/function-cel:v0.4.0
EOF

kubectl wait --for=condition=Healthy function/function-cel --timeout=120s
```

### Manifests

`composition-fatal.yaml`:

```yaml
apiVersion: apiextensions.crossplane.io/v1
kind: Composition
metadata:
  name: widget-composition-fatal
spec:
  compositeTypeRef:
    apiVersion: example.org/v1
    kind: XWidget
  mode: Pipeline
  pipeline:
    - step: always-fatal
      functionRef:
        name: function-cel
      input:
        apiVersion: cel.fn.crossplane.io/v1beta1
        kind: Expressions
        # Some construction that produces a fatal Result.
        # Consult function-cel docs for the exact shape — alternatively use a
        # nonexistent function reference to force fatal.
        expressions:
          - condition: "true"
            result: |
              {
                "results": [{
                  "severity": "SEVERITY_FATAL",
                  "message": "audit: simulated fatal"
                }]
              }
```

> Note: the exact `function-cel` input shape may differ. Consult `https://github.com/crossplane-contrib/function-cel` for current schema. **Alternative:** create a Composition referencing a non-installed function name — this ALSO triggers a Crossplane-side error and approximates fatal behavior.

### Recipe

```bash
# 1. Start with a working Composition + XR (compose alpha-output ConfigMap)
kubectl apply -f composition-alpha.yaml
kubectl apply -f xr-initial.yaml
kubectl wait --for=condition=Synced xwidget/example --timeout=60s
kubectl get configmap alpha-output  # verify present

# 2. Switch to fatal Composition
kubectl apply -f composition-fatal.yaml
kubectl patch xwidget/example --type=merge -p \
  '{"spec":{"compositionRef":{"name":"widget-composition-fatal"}}}'
# Note: if compositionUpdatePolicy is Manual, you'll also need to update compositionRevisionRef

# 3. Observe over a long window
T0=$(date -u +%s)
for i in {1..30}; do
  echo "=== T+$((i*30))s ==="
  kubectl get configmap alpha-output -o jsonpath='{.metadata.name}{"\n"}' 2>/dev/null
  kubectl get xwidget/example -o jsonpath='Synced={.status.conditions[?(@.type=="Synced")].status} Ready={.status.conditions[?(@.type=="Ready")].status}{"\n"}' 2>/dev/null
  sleep 30
done
# This runs for 15 minutes. Adjust loop count for longer windows.
```

### Observation criteria

| Outcome | Interpretation |
|---|---|
| `alpha-output` ConfigMap persists for 15+ minutes AND XR shows `Synced=False, Ready=True` | **F6 fully reproduces.** Both orphan persistence and stale Ready=True confirmed. Strong evidence to post both sub-claims. |
| ConfigMap persists; Ready transitions to False/Unknown after some time | F6 orphan reproduces; Ready=True is transient. Revise the stale-Ready-True draft to "transient stale Ready=True" or similar. |
| ConfigMap deleted within seconds | F6 doesn't reproduce — production has a GC path we don't model. **Likely scenario:** Crossplane's success path on subsequent reconciles eventually GC's it. Investigate the cleanup mechanism. |
| Other (unexpected condition states, errors) | Document. Compare against R-3 source-grounded predictions. |

### Long-running observation

For the strongest claim, leave the cluster running overnight (or 2+ hours) and check `alpha-output` the next morning. If it's still there, "permanent orphan while function stays fatal" is empirically confirmed.

### Cleanup

```bash
kubectl delete xwidget/example
kubectl delete composition widget-composition-alpha widget-composition-fatal
kubectl delete configmap alpha-output -n default
```

---

## R-14 — F3 composition deletion on real cluster

**Hypothesis (original F3):** deleting a Composition while an XR is bound produces a permanent `ReconcileError` on the XR with no self-recovery.

**Hypothesis (SPRINT-0001 retraction-candidate):** the original behavior was a Kamera artifact; on a real cluster the XR self-recovers or behaves cleanly.

### Recipe

```bash
# 1. Setup
kubectl apply -f composition-alpha.yaml
kubectl apply -f xr-initial.yaml  # Manual policy, alpha rev
kubectl wait --for=condition=Synced xwidget/example --timeout=60s

# 2. Capture state before delete
kubectl get xwidget/example -o yaml > /tmp/r14-xr-before.yaml

# 3. Delete the Composition (XR is still using it)
kubectl delete composition widget-composition-alpha

# 4. Observe XR over 5+ minutes
for i in {1..10}; do
  echo "=== T+$((i*30))s ==="
  kubectl get xwidget/example -o yaml | grep -A10 "conditions:"
  kubectl get composition widget-composition-alpha 2>&1 | head -2
  echo "---"
  sleep 30
done

# 5. Snapshot final state
kubectl get xwidget/example -o yaml > /tmp/r14-xr-after.yaml
diff /tmp/r14-xr-before.yaml /tmp/r14-xr-after.yaml
```

### Observation criteria

| Outcome | Interpretation |
|---|---|
| XR enters and stays in `ReconcileError` with "Composition not found" or "no compatible Compositions found" for 5+ min | F3 ORIGINAL reproduces on real cluster. **Un-retract #7222.** |
| XR self-recovers (e.g., Synced=True after Composition is restored, or some other clean exit) | F3 retraction is correct. Original behavior was a Kamera artifact. **Confirm retraction in #7222.** |
| Crossplane prevents the Composition deletion (finalizer) — `kubectl delete` hangs | Production has a finalizer the Kamera report didn't model. Different finding entirely; rewrite #7222. |
| Composition deletes cleanly AND XR composed resources also get cleaned up via cascade | Production cascades; the original "permanent error loop" was simulation artifact. **Confirm retraction.** |

### Bonus probe: re-create the Composition

After observing 5 min of post-delete behavior, re-create the Composition:

```bash
kubectl apply -f composition-alpha.yaml
sleep 30
kubectl get xwidget/example -o yaml | grep -A10 "conditions:"
```

If the XR self-recovers when the Composition reappears, that's another data point that the original "permanent error loop" claim was overstated.

### Cleanup

```bash
kubectl delete xwidget/example  # if still present
kubectl delete composition widget-composition-alpha
```

---

## Phase 99 — overall tear-down

```bash
kind delete cluster --name crossplane-audit
```

If you want to keep the cluster between audits (recommended — saves the Crossplane install time), just delete the test artifacts after each audit and reuse the cluster.

---

## Documenting results

For each audit, after running it, append a short results block to the corresponding audit file:

- R-11 results → `docs/crossplane-audits/R11-f1-real-cluster.md` (new file).
- R-12 results → `docs/crossplane-audits/R12-f5-real-cluster.md`.
- R-13 results → `docs/crossplane-audits/R13-f6-real-cluster.md`.
- R-14 results → `docs/crossplane-audits/R14-f3-real-cluster.md`.

Each results file should include:
1. Date and Crossplane version verified (`kubectl get deployment crossplane -n crossplane-system -o jsonpath='...'`).
2. Exact `kubectl` outputs (or screenshots) showing the observed state.
3. Resolution against the observation criteria table above.
4. Updated posting recommendation for the corresponding upstream-update draft.

When all four are done, update the audits README and the top-level hub doc with ✅ markers for Tier 3.

---

## Decision matrix after Tier 3

Combining Tier 1 (code reads, done) + Tier 2 (in flight) + Tier 3 (this plan), each finding lands at one of:

- **All three tiers green:** post the upstream draft with high confidence. Reference the audits in the comment if useful.
- **Tier 3 contradicts Tier 1/2:** trust Tier 3. Either retract or rewrite the draft.
- **Tier 3 inconclusive (e.g., F5 race didn't fire in 5 trials):** post with the caveat "reproduced in N of M trials" and let the maintainer decide if that's strong enough.

The right outcome is *honest evidence*, not *strong findings at all costs*. If R-12 fails to reproduce F5 in 10 trials, that's a publishable result too.
