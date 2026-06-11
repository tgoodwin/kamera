# Tier 3 cluster artifacts

Trace evidence captured from the kind-based real-cluster reproductions executed 2026-04-29. See [`../../R11-f1-real-cluster.md`](../../R11-f1-real-cluster.md), [`../../R12-f5-real-cluster.md`](../../R12-f5-real-cluster.md), [`../../R13-f6-real-cluster.md`](../../R13-f6-real-cluster.md), [`../../R14-f3-real-cluster.md`](../../R14-f3-real-cluster.md) for the audit narratives that reference these artifacts.

## Cluster freeze-point

[`cluster-info.txt`](./cluster-info.txt) captures the kind/Crossplane/function versions at the time of the audit. Re-run `kubectl get` against the same cluster to verify state still matches the captured yamls.

- kind: v0.30.0 (kindest/node:v1.30.0, Debian 12, kubelet v1.30.0)
- Crossplane: `xpkg.crossplane.io/crossplane/crossplane:v2.2.0` (helm chart 2.2.0)
- Function: `function-patch-and-transform v0.10.0` (FunctionRevision `function-patch-and-transform-9991175eae0e`)

## Shared inputs

These manifests were re-applied for each audit:

- [`xrd.yaml`](./xrd.yaml) — `XWidget` XRD definition.
- [`composition-alpha.yaml`](./composition-alpha.yaml) — Composition producing `ConfigMap/default/alpha-output { source: alpha }`.
- [`composition-beta.yaml`](./composition-beta.yaml) — Composition producing `ConfigMap/default/beta-output { source: beta }`.
- [`composition-fatal-via-missing-fn.yaml`](./composition-fatal-via-missing-fn.yaml) — Composition pipeline referencing a function that doesn't resolve to an active FunctionRevision; used by R-13 to exercise the GC-skip code path.
- [`kind-config.yaml`](./kind-config.yaml) — single-node kind cluster config.

## R-11 (F1: Manual update policy wrong revision)

Folder: [`r11/`](./r11/)

Probe: `kubectl patch xwidget/example` to mutate `compositionRef alpha→beta` while leaving `compositionRevisionRef` pinned to alpha-rev under `compositionUpdatePolicy: Manual`.

| Artifact | What it shows |
|---|---|
| [`r11/xr-pinned-to-alpha-rev.yaml`](./r11/xr-pinned-to-alpha-rev.yaml) | Initial XR manifest applied (Manual policy, pinned to alpha-rev) |
| [`r11/xr-baseline-before-patch.yaml`](./r11/xr-baseline-before-patch.yaml) | XR after initial reconcile: `compositionRef=alpha`, `compositionRevisionRef=alpha-a39f01a`, `Synced=True` |
| [`r11/configmap-alpha-output-baseline.yaml`](./r11/configmap-alpha-output-baseline.yaml) | Composed ConfigMap `alpha-output { source: alpha }` |
| [`r11/xr-after-patch-T+30s.yaml`](./r11/xr-after-patch-T+30s.yaml) | **F1 evidence:** XR shows `compositionRef=beta`, `compositionRevisionRef=alpha-a39f01a`, `Synced=True (ReconcileSuccess)` — cross-referenced state is silently accepted |
| [`r11/configmap-alpha-output-after-patch-T+30s.yaml`](./r11/configmap-alpha-output-after-patch-T+30s.yaml) | Composed ConfigMap STILL `alpha-output` with `source: alpha` — Crossplane reconciles to the pinned revision's content despite the new compositionRef |
| [`r11/configmap-beta-output-after-patch-T+30s.txt`](./r11/configmap-beta-output-after-patch-T+30s.txt) | `Error from server (NotFound)` — `beta-output` was never created |
| [`r11/observation.log`](./r11/observation.log) | 60s sampling (every 15s) showing cross-reference state stable |

## R-13 (F6: orphan persistence while function fatal)

Folder: [`r13/`](./r13/)

Probe: switch XR's `compositionRef` from `widget-composition-alpha` to `widget-composition-fatal` (which references a non-resolvable function). The pipeline cannot complete; GC of composed resources is skipped — same observable consequence as the SEVERITY_FATAL early-return at `composition_functions.go:439`.

| Artifact | What it shows |
|---|---|
| [`r13/xr-baseline-before-fatal-switch.yaml`](./r13/xr-baseline-before-fatal-switch.yaml) | XR after initial reconcile under alpha: `Synced=True`, `resourceRefs: [alpha-output]` |
| [`r13/configmap-alpha-output-baseline.yaml`](./r13/configmap-alpha-output-baseline.yaml) | Composed `alpha-output` ConfigMap, `resourceVersion: 8668` |
| [`r13/xr-after-fatal-switch-T+30s.yaml`](./r13/xr-after-fatal-switch-T+30s.yaml) | **F6 evidence:** XR shows `Synced=False`, message `"cannot find an active FunctionRevision (a FunctionRevision with spec.desiredState: Active)"`, but `resourceRefs` still tracks `[alpha-output]` |
| [`r13/configmap-alpha-output-after-fatal-switch-T+30s.yaml`](./r13/configmap-alpha-output-after-fatal-switch-T+30s.yaml) | Same `resourceVersion: 8668` — orphan ConfigMap untouched |
| [`r13/observation.log`](./r13/observation.log) | 3-minute sampling (every 30s) showing orphan persistence with no GC |

## R-14 (F3: composition deletion error loop)

Folder: [`r14/`](./r14/)

Probe: `kubectl delete composition widget-composition-alpha` while `XWidget/example` is bound to it under Manual policy. Crossplane has no finalizer protecting Composition deletion in v2.2.0, and the owned `CompositionRevision` is GC'd via Kubernetes ownerReferences.

| Artifact | What it shows |
|---|---|
| [`r14/xr-baseline-before-composition-delete.yaml`](./r14/xr-baseline-before-composition-delete.yaml) | XR after initial reconcile: `Synced=True`, `compositionRef=alpha`, `compositionRevisionRef=alpha-a39f01a` |
| [`r14/composition-alpha-baseline.yaml`](./r14/composition-alpha-baseline.yaml) | Composition before delete (no finalizer) |
| [`r14/compositionrevision-alpha-baseline.yaml`](./r14/compositionrevision-alpha-baseline.yaml) | CompositionRevision before delete (`ownerReferences` to Composition) |
| [`r14/configmap-alpha-output-baseline.yaml`](./r14/configmap-alpha-output-baseline.yaml) | Composed `alpha-output` ConfigMap, `resourceVersion: 8803` |
| [`r14/composition-alpha-after-delete.txt`](./r14/composition-alpha-after-delete.txt) | `NotFound` — Composition deleted cleanly, no finalizer hang |
| [`r14/compositionrevision-alpha-after-delete.txt`](./r14/compositionrevision-alpha-after-delete.txt) | `NotFound` — CompositionRevision GC'd by ownerReferences |
| [`r14/xr-after-composition-delete-T+100s.yaml`](./r14/xr-after-composition-delete-T+100s.yaml) | **F3 evidence:** XR shows `Synced=False`, `reason: ReconcileError`, message `"cannot fetch Composition: cannot get CompositionRevision: CompositionRevision.apiextensions.crossplane.io \"widget-composition-alpha-a39f01a\" not found"` — the **`errFetchComp` family** that the harness re-run did NOT surface |
| [`r14/configmap-alpha-output-after-composition-delete-T+100s.yaml`](./r14/configmap-alpha-output-after-composition-delete-T+100s.yaml) | Same `resourceVersion: 8803` — composed dependent untouched |
| [`r14/observation.log`](./r14/observation.log) | 5-minute sampling (every 30s) showing permanent error loop |
| [`r14/xr-before-from-first-run.yaml`](./r14/xr-before-from-first-run.yaml), [`r14/xr-after-from-first-run.yaml`](./r14/xr-after-from-first-run.yaml) | Yamls from the first R-14 run (before artifact directory was set up) — kept for cross-checking |

## R-12 (F5: stale ValidPipeline race)

No artifact files. R-12 was inconclusive on the real cluster (Approach A — direct `status.capabilities` patch — is reverted by the package manager within seconds; Approach B was deferred). The R-12 narrative is in [`../../R12-f5-real-cluster.md`](../../R12-f5-real-cluster.md).

## Reproducing

The kind cluster (`crossplane-audit`) was torn down at the end of the audit session. To reproduce:

```bash
# Phase 0
kind create cluster --name crossplane-audit --image kindest/node:v1.30.0 --config kind-config.yaml
helm repo add crossplane-stable https://charts.crossplane.io/stable
helm install crossplane crossplane-stable/crossplane \
  --namespace crossplane-system --create-namespace --version 2.2.0 --wait
kubectl apply -f xrd.yaml
kubectl wait --for=condition=Established xrd/xwidgets.example.org --timeout=60s
cat <<EOF | kubectl apply -f -
apiVersion: pkg.crossplane.io/v1
kind: Function
metadata:
  name: function-patch-and-transform
spec:
  package: xpkg.upbound.io/crossplane-contrib/function-patch-and-transform:v0.10.0
EOF
kubectl wait --for=condition=Healthy function/function-patch-and-transform --timeout=180s

# Then per-audit, see the per-Rxx audit doc and probe folders.
```

The full per-audit recipes are in [`../../kind-cluster-plan.md`](../../kind-cluster-plan.md).
