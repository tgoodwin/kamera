# Kubernetes registry policy tables

`pkg/tracecheck/manager.go` mirrors Kubernetes 1.35 registry strategy behavior for
built-in resources. The unconditional-update table is sourced from the
`AllowUnconditionalUpdate` implementations under:

- `k8s.io/kubernetes/pkg/registry/core`
- `k8s.io/kubernetes/pkg/registry/apps`
- `k8s.io/kubernetes/pkg/registry/batch`
- `k8s.io/kubernetes/pkg/registry/autoscaling`
- `k8s.io/kubernetes/pkg/registry/certificates`
- `k8s.io/kubernetes/pkg/registry/discovery`
- `k8s.io/kubernetes/pkg/registry/networking`
- `k8s.io/kubernetes/pkg/registry/rbac`
- `k8s.io/kubernetes/pkg/registry/scheduling`
- `k8s.io/kubernetes/pkg/registry/storage`
- `k8s.io/kubernetes/pkg/registry/events`
- `k8s.io/kubernetes/pkg/registry/flowcontrol`
- `k8s.io/kubernetes/pkg/registry/resource`

The status-subresource table is intentionally separate. A resource can have a
status strategy while its main-resource strategy still requires a resource
version, so status-table membership does not imply unconditional main-resource
updates. Its entries come from the `statusStore.UpdateStrategy` registrations
in the same Kubernetes registry tree, plus the API aggregation and API
extension registries.

When upgrading the Kubernetes model, compare both tables with the corresponding
`strategy.go` and `storage.go` files at the new version. Custom resources are
not included because their status and unconditional-update policies come from
the CRD schema and are tracked separately.
