# Vendored CSI manifests

These are unmodified copies of the Kubernetes CSI manifests used by Sieve's
RabbitMQ `resize-pvc` workload. They are checked in so artifact evaluation does
not depend on `raw.githubusercontent.com` availability or rate limits.

| Local file | Upstream repository and release | Upstream path |
|---|---|---|
| `volumesnapshotclasses.yaml` | `kubernetes-csi/external-snapshotter` v4.1.1 (`8e12622e`) | `client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml` |
| `volumesnapshotcontents.yaml` | `kubernetes-csi/external-snapshotter` v4.1.1 (`8e12622e`) | `client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml` |
| `volumesnapshots.yaml` | `kubernetes-csi/external-snapshotter` v4.1.1 (`8e12622e`) | `client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml` |
| `snapshot-controller-rbac.yaml` | `kubernetes-csi/external-snapshotter` v4.1.1 (`8e12622e`) | `deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml` |
| `snapshot-controller.yaml` | `kubernetes-csi/external-snapshotter` v4.1.1 (`8e12622e`) | `deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml` |
| `csi-snapshotter-rbac.yaml` | `kubernetes-csi/external-snapshotter` v4.1.1 (`8e12622e`) | `deploy/kubernetes/csi-snapshotter/rbac-csi-snapshotter.yaml` |
| `csi-provisioner-rbac.yaml` | `kubernetes-csi/external-provisioner` v2.2.1 (`9687057c`) | `deploy/kubernetes/rbac.yaml` |
| `csi-attacher-rbac.yaml` | `kubernetes-csi/external-attacher` v3.2.1 (`f837e1c6`) | `deploy/kubernetes/rbac.yaml` |
| `csi-resizer-rbac.yaml` | `kubernetes-csi/external-resizer` v1.2.0 (`45b15b7a`) | `deploy/kubernetes/rbac.yaml` |
| `csi-external-health-monitor-rbac.yaml` | `kubernetes-csi/external-health-monitor` v0.3.0 (`22799939`) | `deploy/kubernetes/external-health-monitor-controller/rbac.yaml` |

The snapshotter project license is in `LICENSE.external-snapshotter`. The four
sidecar projects use the license in `LICENSE.csi-sidecars`.

SHA-256 checksums:

```text
750709567d9c767248daa5a5e550d7c09d9f2fcd05b75f83b256a0781fc130c3  volumesnapshotclasses.yaml
7d7cbfbefe53ed1965c050c17cfe2586c086987f4bbb1dcca219b5fdc031e592  volumesnapshotcontents.yaml
c6c608935f4b8a0d7c5252c72ff930206f3448c9a672e5eb8fb9ddd4569abb22  volumesnapshots.yaml
e3862bfb2859788aa85854c03eedccfd031e7fde629ef880eb29564cc61c0250  snapshot-controller-rbac.yaml
19ac921a936eca214d3cfe3dcfe29f356be456defc95798edbbd382230287e39  snapshot-controller.yaml
9e4765a77b5e39438daa7cb81aa522e1cbdbd25eb8b3f41fde6c1ea39827e485  csi-snapshotter-rbac.yaml
617310b7a08dac5886249e6ca36d1be95c861ead1dcb3951f12f79fc2064a40e  csi-provisioner-rbac.yaml
3a38b5198b0496927c0ce26c63e71d43e2263e2dbbb93a83acdec71d7c90a766  csi-attacher-rbac.yaml
aff2e17d30e1497035de9179682c66d309dc7664dd25882c2a0d25b60cc616ea  csi-resizer-rbac.yaml
3205599eda9a252e2737acabe22591afd8da373c1aab5275ce54223adfe2d127  csi-external-health-monitor-rbac.yaml
```
