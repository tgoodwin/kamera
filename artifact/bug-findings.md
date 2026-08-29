# Bug Findings and Tested Revisions

This page documents the 31 bugs reported in the SOSP 2026 paper *Testing
Custom Control Planes Without the Cluster*. The inventory is keyed by paper
finding rather than by issue thread because one public report can cover several
findings.

Status values reflect public maintainer signals as of June 25, 2026:

- **Fixed:** addressed by a code change. For KRO-2, the fix is recorded in two
  maintainer-fork commits linked from the public issue.
- **Resolved:** reviewed and closed without an identified code change, for
  example because maintainers accepted the behavior as an intentional tradeoff.
- **Confirmed:** maintainers accepted the reported behavior as a bug or concern,
  but it was not fixed or otherwise resolved.
- **Reported:** filed publicly without a confirming maintainer signal.
- **Not publicly reported:** no public report was located.

The categories are exclusive in the tables below. Fixed and resolved findings
are included in the paper's confirmed count. In total, maintainers confirmed 20
of the 31 bugs, including 3 fixed and 4 otherwise resolved findings.

## Tested CCP revisions

| CCP | Repository | Tested revision |
|---|---|---|
| Karpenter | [`kubernetes-sigs/karpenter`](https://github.com/kubernetes-sigs/karpenter) | [`8ae07cf8`](https://github.com/kubernetes-sigs/karpenter/commit/8ae07cf8b4ecf8ae3f04bc306d97f1ee40d21849) |
| Crossplane | [`crossplane/crossplane`](https://github.com/crossplane/crossplane) | [`v2.2.0`](https://github.com/crossplane/crossplane/releases/tag/v2.2.0) |
| Kratix | [`syntasso/kratix`](https://github.com/syntasso/kratix) | [`4b813b56`](https://github.com/syntasso/kratix/commit/4b813b5616d72dfbeb05633c3025d7e1dc85a3c7) |
| KRO | [`kro-run/kro`](https://github.com/kro-run/kro) | [`c9320ee9`](https://github.com/kro-run/kro/commit/c9320ee963f745637bb622f6b68853a870187d20) |
| KCP | [`kcp-dev/kcp`](https://github.com/kcp-dev/kcp) | [`301a8f74`](https://github.com/kcp-dev/kcp/commit/301a8f749e7b99a0c81f43b37aa5b5e5ff0fc0b4) |

## Karpenter

| Finding | Public report | Status | Description |
|---|---|---|---|
| KAR-1 | [#2915](https://github.com/kubernetes-sigs/karpenter/issues/2915) | Confirmed | Batching bypasses the `nodes` limit. |
| KAR-2 | [#2915](https://github.com/kubernetes-sigs/karpenter/issues/2915) | Confirmed | Sequential off-by-one in `ExceededBy`. |
| KAR-5 | [#2915](https://github.com/kubernetes-sigs/karpenter/issues/2915) | Confirmed | A custom resource limit is silently ignored. |
| KAR-6 | [#2915](https://github.com/kubernetes-sigs/karpenter/issues/2915) | Confirmed | Multiple NodePools cause a spillover failure. |
| KAR-10 | [#2918](https://github.com/kubernetes-sigs/karpenter/issues/2918) | Reported | NodeClass readiness has a time-of-check/time-of-use race. |
| KAR-12 | [#2916](https://github.com/kubernetes-sigs/karpenter/issues/2916) | Resolved | Emptiness disruption deletes a node with an active workload. |
| KAR-13 | [#2922](https://github.com/kubernetes-sigs/karpenter/issues/2922) | Confirmed | Premature consolidation causes a create-delete cycle. |
| KAR-14 | [#2917](https://github.com/kubernetes-sigs/karpenter/issues/2917) | Resolved | A disruption budget is violated across reconcile cycles. |
| KAR-16 | [#2921](https://github.com/kubernetes-sigs/karpenter/issues/2921) | Resolved | Node hydration produces ordering-dependent labels. |
| KAR-19 | [#2915](https://github.com/kubernetes-sigs/karpenter/issues/2915) | Confirmed | A disruption and provisioner race violates the `nodes` limit. |
| KAR-21 | [#2920](https://github.com/kubernetes-sigs/karpenter/issues/2920) | Resolved | A liveness-timeout race causes create-delete churn. |
| KAR-23 | [#2919](https://github.com/kubernetes-sigs/karpenter/issues/2919) | Confirmed | The provisioner places a Pod on a ghost node. |

## Crossplane

| Finding | Public report | Status | Description |
|---|---|---|---|
| CROSS-1 | [#7220](https://github.com/crossplane/crossplane/issues/7220) | Reported | Manual update policy fetches a revision from the wrong Composition. |
| CROSS-2 | [#7221](https://github.com/crossplane/crossplane/issues/7221), [fix #7283](https://github.com/crossplane/crossplane/pull/7283) | Fixed | Unconditional status updates cause an infinite reconcile cycle. |
| CROSS-3 | [#7222](https://github.com/crossplane/crossplane/issues/7222) | Reported | Composition deletion leaves an orphaned `compositionRef`. |
| CROSS-5 | [#7223](https://github.com/crossplane/crossplane/issues/7223) | Reported | A stale `ValidPipeline` condition permits an invalidated function. |
| CROSS-6 | [#7223](https://github.com/crossplane/crossplane/issues/7223) | Confirmed | Switching to a fatal function leaves resources orphaned while the function remains fatal. |

## Kratix

| Finding | Public report | Status | Description |
|---|---|---|---|
| KRA-1 | [#743](https://github.com/syntasso/kratix/issues/743) | Reported | Timestamp-driven status writes trigger redundant reconciles. |
| KRA-5 | [#740](https://github.com/syntasso/kratix/issues/740) | Reported | Work and WorkPlacement status depends on controller ordering. |
| KRA-6 | [#741](https://github.com/syntasso/kratix/issues/741) | Reported | A WorkController crash leaves a Work uninitialized. |
| KRA-7 | [#742](https://github.com/syntasso/kratix/issues/742) | Reported | Destination deletion produces inconsistent final states. |
| KRA-11 | [#740](https://github.com/syntasso/kratix/issues/740) | Reported | Multiple Works amplify ordering divergence. |
| KRA-13 | [#898](https://github.com/syntasso/kratix/issues/898) | Reported | Multiple HealthRecords produce ordering-dependent health status. |

## KRO

| Finding | Public report | Status | Description |
|---|---|---|---|
| KRO-2 | [#1170](https://github.com/kubernetes-sigs/kro/issues/1170), fixes [`ebd58d3`](https://github.com/jakobmoellerdev/kro/commit/ebd58d342785c0e7d45d1336ccb46b54564c14a4) and [`04e0092`](https://github.com/jakobmoellerdev/kro/commit/04e009269d35acad33d8ff3220117db5f639528c) | Fixed | An Instance Controller crash during apply permanently prevents Service creation. |
| KRO-6 | [#1171](https://github.com/kubernetes-sigs/kro/issues/1171) | Reported | An Instance Controller crash during deletion leaves orphaned children. |

## KCP

| Finding | Public report | Status | Description |
|---|---|---|---|
| KCP-4 | [#3925](https://github.com/kcp-dev/kcp/issues/3925) | Confirmed | A late APIExport leaves endpoint URLs unpopulated. |
| KCP-5 | [#3924](https://github.com/kcp-dev/kcp/issues/3924) | Confirmed | A WorkspaceType change causes LogicalCluster condition divergence. |
| KCP-7 | [#3925](https://github.com/kcp-dev/kcp/issues/3925) | Confirmed | APIExport deletion produces systemic state divergence. |
| KCP-8 | [#3925](https://github.com/kcp-dev/kcp/issues/3925) | Confirmed | Partition deletion produces divergent endpoint configurations. |
| KCP-17 | [#3924](https://github.com/kcp-dev/kcp/issues/3924) | Confirmed | Controller ordering causes a condition-write conflict. |
| KCP-18b | [#3926](https://github.com/kcp-dev/kcp/issues/3926), [fix #4004](https://github.com/kcp-dev/kcp/pull/4004) | Fixed | A mid-reconcile crash leaves an inconsistent LogicalCluster. |

## Excluded false-positive reports

These two reports resulted from simulation-fidelity gaps and are not included
in the paper's 31-bug count. Kamera's deletion handling did not preserve
resource fields correctly in CROSS-C2, while its update handling did not enforce
concurrent-write conflicts correctly in CROSS-C4. Both discrepancies have been
corrected, and neither report reproduces in the corrected simulator.

| Finding | Public report | Description |
|---|---|---|
| CROSS-C2 | [#7224](https://github.com/crossplane/crossplane/issues/7224) | Claim deletion appeared to orphan an XR and its composed resources. |
| CROSS-C4 | [#7224](https://github.com/crossplane/crossplane/issues/7224) | Two XRs appeared able to steal ownership of the same composed resource. |
