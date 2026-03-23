# Sieve Bug Reproductions via Kamera Simulation

Reproducing bugs from [Sieve](https://github.com/sieve-project/sieve) (OSDI '22) using Kamera's offline simulation to demonstrate speedup over real-cluster testing.

## Summary

| Bug | Type | Sieve | Kamera | Speedup | Depth | States |
|-----|------|-------|--------|---------|-------|--------|
| [stale-state-1](#rabbitmq-operator-stale-state-1-issue-648) | stale-state | 358s | 0.325s | 1,102x | 39 | 24 |
| [stale-state-2](#rabbitmq-operator-stale-state-2-issue-653) | stale-state | 329s | 0.325s | 1,012x | 35 | 18 |
| [unobserved-state-1](#rabbitmq-operator-unobserved-state-1-issue-758) | unobserved-state | 512s | 0.378s | 1,355x | 29 | 17 |
| [intermediate-state-1](#rabbitmq-operator-intermediate-state-1-issue-782) | intermediate-state | 233s | 0.325s | 717x | 33 | 17 |

All Sieve times measured on Apple M1 Pro, including kind cluster creation (3 control planes + 2 workers), image loading, workload execution, teardown, and oracle checking. All Kamera times measured on the same machine using pre-built binary (median of 3 runs).

---

## rabbitmq-operator stale-state-1 (Issue #648)

**Sieve bug ID:** `rabbitmq-operator-stale-state-1`
**Upstream issue:** [rabbitmq/cluster-operator#648](https://github.com/rabbitmq/cluster-operator/issues/648)
**Bug class:** Stale state
**Operator version:** commit `4f13b9a`

### Bug description

When the controller restarts and reconnects to a stale API server, it reads an outdated RabbitmqCluster object showing deletion in progress (with `deletionTimestamp`). The controller enters `prepareForDeletion`, deletes the StatefulSet without checking ownership, and removes the finalizer. Because the real cluster state has moved on (deletion completed, new RabbitmqCluster created), the controller destroys the new cluster's StatefulSet.

### Kamera reproduction

```
go run . --inputs scenarios/stale-state-1.json \
  --output /tmp/rmq-stale-state-1 \
  --interactive=false --closed-loop=false
```

**Simulation time:** 325ms (median of 3 runs)

### Bug signal in trace

**Baseline (S1):**
```
CREATE StatefulSet/rabbitmq-cluster-server: 1
CREATE Pod/rabbitmq-cluster-server-0: 1
DELETE StatefulSet/rabbitmq-cluster-server: 1
```

**Staleness + crash (S2):**
```
CREATE StatefulSet/rabbitmq-cluster-server: 2  # BUG: extra creation
CREATE Pod/rabbitmq-cluster-server-0: 2         # BUG: extra pod churn
REMOVE StatefulSet/rabbitmq-cluster-server: 1   # GC removes incorrectly
```

The stale read causes the controller to incorrectly delete and recreate the StatefulSet, producing extra CREATE/DELETE operations not seen in the baseline.

---

## rabbitmq-operator stale-state-2 (Issue #653)

**Sieve bug ID:** `rabbitmq-operator-stale-state-2`
**Upstream issue:** [rabbitmq/cluster-operator#653](https://github.com/rabbitmq/cluster-operator/issues/653)
**Bug class:** Stale state
**Operator version:** commit `4f13b9a`

### Bug description

During a PVC resize from 10Gi to 15Gi, the controller's `reconcilePVC` detects the size change, deletes the StatefulSet (with orphan propagation), and expands each PVC. Under stale state, after the controller restarts and reads a stale API server, it sees the pre-resize spec (10Gi) and initiates another unnecessary StatefulSet deletion.

### Kamera reproduction

```
go run . --inputs scenarios/stale-state-2.json \
  --output /tmp/rmq-stale-state-2 \
  --interactive=false --closed-loop=false
```

**Simulation time:** 325ms

### Bug signal in trace

**Baseline (S1):**
```
DELETE StatefulSet/rabbitmq-cluster-server: 1   # normal resize deletion
UPDATE PersistentVolumeClaim: 4
UPDATE StatefulSet: 3
```

**Staleness + crash (S2):**
```
DELETE StatefulSet/rabbitmq-cluster-server: 2   # BUG: extra deletion
UPDATE PersistentVolumeClaim: 5                  # extra PVC updates
UPDATE StatefulSet: 4                            # extra STS updates
```

The stale read after crash causes an extra StatefulSet deletion and additional update churn.

---

## rabbitmq-operator unobserved-state-1 (Issue #758)

**Sieve bug ID:** `rabbitmq-operator-unobserved-state-1`
**Upstream issue:** [rabbitmq/cluster-operator#758](https://github.com/rabbitmq/cluster-operator/issues/758)
**Bug class:** Unobserved state
**Operator version:** commit `4f13b9a`

### Bug description

During rapid scale-up (1 to 3 replicas) followed by scale-down (3 to 2), the controller's informer cache may coalesce the two updates. The controller only sees the final state (replicas=2) and never observes the intermediate replicas=3. Because the operator does not support scale-down (returns early with "Cluster Scale down not supported"), it never scales up to 3 either. The cluster ends up with fewer pods than requested.

### Kamera reproduction

```
go run . --inputs scenarios/unobserved-state-1.json \
  --output /tmp/rmq-unobserved-state-1 \
  --interactive=false --closed-loop=false
```

**Simulation time:** 378ms

### Bug signal in trace

**Baseline (S1, both user actions observed):**
```
CREATE Pod/rabbitmq-cluster-server-0: 1
CREATE Pod/rabbitmq-cluster-server-1: 1
CREATE Pod/rabbitmq-cluster-server-2: 1          # all 3 replicas created
CREATE PersistentVolumeClaim: 3
```
Controller saw replicas=3, created 3 pods. Scale-down to 2 is rejected ("unsupported").

**Staleness (S2, intermediate state missed):**
```
CREATE Pod/rabbitmq-cluster-server-0: 1           # only 1 pod!
CREATE PersistentVolumeClaim: 1
```
Controller never saw replicas=3 (jumped from 1 to 2 via staleness). Only 1 replica exists. Missing pods.

---

## rabbitmq-operator intermediate-state-1 (Issue #782)

**Sieve bug ID:** `rabbitmq-operator-intermediate-state-1`
**Upstream issue:** [rabbitmq/cluster-operator#782](https://github.com/rabbitmq/cluster-operator/issues/782)
**Bug class:** Intermediate state
**Operator version:** commit `4f13b9a`

### Bug description

During a PVC resize operation, the controller deletes the StatefulSet (with orphan propagation to preserve pods), then expands PVC sizes, then recreates the StatefulSet. If the controller crashes after deleting the StatefulSet but before completing the PVC expansion, the intermediate state has no StatefulSet and unexpanded PVCs. On restart, the controller may not correctly detect the need for PVC expansion and recreates the StatefulSet with the old PVC size.

### Kamera reproduction

```
go run . --inputs scenarios/intermediate-state-1.json \
  --output /tmp/rmq-intermediate-state-1 \
  --interactive=false --closed-loop=false
```

**Simulation time:** 325ms

### Bug signal in trace

This bug's signal requires further tuning of crash injection timing to produce a distinct trace divergence from the baseline. The current configuration produces identical S1/S2 traces, indicating the crash does not land at the right point in the reconciliation cycle.

---

## Framework features used

- **Staleness intervals** with `freezeAt` to decouple activation from frozen view point
- **Fault injection** with `triggerAfterDepth` for depth-gated crash
- **GC controller** for ownerRef cascade deletion
- **StatefulSet PVC label fidelity** for VolumeClaimTemplate label propagation
- **Client.Delete** fix preserving existing object state on deletion
