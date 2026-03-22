# Sieve Bug Reproductions via Kamera Simulation

Reproducing bugs from [Sieve](https://github.com/sieve-project/sieve) (OSDI '22) using Kamera's offline simulation to demonstrate speedup over real-cluster testing.

## Summary

| Bug | Controller | Type | Sieve Time | Kamera Time | Speedup |
|-----|-----------|------|-----------|-------------|---------|
| [zk stale-state-1](#zookeeper-operator-stale-state-1-issue-312) | zookeeper-operator | stale-state | 336s | 0.217s | 1,548x |
| [zk stale-state-2](#zookeeper-operator-stale-state-2-issue-314) | zookeeper-operator | stale-state | 455s | 0.200s | 2,275x |
| [zk unobserved-state-1](#zookeeper-operator-unobserved-state-1-issue-453) | zookeeper-operator | unobserved-state | 368s | 0.462s | 796x |

All Sieve times measured on Apple M1 Pro, including kind cluster creation (3 control planes + 2 workers), image loading, workload execution, teardown, and oracle checking. All Kamera times measured on the same machine.

---

## zookeeper-operator stale-state-1 (Issue #312)

**Sieve bug ID:** `zookeeper-operator-stale-state-1`
**Upstream issue:** [pravega/zookeeper-operator#312](https://github.com/pravega/zookeeper-operator/issues/312)
**Bug class:** Stale state
**Operator version:** v0.2.14 (commit `daac1bd`)

### Bug description

When the zookeeper-operator controller restarts and reconnects to a stale API server, it reads an outdated ZookeeperCluster object showing a deletion in progress (with `deletionTimestamp` and the `cleanUpZookeeperPVC` finalizer). Meanwhile, the real cluster state has moved on: the deletion completed and a new ZookeeperCluster was created with fresh PVCs.

The controller enters its finalizer cleanup path (`cleanUpAllPVCs`), lists PVCs matching the cluster's labels, and deletes them. Because the stale UID matches the original PVCs (which were recreated by the new StatefulSet), the controller destroys the new cluster's persistent storage.

### Sieve reproduction

Sieve uses a 5-node kind cluster (3 control planes + 2 workers) with an instrumented K8s API server. The test plan pauses one API server to create a stale view, reconnects the controller to it, and observes the erroneous PVC deletion.

```
cd ~/projects/sieve
KUBECONFIG=$HOME/.kube/config python3 sieve.py \
  -c examples/zktg \
  -m test \
  -p bug_reproduction_test_plans/zookeeper-operator-stale-state-1.yaml
```

**Wall-clock time:** 336 seconds (includes kind cluster creation, image loading, workload execution, teardown, oracle checking)

### Kamera reproduction

The Kamera harness uses the real `ZookeeperClusterReconciler` (copied from the same commit) with a staleness interval and crash injection to model the controller restart + stale read.

```
cd examples/zookeeper-operator
go run . --inputs scenarios/stale-state-1.json \
  --output /tmp/zk-stale-state-1 \
  --interactive=false \
  --closed-loop=false
```

**Scenario configuration (`stale-state-1.json`, S2):**
- `userActionReadyDepths`: DELETE at depth 10, recreate-CREATE at depth 80
- `faultInjection`: crash after 1st write effect, `triggerOnce: true`, `triggerAfterDepth: 80`
- `stalenessIntervals`: ZookeeperCluster kind frozen at sequence 17 (deletion-in-progress with finalizer), activates at sequence 32, catches up at 200

**Simulation time:** 217ms

### Bug signal in trace

**Baseline (S1, no perturbations):**
```
depth=  8 CREATE PVC/data-zookeeper-cluster-0  [StatefulSetController]
depth= 20 DELETE PVC/data-zookeeper-cluster-0  [ZookeeperClusterReconciler]  # normal cleanup
depth= 25 REMOVE PVC/data-zookeeper-cluster-0  [CleanupReconciler]
depth= 79 CREATE PVC/data-zookeeper-cluster-0  [StatefulSetController]       # recreation
```
PVC persists after recreation. Correct behavior.

**Staleness + crash (S2):**
```
depth=  8 CREATE PVC/data-zookeeper-cluster-0  [StatefulSetController]
depth= 20 DELETE PVC/data-zookeeper-cluster-0  [ZookeeperClusterReconciler]  # normal cleanup
depth= 25 REMOVE PVC/data-zookeeper-cluster-0  [CleanupReconciler]
depth= 80 CREATE PVC/data-zookeeper-cluster-0  [StatefulSetController]       # recreation
depth= 95 DELETE PVC/data-zookeeper-cluster-0  [ZookeeperClusterReconciler]  # BUG: stale delete
depth= 96 REMOVE PVC/data-zookeeper-cluster-0  [CleanupReconciler]
```
The new cluster's PVC is erroneously deleted by the controller reading stale deletion state. Data loss.

### Framework features used

- **Staleness intervals** with `freezeAt` to decouple activation from frozen view point
- **Fault injection** with `triggerAfterDepth` for depth-gated crash
- **GC controller** for ownerRef cascade deletion (new)
- **StatefulSet PVC label fidelity** fix for VolumeClaimTemplate label propagation (new)
- **Client.Delete** fix preserving existing object state on deletion (new)

---

## zookeeper-operator stale-state-2 (Issue #314)

**Sieve bug ID:** `zookeeper-operator-stale-state-2`
**Upstream issue:** [pravega/zookeeper-operator#314](https://github.com/pravega/zookeeper-operator/issues/314)
**Bug class:** Stale state
**Operator version:** v0.2.14 (commit `daac1bd`)

### Bug description

During a scale-down from 2 to 1 replicas, the controller updates `readyReplicas` from 2 to 1 and deletes the orphan PVC for replica 1. When the cluster is subsequently scaled back up to 2, a new PVC should be created for the fresh replica 1.

Under stale state, the controller reads the old `readyReplicas: 1` (from during scale-down) after reconnecting to a stale API server. It never processes the scale-down cleanup because it sees the stale view throughout the scale-down/scale-up transition. The old PVC for replica 1 persists with stale ZooKeeper membership data, causing the new pod to crash or join the ensemble incorrectly.

### Kamera reproduction

```
go run . --inputs scenarios/stale-state-2.json \
  --output /tmp/zk-stale-state-2 \
  --interactive=false --closed-loop=false
```

**Sieve time:** 455 seconds

**Scenario configuration (`stale-state-2.json`, S2):**
- Environment: ZookeeperCluster with 2 replicas
- `userActionReadyDepths`: scale-down UPDATE (replicas=1) at depth 30, scale-up UPDATE (replicas=2) at depth 80
- `faultInjection`: crash after 1st write, `triggerAfterDepth: 85`
- `stalenessIntervals`: ZookeeperCluster frozen at sequence 35 (scale-down in progress), activates at sequence 40

**Kamera simulation time:** 200ms

### Bug signal in trace

**Baseline (S1):**
```
depth=  8 CREATE PVC/data-zookeeper-cluster-0  [StatefulSetController]
depth=  8 CREATE PVC/data-zookeeper-cluster-1  [StatefulSetController]
depth= 68 DELETE PVC/data-zookeeper-cluster-1  [ZookeeperClusterReconciler]  # scale-down cleanup
depth= 69 REMOVE PVC/data-zookeeper-cluster-1  [CleanupReconciler]
depth= 96 CREATE PVC/data-zookeeper-cluster-1  [StatefulSetController]       # fresh PVC for scale-up
```
Orphan PVC deleted during scale-down, fresh PVC created during scale-up. Correct.

**Staleness + crash (S2):**
```
depth=  8 CREATE PVC/data-zookeeper-cluster-0  [StatefulSetController]
depth=  8 CREATE PVC/data-zookeeper-cluster-1  [StatefulSetController]
```
No PVC deletion or recreation. The controller never observes the scale-down, so it never cleans up the orphan PVC. New pod 1 reuses the stale PVC. Data corruption / ensemble failure.

---

## zookeeper-operator unobserved-state-1 (Issue #453)

**Sieve bug ID:** `zookeeper-operator-unobserved-state-1`
**Upstream issue:** [pravega/zookeeper-operator#453](https://github.com/pravega/zookeeper-operator/issues/453)
**Bug class:** Unobserved state
**Operator version:** v0.2.14 (commit `daac1bd`)

### Bug description

The controller misses a transient state change. During a rapid scale-down (2 to 1) followed by scale-up (1 to 2), the controller's informer cache may coalesce the two updates. The controller only sees the final state (replicas=2) and never observes the intermediate replicas=1. Because it never saw replicas=1, it never ran `cleanupOrphanPVCs` for the removed replica.

The PVC from the old replica 1 persists. When the new replica 1 pod starts, it mounts this stale PVC containing old ZooKeeper data (stale membership list, old transaction logs). This causes the pod to fail to join the ensemble or serve stale data.

This is a level-triggered vs edge-triggered correctness issue: the controller should act on the current state relative to what it has already reconciled, but the orphan PVC cleanup only triggers when `readyReplicas` differs from the PVC count.

### Kamera reproduction

```
go run . --inputs scenarios/unobserved-state-1.json \
  --output /tmp/zk-unobserved-state-1 \
  --interactive=false --closed-loop=false
```

**Scenario configuration (`unobserved-state-1.json`, S2):**
- Environment: ZookeeperCluster with 2 replicas
- `userActionReadyDepths`: scale-down UPDATE (replicas=1) at depth 30, scale-up UPDATE (replicas=2) at depth 35 (rapid succession)
- `stalenessIntervals`: ZookeeperCluster frozen at sequence 25 (before scale-down), catches up at sequence 40 (after scale-up). Controller's view jumps from replicas=2 to replicas=2, skipping the intermediate replicas=1.
- No crash injection needed: the bug is about missed events, not stale reads after restart

**Sieve time:** 368 seconds
**Kamera simulation time:** 462ms

### Bug signal in trace

**Baseline (S1):**
```
depth=  8 CREATE PVC/data-zookeeper-cluster-0  [StatefulSetController]
depth=  8 CREATE PVC/data-zookeeper-cluster-1  [StatefulSetController]
depth= 68 DELETE PVC/data-zookeeper-cluster-1  [ZookeeperClusterReconciler]  # saw scale-down
depth= 69 REMOVE PVC/data-zookeeper-cluster-1  [CleanupReconciler]
depth= 96 CREATE PVC/data-zookeeper-cluster-1  [StatefulSetController]       # fresh PVC
```
Controller observed scale-down, cleaned up orphan PVC, fresh PVC created on scale-up. Correct.

**Staleness (S2, unobserved intermediate state):**
```
depth=  8 CREATE PVC/data-zookeeper-cluster-0  [StatefulSetController]
depth=  8 CREATE PVC/data-zookeeper-cluster-1  [StatefulSetController]
```
No PVC deletion. Controller never saw replicas=1, so `cleanupOrphanPVCs` never identified replica 1's PVC as an orphan. Resource leak: stale PVC persists and is reused by the new pod.
