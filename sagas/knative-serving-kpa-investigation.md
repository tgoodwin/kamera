# Knative Serving KPA readiness investigation

Working notes on getting the Knative Service example (`examples/knative-serving`) to reach Ready without stubbing PodAutoscaler status. Keep this log updated as we iterate.

## Context
- Goal: `go run ./examples/knative-serving -depth ~30+ -timeout <T> -interactive=false -dump-output /tmp/kamera-results.jsonl` should converge with Service Ready true. Currently Route shows `RevisionMissing` and PA never becomes ready.
- Harness uses fake clients/informers; KPA uses filtered pod informer keyed on `serving.knative.dev/revisionUID`.

## Timeline & Attempts
- **Initial symptom**: Service Ready unknown; Route condition `RevisionMissing`; PA status empty; no SKS created.
- **Harness tweaks (done)**:
  - Added UID seeding on create so filtered informers have UID labels.
  - Dynamic client fallback on update to create if missing.
  - Re-enabled KPA selector `serving.knative.dev/revisionUID`.
  - Synced deployments/podscalable/services/SKS/PA/endpoints (and now pods) to informer caches.
  - Synced filtered pod informer for `serving.knative.dev/revisionUID`; added fake informer imports for deployments/services/endpoints/podautoscalers/SKS/filtered factory.
  - Added logging to label informers and dump keys/lastSyncRV when sync fails.
  - Seed a pod per deployment so the filtered pod informer has content; added pod GVR to dynamic client sync.
- **Current blocker**: During KPA or Service setup, `RunAndSyncInformers` still fails: `failed to wait for cache at index 0 to sync` (multiple indices). Logged failing informers include endpoints and services (empty keys, empty lastSyncRV). Happens before reconcile logic runs.
- **Data**:
  - Latest run `/tmp/kamera-postpatch-logging4.jsonl` shows labeled failing informers; still converged state has Service Ready unknown, Route `RevisionMissing`, PA status empty, no SKS.
  - Filtered pod informer no longer panics, but caches still don’t sync.
- **After pod seeding + fake informer imports**:
  - Seeding a pod per deployment (with template labels) and adding pod GVR to dynamic sync removed the pod informer panic, but informer sync still fails.
  - Failing informers are kubernetes `endpoints`, `services`, and several unlabeled informers (no keys, empty lastSyncRV) during Service setup. Branch aborts with `failed to wait for cache at index 0 to sync`.
  - Revision reconciler now runs far enough to create Deployment/PA/Image; still Route stays `RevisionMissing`, and PA/SKS not ready.
- **Runs with 20s and 90s timeouts**:
  - Headless runs (`/tmp/kamera-run.jsonl`, `/tmp/kamera-long.jsonl`) still abort during KPA setup with `RunAndSyncInformers` failing for services/endpoints/podautoscalers/filtered pods (all `lastSyncRV=""`, empty keys). KPA reconcile never starts; explorer reaches timeout and abandons branches at depth 35.
  - Need to rule out timeout sensitivity by trying a longer (e.g., 5m) bounded run; hypothesis is still that informer wiring/seeding is wrong rather than duration.
- **5m timeout run**:
  - `-timeout 300s` (`/tmp/kamera-5m.jsonl`) still aborts before any KPA work; informer sync failure now shows up while preparing ServerlessService reconciler at depth 35. Same pattern: services/endpoints/podautoscalers filtered pods unsynced (`lastSyncRV=""`, empty keys). Confirms this is not just a short timeout; informers never report synced.
- **Informer list/watch instrumentation (Nov 24, short run)**:
  - Added lightweight list/watch logging reactors. Lists fire and return counts from the fake trackers (e.g., endpoints=3, services=3) with no errors, but informers still never sync (`lastSyncRV=""`, HasSynced=false). Watch logging is suppressed because `RunAndSyncInformers`’ own watch reactor handles watches first, but lack of sync suggests either the reflector isn’t updating `lastSyncRV` (RV empty in the fake tracker) or the DeltaFIFO isn’t draining. This points to fake-apiserver semantics (resource versions, watch behavior) rather than missing seeds.
- **ResourceVersion seeding experiment**:
  - Added a global RV counter to stamp `metadata.resourceVersion` on all seeded objects and the list reactor injected an RV on list responses. Lists now return RV=1 and objects show non-empty RVs (e.g., Revision RV=18), but informer sync still fails (`lastSyncRV=""` on informers). Route/Revision setup still aborts with unsynced services/endpoints/PAs. Suggests the blocker isn’t empty RVs.
- **Align with Knative test informers (Nov 24 evening)**:
  - Trimmed fake informer imports to the sets used by the controllers under test and added per-controller informer collectors mirroring Knative constructors (service/config/revision/route/KPA/SKS). Runs now start and sync informers for Service/Config/Revision/Route/KPA; KPA reconciler runs successfully.
  - ServerlessService and later Service passes still fail at depth ~35 due to `error retrieving deployment selector spec: error fetching Pod Scalable default/kamera-test-deployment: deployments.apps "kamera-test-deployment" not found`. ReplicaSets are created by the sleeve deployment controller, but PodScalable lookup via the duck informer can’t find the Deployment.
  - Removed the explicit podscalable cache seeding call on deployments (it was causing informer start errors); still see the resolver transport warning from Revision controller about missing `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`, but it doesn’t stop reconcile.
- **Event trigger alignment (Nov 25)**:
  - We realized pods were being created (ReplicaSet controller logs “Creating pods”) and are present in state snapshots, but Knative reconcilers are not re-queued when pods/Services/Endpoints change. Added resource deps mirroring Knative controller watches: Pods now trigger the KPA and SKS reconcilers, and core Services/Endpoints trigger the SKS reconciler. Expectation: KPA should rerun after pods appear, and SKS should resync when private/public Services or Endpoints change.
- **Run after trigger wiring (Nov 25)**:
  - Ran `go run . --depth 35 --timeout 300s --interactive=false --log-level panic --dump-output /tmp/kamera-run-after-triggers.jsonl` with `GOCACHE=.gocache` (default GOMODCACHE so toolchain is available offline). Run timed out at 5m, so no dump file was produced.
  - Improvements: KPA now sees pods (Observed pod counts show `ready=1`) and SKS/KPA reconcilers run. A Pod and Deployment chain is visible earlier in the run.
  - New blocker: SKS repeatedly recreates its private/public Services and then errors `failed to get private K8s Service endpoints: endpoints "kamera-test-private" not found`. Core Services/Endpoints don’t appear to persist between SKS reconciles, so it keeps recreating and requeuing. Need to trace where those Service/Endpoints objects land in the shared state.
- **30s run (Nov 25, path `/tmp/kamera-run-30s.jsonl`)**:
  - Command: `GOCACHE=.gocache go run . --depth 35 --timeout 30s --interactive=false --log-level panic --dump-output /tmp/kamera-run-30s.jsonl`.
  - KPA again observes pods (`ready=1`). SKS still loops creating private/public Services and hits `failed to get private K8s Service endpoints: endpoints "kamera-test-private" not found`.
  - Later abort shows `error retrieving deployment selector spec: error getting a lister for a pod scalable resource 'apps/v1, Resource=deployments': failed starting shared index informer for apps/v1, Resource=deployments with type *v1alpha1.PodScalable`. So podscalable informer startup is failing during SKS reconcile, likely because the fake/dynamic informer wiring for deployments/podscalable isn’t sharing state with the controller-runtime-created Deployment.

## Next steps
1) Trace SKS/private service plumbing: confirm Services/Endpoints created by SKS and Service/Endpoints controllers are recorded into the shared state and show up in the next reconcile. If not, bridge fake kube clients and the explorer’s version store.
2) Re-run a shorter headless simulation (e.g., 120–180s) with dumping enabled to capture the SKS/Endpoints state and see whether the Service/Endpoints controllers are firing.
3) If Services/Endpoints persist, check whether the SKS informer/listers are filtering them out (labels, ownership) and mirror Knative’s test setup for those informers.

## Commands
- Typical run:  
  `GOMODCACHE=~/tmp/gomodcache GOCACHE=/private/tmp/kamera-gocache go run ./examples/knative-serving -depth 35 -timeout 10s -interactive=false -log-level debug -dump-output /tmp/kamera-results.jsonl`

## Outstanding questions
- Which informer is index 0/others during KPA setup?
- Are filtered pod informers registered by default in our fake context, or do we need explicit fake imports?
- Do we need to seed pod objects (from ReplicaSets) with the revision UID label to let the filtered informer sync?
