# Context
In this knative example, the code currently finds a converged state where the Knative Service (in the initial state) gets reconciled by all the conrollers, but gets "stuck" with its Ready status condition as `Unknown`, with the reason RevisionMissing. The message is "waiting for a revision to become ready". This status is getting propagated up from the Route resource, which ends up in the same state of status codes stuck in the `Unknown` state with the "RevisionMissing" reason.

# Goal
The end goal is to observe the Knative Service get reconciled to ultimately reflect a status of Ready=True. This is dependent on a variety of preconditions that need to be handled by the rest of the controllers in Knative's control plane, and we'll need to debug which of these preconditions are failing.

# Plan
Let's follow the AGENTS.md file in this directory to run the explore routine iteratively and debug what's going on. We should avoid modifying Knative business logic directly, but it's OK to add print statements to debug it. We should strive to contain our changes to the KnativeStrategy (the way we're wiring up Knative to Kamera) as well as how we're seeding the initial state. I believe the issue is a "plumbing" or "configuration" issue because in theory, Kamera should be able to fully simulate the reconciliation of a Knative Service given that Kamera's PodLifecycleController simulates the underlying Pod successfully becoming ready.

Throughout this process, inspect the Knative Serving source code in the go module cache (see AGENTS.md for where GOMODCACHE is pointed to).

Please document findings under the Work Log section of this file.

## things to investigate
1. From the converged state that's currently being produced, we know the Route resource is stuck on "waiting for revision to become ready" and that the Revision's `ResourcesAvailable`, `Ready`, and `Active` status conditions remain in "Unknown" with the reason "Deploying". So the revision seems to be waiting on something to happen that is never happening.
2. The previous observation might be related to the absence of an `Ingress` resource. We'll need to investigate where that's supposed to get created during this process. We may need to add a "stub" reconciler to the explore harness to handle reconciling an `Ingress` (just something simple to flip its Ready condition to True).
3. Knative's KPA reconciler never runs. Maybe this is an issue with how we configure resource dependencies.
4. We may need to add some additional resources to our initial state beyond just the Knative service to be reconciled. Knative's control plane uses an "activator" service / endpoint, so we may need to ensure that this service is present in our initialState.

## things to avoid doing
1. if it becomes apparent that, for example, a resource R is not being created, do not "solve" that problem by adding code to the KnativeStrategy to manually create it. This is "cheating." Instead, we should figure out what part of the k8s/Knative control plane would create resource R in the real world, and figure out what's blocking that from happening in our simulation.

## Work Log
- Ran headless explore with `GOCACHE=$REPO/.gocache GOMODCACHE=~/tmp/gomodcache go run . -depth 35 -timeout 30s -interactive=false -log-level info -dump-output /tmp/kamera-results3.jsonl`.
- Converged state still shows Service/Route Ready=Unknown (RevisionMissing) and Revision Ready/ResourcesAvailable/Active all Unknown (Deploying) even though Deployment/Pod are Ready.
- PodAutoscaler exists but status fields/conditions are empty; no ServerlessService or Ingress objects were created.
- Explorer path shows KPA reconciler ran (one frame) but recorded no effects or status updates; no CREATEs for serverlessservices appeared in the reactor logs.
- Timeout-related “failed to sync informers” errors appear only right at the explore timeout boundary (e.g., 30s/60s) and don’t change the converged state.
- Saw an execution branch abort when ServiceReconciler attempted a CREATE on an object that already existed; this should be treated as optimistic concurrency, so we need to tolerate `AlreadyExists` from that reconciler instead of failing the entire branch.
- Added global tolerance in `pkg/tracecheck/explore.go` so `IsAlreadyExists` errors from any reconciler are treated as no-ops rather than aborting the branch (optimistic concurrency).
- Reran explorer at depth 100 with toolchain downloads allowed; converged (1 state, 0 aborted). Revision/KPA/ServerlessService/Ingress all Ready=True; PodAutoscaler Ready/SKSReady/Active True. Route remains Ready=Unknown (IngressNotConfigured) and Service Ready=Unknown (RoutesReady Unknown) even though Ingress shows Ready=True — likely the Route reconciler never sees the ingress status update.
- Reran at depth 120 with cached toolchain; same converged state (1) with PodAutoscaler/KPA/ServerlessService/Ingress Ready=True but Route/Service still Ready=Unknown (IngressNotConfigured). No `TimedOut/The target could not be activated` observed in this run; if it appears, it likely comes from KPA seeing desiredScale drop to 0 while still activating.
- Clamped the fake UniScaler in `examples/knative-serving/knative/harness.go` to never return DesiredPodCount < 1, preventing `TimedOut`/“the target could not be activated” while activating.
