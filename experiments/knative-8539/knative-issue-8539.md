# Knative Issue #8539: Revision Stuck in "Ready=Unknown"

Original issue: https://github.com/knative/serving/issues/8539

1. The Symptom
When creating multiple Knative Services in parallel with --min-scale 0, some Revisions become permanently stuck.

Revision Status: Ready=Unknown, Reason: Deploying.

PodAutoscaler (PA) Status: Active=False, Reason: NoTraffic.

Impact: The Service never becomes Ready, even though the underlying deployment succeeded and was scaled down naturally.

2. The Core Race Condition
The failure is a distributed state race between the PodAutoscaler (PA) Controller and the Revision Controller.

The Timeline of Failure
Deployment: A new Revision is created. The PA Controller spins up a pod to verify health.

Transient Success: The PA marks itself Active=True.

Scale-to-Zero: Since min-scale is 0 and there is no traffic, the PA Controller immediately switches to Active=False (NoTraffic) to save resources.

The "Missed" Reconcile: * Under heavy load, the Revision Controller is slow to pick up the change.

It misses the window where Active was True.

When it finally reconciles, it sees Active=False.

3. Fundamental Architectural Issue
The issue highlights a flaw in how Knative handled reconciliation timing across multiple controllers:

State Dependency: The Revision's Ready condition was downstream of the PA's Active condition.

Edge-Triggered vs. Level-Triggered: While Kubernetes controllers are level-triggered (reconciling to the current state), the Revision logic behaved as if it required an edge-trigger (observing the transition to Active=True) to progress its own state machine.

The Deadlock:

The Revision won't mark itself Ready until it sees a "successful" activation.

The PA won't "activate" again because the Revision isn't Ready and there is no incoming traffic to trigger a scale-up.

Result: The system reaches a stable state of "Unknown" readiness.

4. Key Learnings & Fix
The fix required moving away from the assumption that a Revision must be "Currently Active" to be "Ready."

Decoupling: Readiness should represent "The container is valid and capable of running," while Activeness represents "The container is currently running."

Status Propagation: Logic was updated to ensure that if a PA is inactive due to NoTraffic, the Revision Controller can still infer that the initial deployment was successful, allowing it to transition to Ready=True.

