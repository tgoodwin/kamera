# Karpenter Kick-the-Tires Example

This example wires a minimal Karpenter provisioning flow into Kamera:
Pending Pod → Provisioner → NodeClaim → Node registration.

## Approximations
- We simulate API server generateName for NodeClaims in the harness.
- We simulate Node registration by creating a Node from NodeClaim status.
- We map Node→NodeClaim via a label set by the simulator (to approximate providerID matching).
- We avoid multi-object list filtering by keeping a single NodePool/NodeClaim/Node.

## Usage

```bash
go run . -interactive=false -dump-output /tmp/kamera-karpenter.jsonl
```

## Batch inputs

`--inputs` defaults to `inputs.json` in the example directory.
To run a custom file, pass `--inputs <path>` and set dump directories for per-scenario output.
For example:

```bash
go run . \
  --inputs inputs.json \
  --fuzz-cases 12 \
  --fuzz-seed 1337 \
  --dump-output /tmp/karpenter-dumps \
  --emit-stats
```

- `--fuzz-cases` controls how many sampled parameterized variants are generated per input.
- `--fuzz-seed` keeps sampled variants deterministic across runs.

## Input model (`environmentState` vs `userInputs`)

`inputs.json` models this scenario as:
- **Environment state:** a ready `TestNodeClass` and `NodePool` that represent cluster configuration.
- **User action:** creating one unschedulable pending `Pod`.

This mirrors typical Karpenter behavior where provisioning starts when unschedulable
pods appear and Karpenter computes capacity from existing NodePool/NodeClass
configuration.

## Startup Semantics (Important)

In this harness, `environmentState` means "objects already exist in the API state"
but does **not** automatically mean every controller-local cache has already been
hydrated.

Karpenter uses process-local in-memory state (for example, shared `state.Cluster`)
that is populated by reconcilers such as `state.nodepool`. Because of that:

- A reconcile can be behaviorally required even if it writes no API objects.
- `state.nodepool` may look like a no-op in trace effects, but it can still be
  needed so `provisioner` sees NodePools in local cluster state.
- If startup triggers for environment objects are skipped, `provisioner` can
  converge early (for example, "no dynamic nodepools found") even though a
  NodePool object exists in `environmentState`.

For the `Pod` user input specifically, the harness currently seeds that object
into startup state so the initial pending order remains:
`state.pod` -> `provisioner.trigger.pod` -> `provisioner`.
Without this, lexicographic pending ordering can run `provisioner` before pod
state hydration and no NodeClaim/Node gets created.

To keep `External User` visible in traces, the same seeded `CREATE` input is
retained as a later user-action `UPDATE` step after the initial provisioning
converges.

Design intent: users should model inputs via `environmentState` + `userInputs`
without hand-authoring pending reconciles. The harness should derive startup
pending work from simulation semantics (subscriptions/watches/dependencies).

## Observed flow (expected)

1. `state.pod` records the seeded pending pod in cluster state.
2. `provisioner.trigger.pod` marks the pod for provisioning (batcher trigger).
3. `provisioner` drains the batch and creates a `NodeClaim`.
4. `nodeclaim.hydration` and `nodeclaim.lifecycle` launch the claim via the fake cloud provider.
5. `node.registrar` creates a `Node` with the provider ID (simulated kubelet registration).
6. `nodeclaim.lifecycle` registers the nodeclaim to the node and removes startup taints.
7. `nodeclaim.consistency` validates node/claim shape.
8. `node.hydration` copies NodeClass labels onto the Node.
9. `External User` later appears as the replayed user input action.

## Next scopes
- **Medium:** add nodepool validation/readiness/registration-health + nodeclaim GC/expiration/disruption.
- **Full:** wire all controllers from `pkg/controllers/controllers.go` (optionally exclude metrics).
