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

To run a generated inputs file, pass `--inputs` and set dump directories for per-scenario output:

```bash
go run . \
  --inputs /path/to/inputs.json \
  --dump-output /tmp/karpenter-dumps \
  --dump-stats /tmp/karpenter-stats
```

## Observed flow (expected)

1. `state.pod` records the pending pod in cluster state.
2. `provisioner.trigger.pod` marks the pod for provisioning (batcher trigger).
3. `provisioner` drains the batch and creates a `NodeClaim`.
4. `nodeclaim.hydration` and `nodeclaim.lifecycle` launch the claim via the fake cloud provider.
5. `node.registrar` creates a `Node` with the provider ID (simulated kubelet registration).
6. `nodeclaim.lifecycle` registers the nodeclaim to the node and removes startup taints.
7. `nodeclaim.consistency` validates node/claim shape.
8. `node.hydration` copies NodeClass labels onto the Node.

## Next scopes
- **Medium:** add nodepool validation/readiness/registration-health + nodeclaim GC/expiration/disruption.
- **Full:** wire all controllers from `pkg/controllers/controllers.go` (optionally exclude metrics).
