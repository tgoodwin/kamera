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

## Next scopes
- **Medium:** add nodepool validation/readiness/registration-health + nodeclaim GC/expiration/disruption.
- **Full:** wire all controllers from `pkg/controllers/controllers.go` (optionally exclude metrics).
