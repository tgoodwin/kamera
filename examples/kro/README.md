# KRO Quick Start Example

This example wires KRO's documented quick-start `Application` flow into Kamera. It models the
public `ResourceGraphDefinition` and instance shape from the KRO quick-start docs and replays the
resulting `Application -> Deployment/Service/Ingress` reconciliation flow inside Kamera.

## What this example is

- A standalone Kamera example under `examples/kro`
- A doc-derived harness for KRO's quick-start `Application` API
- A way to explore resource ordering, drift repair, and instance updates for a small KRO-style graph

## What this example is not

This harness does **not** boot KRO's full dynamic controller manager or parse arbitrary RGDs at
runtime. KRO's upstream controller stack currently depends on a newer Go toolchain and a runtime
dynamic-controller model that does not drop directly into Kamera's replay client.

Instead, this example makes the simulation boundary explicit:

- It uses the official KRO quick-start schema and resource templates as the source of truth.
- It statically wires the generated `Application` kind into Kamera.
- It simulates the same observable resource graph the docs describe:
  `ResourceGraphDefinition -> generated Application API -> Deployment -> Service -> optional Ingress`.

## Upstream docs used

- KRO quick start: [https://kro.run/docs/getting-started/deploy-a-resource-group-definition](https://kro.run/docs/getting-started/deploy-a-resource-group-definition)
- KRO instances and reconciliation behavior: [https://kro.run/docs/concepts/instances](https://kro.run/docs/concepts/instances)
- Public KRO example repo content: [https://github.com/kubernetes-sigs/kro](https://github.com/kubernetes-sigs/kro)

The checked-in fixture files mirror those public docs:

- [`application-rgd.yaml`](/Users/tgoodwin/projects/kamera/examples/kro/application-rgd.yaml)
- [`application-instance.yaml`](/Users/tgoodwin/projects/kamera/examples/kro/application-instance.yaml)
- [`inputs.json`](/Users/tgoodwin/projects/kamera/examples/kro/inputs.json)

## Harness behavior

The harness registers two reconcilers:

- `ResourceGraphDefinitionController`
  - materializes the quick-start-generated `Application` CRD
  - marks the `ResourceGraphDefinition` active with the documented topological order
- `ApplicationController`
  - reconciles `Application` instances into a `Deployment`, `Service`, and optional `Ingress`
  - watches managed child resources and requeues the owning `Application` when drift occurs
  - projects simple status fields (`state`, `availableReplicas`, `deploymentConditions`) back onto the instance

Kamera's built-in core controllers then drive the created `Deployment`, `ReplicaSet`, `Pod`, `Service`,
and `Endpoints` objects as usual.

## Usage

Run the default interactive example:

```bash
cd examples/kro
go run .
```

That seeds the quick-start `ResourceGraphDefinition` and schedules one user action that creates the
documented `Application` instance.

Run headless and emit a dump:

```bash
cd examples/kro
go run . \
  --interactive=false \
  --output /tmp/kamera-kro.jsonl
```

## Batch inputs

The example ships with doc-derived scenarios in [`inputs.json`](/Users/tgoodwin/projects/kamera/examples/kro/inputs.json):

- create an application with ingress enabled
- create an application without ingress
- create an application and then update replicas from 1 to 3

The checked-in scenarios use conservative `maxDepth` values so the default batch run stays
practical while still surfacing the main KRO and core-controller interactions.

Run them in batch mode:

```bash
cd examples/kro
go run . \
  --inputs ./inputs.json \
  --interactive=false \
  --output /tmp/kro-dumps \
  --emit-stats
```

## Reading the traces

The most useful divergence points in this harness are usually:

- when the `ApplicationController` creates child resources
- when the core deployment controllers react to the generated `Deployment`
- when the optional `Ingress` branch is included or skipped
- when a later `UPDATE` user action scales the application

If you extend this example later, the next natural scope is replacing the static `Application`
graph with a direct bridge to KRO's upstream graph builder and dynamic controller runtime.
