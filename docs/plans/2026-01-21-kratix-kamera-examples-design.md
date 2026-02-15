# Kratix Kamera examples design

Date: 2026-01-21

## Goal

Provide a simple, reproducible example that wires Kratix controllers into Kamera,
mirroring the maintainer-authored PR, while keeping everything in the Kamera
repo. The example should support multiple flows via a flag to leave room for a
future parameterized flow strategy.

## Approach

- Create a standalone Go module at `examples/kratix` with its own `go.mod`.
- Set the module path to `github.com/syntasso/kratix/examples/kratix` so it can
  import Kratix `internal/controller` packages.
- Use `replace` directives to point to local checkouts:
  - `github.com/tgoodwin/kamera => ../..`
  - `github.com/syntasso/kratix => /Users/tgoodwin/projects/kratix`

## Flows

- `works`: Work + WorkPlacement + Destination + BucketStateStore. Uses a fake
  state store writer to avoid external dependencies while exercising controller
  logic.
- `promises`: Promise + PromiseRevision. Uses a client wrapper to default
  namespaces for namespaced Kratix resources in the simulation.

## Execution

- Single binary entrypoint with `-flow=works|promises`.
- Supports Kamera’s config file loading.
- Supports `-dump-output` to write inspector dumps instead of running the
  interactive inspector.

## Future Direction

- Replace the hard-coded flows registry with a parameterized strategy layer.
- Add flow inputs synthesized by automated techniques (e.g., generators or
  templates), keeping the example harness stable.
