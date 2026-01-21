# Kratix + Kamera examples

This example module wires Kratix controllers into Kamera to explore Kratix's multi-controller
"flows" in a simulated environment.

There are currently two Kratix flows configured in this example, based on the experimental PR that Kratix maintainer John Mikos wrote [here](https://github.com/syntasso/kratix/compare/research/kamera?expand=1).

## Module path note

Kratix controllers live under `internal/controller`, so this module is named
`github.com/syntasso/kratix/examples/kratix` to allow importing Kratix internals.
The module still lives inside the Kamera repo and uses `replace` directives to
point at local sources.

## Prereqs

- Go 1.24+
- Kratix cloned locally at `/Users/$USER/projects/kratix` on the branch from
  the PR compare

## Setup

The `go.mod` in this directory includes:

- `replace github.com/tgoodwin/kamera => ../..`
- `replace github.com/syntasso/kratix => /Users/$USER/projects/kratix`

Adjust the path if your local Kratix copy lives elsewhere.

## Running

Default flow (works):

```bash
go run .
```

Explicit flow selection:

```bash
go run . -flow=works
go run . -flow=promises
```

Disable the interactive inspector UI:

```bash
go run . -flow=works -interactive=false
```

## Flows

- `works`: Work + WorkPlacement + Destination + BucketStateStore
- `promises`: Promise + PromiseRevision
