# Kratix + Kamera examples

This example module wires Kratix controllers into Kamera to explore control-plane
flows in a simulated environment. It mirrors the approach used in the Kratix
maintainer’s Kamera wiring, but packages the flows into a single binary with a
`-flow` switch.

## Module path note

Kratix controllers live under `internal/controller`, so this module is named
`github.com/syntasso/kratix/examples/kratix` to allow importing Kratix internals.
The module still lives inside the Kamera repo and uses `replace` directives to
point at local sources.

## Prereqs

- Go 1.24+
- Kratix cloned locally at `/Users/tgoodwin/projects/kratix` on the branch from
  the PR compare

## Setup

The `go.mod` in this directory includes:

- `replace github.com/tgoodwin/kamera => ../..`
- `replace github.com/syntasso/kratix => /Users/tgoodwin/projects/kratix`

Adjust the path if your Kratix checkout lives elsewhere.

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

Write an inspector dump instead of running interactively:

```bash
go run . -flow=works -dump-output=works_dump.json
```

Open a dump later:

```bash
go run github.com/tgoodwin/kamera/cmd/inspect --dump works_dump.json
```

## Flows

- `works`: Work + WorkPlacement + Destination + BucketStateStore
- `promises`: Promise + PromiseRevision
