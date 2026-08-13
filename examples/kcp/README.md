# KCP Kamera Harness

This module integrates real KCP controllers with Kamera's simulated API
server. It exercises workspace initialization, API binding, logical-cluster
cleanup, and API export endpoint discovery without starting a KCP cluster.

The harness targets the published KCP v0.30 dependency line. Its `go.mod`
uses published KCP and staging modules and a repository-relative replacement
for Kamera, so it does not require a separate KCP checkout.

Run the smoke tests and build the harness from this directory:

```bash
go test ./...
go build .
```

Run the checked-in input in batch mode:

```bash
go run . --inputs inputs.json --interactive=false
```

The controllers are constructed from upstream KCP packages. A few unexported
controller entrypoints are adapted with `//go:linkname`; client reactors and
metadata stubs supply the cluster-aware API behavior they expect.
