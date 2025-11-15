# Knative Serving Explorer Example

This standalone module recreates the `explore.go` harness that wires Kamera into the Knative Serving control plane.

## Usage

```bash
cd examples/knative-explore
GOCACHE=$(pwd)/.gocache go mod tidy   # first run, to fetch dependencies (requires Go 1.24+)
GOCACHE=$(pwd)/.gocache go run .      # launches the explorer + inspector UI
```

The example depends on Knative Serving controllers, so the first `go mod tidy` needs outbound network access to download `knative.dev/serving` and its dependencies. The main module already uses a local `replace` directive so it will consume the checked-out copy of Kamera in this repository.
