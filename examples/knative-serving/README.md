# Knative Serving Explorer Example

This standalone module recreates the `explore.go` harness that wires Kamera into the Knative Serving control plane.

## Usage

```bash
cd examples/knative-serving
go mod tidy   # first run, to fetch dependencies (requires Go 1.24+)
go run .      # launches the explorer + inspector UI
```

The example depends on Knative Serving controllers, so the first `go mod tidy` will download `knative.dev/serving` and its dependencies. The main module already uses a local `replace` directive so it will consume the checked-out copy of `kamera` in this repository.
