# Knative Serving Explorer Example

This standalone module recreates the `explore.go` harness that wires Kamera into the Knative Serving control plane.

## Usage

```bash
cd examples/knative-serving
go mod tidy   # first run, to fetch dependencies (requires Go 1.24+)
go run .      # launches the explorer + inspector UI
```

The example depends on Knative Serving controllers, so the first `go mod tidy` will download `knative.dev/serving` and its dependencies. The main module already uses a local `replace` directive so it will consume the checked-out copy of `kamera` in this repository.

## Batch inputs

The example ships with an expanded `inputs.json` file in this directory. Each entry in
that top-level `[]Input` array is treated as a final scenario unit; the harness does not
perform additional parameter expansion at runtime.

Behavior:
- `--inputs <path>` enables batch mode, even if `--parallel` is not set.
- `--parallel` with no `--inputs` loads the default `./inputs.json` (or `examples/knative-serving/inputs.json` from repo root).
- `--timeout` applies per input/scenario run, not as an overall batch timeout.

```bash
go run . \
  --parallel \
  --dump-output /tmp/knative-dumps \
  --dump-stats /tmp/knative-stats
```

To run a generated inputs file, pass `--inputs` and set dump directories for per-scenario output:

```bash
go run . \
  --inputs inputs.json \
  --dump-output /tmp/knative-dumps \
  --depth 100 \
  --timeout 60s \
  --dump-stats /tmp/knative-stats
```
