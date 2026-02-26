# Crossplane Composition Pipeline Example

This example wires a minimal Crossplane Composition pipeline flow into Kamera. It focuses on
Composition → CompositionRevision → XR reconciliation with a stubbed function pipeline.

## What's missing vs real Crossplane

- XRD controller (XR controller is wired manually, not dynamically created).
- Provider/managed resource controllers (no external cloud IO).
- Claim syncer.
- Connection secret publishing.
- Dynamic watch engine for composed resources.
- Package manager controllers (FunctionRevision is seeded manually for capability checks).
- Real function runtime (uses an in-process stubbed FunctionRunner).
- Kubernetes event recording (events are logged locally instead).

## Usage

Run with the standard Kamera explore flags. Example:

```bash
# Run headless and write a dump

go run . \
  -interactive=false \
  -dump-output /tmp/kamera-crossplane.jsonl
```

## Batch inputs

To run a generated inputs file, pass `--inputs` and set a dump directory for per-scenario output:

```bash
go run . \
  --inputs /path/to/inputs.json \
  --dump-output /tmp/crossplane-dumps \
  --emit-stats
```
