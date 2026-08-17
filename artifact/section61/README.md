# Section 6.1 case-study simulations

This workflow reruns all three Section 6.1.2 case studies and checks their
observable outcomes without invoking an LLM:

- **KCP-4:** two converged executions leave the same
  `APIExportEndpointSlice` with zero versus one endpoint.
- **KRO-2:** the bounded execution reaches the reported interrupted-apply
  outcome, leaving the Application without `spec` and its child resources
  absent.
- **KAR-12:** a converged execution retains the bound Pod and Node after the
  corresponding `NodeClaim` is removed. Another converged execution retains
  the `NodeClaim`, establishing that the result depends on the simulated
  ordering rather than the initial state alone.

If `./artifact/setup-figure8-deps.sh` has already run, its pinned KCP, KRO, and
Karpenter checkouts are reused automatically. Run all three simulations from
the Kamera repository root:

```bash
./artifact/reproduce-section61.sh
```

For a standalone Section 6.1 run, prepare the same pinned controller sources
first:

```bash
./artifact/setup-section61-deps.sh
./artifact/reproduce-section61.sh
```

Setup clones KCP at `301a8f749e7b99a0c81f43b37aa5b5e5ff0fc0b4`,
KRO at `c9320ee963f745637bb622f6b68853a870187d20`, and Karpenter at
`8ae07cf8b4ecf8ae3f04bc306d97f1ee40d21849`. It also checks out the pinned
Kamera revision needed for KRO-2 and applies the checked-in KRO and Karpenter
simulation adapters. The KCP harness is checked into this artifact; the
original multi-gigabyte trace outputs are not needed. The complete source
manifest and patch checksums are in `dependencies.json`.

The source manifest pins the upstream controller and Kamera revisions used by
these simulations.

By default, setup writes ignored checkouts under `artifact-deps/section61`.
Pass a different destination as its first argument and set
`KAMERA_AE_DEPS_DIR` to that same path when running the reproducer:

```bash
./artifact/setup-section61-deps.sh /tmp/kamera-section61-deps
KAMERA_AE_DEPS_DIR=/tmp/kamera-section61-deps \
  ./artifact/reproduce-section61.sh
```

The output directory contains the complete dumps, campaign-metrics reports,
per-case oracle JSON files, logs, and a combined `section61.tsv`. A successful
run prints `PASS` for KCP-4 and KAR-12 and `OBSERVED` for KRO-2. The locked
KAR-12 campaign uses depth 100: at depth
50 every submission-time trial was a bounded partial trace. The clean
pinned-source validation at depth 100 yielded six truly converged trials; the
oracle requires both relevant converged outcomes rather than a fixed
convergence count.

## Interpreting the KRO-2 result

An interruption after two Application-controller effects leaves the
Application without `spec`, and the Deployment, Service, and Ingress remain
absent. The exact focused input and pinned KRO source adapter run as part of
`reproduce-section61.sh`. The same simulation can also be run through the
unified Figure 8 workflow with:

```bash
./artifact/setup-figure8-deps.sh
./artifact/reproduce-figure8.sh
```

This is the simulation underlying the KRO-2 Figure 8 curve. The focused
depth-50 run is a bounded partial trace, so its checker reports the observable
outcome and zero converged states separately. This is why its row says
`OBSERVED` rather than `PASS`.

## Checking existing dumps

The oracle checker uses only Python's standard library:

```bash
python3 artifact/section61/check_oracles.py kcp4 /path/to/kcp4.jsonl
python3 artifact/figure8/kro-historical/check_outcome.py /path/to/kro2.jsonl
python3 artifact/section61/check_oracles.py kar12 /path/to/kar12/*.jsonl
```
