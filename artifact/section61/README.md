# Section 6.1 case-study simulations

This developer-preview workflow reruns two of the three Section 6.1.2 case
studies and checks their observable outcomes without invoking an LLM:

- **KCP-4:** two converged executions leave the same
  `APIExportEndpointSlice` with zero versus one endpoint.
- **KAR-12:** a converged execution retains the bound Pod and Node after the
  corresponding `NodeClaim` is removed. Another converged execution retains
  the `NodeClaim`, establishing that the result depends on the simulated
  ordering rather than the initial state alone.

Prepare the pinned controller sources, then run both simulations from the
Kamera repository root:

```bash
./artifact/setup-section61-deps.sh
./artifact/reproduce-section61.sh
```

Setup clones KCP at
`301a8f749e7b99a0c81f43b37aa5b5e5ff0fc0b4` and Karpenter at
`8ae07cf8b4ecf8ae3f04bc306d97f1ee40d21849`. It then applies the checked-in
Karpenter simulation adapter. The KCP harness is checked into this artifact;
the historical multi-gigabyte trace outputs are not needed. The complete
source manifest and patch checksum are in `dependencies.json`.

By default, setup writes ignored checkouts under `artifact-deps/section61`.
Pass a different destination as its first argument and set
`KAMERA_AE_DEPS_DIR` to that same path when running the reproducer:

```bash
./artifact/setup-section61-deps.sh /tmp/kamera-section61-deps
KAMERA_AE_DEPS_DIR=/tmp/kamera-section61-deps \
  ./artifact/reproduce-section61.sh
```

The output directory contains the complete dumps, campaign-metrics reports,
per-case `oracle.json` files, logs, and a combined `section61.tsv`. A successful
run prints two `PASS` rows. The locked KAR-12 campaign uses depth 100: at depth
50 every historical trial was a bounded partial trace. The clean pinned-source
validation at depth 100 yielded six truly converged trials; the oracle requires
both relevant converged outcomes rather than a fixed convergence count.

## KRO-2: historical and current-semantics tracks

The paper-era KRO-2 harness used replacement-like behavior for server-side
apply. In that model, an interruption after two Application-controller effects
leaves the Application without `spec`, and the Deployment, Service, and
Ingress remain absent. The exact focused and exhaustive inputs and the pinned
KRO source adapter are now packaged under `artifact/figure8/kro-historical`.
Run them with:

```bash
./artifact/setup-figure8-kro-deps.sh
./artifact/run-figure8-kro-historical.sh focused
```

This is a paper-snapshot reproduction and is also the simulation underlying
the KRO-2 Figure 8 curve. The focused depth-50 run is a bounded partial trace,
so its checker reports the observable outcome and zero converged states
separately.

Draft PR [#83](https://github.com/tgoodwin/kamera/pull/83), opened after the
paper experiments, prototypes structural-schema registration and schema-aware
apply. Under those semantics, `spec` is preserved and the controller can create
the child resources after recovery. That prototype was still open and unmerged
when this artifact was packaged. It is therefore a useful forward-looking
comparison, not the semantics against which the historical Figure 8 result
should be judged.

## Checking existing dumps

The oracle checker uses only Python's standard library:

```bash
python3 artifact/section61/check_oracles.py kcp4 /path/to/kcp4.jsonl
python3 artifact/section61/check_oracles.py kar12 /path/to/kar12/*.jsonl
```
