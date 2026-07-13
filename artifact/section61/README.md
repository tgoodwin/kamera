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

## Why KRO-2 is not a passing row

The historical KRO-2 harness used replacement-like behavior for server-side
apply. In that model, an interrupted metadata apply removed the Application's
`spec`, after which the child Service remained absent. With structural schema
registration and schema-aware apply semantics, `spec` is preserved and the
controller creates the Deployment, Service, and Ingress after recovery. The
old locked trace therefore does not isolate the mechanism described in the
paper closely enough to serve as reproduced evidence.

KRO-2 remains an investigation item: either a revised schedule must exercise
the reported controller behavior under faithful apply semantics, or the case
study text must be narrowed. The artifact deliberately reports this limitation
instead of treating a bounded or semantically confounded trace as a pass.

## Checking existing dumps

The oracle checker uses only Python's standard library:

```bash
python3 artifact/section61/check_oracles.py kcp4 /path/to/kcp4.jsonl
python3 artifact/section61/check_oracles.py kar12 /path/to/kar12/*.jsonl
```
