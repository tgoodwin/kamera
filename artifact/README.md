# Kamera SOSP 2026 Artifact Evaluation

This artifact accompanies the SOSP 2026 paper about **Kamera**. The anonymous
paper draft calls the system *Leica*; both names refer to the same system.

The artifact targets the Available, Functional, and Reproduced badges. The
archival DOI will be added after the evaluation, when the reviewed revision is
deposited in an immutable public archive.

## What is reproduced

The primary reproduction target is Table 6: the execution time of Kamera's
**perturbed simulation run** for 11 bugs previously studied by Sieve. The
timing does not include a baseline run or a real Kubernetes cluster run.

The paper's bug-discovery campaign used an LLM agent. That campaign is not part
of the standard workflow because it requires external model access and is not
deterministic. See [Optional experiments](#optional-experiments).

## Requirements

- Linux or macOS on x86-64 or arm64
- Go 1.24 or newer
- Bash 3.2 or newer
- `jq`, `awk`, and standard Unix utilities
- Approximately 2 CPU cores, 4 GiB RAM, and 5 GiB free disk space
- Network access for the first Go module download

No Kubernetes cluster, container runtime, cloud account, or LLM credential is
required for the standard workflow. The paper measurements used an Apple M1
Pro (10 cores, 16 GiB RAM); execution times naturally vary by host.

## Quick functional check

From the repository root:

```bash
./artifact/smoke-test.sh
```

This builds the RabbitMQ harness, runs the `unobserved-state-1` perturbation in
an isolated process, verifies convergence, and checks that the trace contains
one Pod creation. A successful run ends with `PASS` and normally takes under
two minutes including a cold build.

## Reproduce Table 6

Run all 11 perturbed simulations:

```bash
./artifact/run-table6.sh
```

The command prints the result directory and writes:

- `table6.md`: human-readable paper-versus-observed timing table;
- `table6.tsv`: machine-readable summary;
- `runs/<experiment>/result.json`: duration and convergence metadata;
- `runs/<experiment>/*.jsonl`: complete exploration trace;
- `runs/<experiment>/run.log`: detailed diagnostic log.

The first run builds three operator harnesses and may take several minutes.
The simulations themselves ordinarily complete in seconds. To run one row:

```bash
./artifact/run-experiment.sh cass/intermediate-state-2
```

Accepted experiment IDs are listed in `artifact/run-table6.sh`. Every timing
is read from `campaignMetrics.durationNs` in the emitted trace. The runner uses
`--parallel-processes` and selects input index 1 so that the perturbed scenario
runs alone; this prevents process-global simulator state from leaking between
the baseline and perturbation.

If the default Go cache is unsuitable, set an isolated cache explicitly:

```bash
KAMERA_AE_GOCACHE=/tmp/kamera-go-cache ./artifact/smoke-test.sh
```

## Evidence status

The standard workflow distinguishes timing reproduction from semantic bug
oracles. It never counts a successful fault injection as proof of a downstream
bug by itself.

| Experiment | Current trace evidence |
|---|---|
| RabbitMQ unobserved-state-1 | Perturbed trace converges with one Pod rather than the baseline's three. |
| RabbitMQ stale-state-2 | Perturbed trace contains a second StatefulSet deletion and an extra PVC update. |
| RabbitMQ intermediate-state-1 | Crash lands after StatefulSet deletion and produces replacement StatefulSet/Pod churn. |
| Cassandra intermediate-state-1 | Crash lands after CA-keystore creation; the second keystore is absent from that reconcile. |
| Cassandra intermediate-state-2 | Exact two-Pod-patch crash trigger and paper-scale timing reproduced; final semantic state currently matches baseline. |
| Remaining rows | Timing harness is runnable; semantic oracles are still being hardened before the AE submission. |

The generated `status` column reports exploration completion only. `converged`
does not by itself mean that a bug oracle passed.

## Optional experiments

The original Sieve real-cluster baselines require Sieve, Docker, kind,
kubectl, and the target operator images. They are intentionally excluded from
Table 6's reported Kamera duration and from the standard workflow. Instructions
will be supplied as an optional extended experiment.

The LLM-driven bug-finding campaign is likewise optional and deferred until a
model-neutral credential/configuration interface is documented. The checked-in
trace scenarios require no model access.

## License and archival plan

Kamera is released under the repository's MIT `LICENSE`. Imported operator
source retains its original copyright headers. Before archival release, the
artifact will include a third-party notices inventory and the final Zenodo DOI.

## Troubleshooting

- Existing result directories are never overwritten; choose a fresh path or
  omit the output argument to get a timestamped directory.
- A `partial` status means the trace contains an aborted state. Inspect
  `run.log` and run `go run ./cmd/kamera analyze campaign-metrics <dump>`.
- Do not time a combined baseline/S2 invocation. Process isolation is required
  for valid results.
