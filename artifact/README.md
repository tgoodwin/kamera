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

Each scenario is a locked schedule that exercises the controller behavior
behind the corresponding historical report. Some rows reproduce the original
resource-level symptom exactly; others expose the same non-atomic or
state-coalescing mechanism in the simulator. Reviewers can inspect the complete
trace manually; an automated per-row oracle is not required for the timing
measurement.

| Experiment | Current trace evidence | Gap to exact Sieve outcome |
|---|---|---|
| ZooKeeper stale-state-1 | A newly created generation is reconciled through the frozen view of its deleting predecessor. | No material gap identified for the reported resource-lifecycle symptom. |
| ZooKeeper stale-state-2 | The stale deletion view produces the historical extra resource lifecycle operations. | No material gap identified for the reported resource-lifecycle symptom. |
| ZooKeeper unobserved-state-1 | The intermediate scale-down is skipped, so Pod/PVC 1 is never removed and the old PVC is retained. | The simulator does not include a literal ZooKeeper client or server, so it does not emit the later `NodeExists` response seen in the upstream report. |
| ZooKeeper indirect-1 | After the PVC deletion and finalizer removal, later reconciliations continue updating the deleting cluster and its owned resources. | Sieve pauses and resumes one reconciliation while garbage collection interleaves; Kamera currently resumes through a newly queued reconciliation. |
| RabbitMQ stale-state-1 | The configured trace contains the additional StatefulSet and Pod create/delete activity reported by Sieve. | The later recreation currently races object removal, although the reported extra lifecycle activity occurs first. |
| RabbitMQ stale-state-2 | The configured trace contains a second StatefulSet deletion and an extra PVC update. | No material gap identified for the reported resource-lifecycle symptom. |
| RabbitMQ unobserved-state-1 | Perturbed trace converges with one Pod rather than the baseline's three. | No material gap identified for the missed-scale-transition outcome. |
| RabbitMQ intermediate-state-1 | StatefulSet deletion becomes visible before PVC expansion; the configured final StatefulSet remains marked for deletion while the baseline is live. | Sieve ends with a live PVC at `10Gi`; Kamera resumes the PVC update to `15Gi` but leaves the StatefulSet in its deletion transition. |
| Cassandra stale-state-1 | After recreation, the selected stale view leaves the new datacenter without its expected finalizer, StatefulSet, and Pod. | Sieve marks the recreated PVC for deletion; the current simulator instead exposes the stale-generation effect one level earlier, at datacenter initialization. |
| Cassandra intermediate-state-1 | The CA keystore is created, while the companion keystore is absent from that reconciliation. | The simulator does not model Cassandra bootstrap closely enough to turn the incomplete credential set into the literal Pod `Running` timeout. |
| Cassandra intermediate-state-2 | After deleting and recreating the datacenter, the trace reaches the second two-Pod-patch transition from `Ready-to-Start` to `Starting` with `seed-node: "true"`; only those two Pod patches are retained. | Sieve's recorded CassandraDatacenter lacks `spec.config` from its first stored version, before the trigger. The issue was marked pending, so the reported final config difference is not yet causally attributable to this transition. |

For Cassandra intermediate-state-2, the Sieve history records the requested
configuration in the `kubectl.kubernetes.io/last-applied-configuration`
annotation while the first stored live `spec` already lacks `config`. This
precedes the configured second Pod transition. Kamera's uninterrupted and
configured final states both preserve the requested Cassandra and JVM settings.
The artifact therefore claims faithful trigger placement and partial-update
evidence for this row, not reproduction of Sieve's unexplained configuration
loss.

The generated `status` column reports exploration completion only. `converged`
does not by itself mean that a bug oracle passed. Cassandra
intermediate-state-1 reports `expected-depth-limit`: its incomplete keystore
sequence leaves recurring controller work, which is the simulated counterpart
of Sieve's Pod readiness timeout. Any other `partial` result is unexpected and
should be inspected.

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
