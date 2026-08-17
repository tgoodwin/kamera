# Table 6 reproduction details

The primary Table 6 reproduction target is the execution time of Kamera's
**perturbed simulation run** for 11 controller scenarios previously studied by
Sieve. The reported duration excludes the baseline simulation and any
real-cluster Sieve execution.

From the repository root, run all rows with:

```bash
./artifact/run-table6.sh
```

A first run normally takes 5–15 minutes, primarily to download Go modules and
build the ZooKeeper, RabbitMQ, and Cassandra harnesses. The simulations
themselves ordinarily finish in seconds. To run one row:

```bash
./artifact/run-experiment.sh cass/intermediate-state-2
```

Accepted experiment IDs are listed in `artifact/run-table6.sh`.

## Outputs and measurement boundary

The complete run writes:

- `table6.md`: human-readable paper-versus-observed timing table;
- `table6.tsv`: machine-readable summary;
- `runs/<experiment>/result.json`: duration and completion metadata;
- `runs/<experiment>/*.jsonl`: complete exploration trace;
- `runs/<experiment>/run.log`: detailed diagnostic log.

Every timing is read from `campaignMetrics.durationNs` in the emitted trace.
The runner uses `--parallel-processes` and selects input index 1 so the
perturbed scenario runs alone. This prevents process-global simulator state
from leaking between the baseline and perturbation.

The generated `status` column describes exploration completion, not whether
the configured bug mechanism occurred. A `partial` or `expected-depth-limit`
row can still provide the intended evidence: some bugs leave recurring work,
and some schedules deliberately retain a bounded prefix around the fault.
Use the row-specific evidence below and inspect the trace when the completion
status differs across hosts; do not treat convergence alone as the oracle.

## Evidence represented by each scenario

Each locked schedule exercises the controller mechanism behind its historical
report. Some rows reproduce the original resource-level outcome exactly;
others expose the same non-atomic or state-coalescing mechanism without every
external component from Sieve's cluster environment.

| Experiment | Trace evidence | Gap from exact Sieve environment or outcome |
|---|---|---|
| ZooKeeper stale-state-1 | A newly created generation is reconciled through the frozen view of its deleting predecessor. | No material gap identified for the reported resource-lifecycle outcome. |
| ZooKeeper stale-state-2 | The stale deletion view produces the historical extra resource lifecycle operations. | No material gap identified for the reported resource-lifecycle outcome. |
| ZooKeeper unobserved-state-1 | The intermediate scale-down is skipped, so Pod/PVC 1 is never removed and the old PVC is retained. | The simulator has no literal ZooKeeper client or server, so it does not emit the later `NodeExists` response. |
| ZooKeeper indirect-1 | After PVC deletion and finalizer removal, later reconciliations continue updating the deleting cluster and its owned resources. | Sieve pauses and resumes one reconciliation while garbage collection interleaves; Kamera resumes through a newly queued reconciliation. |
| RabbitMQ stale-state-1 | The configured trace contains the additional StatefulSet and Pod create/delete activity reported by Sieve. | The later recreation currently races object removal, although the reported extra lifecycle activity occurs first. |
| RabbitMQ stale-state-2 | The configured trace contains a second StatefulSet deletion and an extra PVC update. | No material gap identified for the reported resource-lifecycle outcome. |
| RabbitMQ unobserved-state-1 | The perturbed trace converges with one Pod rather than the baseline's three. | No material gap identified for the missed-scale-transition outcome. |
| RabbitMQ intermediate-state-1 | StatefulSet deletion becomes visible before PVC expansion; the configured final StatefulSet remains marked for deletion while the baseline is live. | Sieve ends with a live PVC at `10Gi`; Kamera resumes the PVC update to `15Gi` but leaves the StatefulSet in its deletion transition. |
| Cassandra stale-state-1 | After recreation, the selected stale view leaves the new datacenter without its expected finalizer, StatefulSet, and Pod. | Sieve marks the recreated PVC for deletion; Kamera exposes the stale-generation effect one level earlier, at datacenter initialization. |
| Cassandra intermediate-state-1 | The CA keystore is created while its companion keystore is absent from that reconciliation. | The simulator does not model Cassandra bootstrap closely enough to produce the literal Pod `Running` timeout. |
| Cassandra intermediate-state-2 | After datacenter recreation, the trace reaches the second two-Pod transition from `Ready-to-Start` to `Starting` with `seed-node: "true"`; only those two Pod patches are retained. | Sieve's recorded CassandraDatacenter lacks `spec.config` from its first stored version, before the configured trigger, so the reported final configuration difference is not attributable to this transition. |

For Cassandra intermediate-state-2, the Sieve history records the requested
configuration in the `kubectl.kubernetes.io/last-applied-configuration`
annotation while the first stored live `spec` already lacks `config`. Kamera's
uninterrupted and configured final states both preserve the requested
Cassandra and JVM settings. The artifact therefore claims faithful trigger
placement and partial-update evidence, not reproduction of Sieve's unexplained
configuration loss.

Reviewers can inspect every complete trace manually. The Table 6 timing claim
does not depend on treating convergence itself as an outcome oracle.

## Optional real-cluster Sieve baselines

To rerun the comparison side with Sieve itself, including its kind-cluster
startup, controller execution, fault injection, oracle, and teardown, follow
the [Sieve baseline guide](../sieve/README.md). Those runs are intentionally
separate from `run-table6.sh` because their legacy toolchain, container images,
and multi-minute measurement boundary are different from Kamera's simulator.
