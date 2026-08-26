# Kamera SOSP 2026 Artifact Evaluation

This artifact accompanies the SOSP 2026 paper *Testing Custom Control Planes
Without the Cluster*. The anonymous draft calls the system *Leica*; both names
refer to Kamera. We seek the **Available**, **Functional**, and **Reproduced**
badges.

## Reproduction map

The [bug findings and tested revisions](bug-findings.md) page links every public
bug report, records the status snapshot used by the paper, and identifies the
exact CCP versions evaluated.

Evaluation time estimates are with respect to an Apple M1 Pro with 16 GB RAM (2020 Macbook Pro). A warm Go module cache is
usually much faster than a cold start run. Budget approximately **45–90 minutes** for the complete
standard path, most of it dependency download, compilation, and simulation runs.

| Paper claim | Evaluator command | Typical first run | Evidence produced |
|---|---|---:|---|
| Functional smoke check | `./artifact/smoke-test.sh` | 1–2 min | build, run simulation to convergence, run oracle check over produced trace |
| Table 6 | `./artifact/run-table6.sh` | 5–15 min | 11 perturbed-run durations, traces, status table |
| Figure 8 | setup, then `./artifact/reproduce-figure8.sh` | 20–60 min including setup | 3 experimental reproductions + plotting |
| Section 6.1 | setup, then `./artifact/reproduce-section61.sh` | 15–40 min | all 3 case studies, including KRO-2 |
| Optional Sieve baselines | follow the [Sieve guide](sieve/README.md) | approximately 1.5–2 hours | real kind-cluster Sieve runs for all 11 Table 6 rows |

The Figure 8 reproduction workflow reruns every exhaustive baseline in addition to the tuned executions. One of the 3 experiments in Figure 8 is reported in the paper to time out after 2 hours. Thus, it takes up to two hours following the paper's two-hour exhaustive search timeout. We provide a pre-generated trace for this 2-hour exhaustive baseline if evaluators choose to not regenerate the exhaustive search trace until the 2 hour timeout fires. See the [Figure 8 guide](figure8/README.md).

## Badge: Available

The public artifact is the
[`sosp-ae` branch](https://github.com/tgoodwin/kamera/tree/sosp-ae). It contains
the source, scripts, pinned scenarios, controller adapters, expected outcomes,
and documentation needed for evaluation. Kamera is released under the
repository's MIT `LICENSE`; imported controller source retains its original
copyright headers.

The reviewed artifact revision is published as the
[Kamera v0.0.18 GitHub release](https://github.com/tgoodwin/kamera/releases/tag/v0.0.18),
created from the `sosp-ae` branch. The release page links the immutable Zenodo
archive and version-specific DOI. The main Kamera branch continues to undergo
development as Kamera is an open-source tool.

## Badge: Functional

### Requirements

- Linux or macOS on x86-64 or arm64
- Go 1.25.0 or newer
- Bash 3.2 or newer
- Python 3; Figure 8's fully pinned plotting environment requires Python 3.11+
- `git`, `jq`, `awk`, and standard Unix utilities
- approximately 2 CPU cores, 4 GiB RAM, and 5 GiB free disk space for the
  standard path
- network access for initial Go modules and pinned controller repositories

The paper's measurements used an Apple M1 Pro with 10 cores and 16 GiB RAM. The
AE workflow was validated on that host with macOS 14.7.6 (build 23H626), Go
1.25.0, Python 3.14.2, and GNU Bash
5.2.37. Absolute durations vary by host; the scripts preserve each reported
measurement boundary and display the evaluator's observed values.

### Build and smoke test

```bash
./artifact/smoke-test.sh
```

This builds Kamera and a test harness for the RabbitMQ controller (1-2 mins), then runs a complete perturbed
simulation, verifies convergence, and checks for the observable outcome of a known bug in the produced trace output (a few seconds). Success ends with:

```text
PASS: Kamera built, the perturbed run converged, and the unobserved-state oracle observed one Pod creation.
```

<details>
<summary>Optional: inspect the smoke test trace to manually verify the `PASS` result.</summary>

The smoke test prints the exact path of the simulation trace, and a command to open it in Kamera's trace inspection TUI:

```text
Trace written to: <path>
Optional: inspect the trace interactively:
  go run ./cmd/kamera inspect exploration "<path>"
```

The TUI opens on the trace's single converged state with one execution path. Press `Enter` to drill into its path and its reconciliation steps. Near the end of the path, look for:

1. An `External User` update that changes `RabbitmqCluster.spec.replicas` from
   1 to 3.
2. The following `RabbitmqClusterReconciler`. Its observations still contain
   the stale `RabbitmqCluster` version with `replicas: 1`, (stale view) so it does not create
   the additional Pods.
3. A second `External User` update that changes the requested replicas from 3
   to 2.
4. The final mismatch: the `RabbitmqCluster` requests 2 replicas, while the
   `StatefulSet` remains at 1 replica and only `rabbitmq-cluster-server-0`
   exists.

That mismatch is the reproduced unobserved-state bug. Use `Tab` to move between
panes, `Enter` or `d` to inspect a selected object, effect, or observation,
`Esc` to go back, and `q` to quit. This inspection is optional and is not an
additional pass criterion.

</details>

The remaining reproduction scripts provide broader functional evidence: they
build the case-study CCP test harnesses with which we found bugs, produce fresh outputs, and fail if
their expected output structure or observable checks deviate from the paper's reported results.

## Badge: Reproduced

### 1. Reproduce Table 6 execution times

```bash
./artifact/run-table6.sh
```

This runs all 11 perturbed simulations and measures only Kamera's
simulation duration, i.e., the values reported in Table 6. It writes a timestamped
directory under `artifact-results/` containing `table6.md`, `table6.tsv`, full
traces, logs, per-row metadata, and observable evidence.

To run one row:

```bash
./artifact/run-experiment.sh cass/intermediate-state-2
```

Success means all 11 rows produced a trace and a recorded perturbed-run
duration. Completion status is diagnostic and is not the bug oracle. See the
[Table 6 guide](table6/README.md) for row-specific evidence and timing
semantics.

### 2. Reproduce Figure 8

Run the complete standard reproduction:

```bash
./artifact/reproduce-figure8.sh
```

On first use, this command automatically installs and verifies the pinned
case-study sources and a managed plotting environment under the ignored
`artifact-deps/figure8/` directory. It then reruns the fixed agent-selected
simulation for KCP-4, KRO-2, and KAR-12, checks all three observable outcomes,
verifies and extracts archived **raw exhaustive simulator output**, derives
all six curves through one extractor, and generates one vertically stacked
`figure8.pdf` plus `figure8-report.md` and `figure8-report.tsv`.

Success reports `OBSERVED` and `CONSISTENT` for all three cases. No LLM is
needed because this workflow evaluates the configurations selected during the
paper experiments, not a new stochastic search trajectory.

To rerun the exhaustive side as well:

```bash
./artifact/reproduce-figure8.sh --exhaustive-source fresh
```

KAR-12 stops after two hours and retains all output completed before the cap,
matching the paper's timeout treatment. Exact source pins, raw-data handling,
plot semantics, expected endpoints, and the explanation for
experiment-specific Kamera revisions are in the
[Figure 8 guide](figure8/README.md).

### 3. Reproduce the directly scriptable Section 6.1 outcomes

This runs KCP-4, KRO-2, and KAR-12 and applies deterministic outcome checks.
Success prints `PASS` for all three cases, then writes traces, logs, oracle
JSON, and `section61.tsv`.

If the Figure 8 setup above has already run, its pinned KCP, KRO, and Karpenter
checkouts are reused automatically:

```bash
./artifact/reproduce-section61.sh
```

For a standalone Section 6.1 run, set up those dependencies first:

```bash
./artifact/setup-section61-deps.sh
./artifact/reproduce-section61.sh
```

The process of *finding* the new bugs that we ultimately report in Section 6.1 was best effort, driven by an LLM,
and thus inherently not reproducible via a deterministic script. Instead, the reproducible claim here is that the reported
configurations for found bugs execute in Kamera and that their stated observable outcomes occur in the executions.
The [Section 6.1 guide](section61/README.md) gives the exact checks.

## Notes on reproduction claims

- **Table 6:** the reported number is perturbed Kamera execution time. Baseline
  execution and real-cluster Sieve time are not included. The optional
  [Sieve guide](sieve/README.md) reruns all 11 comparison rows using Sieve's
  real kind-cluster workflow and preserves its separate timing boundary.
- **Figure 8:** fresh local executions establish all three agent-selected
  outcomes. Archived raw exhaustive outputs reproduce the paper panels through
  the same extraction and plotting path; the extended command recomputes them.
- **Section 6.1:** all three configurations rerun and check their observable
  outcomes. The LLM-driven bug search process itself is excluded.
- **Real-cluster Sieve baselines:** optional because they require Docker, kind,
  kubectl, a separate Python/Go environment, container-registry access, and
  approximately 1.5–2 hours. They are nevertheless documented and runnable
  for every Table 6 row.

## Results and troubleshooting

- Scripts never overwrite an existing result directory. Omit the output path
  to create a timestamped directory, or pass a new one explicitly.
- If the default Go cache is unsuitable, set one explicitly, for example:

  ```bash
  KAMERA_AE_GOCACHE=/tmp/kamera-go-cache ./artifact/smoke-test.sh
  ```

- For Table 6, exploration completion and the per-scenario observable are
  reported separately. Partial and max-depth results are diagnostic metadata,
  not automatic failures; use the row-specific evidence in the Table 6 guide.
- Inspect any unexpected partial trace with:

  ```bash
  go run ./cmd/kamera analyze campaign-metrics /path/to/dump.jsonl
  ```

- Do not time a combined baseline and perturbed invocation. The Table 6 runner
  enforces process isolation and selects only the perturbed input.

If a command fails, include its result directory, host architecture, Go and
Python versions, and the final 100 log lines in the evaluator report.
