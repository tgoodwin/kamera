# Kamera SOSP 2026 Artifact Evaluation

This artifact accompanies the SOSP 2026 paper *Testing Custom Control Planes
Without the Cluster*. The anonymous draft calls the system *Leica*; both names
refer to Kamera. We seek the **Available**, **Functional**, and **Reproduced**
badges.

## Reproduction map

Evaluation time estimates are with respect to an Apple M1 Pro with 16 GB RAM (2020 Macbook Pro). A warm Go module cache is
usually much faster than a cold start run. Budget approximately **45–90 minutes** for the complete
standard path, most of it dependency download, compilation, and simulation runs.

| Paper claim | Evaluator command | Typical first run | Evidence produced |
|---|---|---:|---|
| Functional smoke check | `./artifact/smoke-test.sh` | 1–2 min | build, run simulation to convergence, run oracle check over produced trace |
| Table 6 | `./artifact/run-table6.sh` | 5–15 min | 11 perturbed-run durations, traces, status table |
| Figure 8 | setup, then `./artifact/reproduce-figure8.sh` | 20–60 min including setup | 3 experimental reproductions + plotting |
| Section 6.1 | setup, then `./artifact/reproduce-section61.sh` | 10–30 min | 2 selected bug reproductions exhibiting oracle results |

The Figure 8 reproduction workflow reruns every exhaustive baseline in addition to the tuned executions. One of the 3 experiments in Figure 8 is reported in the paper to time out after 2 hours. Thus, it takes up to two hours following the paper's two-hour exhaustive search timeout. We provide a pre-generated trace for this 2-hour exhaustive baseline if evaluators choose to not regenerate the exhaustive search trace until the 2 hour timeout fires. See the [Figure 8 guide](figure8/README.md).

## Badge: Available

The public artifact is the
[`sosp-ae` branch](https://github.com/tgoodwin/kamera/tree/sosp-ae). It contains
the source, scripts, pinned scenarios, controller adapters, expected outcomes,
and documentation needed for evaluation. Kamera is released under the
repository's MIT `LICENSE`; imported controller source retains its original
copyright headers.

The immutable archive and DOI will be added at the end of the artifact
evaluation process, as permitted by the SOSP AE schedule. The reviewed branch
revision will be archived, as the main Kamera branch will undergo further development as Kamera is an open-source tool.

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

This builds Kamera and a test harness for the RabbitMQ controller, runs a complete perturbed
simulation, verifies convergence, and checks for the observable outcome of a known bug in the produced trace output. Success ends with:

```text
PASS: Kamera built, the perturbed run converged, and the unobserved-state oracle observed one Pod creation.
```

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

Success means all 11 rows have an expected status and a recorded perturbed-run
duration. See the [Table 6 guide](table6/README.md) for exact outcome checks and
timing semantics.

### 2. Reproduce Figure 8

Install the pinned case-study sources and plotting dependency:

```bash
./artifact/setup-figure8-deps.sh

python3 -m venv /tmp/kamera-figure8-venv
source /tmp/kamera-figure8-venv/bin/activate
python3 -m pip install -r artifact/figure8/requirements.txt
```

Then run the standard reproduction:

```bash
./artifact/reproduce-figure8.sh
```

This reruns the fixed
agent-selected simulation for KCP-4, KRO-2, and KAR-12, checks all three
observable outcomes, verifies and extracts archived **raw exhaustive simulator
output**, derives all six curves through one extractor, and generates three
PDF panels plus `figure8-report.md` and `figure8-report.tsv`.

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

This runs two selected bugs: KCP-4 and KAR-12 simulations, verifies campaign completion,
and applies deterministic final-state checks. Success prints two `PASS` rows
and writes traces, logs, oracle JSON, and `section61.tsv`.

If the Figure 8 setup above has already run, its pinned KCP and Karpenter
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
and thus inherently not reproducible via a deterministic script. Instead, the reproducable claim here is that the reported
configurations for found bugs execute in Kamera and that their stated observable outcomes occur in the executions.
The [Section 6.1 guide](section61/README.md) gives the exact checks.

## Notes on reproduction claims

- **Table 6:** the reported number is perturbed Kamera execution time. Baseline
  execution and real-cluster Sieve time are not included. We can optionally provide instructions to run Sieve's own reproducer scripts if desired, but it requires installing the additional dependencies of a different research project.
- **Figure 8:** fresh local executions establish all three agent-selected
  outcomes. Archived raw exhaustive outputs reproduce the paper panels through
  the same extraction and plotting path; the extended command recomputes them.
- **Section 6.1:** KCP-4 and KAR-12 configurations rerun and check their
  observable outcomes; the LLM-driven bug search process itself is excluded.
- **Real-cluster Sieve baselines:** not included for these primary reproduction
  targets, although we can provide additional instructinons for doing so. Running them requires Docker, kind, kubectl, and a Sieve checkout.

## Results and troubleshooting

- Scripts never overwrite an existing result directory. Omit the output path
  to create a timestamped directory, or pass a new one explicitly.
- If the default Go cache is unsuitable, set one explicitly, for example:

  ```bash
  KAMERA_AE_GOCACHE=/tmp/kamera-go-cache ./artifact/smoke-test.sh
  ```

- For Table 6, exploration completion and the per-scenario observable are
  reported separately. One documented row has the expected status
  `expected-depth-limit`.
- Inspect any unexpected partial trace with:

  ```bash
  go run ./cmd/kamera analyze campaign-metrics /path/to/dump.jsonl
  ```

- Do not time a combined baseline and perturbed invocation. The Table 6 runner
  enforces process isolation and selects only the perturbed input.

If a command fails, include its result directory, host architecture, Go and
Python versions, and the final 100 log lines in the evaluator report.
