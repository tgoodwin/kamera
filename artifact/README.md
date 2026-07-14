# Kamera SOSP 2026 Artifact Evaluation

> [!IMPORTANT]
> **Start here.** Run these commands from the repository root on the
> `sosp-ae` branch. The standard workflow needs no Kubernetes cluster,
> container runtime, cloud account, or LLM credential. Begin with the smoke
> test before downloading the larger case-study dependencies.

This artifact accompanies the SOSP 2026 paper about **Kamera**. The anonymous
paper draft calls the system *Leica*; both names refer to the same system. We
target the Available, Functional, and Reproduced badges. The immutable archive
and DOI will be added after artifact evaluation.

- **Available:** the source, scenarios, adapters, and instructions are public
  on `sosp-ae`; the reviewed revision will be archived with a DOI.
- **Functional:** the smoke test builds Kamera and checks a complete simulated
  controller execution.
- **Reproduced:** the remaining scripts regenerate Table 6, Figure 8, and the
  directly scriptable Section 6.1 outcomes described below.

## Reproduction goals at a glance

Runtime estimates are conservative first-run ranges on a laptop with a normal
network connection. A warm Go module cache is usually much faster. For the
core path—smoke test, Table 6, Figure 8 redraw, and Section 6.1—budget roughly
**30–60 minutes**, most of it dependency download and compilation time.

| Goal | Command | Typical first run | Successful result | Details |
|---|---|---:|---|---|
| Functional check | `./artifact/smoke-test.sh` | 1–2 min | Prints `PASS` | [Below](#1-run-the-functional-smoke-test) |
| Table 6 execution times | `./artifact/run-table6.sh` | 5–15 min | Writes `table6.md` with 11 rows | [Table 6 guide](table6/README.md) |
| Figure 8 panels | `./artifact/reproduce-figure8.sh` | Under 1 min after Python setup | Writes three PDFs and a summary | [Figure 8 guide](figure8/README.md) |
| Section 6.1 KCP-4 and KAR-12 outcomes | Setup, then `./artifact/reproduce-section61.sh` | 10–30 min | Prints two `PASS` rows | [Section 6.1 guide](section61/README.md) |
| Historical KRO-2 simulation | Setup, then `./artifact/run-figure8-kro-historical.sh focused` | 5–15 min | Outcome check plus campaign metrics | [Figure 8 guide](figure8/README.md) |

The historical KRO-2 **full** matrix is an extended check. It adds roughly
3–10 minutes after setup and verifies the exact archived invariants: 30 dumps,
1,680 staleness trials, 54,418 node visits, and 131 resource states.

## Requirements

- Linux or macOS on x86-64 or arm64
- Go 1.25.5 or newer
- Bash 3.2 or newer
- Python 3; `matplotlib` is needed only to redraw Figure 8
- `git`, `jq`, `awk`, and standard Unix utilities
- Approximately 2 CPU cores, 4 GiB RAM, and 5 GiB free disk space
- Network access for initial Go modules and pinned case-study repositories

The paper measurements used an Apple M1 Pro with 10 cores and 16 GiB RAM.
Absolute execution times naturally vary by host; the scripts preserve the
paper's measurement boundary and report the evaluator's observed values.

## Recommended evaluator path

### 1. Run the functional smoke test

```bash
./artifact/smoke-test.sh
```

This builds the RabbitMQ harness, runs one perturbed simulation in an isolated
process, verifies convergence, and checks one directly observable outcome. A
successful run ends with:

```text
PASS: Kamera built, the perturbed run converged, and the unobserved-state oracle observed one Pod creation.
```

### 2. Reproduce Table 6

```bash
./artifact/run-table6.sh
```

This runs all 11 locked perturbed simulations and measures only Kamera's
simulation duration—the quantity reported in Table 6. It writes a timestamped
directory under `artifact-results/` containing `table6.md`, `table6.tsv`,
complete traces, logs, and per-row metadata.

To run only one row:

```bash
./artifact/run-experiment.sh cass/intermediate-state-2
```

See the [Table 6 guide](table6/README.md) for timing semantics, expected status
values, per-scenario evidence, and known gaps from literal Sieve behavior.

### 3. Redraw Figure 8

Prepare a disposable Python environment if `matplotlib` is unavailable:

```bash
python3 -m venv /tmp/kamera-figure8-venv
source /tmp/kamera-figure8-venv/bin/activate
python3 -m pip install -r artifact/figure8/requirements.txt
```

Then redraw all three panels:

```bash
./artifact/reproduce-figure8.sh
```

This uses checked-in, plot-equivalent samples from the recorded campaigns and
writes three PDFs, `figure8-summary.tsv`, and a data manifest. It does not
rerun the LLM trajectory, because that would require external model access and
would measure a new search rather than reproduce the paper's recorded one.

### 4. Rerun the Section 6.1 simulations

```bash
./artifact/setup-section61-deps.sh
./artifact/reproduce-section61.sh
```

Setup clones the pinned KCP and Karpenter sources and applies the checksummed
Karpenter simulation adapter. The reproducer runs fixed KCP-4 and KAR-12
simulations, verifies convergence with campaign metrics, and applies
deterministic final-state checks. A successful run prints two `PASS` rows and
writes traces, logs, oracle JSON, and `section61.tsv`.

### 5. Optionally rerun the historical KRO-2 computation

The deterministic simulation selected by the original agent can be run
without an LLM:

```bash
./artifact/setup-figure8-kro-deps.sh
./artifact/run-figure8-kro-historical.sh focused
```

For the exact complete campaign invariants, replace `focused` with `full`.
The focused depth-50 execution reaches the reported observable outcome but is
a bounded trace, so the checker reports zero converged states separately.

## Scope of the reproduced claims

- **Table 6:** perturbed Kamera execution time for 11 controller scenarios.
  Baseline execution and real-cluster Sieve time are not included in the
  reported number.
- **Figure 8:** the published panels are regenerated from archived samples;
  KRO-2 additionally has a deterministic historical simulator rerun.
- **Section 6.1:** fixed KCP-4 and KAR-12 configurations rerun the simulations
  and check their observable outcomes. The search process itself is excluded.
- **LLM-guided discovery:** intentionally not in the standard workflow because
  it is credential-dependent and nondeterministic.
- **Real-cluster Sieve baselines:** optional and not required for the primary
  reproduction target; they require Docker, kind, kubectl, and Sieve.

## Source snapshot provenance

The paper experiments used project-specific Kamera branches and working trees,
not one clean repository commit. The standard Table 6 and Section 6.1 scripts
run from the consolidated `sosp-ae` branch. The exact historical KRO-2 rerun
uses `1c85e5b`, the earliest clean reconstruction found to contain the
submission-time behavior evidenced by the archived March 30 log and to match
the archived campaign invariants.

Other pinned SHAs identify the external controller source for a particular
experiment; they are not competing Kamera revisions. The detailed KRO audit,
later semantic boundaries, adapter hashes, and scenario hashes are recorded in
the [Figure 8 guide](figure8/README.md) and its dependency manifest.

## Results and troubleshooting

- Scripts never overwrite an existing result directory. Omit the output path
  to create a timestamped directory, or pass a new path explicitly.
- If the default Go cache is unsuitable, set one explicitly, for example:

  ```bash
  KAMERA_AE_GOCACHE=/tmp/kamera-go-cache ./artifact/smoke-test.sh
  ```

- For Table 6, `converged` describes exploration completion; it is separate
  from the per-scenario observable evidence. One documented row uses
  `expected-depth-limit`.
- Any other unexpected partial trace should be inspected with:

  ```bash
  go run ./cmd/kamera analyze campaign-metrics /path/to/dump.jsonl
  ```

- Do not time a combined baseline and perturbed invocation. The Table 6 runner
  enforces process isolation and selects only the perturbed input.

## License and archival plan

Kamera is released under the repository's MIT `LICENSE`. Imported controller
source retains its original copyright headers. Before archival release, the
artifact will include a third-party notices inventory and the final Zenodo DOI.
