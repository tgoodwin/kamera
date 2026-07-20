# Reproducing Figure 8

Figure 8 compares exhaustive exploration with the first agent-selected
configuration for KCP-4, KRO-2, and KAR-12. The evaluator workflow reruns all
three fixed agent-selected simulations, checks their observable outcomes, and
builds all three panels with the same extractor and plotting program used for
the exhaustive data.

No LLM access is required. The original search trajectories are not rerun;
the fixed configurations selected by those trajectories are deterministic
artifact inputs.

## Standard workflow: archived exhaustive output

Budget approximately **10–30 minutes** on a laptop after dependency download.
The first setup can take another **10–30 minutes**, depending on the Go module
cache and network connection.

```bash
./artifact/reproduce-figure8.sh
```

That command performs the complete workflow. On first use, it clones and
verifies the pinned case-study sources, creates a managed Python environment,
and installs the fully pinned plotting dependencies. These are cached under
the ignored `artifact-deps/figure8/` directory and reused on subsequent runs.
The host must provide Git, Go, and Python 3.11 or newer; no virtual-environment
activation is required.

This standard path:

1. builds each experiment against its pinned Kamera and controller sources;
2. runs the KCP-4, KRO-2, and KAR-12 agent-selected simulations;
3. checks that each configured observable outcome occurs;
4. verifies and extracts the checked-in raw exhaustive-output archives;
5. derives all six plotting curves with `extract_curves.py`;
6. writes one vertically stacked Figure 8 PDF and a numerical comparison
   report.

The result directory contains:

- `figure8.pdf`, containing the KCP-4, KRO-2, and KAR-12 panels in the same
  vertical layout as the paper;
- `figure8-report.md` and `figure8-report.tsv`;
- the six extracted JSONL curves under `curves/`;
- fresh simulation dumps, logs, campaign metrics, and outcome checks under
  `simulations/`; and
- the verified raw archive manifest and extracted exhaustive evidence.

A successful run reports `OBSERVED` for all three simulations and
`CONSISTENT` for all three comparisons. `CONSISTENT` means the configured
outcome was observed locally and the exhaustive exploration visited more
states than the agent-selected configuration, while the fresh agent S and R
endpoints were each within 20% of the paper values. Exact wall-clock durations
are not asserted because they depend on the evaluator's hardware.

The KRO-2 focused reference and perturbed phases both reach their configured
depth-50 boundary because the controller continues polling. Campaign metrics
therefore report them as bounded traces, not converged states. The outcome
checker inspects the perturbed terminal trace and separately records
`maxDepthStates: 1`; it does not relabel that trace as converged.

## Extended workflow: rerun every exhaustive campaign

To recompute the exhaustive side instead of extracting the archived raw
output:

```bash
./artifact/reproduce-figure8.sh --exhaustive-source fresh
```

Allow roughly **15–30 minutes** for KCP-4 and KRO-2 together, plus up to
**2 hours** for KAR-12. KAR-12 is deliberately capped at two hours, matching
the paper's timeout. If it reaches the cap, completed raw dumps are retained
and plotted; a timeout is an expected completion mode for this experiment.

Individual exhaustive runs are also available after a standard simulation
run has produced its `bin/` directory:

```bash
KAMERA_AE_FIGURE8_BIN_DIR=artifact-results/<simulation-run>/bin \
  ./artifact/run-figure8-exhaustive.sh kcp4

KAMERA_AE_FIGURE8_BIN_DIR=artifact-results/<simulation-run>/bin \
  ./artifact/run-figure8-exhaustive.sh kro2

KAMERA_AE_FIGURE8_BIN_DIR=artifact-results/<simulation-run>/bin \
  ./artifact/run-figure8-exhaustive.sh kar12
```

Set `KAMERA_AE_KAR_TIMEOUT_SECONDS` to a smaller value for a quick mechanics
check. Such a shortened run validates the pipeline but is not a reproduction
of the paper's two-hour comparison.

## What is compared

For each experiment, the plot shows cumulative simulated states visited
(`S`) and distinct resource states (`R`) for exhaustive exploration and the
agent-selected configuration. The accompanying report gives paper and local
endpoints numerically so evaluators need not estimate values from the PDFs.

The agent simulations run locally without model inference. The plots retain
the recorded inference offsets from the paper—68 seconds for KCP-4, 99 seconds
for KRO-2, and 131 seconds for KAR-12—so the fresh simulation curves use the
same comparison boundary as Figure 8. Those offsets are recorded experimental
metadata, not time spent by the local scripts.

The standard path uses raw exhaustive simulator output rather than checked-in
plot-ready curves. The compact JSONL samples previously used by this branch
are not inputs to `reproduce-figure8.sh`.

## Source pins and provenance

The exact machine-readable pins, scenario hashes, and adapter paths are in
[`dependencies.json`](dependencies.json). In summary:

| Case | Kamera | Controller | Packaged experiment material |
|---|---|---|---|
| KCP-4 | `d629dded` | KCP `301a8f74` | exact harness sources and two scenario captures |
| KRO-2 | `1c85e5b8` | KRO `c9320ee9` plus adapter | focused and exhaustive scenario captures |
| KAR-12 | `06bbe01a` | Karpenter `8ae07cf8` plus adapter | focused and exhaustive scenario captures |

The paper experiments were run from project-specific working trees near the
submission deadline, and some experiment material was committed only after
submission. Consequently, there is no single Kamera commit that truthfully
recreates all three panels:

- **KCP-4:** the controller is the public upstream KCP commit. The experiment
  harness was captured in a local post-submission commit that was never
  published as a standalone KCP revision. Its six Go source files are packaged
  under `kcp-historical/harness` and match that capture byte-for-byte. Its
  `go.mod` differs only by replacing a machine-local Kamera path with paths
  supplied by the runner.
- **KRO-2:** `1c85e5b8` is the earliest clean Kamera revision found to contain
  the behavior visible in the archived March 30 run and it reproduces all
  recorded exhaustive invariants: 30 dumps, 1,680 lightweight trials, 54,418
  total node visits, and 131 resource states. Later simulator changes alter
  those counts.
- **KAR-12:** the scenario ran from a working tree based on `06bbe01a`; the
  scenario was first committed later. Re-execution at `06bbe01a` preserves the
  experiment semantics, and the packaged scenario hash records the exact
  input.

These pins identify the experiment snapshots that produced the paper results;
they are not claims that one revision is the preferred modern Kamera version.
Normal development and the other AE workflows use `sosp-ae`.

## Direct component commands

Run only the fresh agent-selected simulations:

```bash
./artifact/run-figure8-simulations.sh
```

Verify the checked-in raw archives without running simulations:

```bash
python3 artifact/figure8/verify_raw_archives.py artifact/data/figure8/raw
```

The scripts refuse to overwrite an existing result directory. Pass a new
directory explicitly with `--output` or omit it to use a timestamped path.
