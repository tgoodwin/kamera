# Coverage Curves & Agent Tuning Experiments

This directory contains data and scripts for the "exhaustive vs agent" evaluation.
For each target project, we run an exhaustive perturbation sweep (baseline) and an
agent-guided tuning experiment, then compare states explored over time.

## Directory Structure

```
coverage-curves/
├── README.md                          # This file
├── generate-timelines.sh              # Master script to regenerate all plots
├── tuning-agent-prompt.md             # Agent prompt template (v1, timeout-based)
├── tuning-agent-prompt-v2.md          # Agent prompt template (v2, relaxed)
├── summary.md                         # Cross-project summary
│
├── kcp/                               # KCP experiments
│   ├── kcp-k4-log.txt                 # KCP4 exhaustive run log
│   ├── kcp-k4-dump.json               # KCP4 exhaustive dump
│   ├── kcp-k7-log.txt                 # KCP7 exhaustive run log
│   ├── kcp-k7-dump.json               # KCP7 exhaustive dump
│   ├── kcp-k17-log.txt                # KCP17 exhaustive run log
│   ├── kcp-k17-dump.json              # KCP17 exhaustive dump
│   ├── relaxed-tuning/
│   │   └── trial2-final/              # AUTHORITATIVE KCP4 agent tuning data
│   │       ├── log.md                 #   Experiment narrative log
│   │       ├── kcp4-trial2-v1-log.txt #   Per-iteration run logs
│   │       ├── kcp4-trial2-v1-dump.json
│   │       └── ...
│   ├── tuning-runs-old/               # Old KCP tuning runs (v1 prompt, superseded)
│   ├── tuning-experiment-kcp4-log.md  # Old experiment log (v1 prompt)
│   └── ...
│
├── kro/                               # KRO experiments
│   ├── kro-k2b-exhaustive-log.txt     # K2b exhaustive run log (50 scenarios)
│   ├── tuning-experiment-k2b-log.md   # AUTHORITATIVE K2b agent tuning narrative
│   ├── tuning-agent-prompt-kro-k2b.md # Agent prompt (adapted for fault injection)
│   ├── tuning-runs/                   # AUTHORITATIVE K2b agent tuning data
│   │   ├── k2b-tuning-v1-log.txt     #   Per-iteration run logs
│   │   ├── k2b-tuning-v1-dump.jsonl
│   │   └── ...
│   ├── plot_k2b_exhaustive.py         # Exhaustive coverage curve plotter
│   └── *.pdf                          # Generated plots
│
└── karpenter/                         # (TODO) Karpenter experiments
```

## Authoritative Data

| Project | Exhaustive Log | Agent Tuning Data | Experiment Start |
|---------|---------------|-------------------|-----------------|
| KCP4 | `kcp/kcp-k4-log.txt` | `kcp/relaxed-tuning/trial2-final/` | `2026-03-28T09:50:01-07:00` |
| KRO K2b | `kro/kro-k2b-exhaustive-log.txt` | `kro/tuning-runs/` | `2026-03-28T16:49:58-07:00` |

## How to Generate Plots

### Agent diagnosis timeline (side-by-side KCP4 + KRO K2b)

Uses `scripts/plot_tuning_timeline.py` for individual timelines, or the
inline script in `kro/` for the comparison figure.

The key inputs are:
- **Experiment start time**: ISO 8601 timestamp from when the agent was prompted
- **Run logs**: Structured JSON logs from kamera with `ts`, `Total States`,
  `# Distinct States`, `Resource States` fields
- **Reproduction status**: Which iterations reproduced the bug (manual from the
  experiment narrative log)

For KCP4 (trial2-final):
- Start: `2026-03-28T09:50:01-07:00`
- Runs: `kcp/relaxed-tuning/trial2-final/kcp4-trial2-v{1..10}-log.txt`
- Reproduced: v1-v4 yes, v5 no, v6 no, v7 yes, v8 no, v9 yes, v10 no

For KRO K2b:
- Start: `2026-03-28T16:49:58-07:00`
- Runs: `kro/tuning-runs/k2b-tuning-v{1..10}-log.txt`
- Reproduced: v1-v9 yes, v10 no

### Exhaustive coverage curve (total states + R over time)

For KRO K2b, use `kro/plot_k2b_exhaustive.py`:

```bash
python3 experiments/coverage-curves/kro/plot_k2b_exhaustive.py \
  examples/kro/evidence/k2b_exhaustive
```

This reconstructs cumulative metrics from dump files:
- Total states: sum of `campaignMetrics.totalNodeVisits` per phase
- Resource states (R): global set union of `contentsHashAfter` across all dumps
- Wall time: sum of `campaignMetrics.durationNs` per phase

For KCP4 exhaustive, use `scripts/plot_tuning_timeline.py` directly on the log:

```bash
python3 scripts/plot_tuning_timeline.py \
  --experiment-start "2026-03-26T17:18:46-07:00" \
  --runs experiments/coverage-curves/kcp/kcp-k4-log.txt \
  --run-labels "KCP4 exhaustive" \
  --title "KCP4 Exhaustive" \
  -o /tmp/kcp4-exhaustive.png
```

## Adding a New Project

1. Create `<project>/` directory
2. Run exhaustive scenario, save log to `<project>/<project>-<bug>-exhaustive-log.txt`
3. Save evidence dumps to `examples/<project>/evidence/<bug>_exhaustive/`
4. Write agent prompt adapted for the project (see `kro/tuning-agent-prompt-kro-k2b.md`)
5. Run agent experiment, save per-iteration logs to `<project>/tuning-runs/`
6. Save experiment narrative to `<project>/tuning-experiment-<bug>-log.md`
7. Add entries to `generate-timelines.sh`

## Notes

- KCP4 bug is a pure **ordering race** (2 converged states). Agent tuning prompt
  looks for "2+ converged states."
- KRO K2b bug requires **fault injection** (different object counts at max depth).
  Agent tuning prompt looks for "terminal states with different numbers of objects."
- The exhaustive coverage curve for KRO uses dump files (offline reconstruction)
  since the runner processes scenarios sequentially with per-phase stats resets.
  KCP uses the raw log (single continuous exploration).
