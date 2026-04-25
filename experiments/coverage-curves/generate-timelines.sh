#!/bin/bash
# Generate agent tuning timeline plots for KCP4, KCP7, and KCP17.
# Outputs to /tmp/kcp{4,7,17}-timeline.png

set -e
cd "$(dirname "$0")/../.."

python3 scripts/plot_tuning_timeline.py \
  --experiment-start "2026-03-26T21:34:41-07:00" \
  --runs \
    experiments/coverage-curves/kcp/tuning-runs/kcp4-tuning-v1-log.txt \
    experiments/coverage-curves/kcp/tuning-runs/kcp4-tuning-v3-log.txt \
    experiments/coverage-curves/kcp/tuning-runs/kcp4-tuning-v4-log.txt \
    experiments/coverage-curves/kcp/tuning-runs/kcp4-tuning-v6-log.txt \
    experiments/coverage-curves/kcp/tuning-runs/kcp4-tuning-v7-log.txt \
  --run-labels "v1" "v3" "v4" "v6" "v7" \
  --milestones "First reproduction" 133 "Minimal reproduction" 680 \
  --title "KCP4: Agent Tuning Timeline" \
  -o /tmp/kcp4-timeline.png

python3 scripts/plot_tuning_timeline.py \
  --experiment-start "2026-03-26T19:58:49-07:00" \
  --runs \
    experiments/coverage-curves/kcp/tuning-runs/kcp7-tuning-v1-log.txt \
    experiments/coverage-curves/kcp/tuning-runs/kcp7-tuning-v4-log.txt \
    experiments/coverage-curves/kcp/tuning-runs/kcp7-tuning-v7-log.txt \
    experiments/coverage-curves/kcp/tuning-runs/kcp7-tuning-v10-log.txt \
  --run-labels "v1" "v4" "v7" "v10" \
  --milestones "First reproduction" 201 "Minimal reproduction" 656 \
  --title "KCP7: Agent Tuning Timeline" \
  -o /tmp/kcp7-timeline.png

python3 scripts/plot_tuning_timeline.py \
  --experiment-start "2026-03-26T19:26:30-07:00" \
  --runs \
    experiments/coverage-curves/kcp/tuning-runs/kcp17-tuning-v1-log.txt \
    experiments/coverage-curves/kcp/tuning-runs/kcp17-tuning-v3-log.txt \
    experiments/coverage-curves/kcp/tuning-runs/kcp17-tuning-v4-log.txt \
    experiments/coverage-curves/kcp/tuning-runs/kcp17-tuning-v11-log.txt \
  --run-labels "v1" "v3" "v4" "v11" \
  --milestones "First reproduction" 192 "Minimal reproduction" 1157 \
  --title "KCP17: Agent Tuning Timeline" \
  -o /tmp/kcp17-timeline.png

# Exhaustive exploration runs (all three on one plot)
python3 scripts/plot_tuning_timeline.py \
  --experiment-start "2026-03-26T17:18:46-07:00" \
  --runs \
    experiments/coverage-curves/kcp/kcp-k4-log.txt \
    experiments/coverage-curves/kcp/kcp-k7-log.txt \
    experiments/coverage-curves/kcp/kcp-k17-log.txt \
  --run-labels "KCP4 (completed)" "KCP7 (timeout)" "KCP17 (timeout)" \
  --label-position end \
  --xlim 5400 \
  --legend-loc "upper left" \
  --title "Exhaustive Exploration Runs" \
  -o /tmp/exhaustive-timelines.png

echo "Generated:"
echo "  /tmp/kcp4-timeline.png"
echo "  /tmp/kcp7-timeline.png"
echo "  /tmp/kcp17-timeline.png"
echo "  /tmp/exhaustive-timelines.png"
