#!/bin/bash
# Generate exhaustive vs agent comparison figures for all projects.
# Outputs to the paper figures directory.
#
# To regenerate preprocessed intermediate files, run with --preprocess.
#
# ============================================================================
# HOW TO ADD A NEW PROJECT (e.g., Karpenter)
# ============================================================================
#
# 1. DATA: You need two log files per project:
#    a) Exhaustive run log — a single file with JSON lines containing:
#         {"ts": "ISO8601", "Total States": N, "# Distinct States": N, "Resource States": N}
#       If your exhaustive run has multiple phases (counters reset), preprocess
#       it into a monotonically accumulated log (see the KRO K2b preprocessing
#       section below for an example).
#    b) Agent first-reproduction log — the log from the agent's first successful
#       trial. If the agent had inference time before execution, create a padded
#       log that includes a flat line at y=1 during the inference period, aligned
#       to the same t=0 as the exhaustive log (see K2b padding below).
#
# 2. PLOT COMMAND: Add a section like this:
#
#    echo "Generating <PROJECT> exhaustive vs agent..."
#    $PLOT_CMD \
#      --runs /path/to/exhaustive-accumulated.txt /path/to/agent-padded.txt \
#      --labels "Exhaustive" "Agent" \
#      --offsets 0 <AGENT_INFERENCE_SECONDS> \   # if not using a padded log
#      --milestones "Agent reproduces" <AGENT_TIME_S> "Exhaustive done" <EXHAUST_TIME_S> \
#      --legend-loc "lower center" \
#      --figwidth 3.33 --figheight 1.2 \
#      --xlim <MAX_SECONDS> \
#      --annotate "Agent reproduces bug (NS, Xms exec)" <X> <Y> <OFFSET_X> \  # optional
#      --title "" \
#      -o "$PAPER_FIGURES/<project>-exhaustive-vs-agent.pdf"
#
#    Key parameters:
#      --offsets:    Shift each run on the x-axis. Use 0 for exhaustive.
#                    For agent, set to inference seconds if NOT using a padded log.
#      --milestones: Vertical dashed red lines. Pair of (label, seconds).
#      --xlim:       Set to just past the exhaustive completion time.
#      --annotate:   Label pointing to the agent's data (useful when the agent
#                    curve is too small to see). Args: LABEL X Y OFFSET_X_POINTS.
#
# 3. PAPER: Update the subfigure in evaluation.tex:
#
#    \begin{subfigure}[t]{\columnwidth}
#      \centering
#      \includegraphics[width=\columnwidth]{figures/<project>-exhaustive-vs-agent.pdf}
#      \caption{<Project> (<bug type>, N controllers)}
#      \label{fig:comparison-<project>}
#    \end{subfigure}
#
#    Add it after the existing subfigures inside \begin{figure}...\end{figure}
#    labeled {fig:exhaustive-vs-agent}. Update the shared \caption and any
#    \Cref references in the text.
#
# ============================================================================

set -e
cd "$(dirname "$0")/../.."

PAPER_FIGURES=".worktrees/kamera-paper/papers/new/figures"
PLOT_CMD="python3 scripts/plot_comparison.py"

# --- Preprocess KRO K2b data if needed ---
if [[ "$1" == "--preprocess" ]] || [[ ! -f experiments/coverage-curves/kro/k2b-exhaustive-accumulated.txt ]]; then
  echo "Preprocessing KRO K2b exhaustive log..."
  python3 -c "
import json
from datetime import datetime

lines = []
for line in open('experiments/coverage-curves/kro/kro-k2b-exhaustive-log.txt'):
    line = line.strip()
    if not line: continue
    try: obj = json.loads(line)
    except: continue
    lines.append(obj)

cum_total = 0
cum_unique = 0
cum_resources = 0
phase_max_total = 0
phase_max_unique = 0
phase_max_resources = 0

for obj in lines:
    ts = obj.get('ts')
    if obj.get('msg') == 'starting!':
        cum_total += phase_max_total
        cum_unique += phase_max_unique
        cum_resources += phase_max_resources
        phase_max_total = 0
        phase_max_unique = 0
        phase_max_resources = 0
        continue
    total = obj.get('Total States')
    unique = obj.get('# Distinct States')
    resources = obj.get('Resource States')
    if ts and total is not None and unique is not None and resources is not None:
        phase_max_total = max(phase_max_total, total)
        phase_max_unique = max(phase_max_unique, unique)
        phase_max_resources = max(phase_max_resources, resources)
        print(json.dumps({'ts': ts, 'Total States': cum_total + total,
              '# Distinct States': cum_unique + unique,
              'Resource States': cum_resources + resources}))
" > experiments/coverage-curves/kro/k2b-exhaustive-accumulated.txt
  echo "  -> experiments/coverage-curves/kro/k2b-exhaustive-accumulated.txt"
fi

if [[ "$1" == "--preprocess" ]] || [[ ! -f experiments/coverage-curves/kro/k2b-agent-v1-padded.txt ]]; then
  echo "Preprocessing KRO K2b agent v1 log (padding inference period)..."
  python3 -c "
import json, datetime

# Align agent to exhaustive t=0
t0_str = '2026-03-28T15:20:57-07:00'
print(json.dumps({'ts': t0_str, 'Total States': 1, '# Distinct States': 1, 'Resource States': 1}))
t99 = (datetime.datetime.fromisoformat(t0_str) + datetime.timedelta(seconds=98)).isoformat()
print(json.dumps({'ts': t99, 'Total States': 1, '# Distinct States': 1, 'Resource States': 1}))

real_t0 = datetime.datetime.fromisoformat('2026-03-28T16:51:37-07:00')
desired_t0 = datetime.datetime.fromisoformat(t0_str) + datetime.timedelta(seconds=99)
delta = desired_t0 - real_t0

for line in open('experiments/coverage-curves/kro/tuning-runs/k2b-tuning-v1-log.txt'):
    line = line.strip()
    if not line: continue
    try: obj = json.loads(line)
    except: continue
    ts = obj.get('ts')
    total = obj.get('Total States')
    unique = obj.get('# Distinct States')
    resources = obj.get('Resource States')
    if ts and total is not None and unique is not None and resources is not None:
        new_ts = (datetime.datetime.fromisoformat(ts) + delta).isoformat()
        print(json.dumps({'ts': new_ts, 'Total States': total,
              '# Distinct States': unique, 'Resource States': resources}))
" > experiments/coverage-curves/kro/k2b-agent-v1-padded.txt
  echo "  -> experiments/coverage-curves/kro/k2b-agent-v1-padded.txt"
fi

# --- KCP4 ---
echo "Generating KCP4 exhaustive vs agent..."
$PLOT_CMD \
  --runs experiments/coverage-curves/kcp/kcp-k4-log.txt \
         experiments/coverage-curves/kcp/relaxed-tuning/trial2-final/kcp4-trial2-v1-log.txt \
  --labels "Exhaustive" "Agent" \
  --offsets 0 68 \
  --milestones "Agent bug found" 133 green "Exhaustive done" 1672 green \
  --legend-loc "lower center" \
  --figwidth 3.33 --figheight 1.2 \
  --xlim 1720 \
  --title "" \
  -o "$PAPER_FIGURES/kcp4-exhaustive-vs-agent.pdf"

# --- KRO K2b ---
echo "Generating KRO K2b exhaustive vs agent..."
$PLOT_CMD \
  --runs \
    experiments/coverage-curves/kro/k2b-exhaustive-accumulated.txt \
    experiments/coverage-curves/kro/k2b-agent-v1-padded.txt \
  --labels "Exhaustive" "Agent" \
  --milestones "Agent reproduces" 99 green "Exhaustive done" 105 green \
  --legend-loc "lower center" \
  --figwidth 3.33 --figheight 1.2 \
  --xlim 110 \
  --annotate "Agent reproduces bug (51S, 279ms exec)" 99 51 -55 \
  --title "" \
  -o "$PAPER_FIGURES/k2b-exhaustive-vs-agent.pdf"

# Copy to experiments dir
cp "$PAPER_FIGURES/k2b-exhaustive-vs-agent.pdf" experiments/coverage-curves/kro/k2b-exhaustive-vs-agent.pdf

# --- Karpenter D12 ---
echo "Generating Karpenter D12 exhaustive vs agent..."
$PLOT_CMD \
  --runs experiments/coverage-curves/karpenter/d12-exhaustive-accumulated.txt \
       experiments/coverage-curves/karpenter/d12-agent-v1-padded.txt \
  --labels "Exhaustive" "Agent" \
  --milestones "Agent reproduces" 133 green "Timeout" 7200 red \
  --legend-loc "lower center" \
  --figwidth 3.33 --figheight 1.2 \
  --xlim 7600 \
  --annotate "Agent (1481S, 2min)" 133 1481 30 \
  --title "" \
  -o "$PAPER_FIGURES/d12-exhaustive-vs-agent.pdf"

cp "$PAPER_FIGURES/d12-exhaustive-vs-agent.pdf" experiments/coverage-curves/karpenter/d12-exhaustive-vs-agent.pdf

echo "Generated:"
echo "  $PAPER_FIGURES/kcp4-exhaustive-vs-agent.pdf"
echo "  $PAPER_FIGURES/k2b-exhaustive-vs-agent.pdf"
echo "  $PAPER_FIGURES/d12-exhaustive-vs-agent.pdf"
