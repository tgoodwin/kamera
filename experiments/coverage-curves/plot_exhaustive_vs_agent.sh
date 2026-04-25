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
#      --legend-loc "lower right" \
#      --figwidth 3.33 --figheight 0.96 \
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
  echo "Preprocessing KRO K2b exhaustive data (JSONL + CSV)..."
  python3 -c "
import csv, json, glob, os
from datetime import datetime, timedelta

evidence_dir = 'experiments/coverage-curves/kro/k2b-exhaustive-output'
jsonl_files = sorted(glob.glob(os.path.join(evidence_dir, '*.jsonl')))
csv_file = os.path.join(evidence_dir, 'staleness_metrics.csv')

phases = []
for f in jsonl_files:
    with open(f) as fh:
        data = json.load(fh)
    metrics = data.get('campaignMetrics', {})
    content_hashes = set()
    for state in data.get('states', []):
        for path in state.get('paths', []):
            for step in path:
                if step is None: continue
                ch = step.get('contentsHashAfter', '')
                if ch: content_hashes.add(ch)
    phases.append({
        'total_states': metrics.get('totalNodeVisits', 0),
        'resource_states': metrics.get('uniqueResourceStates', 0),
        'duration_s': metrics.get('durationNs', 0) / 1e9,
        'content_hashes': content_hashes,
    })

with open(csv_file) as fh:
    reader = csv.DictReader(fh)
    for row in reader:
        hashes = set(row.get('content_hashes', '').split(';')) if row.get('content_hashes') else set()
        hashes.discard('')
        phases.append({
            'total_states': int(row.get('total_states', 0)),
            'resource_states': int(row.get('resource_states', 0)),
            'duration_s': int(row.get('duration_ns', 0)) / 1e9,
            'content_hashes': hashes,
        })

t0 = datetime.fromisoformat('2026-03-30T14:35:31-07:00')
running_total = 0
running_time = 0.0
global_r_hashes = set()

# Emit origin point so the curve starts at (0, 1)
print(json.dumps({
    'ts': t0.isoformat(),
    'Total States': 1,
    '# Distinct States': 1,
    'Resource States': 1,
}))

for p in phases:
    running_total += p['total_states']
    running_time += p['duration_s']
    global_r_hashes |= p['content_hashes']
    ts = (t0 + timedelta(seconds=running_time)).isoformat()
    print(json.dumps({
        'ts': ts,
        'Total States': running_total,
        '# Distinct States': len(global_r_hashes),
        'Resource States': len(global_r_hashes),
    }))
" > experiments/coverage-curves/kro/k2b-exhaustive-accumulated.txt
  echo "  -> experiments/coverage-curves/kro/k2b-exhaustive-accumulated.txt"
fi

if [[ "$1" == "--preprocess" ]] || [[ ! -f experiments/coverage-curves/kro/k2b-agent-v1-padded.txt ]]; then
  echo "Preprocessing KRO K2b agent v1 log (padding inference period)..."
  python3 -c "
import json, datetime

# Align agent to exhaustive t=0 (new exhaustive run)
t0_str = '2026-03-30T14:35:31-07:00'
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
  --legend-loc "lower right" \
  --figwidth 3.33 --figheight 0.96 \
  --xlim 1760 \
  --title "" \
  -o "$PAPER_FIGURES/kcp4-exhaustive-vs-agent.pdf"

# --- KRO K2b ---
echo "Generating KRO K2b exhaustive vs agent..."
$PLOT_CMD \
  --runs \
    experiments/coverage-curves/kro/k2b-exhaustive-accumulated.txt \
    experiments/coverage-curves/kro/k2b-agent-v1-padded.txt \
  --labels "Exhaustive" "Agent" \
  --milestones "Agent reproduces" 99 green "Exhaustive done" 374 green \
  --legend-loc "lower right" \
  --figwidth 3.33 --figheight 0.96 \
  --xlim 394 \
  --x-minutes \
  --annotate "Agent (102S, 279ms exec)" 99 51 55 \
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
  --legend-loc "lower right" \
  --figwidth 3.33 --figheight 0.96 \
  --xlim 7600 \
  --annotate "Agent (1481S, 194ms exec)" 133 1481 30 \
  --title "" \
  -o "$PAPER_FIGURES/d12-exhaustive-vs-agent.pdf"

cp "$PAPER_FIGURES/d12-exhaustive-vs-agent.pdf" experiments/coverage-curves/karpenter/d12-exhaustive-vs-agent.pdf

echo "Generated:"
echo "  $PAPER_FIGURES/kcp4-exhaustive-vs-agent.pdf"
echo "  $PAPER_FIGURES/k2b-exhaustive-vs-agent.pdf"
echo "  $PAPER_FIGURES/d12-exhaustive-vs-agent.pdf"
