#!/usr/bin/env python3
"""
Reconstruct cumulative exploration metrics from D12 exhaustive experiment.

Reads two data sources:
1. JSONL dump files (reference + rerun phases) for full trace data
2. CSV files (staleness interval phases) for lightweight metrics

Produces a cumulative coverage curve: total states explored and unique
resource states (R) over cumulative wall time.
"""

import csv
import json
import glob
import os
import re
import sys
import matplotlib.pyplot as plt


def load_jsonl_phase(filepath):
    """Load metrics from a full JSONL dump file."""
    with open(filepath) as f:
        data = json.load(f)

    metrics = data.get('campaignMetrics', {})
    content_hashes = set()
    for state in data.get('states', []):
        for path in state.get('paths', []):
            for step in path:
                if step is None:
                    continue
                ch = step.get('contentsHashAfter', '')
                if ch:
                    content_hashes.add(ch)

    return {
        'total_states': metrics.get('totalNodeVisits', 0),
        'resource_states': metrics.get('uniqueResourceStates', 0),
        'duration_s': metrics.get('durationNs', 0) / 1e9,
        'content_hashes': content_hashes,
    }


def load_csv_metrics(filepath):
    """Load lightweight metrics from a staleness CSV file."""
    rows = []
    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                'scenario_name': row.get('scenario_name', ''),
                'phase_name': row.get('phase_name', ''),
                'total_states': int(row.get('total_states', 0)),
                'resource_states': int(row.get('resource_states', 0)),
                'duration_s': int(row.get('duration_ns', 0)) / 1e9,
                'terminal_hash': row.get('terminal_hash', ''),
                'converged': row.get('converged', 'false').lower() == 'true',
            })
    return rows


def extract_depth_from_name(name):
    """Extract action depth from scenario name like 'D12/exhaustive/action-depth-42'."""
    m = re.search(r'action-depth-(\d+)', name)
    return int(m.group(1)) if m else -1


def main():
    evidence_dir = sys.argv[1] if len(sys.argv) > 1 else \
        os.path.join(os.path.dirname(__file__), 'exhaustive-output')
    evidence_dir = os.path.abspath(evidence_dir)

    out_path = sys.argv[2] if len(sys.argv) > 2 else \
        os.path.join(os.path.dirname(__file__), 'd12-exhaustive-coverage.pdf')

    # Load JSONL dump files (reference + rerun phases)
    jsonl_files = sorted(glob.glob(os.path.join(evidence_dir, '*.jsonl')))
    csv_files = sorted(glob.glob(os.path.join(evidence_dir, '*.csv')))

    print(f"Found {len(jsonl_files)} JSONL files and {len(csv_files)} CSV files in {evidence_dir}")

    if not jsonl_files and not csv_files:
        print("No data files found.", file=sys.stderr)
        sys.exit(1)

    # Build chronological list of phases ordered by (action_depth, phase_type)
    phases = []

    for f in jsonl_files:
        data = load_jsonl_phase(f)
        phases.append(data)

    # Load CSV staleness metrics
    csv_rows = []
    for f in csv_files:
        csv_rows.extend(load_csv_metrics(f))

    # Sort CSV rows by scenario name (action depth) then phase name
    csv_rows.sort(key=lambda r: (extract_depth_from_name(r['scenario_name']), r['phase_name']))

    for row in csv_rows:
        phases.append({
            'total_states': row['total_states'],
            'resource_states': row['resource_states'],
            'duration_s': row['duration_s'],
            'content_hashes': {row['terminal_hash']} if row['terminal_hash'] else set(),
        })

    if not phases:
        print("No phases found.", file=sys.stderr)
        sys.exit(1)

    # Build cumulative curves
    cum_total = []
    cum_r = []
    cum_time = []

    running_total = 0
    running_time = 0.0
    global_r_hashes = set()

    for p in phases:
        running_total += p['total_states']
        running_time += p['duration_s']
        global_r_hashes |= p.get('content_hashes', set())

        cum_total.append(running_total)
        cum_r.append(len(global_r_hashes))
        cum_time.append(running_time)

    print(f"\nTotal phases: {len(phases)}")
    print(f"  JSONL phases: {len(jsonl_files)}")
    print(f"  CSV staleness trials: {len(csv_rows)}")
    print(f"Cumulative total states: {cum_total[-1]}")
    print(f"Cumulative unique resource states (R): {cum_r[-1]}")
    print(f"Cumulative wall time: {cum_time[-1]:.1f}s")

    # Convergence stats from CSV
    converged = sum(1 for r in csv_rows if r['converged'])
    divergent_hashes = len(set(r['terminal_hash'] for r in csv_rows if r['converged'] and r['terminal_hash']))
    print(f"Staleness trials converged: {converged}/{len(csv_rows)}")
    print(f"Distinct terminal hashes: {divergent_hashes}")

    # Plot
    fig, ax = plt.subplots(figsize=(10, 5))

    t = [0] + cum_time
    ax.plot(t, [1] + cum_total, 'b-', linewidth=1.5, label='Total states explored (S)')
    ax.plot(t, [1] + cum_r, 'g--', linewidth=2, label='Unique resource states (R)')

    ax.set_yscale('log')
    ax.set_xlabel('Cumulative wall time (seconds)')
    ax.set_ylabel('States (log scale)')
    ax.set_title(f'Karpenter D12 Exhaustive Sweep: {len(phases)} phases, '
                 f'{cum_total[-1]} total states')
    ax.legend(loc='upper left')
    ax.grid(True, alpha=0.3, which='both')

    plt.tight_layout()
    plt.savefig(out_path, bbox_inches='tight')
    print(f"\nPlot saved to {out_path}")
    plt.close()


if __name__ == '__main__':
    main()
