#!/usr/bin/env python3
"""
Reconstruct cumulative exploration metrics from K2b exhaustive dump files.

Produces a plot of cumulative total states, unique states (S), and unique
resource states (R) over cumulative wall time, with log-scale Y axis.

S is synthesized from contentsHashAfter + pendingReconciles per step.
R is the global set of contentsHashAfter values.
"""

import json
import glob
import hashlib
import os
import re
import sys
import matplotlib.pyplot as plt


def extract_phase_order(filename):
    base = os.path.basename(filename)
    m = re.search(r'(reference|rerun)_(\d+)\.jsonl$', base)
    if not m:
        return (999, 0)
    phase_type = 0 if m.group(1) == 'reference' else 1
    scenario_idx = int(m.group(2))
    return (scenario_idx, phase_type)


def synthesize_s_hash(contents_hash, pending_reconciles):
    """Synthesize a unique state hash (S) from contents + pending reconciles."""
    pr_canonical = json.dumps(pending_reconciles, sort_keys=True)
    return hashlib.sha256((contents_hash + pr_canonical).encode()).hexdigest()[:16]


def load_phase_data(filepath):
    with open(filepath) as f:
        data = json.load(f)

    metrics = data['campaignMetrics']
    name = data.get('context', {}).get('scenario', {}).get('name', '?')
    phase = data.get('context', {}).get('scenario', {}).get('attributes', {}).get('phase', '?')

    content_hashes = set()
    s_hashes = set()
    for state in data.get('states', []):
        for path in state.get('paths', []):
            for step in path:
                if step is None:
                    continue
                ch = step.get('contentsHashAfter', '')
                pr = step.get('pendingReconciles', [])
                if ch:
                    content_hashes.add(ch)
                    s_hashes.add(synthesize_s_hash(ch, pr))

    return {
        'name': name,
        'phase': phase,
        'total_nodes': metrics['totalNodeVisits'],
        'unique_nodes': metrics['uniqueNodeVisits'],
        'resource_states': metrics['uniqueResourceStates'],
        'duration_s': metrics['durationNs'] / 1e9,
        'content_hashes': content_hashes,
        's_hashes': s_hashes,
    }


def main():
    evidence_dir = sys.argv[1] if len(sys.argv) > 1 else \
        os.path.join(os.path.dirname(__file__), '..', '..', 'examples', 'kro', 'evidence', 'k2b_exhaustive')
    evidence_dir = os.path.abspath(evidence_dir)

    files = sorted(glob.glob(os.path.join(evidence_dir, '*.jsonl')), key=extract_phase_order)
    if not files:
        print(f"No .jsonl files found in {evidence_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading {len(files)} dump files from {evidence_dir}")

    phases = [load_phase_data(f) for f in files]

    # Build cumulative curves
    cum_total = []
    cum_s = []
    cum_r = []
    cum_time = []

    running_total = 0
    running_time = 0.0
    global_r_hashes = set()
    global_s_hashes = set()

    for p in phases:
        running_total += p['total_nodes']
        running_time += p['duration_s']
        global_r_hashes |= p['content_hashes']
        global_s_hashes |= p['s_hashes']

        cum_total.append(running_total)
        cum_s.append(len(global_s_hashes))
        cum_r.append(len(global_r_hashes))
        cum_time.append(running_time)

    print(f"\nTotal phases: {len(phases)}")
    print(f"Cumulative total states: {cum_total[-1]}")
    print(f"Cumulative unique states (S): {cum_s[-1]}")
    print(f"Cumulative unique resource states (R): {cum_r[-1]}")
    print(f"Cumulative wall time: {cum_time[-1]:.1f}s")

    # Plot — prepend origin point for clean start
    fig, ax = plt.subplots(figsize=(10, 5))

    t = [0] + cum_time
    ax.plot(t, [1] + cum_total, 'b-', linewidth=1.5, label='Total states explored')
    ax.plot(t, [1] + cum_s, color='orange', linewidth=2, label='Unique states (S)')
    ax.plot(t, [1] + cum_r, 'g-', linewidth=2, label='Unique resource states (R)')

    ax.set_yscale('log')
    ax.set_xlabel('Cumulative wall time (seconds)')
    ax.set_ylabel('States (log scale)')
    ax.set_title('K2b Exhaustive Sweep: 7 Controllers × 7 Crash Points')
    ax.legend(loc='upper left')
    ax.grid(True, alpha=0.3, which='both')

    plt.tight_layout()

    out_path = os.path.join(os.path.dirname(__file__), 'k2b-exhaustive-coverage.pdf')
    plt.savefig(out_path, bbox_inches='tight')
    print(f"\nPlot saved to {out_path}")
    plt.close()


if __name__ == '__main__':
    main()
