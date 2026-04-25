#!/usr/bin/env python3
"""
Plot agent tuning bar charts: states explored per iteration, colored by
whether the trial reproduced the bug. Three square panels side by side.

Usage:
    python3 experiments/coverage-curves/plot_tuning_bars.py
"""

import json, os, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from datetime import datetime

def parse_log_times(path):
    """Parse log, accumulating Total States across MC trial resets."""
    first_ts = last_ts = None
    cum_total = 0
    phase_max = 0
    for line in open(path):
        line = line.strip()
        if not line: continue
        try: obj = json.loads(line)
        except: continue
        ts = obj.get("ts")
        t = obj.get("Total States")
        if ts:
            dt = datetime.fromisoformat(ts)
            if first_ts is None: first_ts = dt
            last_ts = dt
        if obj.get("msg") == "starting!":
            cum_total += phase_max
            phase_max = 0
        if t is not None:
            phase_max = max(phase_max, t)
    cum_total += phase_max
    return first_ts, last_ts, cum_total

def build_data(runs_info, experiment_start):
    data = []
    prev_end = experiment_start
    for label, path, reproduced in runs_info:
        first_ts, last_ts, total = parse_log_times(path)
        if first_ts is None: continue
        data.append({"label": label, "total_states": total, "reproduced": reproduced})
        prev_end = last_ts
    return data

def plot_bars(ax, data, title):
    x = np.arange(len(data))
    colors = ['#2ca02c' if d["reproduced"] else '#d62728' for d in data]
    ax.bar(x, [d["total_states"] for d in data], 0.6, color=colors, alpha=0.8)
    ax.text(0.97, 0.93, title, transform=ax.transAxes, fontsize=6, fontweight='bold', va='top', ha='right')
    ax.set_xticks(x)
    ax.set_xticklabels([d["label"].replace("v","") for d in data], fontsize=5)
    ax.set_xlabel('Iteration', fontsize=7)
    ax.set_ylabel('States', fontsize=7)
    ax.tick_params(axis='both', labelsize=6)

def main():
    base = os.path.dirname(__file__)

    # --- KCP4 ---
    kcp4_t0 = datetime.fromisoformat("2026-03-28T09:50:01-07:00")
    kcp4_dir = os.path.join(base, "kcp/relaxed-tuning/trial2-final")
    kcp4_repro = {1:True, 2:True, 3:True, 4:True, 5:False, 6:False, 7:True, 8:False, 9:True, 10:False}
    kcp4_info = [(f"v{i}", os.path.join(kcp4_dir, f"kcp4-trial2-v{i}-log.txt"), kcp4_repro.get(i, False))
                 for i in range(1, 11) if os.path.exists(os.path.join(kcp4_dir, f"kcp4-trial2-v{i}-log.txt"))]
    kcp4_data = build_data(kcp4_info, kcp4_t0)

    # --- KRO K2b ---
    k2b_t0 = datetime.fromisoformat("2026-03-28T16:49:58-07:00")
    k2b_repro = {i: (i <= 9) for i in range(1, 11)}
    k2b_dir = os.path.join(base, "kro/tuning-runs")
    k2b_info = [(f"v{i}", os.path.join(k2b_dir, f"k2b-tuning-v{i}-log.txt"), k2b_repro.get(i, False))
                for i in range(1, 11) if os.path.exists(os.path.join(k2b_dir, f"k2b-tuning-v{i}-log.txt"))]
    k2b_data = build_data(k2b_info, k2b_t0)

    # --- Karpenter D12 ---
    d12_t0 = datetime.fromisoformat("2026-03-29T17:08:21-07:00")
    d12_repro = {1:True, 2:True, 3:True, 4:True, 5:True, 6:False, 7:True, 8:True, 9:True, 10:False}
    d12_dir = os.path.join(base, "karpenter/tuning-runs")
    d12_info = [(f"v{i}", os.path.join(d12_dir, f"d12-tuning-v{i}-log.txt"), d12_repro.get(i, False))
                for i in range(1, 11) if os.path.exists(os.path.join(d12_dir, f"d12-tuning-v{i}-log.txt"))]
    d12_data = build_data(d12_info, d12_t0)

    plt.rcParams.update({'font.size': 6})
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(3.33, 1.1))
    plot_bars(ax1, kcp4_data, 'KCP-4')
    plot_bars(ax2, k2b_data, 'KRO-2')
    plot_bars(ax3, d12_data, 'KAR-12')

    # Only leftmost gets y label
    ax2.set_ylabel('')
    ax3.set_ylabel('')

    legend_elements = [
        mpatches.Patch(facecolor='#2ca02c', alpha=0.8, label='Reproduced'),
        mpatches.Patch(facecolor='#d62728', alpha=0.8, label='Not reproduced'),
    ]
    fig.legend(handles=legend_elements, loc='upper center', ncol=2, fontsize=8,
               bbox_to_anchor=(0.5, 1.18), frameon=False)

    plt.subplots_adjust(left=0.0, right=1.0, bottom=0.0, top=0.92, wspace=0.45)
    for ax in (ax1, ax2, ax3):
        ax.margins(x=0.02)

    out = os.path.join(base, "tuning-bars.pdf")
    plt.savefig(out, bbox_inches='tight', pad_inches=0.01, dpi=600)
    print(f'Saved to {out}')

    # Copy to paper figures
    paper_fig = os.path.join(base, "../../.worktrees/kamera-paper/papers/new/figures/tuning-bars.pdf")
    if os.path.isdir(os.path.dirname(paper_fig)):
        import shutil
        shutil.copy2(out, paper_fig)
        print(f'Copied to {paper_fig}')

if __name__ == '__main__':
    main()
