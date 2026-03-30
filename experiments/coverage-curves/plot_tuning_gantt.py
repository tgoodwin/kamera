#!/usr/bin/env python3
"""
Plot agent tuning Gantt chart: elapsed time breakdown per experiment,
stacked vertically (one row per project). Inference time is gray,
execution time is green (reproduced) or red (not reproduced).

Usage:
    python3 experiments/coverage-curves/plot_tuning_gantt.py
"""

import json, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from datetime import datetime
import numpy as np

def parse_log_times(path):
    first_ts = last_ts = None
    total = 0
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
        if t is not None:
            total = t
    return first_ts, last_ts, total

def build_timing_data(runs_info, experiment_start):
    data = []
    prev_end = experiment_start
    for label, path, reproduced in runs_info:
        first_ts, last_ts, total = parse_log_times(path)
        if first_ts is None: continue
        inf_start = (prev_end - experiment_start).total_seconds()
        inf_end = (first_ts - experiment_start).total_seconds()
        exe_start = inf_end
        exe_end = (last_ts - experiment_start).total_seconds()
        data.append({
            "label": label,
            "inf_start": inf_start, "inf_end": inf_end,
            "exe_start": exe_start, "exe_end": exe_end,
            "total_states": total,
            "reproduced": reproduced,
        })
        prev_end = last_ts
    return data

def plot_gantt_row(ax, data, title):
    y = 0.5
    bar_height = 0.6
    total_time = data[-1]["exe_end"]

    for d in data:
        # Inference block
        inf_dur = d["inf_end"] - d["inf_start"]
        if inf_dur > 0.5:
            ax.barh(y, inf_dur / 60, left=d["inf_start"] / 60, height=bar_height,
                    color='#B0BEC5', alpha=0.5, edgecolor='none')

        # Execution block — proportional width with a floor
        exe_dur = d["exe_end"] - d["exe_start"]
        exe_color = '#2ca02c' if d["reproduced"] else '#d62728'

        # Draw proportional bar
        if exe_dur > 0.01:
            ax.barh(y, exe_dur / 60, left=d["exe_start"] / 60, height=bar_height,
                    color=exe_color, alpha=0.7, edgecolor='none')

        # For very short runs, draw a 1pt vertical line as minimum visibility
        fig = ax.get_figure()
        # Convert 1 pixel to data coordinates
        inv = ax.transData.inverted()
        px_width = abs(inv.transform((1, 0))[0] - inv.transform((0, 0))[0])
        if exe_dur / 60 < px_width:
            ax.axvline(x=d["exe_start"] / 60, color=exe_color, linewidth=1,
                       ymin=(1 - bar_height) / 2, ymax=1 - (1 - bar_height) / 2,
                       alpha=0.9)

    # Label inside the bar area
    ax.text(0.01, 0.9, title, transform=ax.transAxes, fontsize=7, fontweight='bold', va='top')
    ax.set_yticks([])
    ax.set_ylim(0, 1)
    ax.set_xlim(left=0, right=total_time / 60 * 1.02)

def main():
    base = os.path.dirname(__file__)

    # --- KCP4 ---
    kcp4_t0 = datetime.fromisoformat("2026-03-28T09:50:01-07:00")
    kcp4_dir = os.path.join(base, "kcp/relaxed-tuning/trial2-final")
    kcp4_repro = {1:True, 2:True, 3:True, 4:True, 5:False, 6:False, 7:True, 8:False, 9:True, 10:False}
    kcp4_info = [(f"v{i}", os.path.join(kcp4_dir, f"kcp4-trial2-v{i}-log.txt"), kcp4_repro.get(i, False))
                 for i in range(1, 11) if os.path.exists(os.path.join(kcp4_dir, f"kcp4-trial2-v{i}-log.txt"))]
    kcp4_data = build_timing_data(kcp4_info, kcp4_t0)

    # --- KRO K2b ---
    k2b_t0 = datetime.fromisoformat("2026-03-28T16:49:58-07:00")
    k2b_repro = {i: (i <= 9) for i in range(1, 11)}
    k2b_dir = os.path.join(base, "kro/tuning-runs")
    k2b_info = [(f"v{i}", os.path.join(k2b_dir, f"k2b-tuning-v{i}-log.txt"), k2b_repro.get(i, False))
                for i in range(1, 11) if os.path.exists(os.path.join(k2b_dir, f"k2b-tuning-v{i}-log.txt"))]
    k2b_data = build_timing_data(k2b_info, k2b_t0)

    # --- Karpenter D12 ---
    d12_t0 = datetime.fromisoformat("2026-03-29T17:08:21-07:00")
    d12_repro = {1:True, 2:True, 3:True, 4:True, 5:True, 6:False, 7:True, 8:True, 9:True, 10:False}
    d12_dir = os.path.join(base, "karpenter/tuning-runs")
    d12_info = [(f"v{i}", os.path.join(d12_dir, f"d12-tuning-v{i}-log.txt"), d12_repro.get(i, False))
                for i in range(1, 11) if os.path.exists(os.path.join(d12_dir, f"d12-tuning-v{i}-log.txt"))]
    d12_data = build_timing_data(d12_info, d12_t0)

    # --- Stacked vertically, each row gets its own x axis ---
    # Column width for ACM sigplan two-column: ~3.33in
    plt.rcParams.update({'font.size': 8})
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(3.33, 2.4))

    plot_gantt_row(ax1, kcp4_data, 'KCP4')
    ax1.set_xlabel('Time (minutes)')

    plot_gantt_row(ax2, k2b_data, 'KRO K2b')
    ax2.set_xlabel('Time (minutes)')

    plot_gantt_row(ax3, d12_data, 'Karpenter D12')
    ax3.set_xlabel('Time (minutes)')

    # Custom diagonal-split patch for kamera execution legend entry
    from matplotlib.offsetbox import AuxTransformBox, DrawingArea
    from matplotlib.transforms import Affine2D

    class DiagonalPatch(matplotlib.patches.FancyBboxPatch):
        pass

    # Build legend with a custom handler for the split box
    gray_patch = mpatches.Patch(facecolor='#B0BEC5', alpha=0.5, label='Agent reasoning')

    # Create a small diagonal-split icon manually
    fig_legend_ax = fig.add_axes([0, 0, 1, 1], frame_on=False, xticks=[], yticks=[])
    fig_legend_ax.set_xlim(0, 1)
    fig_legend_ax.set_ylim(0, 1)
    fig_legend_ax.set_visible(False)

    # Use a proxy with custom handler
    class DiagonalHandler:
        def legend_artist(self, legend, orig_handle, fontsize, handlebox):
            x0, y0 = handlebox.xdescent, handlebox.ydescent
            w, h = handlebox.width, handlebox.height
            # Green triangle (top-left)
            tri_green = plt.Polygon([[x0, y0], [x0 + w, y0 + h], [x0, y0 + h]],
                                     closed=True, facecolor='#2ca02c', alpha=0.7, edgecolor='none')
            # Red triangle (bottom-right)
            tri_red = plt.Polygon([[x0, y0], [x0 + w, y0], [x0 + w, y0 + h]],
                                   closed=True, facecolor='#d62728', alpha=0.7, edgecolor='none')
            handlebox.add_artist(tri_green)
            handlebox.add_artist(tri_red)
            return tri_green

    split_proxy = plt.Line2D([], [], label='Kamera execution')

    fig.legend(handles=[gray_patch, split_proxy],
               handler_map={split_proxy: DiagonalHandler()},
               loc='upper center', ncol=2, fontsize=6,
               bbox_to_anchor=(0.5, 1.06), frameon=False)

    plt.subplots_adjust(left=0.02, right=0.98, top=0.85, bottom=0.15, hspace=1.2)
    out = os.path.join(base, "tuning-gantt.pdf")
    plt.savefig(out, bbox_inches='tight', pad_inches=0.01, dpi=600)
    print(f'Saved to {out}')

if __name__ == '__main__':
    main()
