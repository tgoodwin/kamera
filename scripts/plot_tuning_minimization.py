#!/usr/bin/env python3
"""Plot agent tuning minimization: sequential iterations with successes and failures.

Each run is plotted as a triangle (S and R lines) in chronological order.
Failed runs (0 or 1 converged state) are marked with an X at their endpoint.

Usage:
    python scripts/plot_tuning_minimization.py \
        --experiment-start "2026-03-26T21:34:41-07:00" \
        --runs v1-log.txt v2-log.txt ... \
        --labels v1 v2 ... \
        --failed v2 v5 v9 \
        --title "KCP4: Agent Tuning Minimization" \
        -o output.pdf
"""

import argparse
import json
import sys
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.patches as mpatches


def parse_iso(ts_str):
    return datetime.fromisoformat(ts_str)


def parse_log_with_time(path):
    points = []
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        ts = obj.get("ts")
        total = obj.get("Total States")
        unique = obj.get("# Distinct States")
        resources = obj.get("Resource States")
        if ts and total is not None and unique is not None and resources is not None:
            points.append({
                "ts": parse_iso(ts),
                "total": total,
                "unique": unique,
                "resources": resources,
            })
    return points


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-start", required=True)
    parser.add_argument("--runs", nargs="+", required=True)
    parser.add_argument("--labels", nargs="+", required=True)
    parser.add_argument("--failed", nargs="*", default=[], help="Labels of failed runs")
    parser.add_argument("--milestones", nargs="*")
    parser.add_argument("--title", default="")
    parser.add_argument("--xlim", type=float, default=None)
    parser.add_argument("--legend-loc", default="lower right")
    parser.add_argument("-o", "--output")
    args = parser.parse_args()

    t0 = parse_iso(args.experiment_start)
    failed_set = set(args.failed)

    milestones = []
    if args.milestones:
        for i in range(0, len(args.milestones), 2):
            milestones.append((args.milestones[i], float(args.milestones[i + 1])))

    fig, ax = plt.subplots(figsize=(3.5, 1.6))
    plt.rcParams.update({'font.size': 8})

    success_color = "#2ca02c"
    fail_color = "#d62728"

    for i, (path, label) in enumerate(zip(args.runs, args.labels)):
        points = parse_log_with_time(path)
        if not points:
            print(f"WARNING: no data in {path}", file=sys.stderr)
            continue

        is_failed = label in failed_set
        seconds = [(p["ts"] - t0).total_seconds() for p in points]
        uniques = [p["unique"] for p in points]
        resources = [p["resources"] for p in points]
        final = points[-1]

        color = fail_color if is_failed else success_color
        alpha = 0.4 if is_failed else 0.7

        ax.plot(seconds, uniques, "-", color=color, linewidth=0.8, alpha=alpha)
        ax.plot(seconds, resources, "--", color=color, linewidth=0.8, alpha=alpha)
        ax.fill_between(seconds, resources, uniques, alpha=0.04 if is_failed else 0.06, color=color)

        # Label at the midpoint of the triangle
        mid_idx = len(seconds) // 2
        mid_x = seconds[mid_idx]
        mid_y = (uniques[mid_idx] + resources[mid_idx]) / 2

        if is_failed:
            ax.plot(seconds[-1], final["unique"], "x", color=fail_color,
                    markersize=5, markeredgewidth=1.5, zorder=5)
        else:
            ax.plot(seconds[-1], final["unique"], "o", color=success_color,
                    markersize=3, zorder=5)
            ax.annotate(f"{final['unique']}S",
                        xy=(mid_x, mid_y), fontsize=5.5, ha="center", va="center",
                        color=color, fontweight="bold",
                        bbox=dict(boxstyle="round,pad=0.1", facecolor="white",
                                  edgecolor=color, alpha=0.85, linewidth=0.5))

        status = "FAIL" if is_failed else "OK"
        print(f"{label} [{status}]: {seconds[0]:.0f}s-{seconds[-1]:.0f}s, {final['total']} total, {final['unique']} unique, {final['resources']} R")

    for label, secs in milestones:
        ax.axvline(x=secs, color="red", linewidth=1, linestyle="--", alpha=0.7)
        ax.annotate(label, xy=(secs, ax.get_ylim()[1] * 0.95),
                    fontsize=6, ha="right", va="top", color="red", fontweight="bold",
                    bbox=dict(boxstyle="round,pad=0.2", facecolor="white",
                              edgecolor="red", alpha=0.8, linewidth=0.5))

    if args.xlim:
        ax.set_xlim(right=args.xlim)

    x_max = args.xlim or ax.get_xlim()[1]
    if x_max > 3600:
        tick_interval = 600
    elif x_max > 1200:
        tick_interval = 300
    else:
        tick_interval = 120
    ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_interval))
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x / 60:.0f}"))
    ax.set_xlabel("Time (minutes)", fontsize=8)
    ax.set_ylabel("Unique states", fontsize=8)
    ax.set_title(args.title, fontsize=9)
    ax.tick_params(labelsize=7)
    ax.grid(alpha=0.2)
    ax.set_ylim(bottom=0)

    legend_elements = [
        plt.Line2D([0], [0], color=success_color, linewidth=1, label="Reproduced (S)"),
        plt.Line2D([0], [0], color=success_color, linewidth=1, linestyle="--", label="Reproduced (R)"),
        plt.Line2D([0], [0], color=fail_color, marker="x", linewidth=0, markersize=5,
                   markeredgewidth=1.5, label="Not reproduced"),
    ]
    ax.legend(handles=legend_elements, fontsize=5.5, loc=args.legend_loc)

    plt.tight_layout(pad=0.3)
    if args.output:
        fig.savefig(args.output, dpi=150, bbox_inches="tight", pad_inches=0)
        print(f"Saved to {args.output}")
    else:
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        fig.savefig(tmp.name, dpi=150, bbox_inches="tight", pad_inches=0)
        print(f"Saved to {tmp.name}")


if __name__ == "__main__":
    main()
