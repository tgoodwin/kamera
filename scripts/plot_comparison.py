#!/usr/bin/env python3
"""Plot multiple simulation runs aligned to t=0 for comparison.

Each run is independently aligned so its first data point starts at t=0.
This allows comparing runs from different sessions on the same time axis.

Usage:
    python scripts/plot_comparison.py \
        --runs log1.txt log2.txt \
        --labels "Exhaustive" "Agent" \
        --milestones "Bug reproduced" 65 \
        --title "KCP4: Exhaustive vs Agent" \
        -o output.png
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
                "ts": datetime.fromisoformat(ts),
                "total": total,
                "unique": unique,
                "resources": resources,
            })
    return points


def main():
    parser = argparse.ArgumentParser(description="Plot runs aligned to t=0")
    parser.add_argument("--runs", nargs="+", required=True)
    parser.add_argument("--labels", nargs="+", required=True)
    parser.add_argument("--milestones", nargs="*", help="Triplets of (label, seconds, color) or pairs of (label, seconds) defaulting to green")
    parser.add_argument("--title", default="")
    parser.add_argument("--xlim", type=float, default=None, help="X axis max in seconds")
    parser.add_argument("--offsets", nargs="*", type=float, help="Time offset (seconds) to shift each run by")
    parser.add_argument("--legend-loc", default="lower right")
    parser.add_argument("--figwidth", type=float, default=3.5)
    parser.add_argument("--figheight", type=float, default=1.6)
    parser.add_argument("--annotate", nargs=4, action="append", metavar=("LABEL", "X", "Y", "OFFSET_X"),
                        help="Add annotation: LABEL X Y OFFSET_X (offset_x in points)")
    parser.add_argument("--x-minutes", action="store_true", help="Force x-axis to show minutes")
    parser.add_argument("-o", "--output")
    args = parser.parse_args()

    milestones = []
    if args.milestones:
        i = 0
        while i < len(args.milestones):
            label = args.milestones[i]
            secs = float(args.milestones[i + 1])
            # Check if next arg is a color (not a number and not the start of another milestone)
            color = "green"
            if i + 2 < len(args.milestones):
                try:
                    float(args.milestones[i + 2])
                    # It's a number, so it's the next milestone's seconds — no color specified
                except ValueError:
                    color = args.milestones[i + 2]
                    i += 1
            milestones.append((label, secs, color))
            i += 2

    offsets = args.offsets or [0.0] * len(args.runs)
    if len(offsets) < len(args.runs):
        offsets.extend([0.0] * (len(args.runs) - len(offsets)))

    fig, ax = plt.subplots(figsize=(args.figwidth, args.figheight))
    plt.rcParams.update({'font.size': 8})
    colors = plt.cm.tab10.colors

    for i, (path, label) in enumerate(zip(args.runs, args.labels)):
        points = parse_log_with_time(path)
        if not points:
            print(f"WARNING: no data in {path}", file=sys.stderr)
            continue

        # Align to t=0 for this run, with optional offset
        t0 = points[0]["ts"]
        offset = offsets[i]
        seconds = [(p["ts"] - t0).total_seconds() + offset for p in points]
        totals = [p["total"] for p in points]
        resources = [p["resources"] for p in points]

        color = colors[i % len(colors)]
        ax.plot(seconds, totals, "-", color=color, linewidth=1, alpha=0.8, label=f"{label} (S)")
        ax.plot(seconds, resources, "--", color=color, linewidth=1, alpha=0.8, label=f"{label} (R)")
        ax.fill_between(seconds, resources, totals, alpha=0.08, color=color)

        final = points[-1]

        print(f"{label}: {seconds[-1]:.0f}s, {final['total']} total, {final['unique']} unique, {final['resources']} R")

    for label, secs, color in milestones:
        ax.axvline(x=secs, color=color, linewidth=1, linestyle="--", alpha=0.7)

    if args.annotate:
        for ann in args.annotate:
            text, x_str, y_str, ox_str = ann
            x, y, ox = float(x_str), float(y_str), float(ox_str)
            ax.annotate(text, xy=(x, y), xytext=(ox, 8), textcoords="offset points",
                        fontsize=5.5, ha="center",
                        bbox=dict(boxstyle="round,pad=0.2", facecolor="white", edgecolor="tab:orange", alpha=0.9),
                        arrowprops=dict(arrowstyle="->", color="tab:orange", lw=0.8))

    if args.xlim:
        ax.set_xlim(right=args.xlim)

    # Adaptive tick spacing
    x_max = args.xlim or ax.get_xlim()[1]
    if x_max > 600 or args.x_minutes:
        # Show minutes — pick tick interval to get ~6-8 ticks
        minutes = x_max / 60
        if minutes > 600:
            tick_interval = 60 * 300  # every 300 min
        elif minutes > 120:
            tick_interval = 60 * 30   # every 30 min
        elif minutes > 60:
            tick_interval = 60 * 10   # every 10 min
        elif minutes > 20:
            tick_interval = 60 * 5    # every 5 min
        elif minutes > 5:
            tick_interval = 60 * 2    # every 2 min
        else:
            tick_interval = 60        # every 1 min
        ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_interval))
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x / 60:.0f}"))
        ax.set_xlabel("Time (minutes)", fontsize=6)
    else:
        # Show seconds
        if x_max > 200:
            tick_interval = 30
        elif x_max > 60:
            tick_interval = 15
        else:
            tick_interval = 10
        ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_interval))
        ax.set_xlabel("Time (seconds)", fontsize=6)
    ax.set_ylabel("States visited", fontsize=6)
    ax.set_yscale("log")
    ax.set_ylim(bottom=1)
    ax.yaxis.set_major_locator(ticker.LogLocator(base=10, numticks=3))
    ax.yaxis.set_minor_locator(ticker.NullLocator())
    ax.yaxis.set_major_formatter(ticker.LogFormatterSciNotation())
    ax.set_title(args.title, fontsize=9)
    ax.tick_params(labelsize=7)
    ax.grid(alpha=0.2)

    if "center" in args.legend_loc:
        ax.legend(fontsize=5, loc=args.legend_loc, ncol=2,
                  bbox_to_anchor=(0.5, 0.0) if "lower" in args.legend_loc else (0.5, 1.0),
                  framealpha=0.9)
    else:
        ax.legend(fontsize=5, loc=args.legend_loc, ncol=2, framealpha=0.9)

    plt.subplots_adjust(left=0.15, right=0.97, top=0.95, bottom=0.22)
    if args.output:
        fig.savefig(args.output, dpi=600, bbox_inches="tight", pad_inches=0)
        print(f"Saved to {args.output}")
    else:
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        fig.savefig(tmp.name, dpi=150, bbox_inches="tight")
        print(f"Saved to {tmp.name}")


if __name__ == "__main__":
    main()
