#!/usr/bin/env python3
"""Plot agent tuning timeline: wall-clock time on X, coverage counters on Y.

Stitches together multiple sequential simulation runs into one continuous timeline,
showing the agent's iterative tuning process. Gaps between runs represent agent
thinking/editing time.

Usage:
    python scripts/plot_tuning_timeline.py \
        --experiment-start "2026-03-26T21:34:41-07:00" \
        --runs runs/kcp4-tuning-v1-log.txt runs/kcp4-tuning-v3-log.txt runs/kcp4-tuning-v7-log.txt \
        --run-labels "v1 (6 ctrl)" "v3 (2 ctrl)" "v7 (tuned)" \
        --milestones "First reproduction" 65 "Minimal reproduction" 600 \
        --title "KCP4: Agent Tuning Timeline" \
        -o kcp4-timeline.png
"""

import argparse
import json
import sys
from datetime import datetime, timezone, timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.patches as mpatches


def parse_iso(ts_str):
    """Parse ISO 8601 timestamp with timezone."""
    # Handle -07:00 style offsets
    return datetime.fromisoformat(ts_str)


def parse_log_with_time(path):
    """Extract (timestamp, total, unique, resources) from log lines."""
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
    parser = argparse.ArgumentParser(description="Plot agent tuning timeline")
    parser.add_argument("--experiment-start", required=True, help="ISO 8601 timestamp of experiment start")
    parser.add_argument("--runs", nargs="+", required=True, help="Log files in chronological order")
    parser.add_argument("--run-labels", nargs="*", help="Labels for each run")
    parser.add_argument("--milestones", nargs="*", help="Pairs of (label, seconds_from_start)")
    parser.add_argument("--title", default="Agent Tuning Timeline")
    parser.add_argument("--label-position", choices=["middle", "end"], default="middle",
                        help="Place labels in middle of triangle or at the end")
    parser.add_argument("--xlim", type=float, default=None, help="X axis max in seconds")
    parser.add_argument("--legend-loc", default="lower right", help="Legend location")
    parser.add_argument("-o", "--output", help="Output file path")
    args = parser.parse_args()

    t0 = parse_iso(args.experiment_start)
    run_labels = args.run_labels or [f"Run {i+1}" for i in range(len(args.runs))]

    # Parse milestones as (label, seconds) pairs
    milestones = []
    if args.milestones:
        for i in range(0, len(args.milestones), 2):
            milestones.append((args.milestones[i], float(args.milestones[i + 1])))

    fig, ax = plt.subplots(figsize=(10, 5))

    colors = plt.cm.tab10.colors
    run_segments = []

    for i, (path, label) in enumerate(zip(args.runs, run_labels)):
        points = parse_log_with_time(path)
        if not points:
            print(f"WARNING: no data in {path}", file=sys.stderr)
            continue

        # Convert to seconds from experiment start
        seconds = [(p["ts"] - t0).total_seconds() for p in points]
        totals = [p["total"] for p in points]
        uniques = [p["unique"] for p in points]
        resources = [p["resources"] for p in points]

        color = colors[i % len(colors)]

        # Plot unique states (S) and resource states (R) for this run
        ax.plot(seconds, uniques, "-", color=color, linewidth=1.5, alpha=0.8)
        ax.plot(seconds, resources, "--", color=color, linewidth=1.5, alpha=0.8)

        # Shade between S and R
        ax.fill_between(seconds, resources, uniques, alpha=0.08, color=color)

        # Mark start of run
        ax.axvline(x=seconds[0], color=color, linewidth=0.5, alpha=0.3, linestyle=":")

        # Label placement
        final = points[-1]
        if args.label_position == "end":
            # If xlim is set, use last point within the limit
            if args.xlim:
                visible = [(s, p) for s, p in zip(seconds, points) if s <= args.xlim]
                if visible:
                    lbl_x = visible[-1][0]
                    final = visible[-1][1]
                else:
                    lbl_x = seconds[-1]
            else:
                lbl_x = seconds[-1]
            lbl_y = final["unique"]
            lbl_ha, lbl_va = "right", "top"
            # Stagger downward to prevent overlapping end labels
            y_nudge = -5 - i * 35
            lbl_offset = (-5, y_nudge)
        else:
            mid_idx = len(seconds) // 2
            lbl_x = seconds[mid_idx]
            lbl_y = (uniques[mid_idx] + resources[mid_idx]) / 2
            lbl_ha, lbl_va = "center", "center"
            lbl_offset = (0, 0)
        ax.annotate(
            f"{label}\n{final['unique']}S / {final['resources']}R",
            xy=(lbl_x, lbl_y),
            xytext=lbl_offset,
            textcoords="offset points",
            fontsize=9,
            ha=lbl_ha,
            va=lbl_va,
            color=color,
            fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.2", facecolor="white", edgecolor=color, alpha=0.85),
        )

        run_segments.append({
            "label": label,
            "start": seconds[0],
            "end": seconds[-1],
            "final_unique": final["unique"],
            "final_resources": final["resources"],
            "final_total": final["total"],
        })

        print(f"{label}: {seconds[0]:.0f}s - {seconds[-1]:.0f}s, {final['total']} total, {final['unique']} unique, {final['resources']} R")

    # Draw milestones
    for label, secs in milestones:
        ax.axvline(x=secs, color="red", linewidth=1.5, linestyle="--", alpha=0.7)
        ax.annotate(
            label,
            xy=(secs, ax.get_ylim()[1] * 0.95),
            fontsize=8,
            ha="right",
            va="top",
            color="red",
            fontweight="bold",
            rotation=0,
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="red", alpha=0.8),
        )

    # Convert X axis to minutes with adaptive tick spacing
    x_max = ax.get_xlim()[1]
    if x_max > 3600:
        tick_interval = 600  # 10 min for >1hr spans
    elif x_max > 1200:
        tick_interval = 300  # 5 min for >20min spans
    else:
        tick_interval = 120  # 2 min for short spans
    ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_interval))
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x / 60:.0f}"))
    if args.xlim:
        ax.set_xlim(right=args.xlim)
    ax.set_xlabel("Time from experiment start (minutes)")
    ax.set_ylabel("Count")
    ax.set_title(args.title)
    ax.grid(alpha=0.2)

    # Custom legend
    legend_elements = [
        plt.Line2D([0], [0], color="gray", linewidth=1.5, label="Unique simulation states (S)"),
        plt.Line2D([0], [0], color="gray", linewidth=1.5, linestyle="--", label="Unique resource states (R)"),
        mpatches.Patch(alpha=0.15, color="gray", label="Q+L gap (S minus R)"),
    ]
    if milestones:
        legend_elements.append(plt.Line2D([0], [0], color="red", linewidth=1.5, linestyle="--", label="Milestone"))
    ax.legend(handles=legend_elements, fontsize=8, loc=args.legend_loc)

    plt.tight_layout()

    if args.output:
        fig.savefig(args.output, dpi=150, bbox_inches="tight")
        print(f"Saved to {args.output}")
    else:
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        fig.savefig(tmp.name, dpi=150, bbox_inches="tight")
        print(f"Saved to {tmp.name}")


if __name__ == "__main__":
    main()
