#!/usr/bin/env python3
"""Plot agent tuning as a hybrid timeline: state exploration over wall-clock time.

Combines the Gantt-style temporal structure with state-count Y axis. Each Kamera
run appears as a rising curve (states explored), with flat gaps between runs
representing agent thinking time. Runs are colored by result.

The agent inference lane is shown as a shaded band above the main plot.

Usage:
    python scripts/plot_tuning_hybrid.py \
        --experiment-start "2026-03-28T09:50:01-07:00" \
        --runs log1.txt log2.txt ... \
        --labels v1 v2 ... \
        --results 2 2 2 2 0 0 2 0 2 1 \
        --title "KCP4: Agent Diagnosis Timeline" \
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


def parse_log(path):
    """Extract (timestamp, unique_states, resource_states) from log lines."""
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
        unique = obj.get("# Distinct States")
        resources = obj.get("Resource States")
        if ts and unique is not None and resources is not None:
            points.append({
                "ts": parse_iso(ts),
                "unique": unique,
                "resources": resources,
            })
    return points


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-start", required=True)
    parser.add_argument("--runs", nargs="+", required=True, help="Log files in order")
    parser.add_argument("--labels", nargs="+", required=True)
    parser.add_argument("--results", nargs="+", type=int, required=True,
                        help="Converged count per run (2=reproduced, 0=failed, 1=inconclusive)")
    parser.add_argument("--milestones", nargs="*", help="Pairs of (label, seconds)")
    parser.add_argument("--title", default="Agent Diagnosis Timeline")
    parser.add_argument("--xlim", type=float, default=None)
    parser.add_argument("--log-y", action="store_true", help="Use log scale on Y axis")
    parser.add_argument("-o", "--output")
    args = parser.parse_args()

    t0 = parse_iso(args.experiment_start)

    ok_color = "#2ca02c"
    fail_color = "#d62728"
    inconclusive_color = "#ff7f0e"
    think_color = "#4e79a7"

    milestones = []
    if args.milestones:
        for i in range(0, len(args.milestones), 2):
            milestones.append((args.milestones[i], float(args.milestones[i + 1])))

    fig, ax = plt.subplots(figsize=(3.5, 1.6))
    plt.rcParams.update({"font.size": 8})

    all_run_data = []
    for path, label, conv in zip(args.runs, args.labels, args.results):
        points = parse_log(path)
        if not points:
            print(f"WARNING: no data in {path}", file=sys.stderr)
            continue
        seconds = [(p["ts"] - t0).total_seconds() for p in points]
        uniques = [p["unique"] for p in points]
        resources = [p["resources"] for p in points]
        all_run_data.append({
            "label": label, "conv": conv,
            "seconds": seconds, "uniques": uniques, "resources": resources,
            "start": seconds[0], "end": seconds[-1],
        })

    # Draw thinking gaps as vertical shaded bands
    for i in range(len(all_run_data)):
        if i == 0 and all_run_data[0]["start"] > 5:
            # Initial thinking period
            ax.axvspan(0, all_run_data[0]["start"], color=think_color, alpha=0.08)
        if i < len(all_run_data) - 1:
            gap_start = all_run_data[i]["end"]
            gap_end = all_run_data[i + 1]["start"]
            if gap_end - gap_start > 3:
                ax.axvspan(gap_start, gap_end, color=think_color, alpha=0.08)

    # Draw each run as a curve
    for rd in all_run_data:
        if rd["conv"] >= 2:
            color = ok_color
        elif rd["conv"] == 1:
            color = inconclusive_color
        else:
            color = fail_color

        ax.plot(rd["seconds"], rd["uniques"], "-", color=color, linewidth=1.2, alpha=0.8)

        # Run labels omitted for cleaner presentation

    # Milestones — place labels at top
    for mi, (label, secs) in enumerate(milestones):
        ax.axvline(x=secs, color="#333333", linewidth=0.8, linestyle="--", alpha=0.5)
        ax.annotate(label, xy=(secs, ax.get_ylim()[1]),
                    xytext=(0, -3), textcoords="offset points",
                    fontsize=6, ha="center", va="top", color="#333333",
                    fontweight="bold",
                    bbox=dict(boxstyle="round,pad=0.15", facecolor="white",
                              edgecolor="#333333", alpha=0.85, linewidth=0.5))

    # X axis
    x_max = args.xlim or max(rd["end"] for rd in all_run_data) * 1.08
    ax.set_xlim(0, x_max)
    if x_max > 3600:
        tick_interval = 600
    elif x_max > 1200:
        tick_interval = 300
    elif x_max > 600:
        tick_interval = 120
    else:
        tick_interval = 60
    ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_interval))
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x / 60:.0f}"))

    ax.set_xlabel("Time (minutes)", fontsize=8)
    ax.set_ylabel("States explored", fontsize=8)
    ax.set_title(args.title, fontsize=9)
    ax.tick_params(labelsize=7)
    ax.grid(alpha=0.15)
    if args.log_y:
        ax.set_yscale("log")
        ax.set_ylim(bottom=1)
    else:
        ax.set_ylim(bottom=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Legend
    legend_elements = [
        plt.Line2D([0], [0], color=ok_color, linewidth=1.2, label="Reproduced"),
        plt.Line2D([0], [0], color=fail_color, linewidth=1.2, label="Not reproduced"),
        mpatches.Patch(color=think_color, alpha=0.15, label="Agent inference"),
    ]
    ax.legend(handles=legend_elements, fontsize=5.5, loc="center left",
              framealpha=0.85, edgecolor="gray")

    plt.tight_layout(pad=0.3)
    if args.output:
        fig.savefig(args.output, dpi=300, bbox_inches="tight", pad_inches=0)
        print(f"Saved to {args.output}")
    else:
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        fig.savefig(tmp.name, dpi=150, bbox_inches="tight", pad_inches=0)
        print(f"Saved to {tmp.name}")


if __name__ == "__main__":
    main()
