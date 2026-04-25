#!/usr/bin/env python3
"""Plot agent tuning as a two-lane Gantt chart: inference vs execution.

Shows the alternation between agent thinking time and Kamera execution time,
with overlapping runs visible. Each Kamera run is colored by result (reproduced
vs not) and annotated with the configuration tried.

Usage:
    python scripts/plot_tuning_gantt.py \
        --experiment-start "2026-03-28T08:40:40-07:00" \
        --runs-csv data.csv \
        --title "KCP4: Agent Tuning Timeline" \
        -o output.pdf

CSV format (no header):
    label,start_iso,end_iso,converged_count,description[,background]

If the optional 6th column is "bg", the run is drawn with reduced opacity
as a background/speculative run that didn't inform the agent's decisions.
"""

import argparse
import csv
import sys
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as ticker


def parse_iso(ts_str):
    return datetime.fromisoformat(ts_str)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-start", required=True)
    parser.add_argument("--runs-csv", required=True, help="CSV file with run data")
    parser.add_argument("--title", default="Agent Tuning Timeline")
    parser.add_argument("--xlim", type=float, default=None, help="X axis max in seconds")
    parser.add_argument("--milestones", nargs="*", help="Pairs of (label, seconds)")
    parser.add_argument("-o", "--output")
    args = parser.parse_args()

    t0 = parse_iso(args.experiment_start)

    # Parse runs from CSV
    runs = []
    with open(args.runs_csv) as f:
        for row in csv.reader(f):
            if not row or row[0].startswith("#"):
                continue
            label, start, end, conv, desc = row[0].strip(), row[1].strip(), row[2].strip(), int(row[3].strip()), row[4].strip()
            bg = len(row) > 5 and row[5].strip() == "bg"
            s = (parse_iso(start) - t0).total_seconds()
            e = (parse_iso(end) - t0).total_seconds()
            runs.append({"label": label, "start": s, "end": e, "converged": conv, "desc": desc, "bg": bg})

    milestones = []
    if args.milestones:
        for i in range(0, len(args.milestones), 2):
            milestones.append((args.milestones[i], float(args.milestones[i + 1])))

    sorted_runs = sorted(runs, key=lambda r: r["start"])

    # Compute thinking segments: gaps between consecutive run launches where
    # the agent is reading results, analyzing, and designing the next config.
    # We define "thinking" as: from the end of a run to the start of the next
    # run that was launched AFTER this one ended. Only show gaps > min_gap.
    min_gap = 3  # seconds — show all non-trivial gaps
    think_segments = []

    # From experiment start to first run
    if sorted_runs[0]["start"] > min_gap:
        think_segments.append((0, sorted_runs[0]["start"]))

    # Find gaps where no run is executing
    events = []
    for r in sorted_runs:
        events.append((r["start"], "start"))
        events.append((r["end"], "end"))
    events.sort()

    active = 0
    gap_start = None
    for time, typ in events:
        if typ == "start":
            if active == 0 and gap_start is not None and time > gap_start + min_gap:
                think_segments.append((gap_start, time))
            active += 1
        else:
            active -= 1
            if active == 0:
                gap_start = time

    # --- Plot ---
    fig, ax = plt.subplots(figsize=(5.5, 1.8))
    plt.rcParams.update({"font.size": 7})

    ok_color = "#2ca02c"
    fail_color = "#d62728"
    think_color = "#4e79a7"
    inconclusive_color = "#ff7f0e"

    bar_height = 0.4
    exec_y = 0.0
    think_y = 0.65

    # Draw thinking segments
    for s, e in think_segments:
        ax.barh(think_y, e - s, left=s, height=bar_height, color=think_color,
                alpha=0.5, edgecolor=think_color, linewidth=0.5, zorder=1)

    # Draw execution segments (stack overlapping runs vertically)
    # Assign lanes to avoid overlap
    lanes = []  # list of (end_time, lane_index)
    run_lanes = []
    for r in sorted_runs:
        # Find first available lane
        placed = False
        for i, (lane_end, _) in enumerate(lanes):
            if r["start"] >= lane_end:
                lanes[i] = (r["end"], i)
                run_lanes.append(i)
                placed = True
                break
        if not placed:
            run_lanes.append(len(lanes))
            lanes.append((r["end"], len(lanes)))

    max_lanes = max(run_lanes) + 1 if run_lanes else 1
    lane_height = bar_height / max_lanes

    for r, lane in zip(sorted_runs, run_lanes):
        if r["converged"] >= 2:
            color = ok_color
        elif r["converged"] == 1:
            color = inconclusive_color
        else:
            color = fail_color

        alpha = 0.2 if r.get("bg") else 0.7
        y = exec_y + lane * lane_height
        # Enforce minimum visual width so short runs are visible
        duration = r["end"] - r["start"]
        x_range = args.xlim or max(rr["end"] for rr in runs) * 1.05
        min_width = x_range * 0.025  # at least 2.5% of chart width
        bar_width = max(duration, min_width)
        ax.barh(y, bar_width, left=r["start"], height=lane_height * 0.9,
                color=color, alpha=alpha, edgecolor=color, linewidth=0.5,
                linestyle=":" if r.get("bg") else "-", zorder=2)

        # Label runs: long runs get text inside, short runs get text above
        duration = r["end"] - r["start"]
        mid = r["start"] + duration / 2
        if duration > 100:
            ax.text(mid, y + lane_height * 0.45, f'{r["label"]}: {r["desc"]}',
                    ha="center", va="center", fontsize=5, color="white", fontweight="bold")
        elif duration > 30:
            ax.text(mid, y + lane_height * 0.45, r["label"],
                    ha="center", va="center", fontsize=4.5, color="white", fontweight="bold")

    # Milestones
    for mi, (label, secs) in enumerate(milestones):
        ax.axvline(x=secs, color="#333333", linewidth=0.8, linestyle="--", alpha=0.6)
        # Alternate label position to avoid overlap
        va = "bottom"
        y_pos = think_y + bar_height + 0.08 + mi * 0.18
        ax.annotate(label, xy=(secs, y_pos),
                    fontsize=5.5, ha="center", va=va, color="#333333",
                    fontweight="bold",
                    bbox=dict(boxstyle="round,pad=0.15", facecolor="white",
                              edgecolor="#333333", alpha=0.85, linewidth=0.5))

    # X axis: time in minutes
    x_max = args.xlim or max(r["end"] for r in runs) * 1.05
    ax.set_xlim(0, x_max)
    if x_max > 3600:
        tick_interval = 600
    elif x_max > 1200:
        tick_interval = 300
    else:
        tick_interval = 120
    ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_interval))
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x / 60:.0f}"))

    # Y axis labels
    ax.set_yticks([exec_y + bar_height / 2, think_y + bar_height / 2])
    ax.set_yticklabels(["Kamera", "Agent"], fontsize=7)
    n_milestones = len(milestones) if milestones else 0
    ax.set_ylim(-0.15, think_y + bar_height + 0.15 + n_milestones * 0.2)

    ax.set_xlabel("Time (minutes)", fontsize=7)
    ax.set_title(args.title, fontsize=8)
    ax.tick_params(labelsize=6)
    ax.grid(axis="x", alpha=0.2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Legend
    legend_elements = [
        mpatches.Patch(color=think_color, alpha=0.5, label="Agent inference"),
        mpatches.Patch(color=ok_color, alpha=0.7, label="Reproduced"),
        mpatches.Patch(color=fail_color, alpha=0.7, label="Not reproduced"),
    ]
    ax.legend(handles=legend_elements, fontsize=5, loc="lower right",
              framealpha=0.85, edgecolor="gray")

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
