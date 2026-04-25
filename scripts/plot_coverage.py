#!/usr/bin/env python3
"""Parse simulator log output and plot coverage curves.

Usage:
    # Single log file
    python scripts/plot_coverage.py /tmp/kro-k1-log.txt

    # Multiple log files (overlaid)
    python scripts/plot_coverage.py /tmp/kro-k1.txt /tmp/kro-k2.txt --labels k1 k2

    # Save to file instead of showing
    python scripts/plot_coverage.py /tmp/kro-k1-log.txt -o coverage.pdf
"""

import argparse
import json
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def parse_log(path):
    """Extract coverage series from simulator JSON log lines.

    Returns list of dicts with keys: total, unique, resources.
    We deduplicate by total (only keep the last reading for each total value).
    """
    points = {}
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue

        total = obj.get("Total States")
        unique = obj.get("# Distinct States")
        resources = obj.get("Resource States")

        if total is not None and unique is not None and resources is not None:
            points[total] = {
                "total": total,
                "unique": unique,
                "resources": resources,
            }

    # Sort by total step count
    return [points[k] for k in sorted(points.keys())]


def plot_single(ax, series, label=None):
    """Plot coverage curves for a single run on the given axes."""
    steps = [p["total"] for p in series]
    unique = [p["unique"] for p in series]
    resources = [p["resources"] for p in series]

    prefix = f"{label}: " if label else ""

    ax.plot(steps, steps, ":", color="#999999", linewidth=1, label=f"{prefix}Total states (identity)")
    ax.plot(steps, unique, "-", linewidth=1.5, label=f"{prefix}Unique states (S)")
    ax.plot(steps, resources, "-", linewidth=1.5, label=f"{prefix}Unique resource states (R)")

    # Shade the gaps
    ax.fill_between(steps, unique, steps, alpha=0.08, color="blue", label=f"{prefix}Pruning gap")
    ax.fill_between(steps, resources, unique, alpha=0.08, color="orange", label=f"{prefix}Q+L gap")


def main():
    parser = argparse.ArgumentParser(description="Plot simulator coverage curves from log output")
    parser.add_argument("logs", nargs="+", help="Log file paths")
    parser.add_argument("--labels", nargs="*", help="Labels for each log file")
    parser.add_argument("-o", "--output", help="Output file path (pdf/png). If omitted, shows plot.")
    parser.add_argument("--title", default="State Space Coverage", help="Plot title")
    args = parser.parse_args()

    labels = args.labels or [None] * len(args.logs)
    if len(labels) < len(args.logs):
        labels.extend([None] * (len(args.logs) - len(labels)))

    fig, ax = plt.subplots(figsize=(8, 5))

    for path, label in zip(args.logs, labels):
        series = parse_log(path)
        if not series:
            print(f"WARNING: no coverage data found in {path}", file=sys.stderr)
            continue
        plot_single(ax, series, label=label)
        final = series[-1]
        print(f"{label or path}: {final['total']} total, {final['unique']} unique states, {final['resources']} unique resource states")

    ax.set_xlabel("Simulation step")
    ax.set_ylabel("Count")
    ax.set_title(args.title)
    ax.legend(fontsize=8)
    ax.grid(alpha=0.2)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v):,}"))

    plt.tight_layout()

    if args.output:
        fig.savefig(args.output, dpi=150, bbox_inches="tight")
        print(f"Saved to {args.output}")
    else:
        # Save to a temp file and print path
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        fig.savefig(tmp.name, dpi=150, bbox_inches="tight")
        print(f"Saved to {tmp.name}")


if __name__ == "__main__":
    main()
