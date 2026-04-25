#!/usr/bin/env python3
"""Plot coverage curves against wall-clock time from simulator logs.

Usage:
    python scripts/plot_coverage_time.py experiments/coverage-curves/kcp/kcp-k17-log.txt
    python scripts/plot_coverage_time.py experiments/coverage-curves/kcp/kcp-k17-log.txt -o kcp17_coverage.pdf
"""

import argparse
import json
import sys
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def parse_log(path):
    """Extract coverage series with timestamps from simulator JSON log lines."""
    points = []
    t0 = None
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue

        ts_str = obj.get("ts")
        total = obj.get("Total States")
        unique = obj.get("# Distinct States")
        resources = obj.get("Resource States")

        if ts_str is None or total is None or unique is None or resources is None:
            continue

        ts = datetime.fromisoformat(ts_str)
        if t0 is None:
            t0 = ts

        elapsed = (ts - t0).total_seconds()
        points.append({
            "elapsed_s": elapsed,
            "total": total,
            "unique": unique,
            "resources": resources,
        })

    return points


def main():
    parser = argparse.ArgumentParser(description="Plot coverage curves against wall-clock time")
    parser.add_argument("log", help="Log file path")
    parser.add_argument("-o", "--output", help="Output file path (pdf/png)")
    parser.add_argument("--title", default=None, help="Plot title")
    args = parser.parse_args()

    series = parse_log(args.log)
    if not series:
        print(f"ERROR: no coverage data found in {args.log}", file=sys.stderr)
        sys.exit(1)

    final = series[-1]
    duration_m = final["elapsed_s"] / 60.0
    print(f"Duration: {duration_m:.1f} min, "
          f"{final['total']} total, {final['unique']} unique states, "
          f"{final['resources']} resource states")

    t = [p["elapsed_s"] / 60.0 for p in series]  # minutes
    total = [p["total"] for p in series]
    unique = [p["unique"] for p in series]
    resources = [p["resources"] for p in series]

    fig, ax = plt.subplots(figsize=(8, 5))

    ax.plot(t, total, ":", color="#999999", linewidth=1, label="Total states (S)")
    ax.plot(t, unique, "-", linewidth=1.5, label="Unique states (S)")
    ax.plot(t, resources, "-", linewidth=1.5, label="Unique resource states (R)")

    ax.fill_between(t, unique, total, alpha=0.08, color="blue", label="Pruning gap")
    ax.fill_between(t, resources, unique, alpha=0.08, color="orange", label="Q+L gap")

    ax.set_xlabel("Time (minutes)")
    ax.set_ylabel("Count")
    ax.set_title(args.title or f"KCP17 State Space Coverage")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.2)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v):,}"))

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
