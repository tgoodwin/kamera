#!/usr/bin/env python3
"""Render the three Figure 8 comparisons as one vertically stacked PDF."""

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from plot_comparison import plot_runs  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--curves", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    plt.rcParams.update({"font.size": 8})
    figure, axes = plt.subplots(3, 1, figsize=(3.33, 4.65))

    plot_runs(
        axes[0],
        [
            args.curves / "kcp4-exhaustive.jsonl",
            args.curves / "kcp4-agent.jsonl",
        ],
        ["Exhaustive", "Agent"],
        offsets=[0, 68],
        milestones=[
            ("Paper: agent reproduces", 133, "green"),
            ("Paper: exhaustive done", 1672, "green"),
        ],
        legend_loc="lower right",
        xlim=1760,
    )

    plot_runs(
        axes[1],
        [
            args.curves / "kro2-exhaustive.jsonl",
            args.curves / "kro2-agent.jsonl",
        ],
        ["Exhaustive", "Agent"],
        offsets=[0, 99],
        milestones=[
            ("Paper: agent reproduces", 99, "green"),
            ("Paper: exhaustive done", 374, "green"),
        ],
        annotations=[("Agent (102S, 279ms exec)", 99, 51, 55)],
        legend_loc="lower right",
        xlim=394,
        x_minutes=True,
    )

    plot_runs(
        axes[2],
        [
            args.curves / "kar12-exhaustive.jsonl",
            args.curves / "kar12-agent.jsonl",
        ],
        ["Exhaustive", "Agent"],
        offsets=[0, 131],
        milestones=[
            ("Paper: agent reproduces", 133, "green"),
            ("Paper: timeout", 7200, "red"),
        ],
        annotations=[("Agent (1481S, 194ms exec)", 133, 1481, 30)],
        legend_loc="lower right",
        xlim=7600,
    )

    captions = (
        "(a) KCP-4 (9 controllers, 6 resources)",
        "(b) KRO-2 (2 controllers, 2 resources)",
        "(c) KAR-12 (14 controllers, 5 resources)",
    )
    for axis, caption in zip(axes, captions):
        axis.text(
            0.5,
            -0.43,
            caption,
            transform=axis.transAxes,
            ha="center",
            va="top",
            fontsize=9,
        )

    figure.subplots_adjust(
        left=0.15,
        right=0.97,
        top=0.98,
        bottom=0.09,
        hspace=1.0,
    )
    figure.savefig(args.output, dpi=600, bbox_inches="tight", pad_inches=0.02)
    print(f"Saved Figure 8 to {args.output}")


if __name__ == "__main__":
    main()
