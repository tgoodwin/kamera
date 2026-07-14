#!/usr/bin/env python3
"""Write evaluator-facing Figure 8 numerical comparisons."""

import argparse
import csv
import json
from pathlib import Path


PAPER = {
    "KCP-4": {
        "exhaustive": {"totalStates": 9216, "resourceStates": 254, "durationSeconds": 1672},
        "agent": {"totalStates": 584, "resourceStates": 72, "executionSeconds": 57, "inferenceOffsetSeconds": 68, "milestoneSeconds": 133},
    },
    "KRO-2": {
        "exhaustive": {"totalStates": 54418, "resourceStates": 131, "durationSeconds": 374},
        "agent": {"totalStates": 102, "resourceStates": 7, "executionSeconds": 0.279, "inferenceOffsetSeconds": 99, "milestoneSeconds": 99},
    },
    "KAR-12": {
        "exhaustive": {"totalStates": 34211240, "resourceStates": 794, "durationSeconds": 7200},
        "agent": {"totalStates": 1481, "resourceStates": 203, "executionSeconds": 0.194, "inferenceOffsetSeconds": 131, "milestoneSeconds": 133},
    },
}

CURVES = {
    "KCP-4": ("kcp4-exhaustive", "kcp4-agent"),
    "KRO-2": ("kro2-exhaustive", "kro2-agent"),
    "KAR-12": ("kar12-exhaustive", "kar12-agent"),
}

AGENT_ENDPOINT_TOLERANCE = 0.20


def load(path):
    with path.open() as source:
        return json.load(source)


def experiment_name(item):
    name = item.get("experiment") or item.get("case", "")
    if name.startswith("KRO-2"):
        return "KRO-2"
    return name


def within_fraction(observed, expected, tolerance):
    return abs(observed - expected) <= expected * tolerance


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--curves", type=Path, required=True)
    parser.add_argument("--simulations", type=Path, required=True)
    parser.add_argument("--exhaustive-source", choices=("archived", "fresh"), required=True)
    parser.add_argument("--markdown", type=Path, required=True)
    parser.add_argument("--tsv", type=Path, required=True)
    args = parser.parse_args()

    curves = load(args.curves)["curves"]
    simulations = load(args.simulations)["results"]
    simulations = {experiment_name(item): item for item in simulations}

    rows = []
    for experiment, (exhaustive_name, agent_name) in CURVES.items():
        observed_exhaustive = curves[exhaustive_name]
        observed_agent = curves[agent_name]
        paper = PAPER[experiment]
        simulation = simulations.get(experiment, {})
        observed_agent_s = observed_agent.get(
            "cumulativeTotalStates", observed_agent["totalStates"]
        )
        agent_s_near_paper = within_fraction(
            observed_agent_s,
            paper["agent"]["totalStates"],
            AGENT_ENDPOINT_TOLERANCE,
        )
        agent_r_near_paper = within_fraction(
            observed_agent["resourceStates"],
            paper["agent"]["resourceStates"],
            AGENT_ENDPOINT_TOLERANCE,
        )
        consistent = (
            simulation.get("status") == "OBSERVED"
            and observed_exhaustive["totalStates"] > observed_agent_s
            and agent_s_near_paper
            and agent_r_near_paper
        )
        rows.append(
            {
                "experiment": experiment,
                "source": args.exhaustive_source,
                "paper_exhaustive_s": paper["exhaustive"]["totalStates"],
                "observed_exhaustive_s": observed_exhaustive["totalStates"],
                "paper_exhaustive_r": paper["exhaustive"]["resourceStates"],
                "observed_exhaustive_r": observed_exhaustive["resourceStates"],
                "paper_exhaustive_seconds": paper["exhaustive"]["durationSeconds"],
                "observed_exhaustive_seconds": observed_exhaustive["durationSeconds"],
                "paper_agent_s": paper["agent"]["totalStates"],
                "observed_agent_s": observed_agent_s,
                "paper_agent_r": paper["agent"]["resourceStates"],
                "observed_agent_r": observed_agent["resourceStates"],
                "agent_s_within_20pct": agent_s_near_paper,
                "agent_r_within_20pct": agent_r_near_paper,
                "paper_agent_execution_seconds": paper["agent"]["executionSeconds"],
                "observed_agent_execution_seconds": simulation.get("simulationSeconds"),
                "paper_inference_offset_seconds": paper["agent"]["inferenceOffsetSeconds"],
                "paper_agent_milestone_seconds": paper["agent"]["milestoneSeconds"],
                "simulation_outcome": simulation.get("status", "UNKNOWN"),
                "comparison": "CONSISTENT" if consistent else "CHECK",
            }
        )

    args.markdown.parent.mkdir(parents=True, exist_ok=True)
    with args.tsv.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    with args.markdown.open("w") as output:
        output.write("# Figure 8 reproduction report\n\n")
        output.write(f"Exhaustive evidence source: **{args.exhaustive_source} raw simulator output**.\n\n")
        output.write(
            "The agent-side numbers come from fresh executions of the fixed first-iteration "
            "configurations selected during the paper experiments. Recorded model-inference "
            "offsets are used only when drawing the time axis. Exact durations vary by host.\n\n"
        )
        output.write("| Experiment | Exhaustive S paper / observed | Exhaustive R paper / observed | Agent S paper / observed | Agent R paper / observed | Outcome | Comparison |\n")
        output.write("|---|---:|---:|---:|---:|---|---|\n")
        for row in rows:
            output.write(
                f"| {row['experiment']} | {row['paper_exhaustive_s']:,} / {row['observed_exhaustive_s']:,} "
                f"| {row['paper_exhaustive_r']:,} / {row['observed_exhaustive_r']:,} "
                f"| {row['paper_agent_s']:,} / {row['observed_agent_s']:,} "
                f"| {row['paper_agent_r']:,} / {row['observed_agent_r']:,} "
                f"| {row['simulation_outcome']} | {row['comparison']} |\n"
            )
        output.write(
            "\n`CONSISTENT` means the local simulation observed the configured outcome and "
            "the exhaustive campaign visited more states than the agent-selected configuration, "
            "while the fresh agent S and R endpoints were each within 20% of the paper values. "
            "It is not an assertion that wall-clock timing or Monte Carlo samples are byte-identical.\n"
        )
        output.write(
            "\nKRO-2's agent `S` value is the cumulative total across its 51-state "
            "reference and 51-state perturbed phases. The plotted raw curve preserves the "
            "phase-boundary counter reset, matching the paper panel.\n"
        )
        output.write("\n## Timing context\n\n")
        output.write("| Experiment | Exhaustive curve span: paper / observed | Agent simulator execution: paper / observed | Recorded inference offset | Paper outcome milestone |\n")
        output.write("|---|---:|---:|---:|---:|\n")
        for row in rows:
            output.write(
                f"| {row['experiment']} | {row['paper_exhaustive_seconds']:.0f}s / "
                f"{row['observed_exhaustive_seconds']:.1f}s | "
                f"{row['paper_agent_execution_seconds']:.3f}s / "
                f"{row['observed_agent_execution_seconds']:.3f}s | "
                f"{row['paper_inference_offset_seconds']:.0f}s | "
                f"{row['paper_agent_milestone_seconds']:.0f}s |\n"
            )
        output.write(
            "\nThe observed exhaustive span and simulator durations are read from raw "
            "`campaignMetrics` used for this run. The paper outcome milestone includes the "
            "recorded agent-search interval; the local scripts do not rerun model inference.\n"
        )

    print(args.markdown)


if __name__ == "__main__":
    main()
