#!/usr/bin/env python3
"""Extract Figure 8 plotting curves from raw simulator evidence."""

import argparse
import csv
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path


FIELDS = ("ts", "Total States", "# Distinct States", "Resource States")
ORIGIN = datetime(2000, 1, 1, tzinfo=timezone.utc)


def coverage_points(path):
    points = []
    with path.open(errors="replace") as source:
        for line in source:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if all(value.get(field) is not None for field in FIELDS):
                points.append({field: value[field] for field in FIELDS})
    if not points:
        raise ValueError(f"no coverage records found in {path}")
    return points


def accumulate_resets(points):
    accumulated = []
    offsets = {field: 0 for field in FIELDS[1:]}
    previous = None
    for point in points:
        if previous is not None and point["Total States"] < previous["Total States"]:
            for field in FIELDS[1:]:
                offsets[field] += previous[field]
        value = {"ts": point["ts"]}
        for field in FIELDS[1:]:
            value[field] = offsets[field] + point[field]
        accumulated.append(value)
        previous = point
    return accumulated


def load_dump(path):
    with path.open() as source:
        return json.load(source)


def dump_phase(path):
    dump = load_dump(path)
    metrics = dump.get("campaignMetrics", {})
    scenario = dump.get("context", {}).get("scenario", {})
    hashes = set()
    for state in dump.get("states", []):
        for trace in state.get("paths", []):
            for step in trace:
                if step and step.get("contentsHashAfter"):
                    hashes.add(step["contentsHashAfter"])
    return {
        "duration": metrics.get("durationNs", 0) / 1e9,
        "hashes": hashes,
        "total": metrics.get("totalNodeVisits", 0),
        "scenario": scenario.get("name", ""),
        "runIndex": scenario.get("runIndex", -1),
        "phase": scenario.get("attributes", {}).get("phase", ""),
    }


def action_depth(value):
    match = re.search(r"action[-_]depth[-_](\d+)", value)
    return int(match.group(1)) if match else -1


def phase_order(row):
    phase = 0 if row.get("phase") == "reference" else 1
    return (
        action_depth(row.get("scenario", "")),
        row.get("scenario", ""),
        phase,
        row.get("runIndex", -1),
    )


def csv_phases(path):
    phases = []
    with path.open(newline="") as source:
        for row in csv.DictReader(source):
            try:
                duration = int(row.get("duration_ns", 0)) / 1e9
                total = int(row.get("total_states", 0))
            except (TypeError, ValueError):
                continue
            hashes = set(filter(None, row.get("content_hashes", "").split(";")))
            if not hashes and row.get("terminal_hash"):
                hashes.add(row["terminal_hash"])
            phases.append(
                {
                    "duration": duration,
                    "hashes": hashes,
                    "total": total,
                    "scenario": row.get("scenario_name", ""),
                    "phase": row.get("phase_name", ""),
                }
            )
    return phases


def wall_seconds(directory):
    candidates = list(directory.rglob("run-status.json"))
    if not candidates:
        return None
    status = load_dump(candidates[0])
    return status.get("elapsedWallSeconds")


def phase_curve(directory):
    dumps = sorted(directory.rglob("*.jsonl"))
    csv_paths = sorted(directory.rglob("*.csv"))
    phases = []
    skipped_dumps = []
    for path in dumps:
        try:
            phases.append(dump_phase(path))
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
            skipped_dumps.append(str(path.relative_to(directory)))
    phases.sort(key=phase_order)
    valid_dump_count = len(phases)
    rows = []
    for path in csv_paths:
        rows.extend(csv_phases(path))
    rows.sort(key=phase_order)
    phases.extend(rows)
    if not phases:
        raise ValueError(f"no JSONL dumps or CSV metrics found in {directory}")

    total_duration = sum(phase["duration"] for phase in phases)
    observed_wall = wall_seconds(directory)
    scale = observed_wall / total_duration if observed_wall and total_duration else 1.0
    running_time = 0.0
    running_total = 0
    hashes = set()
    points = [
        {
            "ts": ORIGIN.isoformat(),
            "Total States": 1,
            "# Distinct States": 1,
            "Resource States": 1,
        }
    ]
    for phase in phases:
        running_time += phase["duration"] * scale
        running_total += phase["total"]
        hashes.update(phase["hashes"])
        points.append(
            {
                "ts": (ORIGIN + timedelta(seconds=running_time)).isoformat(),
                "Total States": running_total,
                "# Distinct States": len(hashes),
                "Resource States": len(hashes),
            }
        )
    return points, {
        "csvRows": len(rows),
        "dumpCount": valid_dump_count,
        "skippedIncompleteDumps": skipped_dumps,
        "wallSeconds": running_time,
    }


def write_points(path, points):
    with path.open("w") as output:
        for point in points:
            output.write(json.dumps(point, separators=(",", ":")) + "\n")


def endpoint(points):
    start = datetime.fromisoformat(points[0]["ts"])
    end = datetime.fromisoformat(points[-1]["ts"])
    cumulative_total = 0
    previous = None
    for point in points:
        if previous is not None and point["Total States"] < previous["Total States"]:
            cumulative_total += previous["Total States"]
        previous = point
    cumulative_total += points[-1]["Total States"]
    return {
        "durationSeconds": (end - start).total_seconds(),
        "points": len(points),
        "totalStates": points[-1]["Total States"],
        "cumulativeTotalStates": cumulative_total,
        "distinctStates": points[-1]["# Distinct States"],
        "resourceStates": points[-1]["Resource States"],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kcp-exhaustive-log", type=Path, required=True)
    parser.add_argument("--kcp-agent-log", type=Path, required=True)
    parser.add_argument("--kro-exhaustive-dir", type=Path, required=True)
    parser.add_argument("--kro-agent-log", type=Path, required=True)
    parser.add_argument("--kar-exhaustive-dir", type=Path, required=True)
    parser.add_argument("--kar-agent-log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)

    curves = {
        "kcp4-exhaustive": coverage_points(args.kcp_exhaustive_log),
        "kcp4-agent": coverage_points(args.kcp_agent_log),
        "kro2-agent": coverage_points(args.kro_agent_log),
        "kar12-agent": accumulate_resets(coverage_points(args.kar_agent_log)),
    }
    curves["kro2-exhaustive"], kro_meta = phase_curve(args.kro_exhaustive_dir)
    curves["kar12-exhaustive"], kar_meta = phase_curve(args.kar_exhaustive_dir)

    summary = {"format": "kamera-figure8-curves-v2", "curves": {}}
    for name, points in curves.items():
        write_points(args.output / f"{name}.jsonl", points)
        summary["curves"][name] = endpoint(points)
    summary["rawEvidence"] = {"kro2": kro_meta, "kar12": kar_meta}
    with (args.output / "curve-summary.json").open("w") as output:
        json.dump(summary, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
