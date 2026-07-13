#!/usr/bin/env python3
"""Summarize and validate the reconstructed KRO-2 exhaustive campaign."""

import argparse
import csv
import json
import sys
from pathlib import Path


EXPECTED = {
    "fullDumps": 30,
    "stalenessTrials": 1680,
    "totalNodeVisits": 54418,
    "globalResourceStates": 131,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    dumps = sorted(args.directory.glob("*.jsonl"))
    csv_paths = sorted(args.directory.glob("*.csv"))
    total_visits = 0
    duration_ns = 0
    hashes = set()

    for path in dumps:
        with path.open() as source:
            dump = json.load(source)
        metrics = dump.get("campaignMetrics", {})
        total_visits += metrics.get("totalNodeVisits", 0)
        duration_ns += metrics.get("durationNs", 0)
        for state in dump.get("states", []):
            for trace in state.get("paths", []):
                for step in trace:
                    if step and step.get("contentsHashAfter"):
                        hashes.add(step["contentsHashAfter"])

    staleness_trials = 0
    for path in csv_paths:
        with path.open(newline="") as source:
            for row in csv.DictReader(source):
                staleness_trials += 1
                total_visits += int(row.get("total_states", 0))
                duration_ns += int(row.get("duration_ns", 0))
                hashes.update(filter(None, row.get("content_hashes", "").split(";")))

    result = {
        "status": "MATCH",
        "fullDumps": len(dumps),
        "stalenessTrials": staleness_trials,
        "totalNodeVisits": total_visits,
        "globalResourceStates": len(hashes),
        "summedSimulationDurationNs": duration_ns,
        "expected": EXPECTED,
    }
    mismatches = {
        key: {"observed": result[key], "expected": expected}
        for key, expected in EXPECTED.items()
        if result[key] != expected
    }
    if mismatches:
        result["status"] = "MISMATCH"
        result["mismatches"] = mismatches

    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(
            result["status"],
            result["fullDumps"],
            result["stalenessTrials"],
            result["totalNodeVisits"],
            result["globalResourceStates"],
            sep="\t",
        )
    return 0 if result["status"] == "MATCH" else 1


if __name__ == "__main__":
    sys.exit(main())
