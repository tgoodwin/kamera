#!/usr/bin/env python3
"""Package compact, plot-equivalent JSONL inputs for Figure 8.

The historical simulator logs contain many diagnostic lines that the plotter
ignores. This tool retains only timestamped coverage samples and evenly reduces
very long curves while preserving their first and last samples.
"""

import argparse
import hashlib
import json
from pathlib import Path


FIELDS = ("ts", "Total States", "# Distinct States", "Resource States")


def read_points(path):
    points = []
    with path.open() as source:
        for line in source:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if all(value.get(field) is not None for field in FIELDS):
                points.append({field: value[field] for field in FIELDS})
    if not points:
        raise ValueError(f"no coverage samples found in {path}")
    return points


def select_evenly(points, limit):
    if len(points) <= limit:
        return points
    indexes = {
        round(position * (len(points) - 1) / (limit - 1))
        for position in range(limit)
    }
    return [points[index] for index in sorted(indexes)]


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-points", type=int, default=600)
    parser.add_argument(
        "inputs",
        nargs="+",
        metavar="NAME=PATH",
        help="named source logs, for example kcp-exhaustive=/path/log.txt",
    )
    args = parser.parse_args()
    if args.max_points < 2:
        parser.error("--max-points must be at least 2")

    args.output.mkdir(parents=True, exist_ok=True)
    manifest = {"format": "kamera-figure8-coverage-v1", "inputs": {}}
    for assignment in args.inputs:
        if "=" not in assignment:
            parser.error(f"invalid input {assignment!r}; expected NAME=PATH")
        name, raw_path = assignment.split("=", 1)
        path = Path(raw_path)
        points = read_points(path)
        packaged = select_evenly(points, args.max_points)
        destination = args.output / f"{name}.jsonl"
        with destination.open("w") as output:
            for point in packaged:
                output.write(json.dumps(point, separators=(",", ":")) + "\n")
        manifest["inputs"][name] = {
            "sourceFile": path.name,
            "sourceSha256": sha256(path),
            "packagedSha256": sha256(destination),
            "sourcePoints": len(points),
            "packagedPoints": len(packaged),
            "first": packaged[0],
            "last": packaged[-1],
        }

    with (args.output / "manifest.json").open("w") as output:
        json.dump(manifest, output, indent=2)
        output.write("\n")


if __name__ == "__main__":
    main()
