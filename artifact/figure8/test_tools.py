#!/usr/bin/env python3
"""Unit tests for the Figure 8 evidence-processing tools."""

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from create_raw_archives import archive_directory
from extract_curves import accumulate_resets, endpoint, phase_curve
from verify_raw_archives import main as verify_raw_archives
from write_report import experiment_name, within_fraction


class ExtractCurvesTest(unittest.TestCase):
    def test_accumulate_resets(self):
        points = [
            {"ts": "2000-01-01T00:00:00+00:00", "Total States": 2, "# Distinct States": 2, "Resource States": 1},
            {"ts": "2000-01-01T00:00:01+00:00", "Total States": 3, "# Distinct States": 3, "Resource States": 2},
            {"ts": "2000-01-01T00:00:02+00:00", "Total States": 1, "# Distinct States": 1, "Resource States": 1},
        ]
        self.assertEqual(endpoint(points)["cumulativeTotalStates"], 4)
        result = accumulate_resets(points)
        self.assertEqual(result[-1]["Total States"], 4)
        self.assertEqual(result[-1]["# Distinct States"], 4)
        self.assertEqual(result[-1]["Resource States"], 3)

    def test_phase_curve_uses_wall_time_and_skips_incomplete_dump(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dump = {
                "campaignMetrics": {"durationNs": 1_000_000_000, "totalNodeVisits": 3},
                "context": {"scenario": {"name": "case", "runIndex": 0, "attributes": {"phase": "reference"}}},
                "states": [{"paths": [[{"contentsHashAfter": "hash-a"}]]}],
            }
            (root / "complete.jsonl").write_text(json.dumps(dump))
            (root / "incomplete.jsonl").write_text("{")
            with (root / "staleness.csv").open("w", newline="") as output:
                writer = csv.DictWriter(
                    output,
                    fieldnames=("scenario_name", "phase_name", "duration_ns", "total_states", "content_hashes"),
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "scenario_name": "case",
                        "phase_name": "staleness_interval_0",
                        "duration_ns": "1000000000",
                        "total_states": "2",
                        "content_hashes": "hash-b",
                    }
                )
                writer.writerow({"duration_ns": "partial", "total_states": ""})
            (root / "run-status.json").write_text(
                json.dumps({"elapsedWallSeconds": 10})
            )

            points, metadata = phase_curve(root)
            final = endpoint(points)
            self.assertEqual(final["totalStates"], 5)
            self.assertEqual(final["resourceStates"], 2)
            self.assertEqual(final["durationSeconds"], 10)
            self.assertEqual(metadata["dumpCount"], 1)
            self.assertEqual(metadata["csvRows"], 1)
            self.assertEqual(metadata["skippedIncompleteDumps"], ["incomplete.jsonl"])


class WriteReportTest(unittest.TestCase):
    def test_normalizes_kro_case_name(self):
        self.assertEqual(experiment_name({"case": "KRO-2 paper snapshot"}), "KRO-2")
        self.assertEqual(experiment_name({"experiment": "KCP-4"}), "KCP-4")

    def test_endpoint_tolerance_is_inclusive(self):
        self.assertTrue(within_fraction(80, 100, 0.20))
        self.assertTrue(within_fraction(120, 100, 0.20))
        self.assertFalse(within_fraction(79, 100, 0.20))


class RawArchiveVerificationTest(unittest.TestCase):
    def test_failure_in_first_archive_is_not_masked_by_later_passes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = {"archives": {}}
            for name in ("kcp4", "kro2", "kar12"):
                source = root / f"{name}-source"
                source.mkdir()
                (source / "evidence.txt").write_text(name)
                manifest["archives"][name] = archive_directory(
                    name, source, root / f"{name}.tar.gz"
                )
            (root / "manifest.json").write_text(json.dumps(manifest))
            with (root / "kcp4.tar.gz").open("ab") as output:
                output.write(b"corrupt")

            with patch.object(sys, "argv", ["verify_raw_archives.py", str(root)]):
                self.assertEqual(verify_raw_archives(), 1)


if __name__ == "__main__":
    unittest.main()
