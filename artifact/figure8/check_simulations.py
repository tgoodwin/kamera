#!/usr/bin/env python3
"""Validate the locally rerun Figure 8 agent-side simulations."""

import argparse
import json
import sys
from pathlib import Path


def load(path):
    with path.open() as source:
        return json.load(source)


def kcp_result(path):
    dump = load(path)
    metrics = dump.get("campaignMetrics", {})
    states = dump.get("states", [])
    ids = [state.get("id", "") for state in states]
    objects = {
        item["hash"]["Value"]: item["object"] for item in dump.get("objects", [])
    }
    endpoint_counts = []
    for state in states:
        if state.get("error"):
            continue
        for obj in resolved_terminal_objects(state, objects):
            metadata = obj.get("metadata", {})
            if (
                obj.get("kind") == "APIExportEndpointSlice"
                and metadata.get("namespace") == "root:provider"
                and metadata.get("name") == "widgets"
            ):
                endpoint_counts.append(len(obj.get("status", {}).get("endpoints", [])))
    observed = (
        metrics.get("totalNodeVisits") == 586
        and metrics.get("uniqueNodeVisits") == 579
        and metrics.get("uniqueResourceStates") == 72
        and len(states) == 4
        and sum(value.startswith("aborted-") for value in ids) == 2
        and 0 in endpoint_counts
        and any(count > 0 for count in endpoint_counts)
    )
    return {
        "experiment": "KCP-4",
        "status": "OBSERVED" if observed else "NOT_OBSERVED",
        "simulationSeconds": metrics.get("durationNs", 0) / 1e9,
        "totalNodeVisits": metrics.get("totalNodeVisits"),
        "uniqueNodeVisits": metrics.get("uniqueNodeVisits"),
        "uniqueResourceStates": metrics.get("uniqueResourceStates"),
        "terminalStates": len(states),
        "maxDepthStates": sum(value.startswith("aborted-") for value in ids),
        "endpointCounts": sorted(endpoint_counts),
        "observable": "converged executions include both an empty and a populated APIExportEndpointSlice",
    }


def resolved_terminal_objects(state, objects):
    resolved = []
    for ref in state.get("state", {}).get("contents", {}).get("objects", []):
        obj = objects.get(ref.get("hash", {}).get("Value"))
        if obj is not None:
            resolved.append(obj)
    return resolved


def object_key(obj):
    metadata = obj.get("metadata", {})
    return obj.get("kind"), metadata.get("namespace", ""), metadata.get("name", "")


def kar_result(directory):
    paths = sorted(directory.glob("*.jsonl"))
    converged = 0
    bounded = 0
    outcome_trials = []
    retained_trials = []
    total_visits = 0
    unique_visits = 0
    resource_state_sum = 0
    duration_ns = 0
    for path in paths:
        dump = load(path)
        metrics = dump.get("campaignMetrics", {})
        objects = {
            item["hash"]["Value"]: item["object"] for item in dump.get("objects", [])
        }
        total_visits += metrics.get("totalNodeVisits", 0)
        unique_visits += metrics.get("uniqueNodeVisits", 0)
        resource_state_sum += metrics.get("uniqueResourceStates", 0)
        duration_ns += metrics.get("durationNs", 0)
        for state in dump.get("states", []):
            state_id = state.get("id", "")
            if state_id.startswith("aborted-"):
                bounded += 1
                continue
            converged += 1
            final = resolved_terminal_objects(state, objects)
            pods = [obj for obj in final if object_key(obj) == ("Pod", "default", "workload-pod")]
            bound = bool(pods and pods[0].get("spec", {}).get("nodeName"))
            node_present = any(obj.get("kind") == "Node" for obj in final)
            claim_present = any(
                object_key(obj) == ("NodeClaim", "", "default-00001") for obj in final
            )
            if bound and node_present and not claim_present:
                outcome_trials.append(path.name)
            if bound and node_present and claim_present:
                retained_trials.append(path.name)

    observed = (
        len(paths) == 20
        and converged > 0
        and bool(outcome_trials)
        and bool(retained_trials)
    )
    return {
        "experiment": "KAR-12",
        "status": "OBSERVED" if observed else "NOT_OBSERVED",
        "dumpCount": len(paths),
        "convergedStates": converged,
        "maxDepthStates": bounded,
        "outcomeTrials": outcome_trials,
        "comparisonTrials": retained_trials,
        "summedTotalNodeVisits": total_visits,
        "summedUniqueNodeVisits": unique_visits,
        "summedResourceStateCounts": resource_state_sum,
        "simulationSeconds": duration_ns / 1e9,
        "observable": "a bound Pod and its Node remain in one converged execution after the NodeClaim is removed",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kcp", type=Path, required=True)
    parser.add_argument("--kar", type=Path, required=True)
    parser.add_argument("--kro", type=Path, required=True)
    parser.add_argument("--kro-dumps", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    kro = load(args.kro)
    kro["simulationSeconds"] = sum(
        load(path).get("campaignMetrics", {}).get("durationNs", 0)
        for path in args.kro_dumps.rglob("*.jsonl")
    ) / 1e9
    results = [kcp_result(args.kcp), kro, kar_result(args.kar)]
    status = "PASS" if all(item.get("status") in {"OBSERVED", "PASS"} for item in results) else "FAIL"
    report = {"status": status, "results": results}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w") as output:
        json.dump(report, output, indent=2, sort_keys=True)
        output.write("\n")

    for item in results:
        print(f"{item.get('experiment', item.get('case'))}\t{item.get('status')}")
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
