#!/usr/bin/env python3
"""Check the observable outcomes of the Section 6.1 case-study runs."""

import argparse
import json
import sys
from pathlib import Path


def load_dump(path):
    with path.open() as source:
        dump = json.load(source)
    objects = {
        item["hash"]["Value"]: item["object"]
        for item in dump.get("objects", [])
    }
    return dump, objects


def converged_states(dump):
    return [state for state in dump.get("states", []) if not state.get("error")]


def terminal_objects(state, objects):
    resolved = []
    for ref in state["state"]["contents"].get("objects", []):
        obj = objects.get(ref["hash"]["Value"])
        if obj is not None:
            resolved.append(obj)
    return resolved


def object_key(obj):
    metadata = obj.get("metadata", {})
    return obj.get("kind"), metadata.get("namespace", ""), metadata.get("name", "")


def check_kcp4(paths):
    endpoint_counts = []
    state_count = 0
    for path in paths:
        dump, objects = load_dump(path)
        for state in converged_states(dump):
            state_count += 1
            endpoint_slices = [
                obj
                for obj in terminal_objects(state, objects)
                if object_key(obj) == ("APIExportEndpointSlice", "root:provider", "widgets")
            ]
            if len(endpoint_slices) != 1:
                continue
            endpoint_counts.append(len(endpoint_slices[0].get("status", {}).get("endpoints", [])))

    passed = state_count >= 2 and 0 in endpoint_counts and any(count > 0 for count in endpoint_counts)
    return {
        "case": "KCP-4",
        "status": "PASS" if passed else "FAIL",
        "convergedStates": state_count,
        "endpointCounts": sorted(endpoint_counts),
        "observable": "converged executions include both an empty and a populated APIExportEndpointSlice",
    }


def check_kar12(paths):
    converged = 0
    retained = 0
    removed = 0
    for path in paths:
        dump, objects = load_dump(path)
        for state in converged_states(dump):
            converged += 1
            final = terminal_objects(state, objects)
            pods = [obj for obj in final if object_key(obj) == ("Pod", "default", "workload-pod")]
            bound = bool(pods and pods[0].get("spec", {}).get("nodeName"))
            node_present = any(obj.get("kind") == "Node" for obj in final)
            claim_present = any(
                object_key(obj) == ("NodeClaim", "", "default-00001") for obj in final
            )
            if bound and node_present and claim_present:
                retained += 1
            if bound and node_present and not claim_present:
                removed += 1

    passed = converged > 0 and retained > 0 and removed > 0
    return {
        "case": "KAR-12",
        "status": "PASS" if passed else "FAIL",
        "convergedStates": converged,
        "boundPodNodeClaimRetained": retained,
        "boundPodNodeClaimRemoved": removed,
        "observable": "a bound Pod and its Node remain in one converged execution after the NodeClaim is removed",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("case", choices=("kcp4", "kar12"))
    parser.add_argument("dumps", type=Path, nargs="+")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    missing = [str(path) for path in args.dumps if not path.is_file()]
    if missing:
        parser.error("dump file not found: " + ", ".join(missing))

    result = check_kcp4(args.dumps) if args.case == "kcp4" else check_kar12(args.dumps)
    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(f"{result['case']}\t{result['status']}\t{result['observable']}")
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
