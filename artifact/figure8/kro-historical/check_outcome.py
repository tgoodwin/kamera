#!/usr/bin/env python3
"""Check the directly observable outcome in the historical KRO-2 trace."""

import argparse
import json
import sys
from pathlib import Path


CHILD_KINDS = {"Deployment", "Service", "Ingress"}


def resolve_terminal_objects(dump, state):
    objects = {
        item["hash"]["Value"]: item["object"]
        for item in dump.get("objects", [])
    }
    return [
        objects[ref["hash"]["Value"]]
        for ref in state["state"]["contents"].get("objects", [])
        if ref["hash"]["Value"] in objects
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("dump", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    with args.dump.open() as source:
        dump = json.load(source)

    states = dump.get("states", [])
    bounded = sum(state.get("error") == "max depth reached" for state in states)
    converged = sum(not state.get("error") for state in states)
    two_effect_steps = 0
    for state in states:
        for path in state.get("paths", []):
            for step in path:
                if (
                    step
                    and step.get("controllerId") == "ApplicationController"
                    and len(step.get("changes", {}).get("effects", [])) == 2
                ):
                    two_effect_steps += 1

    terminal = []
    for state in states:
        terminal.extend(resolve_terminal_objects(dump, state))
    applications = [
        obj
        for obj in terminal
        if obj.get("kind") == "Application"
        and obj.get("metadata", {}).get("name") == "my-app-instance"
    ]
    application_spec_absent = bool(applications) and all(
        not obj.get("spec") for obj in applications
    )
    child_kinds = sorted({
        obj.get("kind") for obj in terminal if obj.get("kind") in CHILD_KINDS
    })

    observed = (
        two_effect_steps > 0
        and application_spec_absent
        and not child_kinds
    )
    result = {
        "case": "KRO-2 historical",
        "status": "OBSERVED" if observed else "NOT_OBSERVED",
        "convergedStates": converged,
        "maxDepthStates": bounded,
        "applicationControllerStepsWithTwoEffects": two_effect_steps,
        "terminalApplicationSpecAbsent": application_spec_absent,
        "terminalChildKinds": child_kinds,
        "observable": (
            "after the configured two-effect interruption, the terminal "
            "paper-era partial trace has an Application without spec and no "
            "Deployment, Service, or Ingress"
        ),
        "scope": (
            "paper-era replacement-like apply semantics; this does not assess "
            "the later schema-backed apply prototype"
        ),
    }

    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(f"{result['case']}\t{result['status']}\t{result['observable']}")
    return 0 if observed else 1


if __name__ == "__main__":
    sys.exit(main())
