#!/usr/bin/env python3
"""Run a command in its own process group with a portable wall-clock cap."""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, required=True)
    parser.add_argument("--cwd", type=Path, required=True)
    parser.add_argument("--stdout", type=Path, required=True)
    parser.add_argument("--status-json", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("a command is required after --")

    args.stdout.parent.mkdir(parents=True, exist_ok=True)
    args.status_json.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    timed_out = False
    with args.stdout.open("wb") as output:
        process = subprocess.Popen(
            command,
            cwd=args.cwd,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            return_code = process.wait(timeout=args.seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            return_code = 124

    elapsed = time.monotonic() - started
    status = {
        "command": command,
        "elapsedWallSeconds": elapsed,
        "returnCode": return_code,
        "status": "timed-out" if timed_out else ("completed" if return_code == 0 else "failed"),
        "timeoutSeconds": args.seconds,
    }
    with args.status_json.open("w") as output:
        json.dump(status, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps(status, sort_keys=True))
    return return_code


if __name__ == "__main__":
    sys.exit(main())
