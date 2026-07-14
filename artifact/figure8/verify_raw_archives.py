#!/usr/bin/env python3
"""Verify the checked-in Figure 8 raw-evidence archives."""

import argparse
import hashlib
import json
import sys
import tarfile
from pathlib import Path


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_stream(stream):
    digest = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def verify_members(name, path, expected):
    contents = expected.get("contents", [])
    expected_by_name = {f"{name}/{item['path']}": item for item in contents}
    with tarfile.open(path, "r:gz") as archive:
        members = {member.name: member for member in archive.getmembers() if member.isfile()}
        if set(members) != set(expected_by_name):
            return False
        for member_name, item in expected_by_name.items():
            member = members[member_name]
            if member.size != item["sizeBytes"]:
                return False
            stream = archive.extractfile(member)
            if stream is None or sha256_stream(stream) != item["sha256"]:
                return False
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    manifest_path = args.directory / "manifest.json"
    with manifest_path.open() as source:
        manifest = json.load(source)

    archives = manifest.get("archives", {})
    required = {"kcp4", "kro2", "kar12"}
    if set(archives) != required:
        print(
            "manifest\tFAIL\texpected exactly: " + ", ".join(sorted(required)),
            file=sys.stderr,
        )
        return 1

    all_valid = True
    for name, expected in archives.items():
        path = args.directory / expected["archive"]
        actual = sha256(path) if path.is_file() else "MISSING"
        archive_valid = actual == expected["sha256"]
        if archive_valid:
            archive_valid = verify_members(name, path, expected)
        status = "PASS" if archive_valid else "FAIL"
        print(f"{name}\t{status}\t{actual}")
        all_valid &= archive_valid
    return 0 if all_valid else 1


if __name__ == "__main__":
    sys.exit(main())
