#!/usr/bin/env python3
"""Create checksummed raw-evidence archives for the standard Figure 8 path."""

import argparse
import gzip
import hashlib
import json
import tarfile
from pathlib import Path


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def archive_directory(name, source, destination):
    paths = sorted(path for path in source.rglob("*") if path.is_file())
    entries = [
        {
            "path": str(path.relative_to(source)),
            "sha256": sha256(path),
            "sizeBytes": path.stat().st_size,
        }
        for path in paths
    ]
    with destination.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                for path in paths:
                    relative = path.relative_to(source)
                    info = archive.gettarinfo(str(path), arcname=str(Path(name) / relative))
                    info.uid = info.gid = 0
                    info.uname = info.gname = ""
                    info.mtime = 0
                    with path.open("rb") as stream:
                        archive.addfile(info, stream)
    return {
        "archive": destination.name,
        "contents": entries,
        "fileCount": len(paths),
        "sha256": sha256(destination),
        "sizeBytes": destination.stat().st_size,
        "uncompressedBytes": sum(path.stat().st_size for path in paths),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kcp", type=Path, required=True)
    parser.add_argument("--kro", type=Path, required=True)
    parser.add_argument("--kar", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)

    manifest = {
        "format": "kamera-figure8-raw-archives-v1",
        "provenance": {
            "kcp4": "surviving raw log and dump from the paper exhaustive run",
            "kro2": "raw output regenerated from the pinned paper experiment snapshot; all archived invariants matched",
            "kar12": "raw output regenerated from the pinned paper experiment snapshot with the paper's two-hour cap",
        },
        "archives": {},
    }
    for name, source in (("kcp4", args.kcp), ("kro2", args.kro), ("kar12", args.kar)):
        if not source.is_dir():
            raise SystemExit(f"raw evidence directory is missing: {source}")
        destination = args.output / f"{name}.tar.gz"
        manifest["archives"][name] = archive_directory(name, source, destination)

    with (args.output / "manifest.json").open("w") as output:
        json.dump(manifest, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()
