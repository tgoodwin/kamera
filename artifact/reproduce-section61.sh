#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_root="${1:-artifact-results/section61-$(date +%Y%m%d-%H%M%S)}"
[[ "$output_root" = /* ]] || output_root="$PWD/$output_root"

if [[ -n "${KAMERA_AE_GOCACHE:-}" ]]; then
  export GOCACHE="$KAMERA_AE_GOCACHE"
fi

if [[ -e "$output_root" ]]; then
  echo "refusing to overwrite existing output: $output_root" >&2
  exit 1
fi

deps_root="${KAMERA_AE_DEPS_DIR:-$repo_root/artifact-deps/section61}"
kcp_dir="${KAMERA_AE_KCP_DIR:-$deps_root/kcp}"
karpenter_dir="${KAMERA_AE_KARPENTER_DIR:-$deps_root/karpenter}"
kcp_harness="$repo_root/artifact/section61/kcp-harness"
karpenter_harness="$repo_root/examples/karpenter"
kcp_sha="301a8f749e7b99a0c81f43b37aa5b5e5ff0fc0b4"
karpenter_sha="8ae07cf8b4ecf8ae3f04bc306d97f1ee40d21849"
karpenter_patch="$repo_root/artifact/section61/patches/karpenter-simulation.patch"

if [[ ! -f "$kcp_dir/go.mod" ]]; then
  echo "KCP source not found at $kcp_dir" >&2
  echo "run ./artifact/setup-section61-deps.sh first" >&2
  exit 1
fi
if [[ ! -f "$karpenter_dir/go.mod" ]]; then
  echo "Karpenter source not found at $karpenter_dir" >&2
  echo "run ./artifact/setup-section61-deps.sh first" >&2
  exit 1
fi
if [[ "$(git -C "$kcp_dir" rev-parse HEAD)" != "$kcp_sha" ]]; then
  echo "KCP source is not at pinned commit $kcp_sha" >&2
  exit 1
fi
if [[ "$(git -C "$karpenter_dir" rev-parse HEAD)" != "$karpenter_sha" ]]; then
  echo "Karpenter source is not at pinned commit $karpenter_sha" >&2
  exit 1
fi
if [[ -n "$(git -C "$kcp_dir" status --short)" ]]; then
  echo "KCP source checkout has unexpected changes" >&2
  exit 1
fi
if ! cmp -s <(git -C "$karpenter_dir" diff --binary --unified=0 --abbrev=8) "$karpenter_patch"; then
  echo "Karpenter source does not contain exactly the pinned simulation adapter" >&2
  echo "rerun ./artifact/setup-section61-deps.sh with a fresh dependency directory" >&2
  exit 1
fi

mkdir -p "$output_root/bin" "$output_root/build" "$output_root/kcp4" "$output_root/kar12"

cp "$kcp_harness/go.mod" "$output_root/build/kcp4.mod"
cp "$kcp_harness/go.sum" "$output_root/build/kcp4.sum"
go mod edit -modfile="$output_root/build/kcp4.mod" \
  -replace="github.com/kcp-dev/kcp=$kcp_dir" \
  -replace="github.com/kcp-dev/sdk=$kcp_dir/staging/src/github.com/kcp-dev/sdk" \
  -replace="github.com/tgoodwin/kamera=$repo_root"

cp "$karpenter_harness/go.mod" "$output_root/build/kar12.mod"
cp "$karpenter_harness/go.sum" "$output_root/build/kar12.sum"
go mod edit -modfile="$output_root/build/kar12.mod" \
  -replace="github.com/tgoodwin/kamera=$repo_root" \
  -replace="sigs.k8s.io/karpenter=$karpenter_dir"

echo "building KCP-4 harness"
(
  cd "$kcp_harness"
  go build -modfile="$output_root/build/kcp4.mod" -o "$output_root/bin/kcp4" .
)

echo "running KCP-4"
"$output_root/bin/kcp4" \
  --inputs "$repo_root/artifact/section61/kcp4.json" \
  --output "$output_root/kcp4/dump.jsonl" \
  --interactive=false \
  --closed-loop=false \
  --emit-stats \
  >"$output_root/kcp4/run.log" 2>&1
go run "$repo_root/cmd/kamera" analyze campaign-metrics \
  "$output_root/kcp4/dump.jsonl" >"$output_root/kcp4/campaign-metrics.txt"
python3 "$repo_root/artifact/section61/check_oracles.py" kcp4 \
  "$output_root/kcp4/dump.jsonl" --json >"$output_root/kcp4/oracle.json"

echo "building KAR-12 harness"
(
  cd "$karpenter_harness"
  go build -modfile="$output_root/build/kar12.mod" -o "$output_root/bin/kar12" .
)

echo "running KAR-12"
"$output_root/bin/kar12" \
  --parallel-processes \
  --inputs "$karpenter_harness/scenarios/d12-tuning-v9.json" \
  --output "$output_root/kar12/dumps" \
  --interactive=false \
  --closed-loop=false \
  --emit-stats \
  >"$output_root/kar12/run.log" 2>&1

: >"$output_root/kar12/campaign-metrics.txt"
while IFS= read -r dump; do
  go run "$repo_root/cmd/kamera" analyze campaign-metrics "$dump" \
    >>"$output_root/kar12/campaign-metrics.txt"
done < <(find "$output_root/kar12/dumps" -type f -name '*.jsonl' | sort)

mapfile_supported=true
if ! command -v mapfile >/dev/null 2>&1; then
  mapfile_supported=false
fi
if [[ "$mapfile_supported" == true ]]; then
  mapfile -t kar_dumps < <(find "$output_root/kar12/dumps" -type f -name '*.jsonl' | sort)
else
  kar_dumps=()
  while IFS= read -r dump; do kar_dumps+=("$dump"); done \
    < <(find "$output_root/kar12/dumps" -type f -name '*.jsonl' | sort)
fi
python3 "$repo_root/artifact/section61/check_oracles.py" kar12 \
  "${kar_dumps[@]}" --json >"$output_root/kar12/oracle.json"

printf 'case\tstatus\tconverged_states\toracle\n' >"$output_root/section61.tsv"
python3 - "$output_root/kcp4/oracle.json" "$output_root/kar12/oracle.json" \
  >>"$output_root/section61.tsv" <<'PY'
import json
import sys

for path in sys.argv[1:]:
    with open(path) as source:
        result = json.load(source)
    print("\t".join((
        result["case"],
        result["status"],
        str(result["convergedStates"]),
        result["observable"],
    )))
PY

cat "$output_root/section61.tsv"
echo "wrote Section 6.1 results to $output_root"
