#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_root="${1:-artifact-results/figure8-simulations-$(date +%Y%m%d-%H%M%S)}"
[[ "$output_root" = /* ]] || output_root="$PWD/$output_root"
deps_root="${KAMERA_AE_FIGURE8_DEPS_DIR:-$repo_root/artifact-deps/figure8}"

if [[ -e "$output_root" ]]; then
  echo "refusing to overwrite existing output: $output_root" >&2
  exit 1
fi

kcp_kamera="$deps_root/kcp/kamera"
kcp_source="$deps_root/kcp/kcp"
kro_kamera="$deps_root/kro/kamera-paper"
kro_source="$deps_root/kro/kro"
kar_kamera="$deps_root/kar/kamera"
kar_source="$deps_root/kar/karpenter"

require_commit() {
  local name="$1"
  local directory="$2"
  local expected="$3"
  if [[ ! -f "$directory/go.mod" ]]; then
    echo "$name dependency is missing: $directory" >&2
    echo "run ./artifact/setup-figure8-deps.sh first" >&2
    exit 1
  fi
  local actual
  actual="$(git -C "$directory" rev-parse HEAD)"
  if [[ "$actual" != "$expected" ]]; then
    echo "$name is at $actual; expected $expected" >&2
    exit 1
  fi
}

require_clean() {
  local name="$1"
  local directory="$2"
  if [[ -n "$(git -C "$directory" status --short)" ]]; then
    echo "$name checkout has unexpected working-tree changes: $directory" >&2
    exit 1
  fi
}

require_patch() {
  local name="$1"
  local directory="$2"
  local patch="$3"
  if ! cmp -s <(git -C "$directory" diff --binary --unified=0 --abbrev=8) "$patch"; then
    echo "$name checkout does not match the pinned adapter: $patch" >&2
    exit 1
  fi
}

require_commit "KCP Kamera" "$kcp_kamera" "d629dded905603903a3440095f20baf460358205"
require_commit "KCP" "$kcp_source" "301a8f749e7b99a0c81f43b37aa5b5e5ff0fc0b4"
require_commit "KRO Kamera" "$kro_kamera" "1c85e5b89fa46cc8470dbd63159d6640921fdeee"
require_commit "KRO" "$kro_source" "c9320ee963f745637bb622f6b68853a870187d20"
require_commit "KAR Kamera" "$kar_kamera" "06bbe01af6545280b282e2d2a3f5964685b2bae5"
require_commit "Karpenter" "$kar_source" "8ae07cf8b4ecf8ae3f04bc306d97f1ee40d21849"
require_clean "KCP Kamera" "$kcp_kamera"
require_clean "KCP" "$kcp_source"
require_clean "KRO Kamera" "$kro_kamera"
require_clean "KAR Kamera" "$kar_kamera"
require_patch "KRO" "$kro_source" "$repo_root/artifact/figure8/kro-historical/kro-simulation.patch"
require_patch "Karpenter" "$kar_source" "$repo_root/artifact/section61/patches/karpenter-simulation.patch"

mkdir -p "$output_root/bin" "$output_root/build" "$output_root/kcp4" "$output_root/kar12/dumps"

echo "building campaign-metrics analyzer"
go build -o "$output_root/bin/kamera-ae" "$repo_root/cmd/kamera"

echo "building pinned KCP-4 harness"
kcp_harness="$repo_root/artifact/figure8/kcp-historical/harness"
cp "$kcp_harness/go.mod" "$output_root/build/kcp.mod"
cp "$kcp_harness/go.sum" "$output_root/build/kcp.sum"
go mod edit -modfile="$output_root/build/kcp.mod" \
  -replace="github.com/kcp-dev/kcp=$kcp_source" \
  -replace="github.com/kcp-dev/sdk=$kcp_source/staging/src/github.com/kcp-dev/sdk" \
  -replace="github.com/tgoodwin/kamera=$kcp_kamera"
(
  cd "$kcp_harness"
  go build -modfile="$output_root/build/kcp.mod" -o "$output_root/bin/kcp4" .
)

echo "running pinned KCP-4 agent-selected simulation"
(
  cd "$kcp_harness"
  "$output_root/bin/kcp4" \
    --interactive=false \
    --inputs "$repo_root/artifact/figure8/kcp-historical/scenarios/kcp4_trial2-v1.json" \
    --output "$output_root/kcp4/dump.jsonl" \
    --emit-stats
) >"$output_root/kcp4/run.log" 2>&1
"$output_root/bin/kamera-ae" analyze campaign-metrics "$output_root/kcp4/dump.jsonl" \
  >"$output_root/kcp4/campaign-metrics.txt"

echo "running pinned KRO-2 agent-selected simulation"
KAMERA_AE_KRO_DEPS_DIR="$deps_root/kro" \
KAMERA_AE_HISTORICAL_KAMERA_DIR="$kro_kamera" \
KAMERA_AE_KRO_DIR="$kro_source" \
  "$repo_root/artifact/run-figure8-kro-historical.sh" focused "$output_root/kro2"

echo "building pinned KAR-12 harness"
kar_harness="$kar_kamera/examples/karpenter"
cp "$kar_harness/go.mod" "$output_root/build/kar.mod"
cp "$kar_harness/go.sum" "$output_root/build/kar.sum"
go mod edit -modfile="$output_root/build/kar.mod" \
  -replace="github.com/tgoodwin/kamera=$kar_kamera" \
  -replace="sigs.k8s.io/karpenter=$kar_source"
(
  cd "$kar_harness"
  go build -mod=mod -modfile="$output_root/build/kar.mod" -o "$output_root/bin/kar12" .
)

echo "running pinned KAR-12 agent-selected Monte Carlo simulation"
(
  cd "$kar_harness"
  "$output_root/bin/kar12" \
    --interactive=false \
    --inputs "$repo_root/artifact/figure8/kar-historical/d12-tuning-v1.json" \
    --output "$output_root/kar12/dumps" \
    --closed-loop=false \
    --emit-stats
) >"$output_root/kar12/run.log" 2>&1

: >"$output_root/kar12/campaign-metrics.txt"
while IFS= read -r dump; do
  "$output_root/bin/kamera-ae" analyze campaign-metrics "$dump" \
    >>"$output_root/kar12/campaign-metrics.txt"
done < <(find "$output_root/kar12/dumps" -type f -name '*.jsonl' | sort)

python3 "$repo_root/artifact/figure8/check_simulations.py" \
  --kcp "$output_root/kcp4/dump.jsonl" \
  --kro "$output_root/kro2/focused/outcome.json" \
  --kro-dumps "$output_root/kro2/focused/dumps" \
  --kar "$output_root/kar12/dumps" \
  --output "$output_root/simulation-summary.json"

cp "$repo_root/artifact/figure8/dependencies.json" "$output_root/dependencies.json"
echo "wrote pinned Figure 8 simulation results to $output_root"
