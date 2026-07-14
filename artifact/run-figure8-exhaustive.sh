#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
experiment="${1:-all}"
output_root="${2:-artifact-results/figure8-exhaustive-${experiment}-$(date +%Y%m%d-%H%M%S)}"
[[ "$output_root" = /* ]] || output_root="$PWD/$output_root"
deps_root="${KAMERA_AE_FIGURE8_DEPS_DIR:-$repo_root/artifact-deps/figure8}"
bin_dir="${KAMERA_AE_FIGURE8_BIN_DIR:-}"

case "$experiment" in
  all|kcp4|kro2|kar12) ;;
  *) echo "usage: $0 [all|kcp4|kro2|kar12] [output-directory]" >&2; exit 2 ;;
esac
if [[ -e "$output_root" ]]; then
  echo "refusing to overwrite existing output: $output_root" >&2
  exit 1
fi
if [[ -z "$bin_dir" || ! -x "$bin_dir/kamera-ae" ]]; then
  echo "KAMERA_AE_FIGURE8_BIN_DIR must name the bin directory from run-figure8-simulations.sh" >&2
  exit 1
fi

mkdir -p "$output_root"

if [[ "$experiment" == "all" || "$experiment" == "kcp4" ]]; then
  echo "running complete KCP-4 exhaustive exploration"
  kcp_harness="$repo_root/artifact/figure8/kcp-historical/harness"
  mkdir -p "$output_root/kcp4"
  (
    cd "$kcp_harness"
    "$bin_dir/kcp4" \
      --interactive=false \
      --inputs "$repo_root/artifact/figure8/kcp-historical/scenarios/kcp4_late-apiexport.json" \
      --output "$output_root/kcp4/dump.jsonl" \
      --emit-stats
  ) >"$output_root/kcp4/run.log" 2>&1
  "$bin_dir/kamera-ae" analyze campaign-metrics "$output_root/kcp4/dump.jsonl" \
    >"$output_root/kcp4/campaign-metrics.txt"
fi

if [[ "$experiment" == "all" || "$experiment" == "kro2" ]]; then
  echo "running complete KRO-2 exhaustive matrix"
  KAMERA_AE_KRO_DEPS_DIR="$deps_root/kro" \
  KAMERA_AE_HISTORICAL_KAMERA_DIR="$deps_root/kro/kamera-paper" \
  KAMERA_AE_KRO_DIR="$deps_root/kro/kro" \
    "$repo_root/artifact/run-figure8-kro-historical.sh" full "$output_root/kro2"
fi

if [[ "$experiment" == "all" || "$experiment" == "kar12" ]]; then
  timeout_seconds="${KAMERA_AE_KAR_TIMEOUT_SECONDS:-7200}"
  echo "running KAR-12 exhaustive exploration with ${timeout_seconds}s wall-clock cap"
  kar_harness="$deps_root/kar/kamera/examples/karpenter"
  mkdir -p "$output_root/kar12/dumps"
  set +e
  python3 "$repo_root/artifact/figure8/run_with_timeout.py" \
    --seconds "$timeout_seconds" \
    --cwd "$kar_harness" \
    --stdout "$output_root/kar12/run.log" \
    --status-json "$output_root/kar12/run-status.json" \
    -- \
    "$bin_dir/kar12" \
      --inputs "$repo_root/artifact/figure8/kar-historical/d12-exhaustive.json" \
      --output "$output_root/kar12/dumps" \
      --interactive=false \
      --timeout 7200s \
      --fuzz-cases 0 \
      --metrics-only-staleness \
      --parallel-processes
  kar_status=$?
  set -e
  if [[ "$kar_status" -ne 0 && "$kar_status" -ne 124 ]]; then
    echo "KAR-12 exhaustive runner failed with status $kar_status" >&2
    exit "$kar_status"
  fi

  : >"$output_root/kar12/campaign-metrics.txt"
  while IFS= read -r dump; do
    if python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$dump" 2>/dev/null; then
      "$bin_dir/kamera-ae" analyze campaign-metrics "$dump" \
        >>"$output_root/kar12/campaign-metrics.txt"
    else
      echo "skipped incomplete dump: $dump" \
        >>"$output_root/kar12/campaign-metrics.txt"
    fi
  done < <(find "$output_root/kar12/dumps" -type f -name '*.jsonl' | sort)
fi

cp "$repo_root/artifact/figure8/dependencies.json" "$output_root/dependencies.json"
echo "wrote exhaustive Figure 8 evidence to $output_root"
