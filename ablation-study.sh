#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
CONFIG_DIR="$ROOT/ablation/configs"
OUT_ROOT="$ROOT/ablation/output"
RUN_DIR="$OUT_ROOT/$(date +%Y%m%d-%H%M%S)"

CONFIGS=(
  all-on
  none
  early-only
  completed-dedup-only
  ordering-only
  cache-only
  subtree-only
)

mkdir -p "$RUN_DIR"

echo "Ablation run output: $RUN_DIR"

for name in "${CONFIGS[@]}"; do
  cfg="$CONFIG_DIR/${name}.json"
  if [[ ! -f "$cfg" ]]; then
    echo "missing config: $cfg" >&2
    exit 1
  fi

  result_path="$RUN_DIR/${name}-results.json"
  stats_path="$RUN_DIR/${name}-stats.json"

  echo "==> Running config '${name}'"
  go run ./examples/knative-serving \
    --interactive=false \
    --dump-output "$result_path" \
    --dump-stats "$stats_path" \
    --explore-config "$cfg"
done

echo "Ablation study complete. Outputs in $RUN_DIR"
