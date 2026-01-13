#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
examples_dir="$root/examples/knative-serving"

configs=(
  "knative-1-all.json"
  "knative-2-no-subtree.json"
  "knative-3-no-early-convergence.json"
  "knative-4-no-memoization.json"
  "knative-5-no-path-dedup.json"
  "knative-6-nothing.json"
  "knative-7-permute-all-pending.json"
)

ts=$(date +%Y%m%d-%H%M%S)

results_rows=()
fires_rows=()

extract_stat() {
  local log_file="$1"
  local pattern="$2"
  awk -F': ' -v pat="$pattern" '$0 ~ pat {print $2; exit}' "$log_file"
}

for cfg in "${configs[@]}"; do
  name="${cfg%.json}"
  log="/tmp/knative-matrix-${ts}-${name}.log"
  echo "=== ${name} (${cfg}) ===" | tee "$log"
  (
    cd "$examples_dir"
    KAMERA_ORDER_PRUNE_ORDER_HASH=1 \
    GOMODCACHE=~/tmp/gomodcache GOCACHE=~/tmp/gocache \
      go run . \
        --depth 100 \
        --timeout 0 \
        --emit-stats \
        --log-level info \
        --interactive=false \
        --timeout 60m \
        --explore-config "$root/ablation/configs/$cfg"
  ) 2>&1 | tee -a "$log"
  echo "" | tee -a "$log"

  total_time=$(extract_stat "$log" '^Total time:')
  total_visits=$(extract_stat "$log" '^Total node visits:')
  unique_visits=$(extract_stat "$log" '^Unique node visits:')
  unique_logical=$(extract_stat "$log" '^Unique resource states:')
  skipped_visits=$(extract_stat "$log" '^Skipped node visits:')
  early_conv=$(extract_stat "$log" '^Early convergence:')
  results_rows+=("| ${name} | ${total_time:-?} | ${total_visits:-?} | ${unique_visits:-?} | ${unique_logical:-?} | ${skipped_visits:-?} | ${early_conv:-?} |")

  ordering_branch=$(extract_stat "$log" 'orderingPruning branch skips:')
  ordering_noop=$(extract_stat "$log" 'orderingPruning no-op skips:')
  completed_dedup=$(extract_stat "$log" 'completedPathDedup skips:')
  cache_pred=$(extract_stat "$log" 'cachePrediction skips:')
  early_skips=$(extract_stat "$log" 'earlyConvergence skips:')
  subtree_skips=$(extract_stat "$log" 'subtreeCompletion skips:')
  diamond_skips=$(extract_stat "$log" 'subtreeCompletion diamond skips:')
  fires_rows+=("| ${name} | ${ordering_branch:-?} | ${ordering_noop:-?} | ${completed_dedup:-?} | ${cache_pred:-?} | ${early_skips:-?} | ${subtree_skips:-?} | ${diamond_skips:-?} |")
done

echo "## Results"
echo "| Experiment | Time | Total Visits | Unique Visits | Unique Logical States | Skipped Visits | Early Convergence |"
echo "| --- | --- | --- | --- | --- | --- | --- |"
for row in "${results_rows[@]}"; do
  echo "$row"
done
echo ""
echo "## Optimization Fires"
echo "| Experiment | orderingPruning branch skips | orderingPruning no-op skips | completedPathDedup skips | cachePrediction skips | earlyConvergence skips | subtreeCompletion skips | subtreeCompletion diamond skips |"
echo "| --- | --- | --- | --- | --- | --- | --- | --- |"
for row in "${fires_rows[@]}"; do
  echo "$row"
done
