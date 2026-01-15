#!/usr/bin/env bash
#
# Study: Does Subtree Completion Subsume Completed-Path-Dedup?
#
# Hypothesis: The subtree completion optimization should subsume the completed-path-dedup
# optimization. If this is true, when subtree completion is enabled:
#   - completedPathDedup should fire 0 (or near 0) times
#   - Performance should be similar with/without path-dedup enabled
#
# Test Matrix:
#   1. Both enabled        - baseline "all optimizations"
#   2. Subtree only        - subtree completion ON, path-dedup OFF
#   3. Path-dedup only     - subtree completion OFF, path-dedup ON
#   4. Neither             - both OFF (shows combined benefit)
#
# Key Metrics to Compare:
#   - Total time
#   - completedPathDedup skips (should be 0 when subtree completion is ON)
#   - subtreeCompletion skips
#   - Total node visits
#
# If subtree completion subsumes path-dedup:
#   - study-1 and study-2 should have similar performance
#   - study-1 should show 0 (or very low) completedPathDedup skips
#   - study-3 vs study-4 shows the standalone benefit of path-dedup
#
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
examples_dir="$root/examples/knative-serving"

configs=(
  "study-1-both.json"
  "study-2-subtree-only.json"
  "study-3-pathdedup-only.json"
  "study-4-neither.json"
)

ts=$(date +%Y%m%d-%H%M%S)

results_rows=()
fires_rows=()

extract_stat() {
  local log_file="$1"
  local pattern="$2"
  awk -F': ' -v pat="$pattern" '$0 ~ pat {print $2; exit}' "$log_file"
}

echo "=============================================="
echo "Study: Subtree Completion vs Completed-Path-Dedup"
echo "=============================================="
echo ""
echo "Testing whether subtree completion subsumes path-dedup..."
echo ""

for cfg in "${configs[@]}"; do
  name="${cfg%.json}"
  log="/tmp/subsumes-study-${ts}-${name}.log"
  echo "=== Running: ${name} ===" | tee "$log"
  echo "Config: $root/ablation/configs/$cfg"
  echo ""
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
  results_rows+=("| ${name} | ${total_time:-?} | ${total_visits:-?} | ${unique_visits:-?} | ${unique_logical:-?} | ${skipped_visits:-?} |")

  completed_dedup=$(extract_stat "$log" 'completedPathDedup skips:')
  subtree_skips=$(extract_stat "$log" 'subtreeCompletion skips:')
  diamond_skips=$(extract_stat "$log" 'subtreeCompletion diamond skips:')
  early_skips=$(extract_stat "$log" 'earlyConvergence skips:')
  cache_pred=$(extract_stat "$log" 'cachePrediction skips:')
  fires_rows+=("| ${name} | ${completed_dedup:-0} | ${subtree_skips:-0} | ${diamond_skips:-0} | ${early_skips:-0} | ${cache_pred:-0} |")
done

echo ""
echo "=============================================="
echo "RESULTS"
echo "=============================================="
echo ""

echo "## Performance Summary"
echo ""
echo "| Experiment | Time | Total Visits | Unique Visits | Unique Logical | Skipped |"
echo "| --- | --- | --- | --- | --- | --- |"
for row in "${results_rows[@]}"; do
  echo "$row"
done

echo ""
echo "## Optimization Fire Counts (Key Metrics)"
echo ""
echo "| Experiment | pathDedup skips | subtree skips | diamond skips | early skips | cache skips |"
echo "| --- | --- | --- | --- | --- | --- |"
for row in "${fires_rows[@]}"; do
  echo "$row"
done

echo ""
echo "## Analysis"
echo ""
echo "### Hypothesis: Subtree Completion Subsumes Path-Dedup"
echo ""
echo "Compare:"
echo "- **study-1-both** vs **study-2-subtree-only**: If pathDedup skips ≈ 0 in study-1,"
echo "  and performance is similar, subtree completion is handling all the work."
echo ""
echo "- **study-3-pathdedup-only** vs **study-4-neither**: Shows the standalone benefit"
echo "  of path-dedup when subtree completion is disabled."
echo ""
echo "### Interpretation Guide"
echo ""
echo "If subtree completion DOES subsume path-dedup:"
echo "  - study-1 pathDedup skips should be 0 or very low"
echo "  - study-1 ≈ study-2 in time and visits"
echo "  - study-2 should be faster than study-3"
echo ""
echo "If subtree completion does NOT fully subsume path-dedup:"
echo "  - study-1 pathDedup skips > 0 indicates additional pruning"
echo "  - study-1 faster than study-2 indicates complementary benefit"
echo ""
