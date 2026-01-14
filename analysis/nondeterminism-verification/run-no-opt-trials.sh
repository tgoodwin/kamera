#!/bin/bash
# Run 3 quick trials WITHOUT optimizations to verify non-determinism

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/no-opt-trials"
EXAMPLE_DIR="$REPO_ROOT/examples/knative-serving"
CONFIG_FILE="$SCRIPT_DIR/explore-config-no-opt.json"

NUM_TRIALS=3
DEPTH=100

mkdir -p "$OUTPUT_DIR"

echo "=== No-Optimization Trials (verify non-determinism) ==="
echo "Running $NUM_TRIALS trials with depth=$DEPTH"
echo "Config file: $CONFIG_FILE"
echo ""

cd "$EXAMPLE_DIR"

for i in $(seq 1 $NUM_TRIALS); do
    TRIAL_DIR="$OUTPUT_DIR/trial-$i"
    mkdir -p "$TRIAL_DIR"

    echo "--- Trial $i of $NUM_TRIALS ---"

    GOCACHE=~/tmp/gocache \
    GOMODCACHE=~/tmp/gomodcache \
    go run . \
        -depth "$DEPTH" \
        -interactive=false \
        -dump-output "$TRIAL_DIR/dump.jsonl" \
        -explore-config "$CONFIG_FILE" \
        -log-level info \
        -emit-stats \
        2>&1 | tee "$TRIAL_DIR/output.log"

    # Extract stats
    grep -E "^(Total time|Total node|Unique|Early|Converged)" "$TRIAL_DIR/output.log" > "$TRIAL_DIR/stats.txt" 2>/dev/null || true

    echo "Trial $i complete"
    echo ""
done

echo "=== Quick Comparison (No Optimizations) ==="
for i in $(seq 1 $NUM_TRIALS); do
    echo "Trial $i:"
    cat "$OUTPUT_DIR/trial-$i/stats.txt" 2>/dev/null || echo "  (no stats)"
    echo ""
done
