#!/bin/bash
# Run 5 trials of knative-serving exploration to verify KPA non-determinism
# Each trial runs in a fresh process to get different Go map hash seeds

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUTPUT_DIR="$SCRIPT_DIR"
EXAMPLE_DIR="$REPO_ROOT/examples/knative-serving"
CONFIG_FILE="$OUTPUT_DIR/explore-config-original.json"

NUM_TRIALS=5
DEPTH=100

echo "=== KPA Non-Determinism Verification ==="
echo "Running $NUM_TRIALS trials with depth=$DEPTH"
echo "Output directory: $OUTPUT_DIR"
echo "Example directory: $EXAMPLE_DIR"
echo "Config file: $CONFIG_FILE"
echo ""

cd "$EXAMPLE_DIR"

for i in $(seq 1 $NUM_TRIALS); do
    TRIAL_DIR="$OUTPUT_DIR/trial-$i"
    mkdir -p "$TRIAL_DIR"

    echo "--- Trial $i of $NUM_TRIALS ---"
    echo "Output: $TRIAL_DIR"

    # Run exploration with dump output and optimizations
    # Each invocation is a fresh process with new Go map hash seed
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

    # Extract final stats from output
    grep -E "^(Converged|Unique|Total|Early)" "$TRIAL_DIR/output.log" > "$TRIAL_DIR/stats.txt" 2>/dev/null || true

    echo "Trial $i complete"
    echo ""
done

echo "=== All trials complete ==="
echo ""

# Summary comparison
echo "=== Quick Comparison ==="
for i in $(seq 1 $NUM_TRIALS); do
    echo "Trial $i:"
    cat "$OUTPUT_DIR/trial-$i/stats.txt" 2>/dev/null || echo "  (no stats captured)"
    echo ""
done
