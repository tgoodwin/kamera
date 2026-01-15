#!/bin/bash
# diagnose-nondeterminism.sh
#
# Runs multiple independent trials of the Knative exploration to capture
# non-determinism in the exploration results. Each trial is a fresh program
# invocation to get a new Go map hash seed.
#
# Usage: ./diagnose-nondeterminism.sh [num_trials]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
EXAMPLE_DIR="$ROOT_DIR/examples/knative-serving"
OUTPUT_DIR="$ROOT_DIR/analysis/nondeterminism-diagnostics"

NUM_TRIALS="${1:-5}"

# Use a config with both subtree completion and pathdedup enabled
CONFIG_FILE="$ROOT_DIR/ablation/configs/study-1-both.json"

# Clean up previous diagnostics
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

echo "============================================="
echo "Non-Determinism Diagnostic Study"
echo "============================================="
echo "Config: $CONFIG_FILE"
echo "Trials: $NUM_TRIALS"
echo "Output: $OUTPUT_DIR"
echo ""

# Run multiple trials
for i in $(seq 1 $NUM_TRIALS); do
    echo "---------------------------------------------"
    echo "Trial $i of $NUM_TRIALS"
    echo "---------------------------------------------"
    
    TRIAL_DIR="$OUTPUT_DIR/trial-$i"
    mkdir -p "$TRIAL_DIR"
    
    # Run with verbose logging (log-level=debug) to capture diagnostic output
    # The diagnostic logs will include:
    # - EFFECTS_ORDER_DIAGNOSTIC: Effect ordering after each reconcile
    # - ORDERING_VARIANTS_DIAGNOSTIC: Ordering variants generated
    # - PENDING_LIST_DIAGNOSTIC: Pending list at each step
    # - "Taking reconcile step" logs track: Depth, # Distinct States, Total States, Resource States
    echo "Running exploration with verbose diagnostics..."
    
    STATS_FILE="$TRIAL_DIR/stats.json"
    
    cd "$EXAMPLE_DIR"
    go run . \
        -explore-config "$CONFIG_FILE" \
        -interactive=false \
        -log-level=debug \
        -depth=100 \
        -emit-stats \
        -dump-stats="$STATS_FILE" \
        2>&1 | tee "$TRIAL_DIR/full-output.log"
    
    # Extract just the diagnostic lines for easier comparison
    grep -E "(EFFECTS_ORDER_DIAGNOSTIC|ORDERING_VARIANTS_DIAGNOSTIC|PENDING_LIST_DIAGNOSTIC)" \
        "$TRIAL_DIR/full-output.log" > "$TRIAL_DIR/diagnostics.log" 2>/dev/null || true
    
    # Extract "Taking reconcile step" logs for analysis
    grep "Taking reconcile step" "$TRIAL_DIR/full-output.log" > "$TRIAL_DIR/reconcile-steps.log" 2>/dev/null || true
    
    # Extract the summary line
    grep -E "(explored|converged|unique|states available)" "$TRIAL_DIR/full-output.log" > "$TRIAL_DIR/summary.log" 2>/dev/null || true
    
    # Show trial summary
    echo "Trial $i summary:"
    cat "$TRIAL_DIR/summary.log"
    
    # Show stats if available
    if [ -f "$STATS_FILE" ]; then
        echo "Stats:"
        cat "$STATS_FILE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  TotalStates: {d.get(\"TotalStates\",\"N/A\")}'); print(f'  UniqueStates: {d.get(\"UniqueStates\",\"N/A\")}'); print(f'  ConvergedPaths: {d.get(\"ConvergedPaths\",\"N/A\")}'); print(f'  AbortedPaths: {d.get(\"AbortedPaths\",\"N/A\")}')" 2>/dev/null || echo "  (could not parse stats)"
    fi
    
    # Count reconcile steps
    STEP_COUNT=$(wc -l < "$TRIAL_DIR/reconcile-steps.log" 2>/dev/null | tr -d ' ')
    echo "Reconcile steps: $STEP_COUNT"
    echo ""
done

echo "============================================="
echo "Comparing Results Across Trials"
echo "============================================="
echo ""

# Compare summaries
echo "Summary and Stats comparison:"
echo ""
for i in $(seq 1 $NUM_TRIALS); do
    echo "Trial $i:"
    cat "$OUTPUT_DIR/trial-$i/summary.log" 2>/dev/null || echo "  (no summary)"
    STATS_FILE="$OUTPUT_DIR/trial-$i/stats.json"
    if [ -f "$STATS_FILE" ]; then
        cat "$STATS_FILE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  TotalStates: {d.get(\"TotalStates\",\"N/A\")}'); print(f'  UniqueStates: {d.get(\"UniqueStates\",\"N/A\")}'); print(f'  ConvergedPaths: {d.get(\"ConvergedPaths\",\"N/A\")}'); print(f'  AbortedPaths: {d.get(\"AbortedPaths\",\"N/A\")}')" 2>/dev/null || echo "  (could not parse stats)"
    fi
    STEP_COUNT=$(wc -l < "$OUTPUT_DIR/trial-$i/reconcile-steps.log" 2>/dev/null | tr -d ' ')
    echo "  ReconcileSteps: $STEP_COUNT"
    echo ""
done

# Check if diagnostics differ
echo ""
echo "Checking if diagnostic output differs between trials..."
echo ""

FIRST_DIAG="$OUTPUT_DIR/trial-1/diagnostics.log"
ALL_SAME=true
for i in $(seq 2 $NUM_TRIALS); do
    CURR_DIAG="$OUTPUT_DIR/trial-$i/diagnostics.log"
    if ! diff -q "$FIRST_DIAG" "$CURR_DIAG" > /dev/null 2>&1; then
        echo "DIFFERENCE FOUND: Trial 1 vs Trial $i"
        echo "First difference:"
        diff "$FIRST_DIAG" "$CURR_DIAG" | head -20
        echo ""
        ALL_SAME=false
    fi
done

if [ "$ALL_SAME" = true ]; then
    echo "All trials produced identical diagnostic output."
    echo "Non-determinism may not be observable with this workload or logging level."
else
    echo ""
    echo "NON-DETERMINISM DETECTED!"
    echo "See $OUTPUT_DIR for detailed logs."
fi

echo ""
echo "============================================="
echo "Done. Output in: $OUTPUT_DIR"
echo "============================================="
