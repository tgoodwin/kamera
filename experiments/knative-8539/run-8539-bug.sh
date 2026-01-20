#!/bin/bash
# Run Kamera DFS exploration with the buggy Knative code that reintroduces issue #8539
# The bug is in revision_lifecycle.go:202 where IsScaleTargetInitialized() was changed to IsActive()
# This creates a race condition where Revision can get stuck in Ready=Unknown if it misses
# the transient Active=True state before the PA scales to zero.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}"
OUTPUT_FILE="${OUTPUT_DIR}/kamera-results-$(date +%Y%m%d-%H%M%S).jsonl"

# Create output directory if it doesn't exist
mkdir -p "${OUTPUT_DIR}"

echo "Running Kamera DFS with buggy Knative code (issue #8539)..."
echo "Output will be written to: ${OUTPUT_FILE}"
echo ""

cd ../../examples/knative-serving
# Use the modified gomodcache with the bug reintroduced
# Note: We don't set GOCACHE since we only modified the module source, not build artifacts
GOMODCACHE=~/tmp/gomodcache-knative-8539 \
go run . \
  -depth 100 \
  -timeout 300s \
  -interactive=true \
  -dump-output "${OUTPUT_FILE}" \
  -log-level info \
  -emit-stats \
  -explore-config "${SCRIPT_DIR}/explore-config.json"

echo ""
echo "Exploration complete. Results written to: ${OUTPUT_FILE}"
echo ""
echo "To analyze results, look for terminal states with different Revision Ready conditions."
echo "Expected outcomes:"
echo "  - Happy path: Revision Ready=True (PA was Active when Revision reconciled)"
echo "  - Bug path: Revision Ready=Unknown (PA scaled to zero before Revision saw Active=True)"
