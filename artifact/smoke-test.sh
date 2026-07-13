#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_root="${1:-artifact-results/smoke-$(date +%Y%m%d-%H%M%S)}"
[[ "$output_root" = /* ]] || output_root="$PWD/$output_root"

line="$($repo_root/artifact/run-experiment.sh rmq/unobserved-state-1 "$output_root")"
result="$(printf '%s\n' "$line" | awk -F '\t' '{print $5}')"
dump="$(jq -r '.dump' "$result")"

pod_creates="$(jq '[.states[0].paths[0][] | .changes.effects[]? | select(.OpType == "CREATE" and .Key.resourceKind == "Pod")] | length' "$dump")"
status="$(jq -r '.status' "$result")"

[[ "$status" == converged ]] || { echo "FAIL: perturbed run did not converge"; exit 1; }
[[ "$pod_creates" -eq 1 ]] || { echo "FAIL: expected one Pod creation, got $pod_creates"; exit 1; }
echo "PASS: Kamera built, the perturbed run converged, and the unobserved-state oracle observed one Pod creation."
