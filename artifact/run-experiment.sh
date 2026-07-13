#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <zk|rmq|cass>/<scenario> [output-root]" >&2
  exit 2
}

[[ $# -ge 1 && $# -le 2 ]] || usage

experiment="$1"
output_root="${2:-artifact-results}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ "$output_root" = /* ]] || output_root="$PWD/$output_root"

case "$experiment" in
  zk/stale-state-1) operator=zookeeper-operator; paper_ms=215; sieve_s=336 ;;
  zk/stale-state-2) operator=zookeeper-operator; paper_ms=308; sieve_s=455 ;;
  zk/unobserved-state-1) operator=zookeeper-operator; paper_ms=578; sieve_s=368 ;;
  zk/indirect-1) operator=zookeeper-operator; paper_ms=282; sieve_s=278 ;;
  rmq/stale-state-1) operator=rabbitmq-operator; paper_ms=136; sieve_s=358 ;;
  rmq/stale-state-2) operator=rabbitmq-operator; paper_ms=127; sieve_s=329 ;;
  rmq/unobserved-state-1) operator=rabbitmq-operator; paper_ms=111; sieve_s=512 ;;
  rmq/intermediate-state-1) operator=rabbitmq-operator; paper_ms=120; sieve_s=233 ;;
  cass/stale-state-1) operator=cass-operator; paper_ms=259; sieve_s=580 ;;
  cass/intermediate-state-1) operator=cass-operator; paper_ms=344; sieve_s=842 ;;
  cass/intermediate-state-2) operator=cass-operator; paper_ms=361; sieve_s=428 ;;
  *) usage ;;
esac

scenario="${experiment#*/}"
scenario_file="$repo_root/examples/$operator/scenarios/$scenario.json"
binary_dir="${KAMERA_AE_BIN_DIR:-$output_root/bin}"
binary="$binary_dir/${operator%-operator}"
run_dir="$output_root/runs/${experiment//\//-}"

if [[ -e "$run_dir" ]]; then
  echo "refusing to overwrite existing run directory: $run_dir" >&2
  exit 1
fi

mkdir -p "$binary_dir"
if [[ ! -x "$binary" ]]; then
  echo "building $operator" >&2
  (
    cd "$repo_root/examples/$operator"
    GOCACHE="${KAMERA_AE_GOCACHE:-$output_root/go-build-cache}" go build -o "$binary" .
  )
fi

mkdir -p "$run_dir"

echo "running perturbed simulation: $experiment" >&2
(
  cd "$repo_root/examples/$operator"
  "$binary" \
    --inputs "$scenario_file" \
    --output "$run_dir" \
    --interactive=false \
    --closed-loop=false \
    --emit-stats \
    --parallel-processes \
    --parallel-child-index=1 \
    --parallel-child-job-index=0 \
    >"$run_dir/run.log" 2>&1
)

dump="$(find "$run_dir" -maxdepth 1 -type f -name '*.jsonl' -print -quit)"
[[ -n "$dump" ]] || { echo "run produced no dump" >&2; exit 1; }

duration_ns="$(jq -er '.campaignMetrics.durationNs | select(. > 0)' "$dump")"
observed_ms="$(awk -v ns="$duration_ns" 'BEGIN { printf "%.3f", ns / 1000000 }')"
error_states="$(jq '[.states[] | select(.error != null)] | length' "$dump")"
status=converged
[[ "$error_states" -eq 0 ]] || status=partial

jq -n \
  --arg experiment "$experiment" \
  --arg status "$status" \
  --arg dump "$dump" \
  --argjson paper_ms "$paper_ms" \
  --argjson sieve_s "$sieve_s" \
  --argjson duration_ns "$duration_ns" \
  --argjson error_states "$error_states" \
  '{experiment:$experiment,status:$status,paperKameraMs:$paper_ms,paperSieveSeconds:$sieve_s,durationNs:$duration_ns,observedMs:($duration_ns/1000000),errorStates:$error_states,dump:$dump}' \
  >"$run_dir/result.json"

printf '%s\t%s\t%s\t%s\t%s\n' "$experiment" "$paper_ms" "$observed_ms" "$status" "$run_dir/result.json"
