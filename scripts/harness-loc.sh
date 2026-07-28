#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
systems=(karpenter crossplane kratix kro kcp)

for command_name in scc jq; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "missing required command: $command_name" >&2
    exit 1
  fi
done

go_code() {
  if (($# == 0)); then
    echo 0
    return
  fi
  scc "$@" --format json |
    jq -r '[.[] | select(.Name == "Go")][0].Code // 0'
}

printf '| Project | Production Go LOC | Test Go LOC | Total Go LOC |\n'
printf '|---|---:|---:|---:|\n'

production_total=0
test_total=0
for system in "${systems[@]}"; do
  harness_dir="$repo_root/examples/$system"
  if [[ ! -d "$harness_dir" ]]; then
    printf '| %s | unavailable | unavailable | unavailable |\n' "$system"
    continue
  fi

  production_files=()
  test_files=()
  while IFS= read -r -d '' file; do
    if [[ "$file" == *_test.go ]]; then
      test_files+=("$file")
    else
      production_files+=("$file")
    fi
  done < <(find -L "$harness_dir" -maxdepth 1 -type f -name '*.go' -print0)

  production="$(go_code "${production_files[@]}")"
  tests="$(go_code "${test_files[@]}")"
  total=$((production + tests))
  production_total=$((production_total + production))
  test_total=$((test_total + tests))
  printf '| %s | %d | %d | %d |\n' "$system" "$production" "$tests" "$total"
done

printf '| **Total** | **%d** | **%d** | **%d** |\n' \
  "$production_total" \
  "$test_total" \
  "$((production_total + test_total))"
