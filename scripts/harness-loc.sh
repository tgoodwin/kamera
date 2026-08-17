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

printf '| Project | Harness Go LOC |\n'
printf '|---|---:|\n'

harness_total=0
for system in "${systems[@]}"; do
  harness_dir="$repo_root/examples/$system"
  if [[ ! -d "$harness_dir" ]]; then
    printf '| %s | unavailable |\n' "$system"
    continue
  fi

  harness_files=()
  while IFS= read -r -d '' file; do
    if [[ "$file" != *_test.go ]]; then
      harness_files+=("$file")
    fi
  done < <(find -L "$harness_dir" -maxdepth 1 -type f -name '*.go' -print0)

  harness_loc="$(go_code "${harness_files[@]}")"
  harness_total=$((harness_total + harness_loc))
  printf '| %s | %d |\n' "$system" "$harness_loc"
done

printf '| **Total** | **%d** |\n' "$harness_total"
