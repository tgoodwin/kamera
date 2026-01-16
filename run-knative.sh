#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  echo "Usage: $0 <config.json>" >&2
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

config_path="$1"
# if [[ ! -f "$config_path" ]]; then
#   if [[ -f "$root/$config_path" ]]; then
#     config_path="$root/$config_path"
#   else
#     echo "Config not found: $config_path" >&2
#     exit 1
#   fi
# fi

(
  cd "$root/examples/knative-serving"
  GOMODCACHE=~/tmp/gomodcache \
  GOCACHE=~/tmp/gocache \
  go run . \
      --depth 100 \
      --log-level info \
      --interactive=true \
      --emit-stats \
      --timeout 300s \
      --explore-config "$root/$config_path"
)
