#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: ./artifact/run-sieve-baselines.sh [--dry-run] [--only <experiment>] <sieve-checkout> [output-root]

Runs the 11 Table 6 baselines through Sieve's real kind-cluster reproducer.
See artifact/sieve/README.md before running; Sieve deletes the kind cluster
named "kind" on every invocation.
EOF
  exit 2
}

dry_run=false
only_experiment=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      dry_run=true
      shift
      ;;
    --only)
      [[ $# -ge 2 ]] || usage
      only_experiment="$2"
      shift 2
      ;;
    *)
      break
      ;;
  esac
done
[[ $# -ge 1 && $# -le 2 ]] || usage

sieve_root="$1"
[[ "$sieve_root" = /* ]] || sieve_root="$PWD/$sieve_root"
output_root="${2:-artifact-results/table6-sieve-$(date +%Y%m%d-%H%M%S)}"
[[ "$output_root" = /* ]] || output_root="$PWD/$output_root"
if [[ -n "${KAMERA_AE_SIEVE_PYTHON:-}" ]]; then
  python="$KAMERA_AE_SIEVE_PYTHON"
elif [[ -x "$sieve_root/.venv/bin/python" ]]; then
  python="$sieve_root/.venv/bin/python"
else
  python="python3"
fi
registry="${KAMERA_AE_SIEVE_REGISTRY:-ghcr.io/sieve-project/action}"
script_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python_path="$(command -v "$python" 2>/dev/null || true)"
if [[ -n "$python_path" ]]; then
  export PATH="$(dirname "$python_path"):$PATH"
fi
export SIEVE_CSI_MANIFEST_DIR="${SIEVE_CSI_MANIFEST_DIR:-$script_root/sieve/manifests}"
if [[ "$registry" == "docker.io/tlg2132" ]]; then
  export SIEVE_KIND_IMAGE="${SIEVE_KIND_IMAGE:-docker.io/tlg2132/node@sha256:2f1538ba2b9c7af70e80a5237959ffd8e02692e54d439d22546fd9a508a474a3}"
fi
expected_sieve_commit="6c97abeb79e644fa5eda889a2c174b2436dbc264"

if [[ ! -f "$sieve_root/reproduce_bugs.py" ]]; then
  echo "Sieve checkout not found at $sieve_root" >&2
  exit 1
fi
if [[ ! -d "$SIEVE_CSI_MANIFEST_DIR" ]]; then
  echo "vendored CSI manifest directory not found: $SIEVE_CSI_MANIFEST_DIR" >&2
  exit 1
fi
actual_sieve_commit="$(git -C "$sieve_root" rev-parse HEAD)"
if [[ "$actual_sieve_commit" != "$expected_sieve_commit" &&
      "${KAMERA_AE_SIEVE_ALLOW_OTHER_COMMIT:-}" != "1" ]]; then
  echo "Sieve is at $actual_sieve_commit; expected $expected_sieve_commit" >&2
  echo "set KAMERA_AE_SIEVE_ALLOW_OTHER_COMMIT=1 to test another revision" >&2
  exit 1
fi

rows=(
  $'zk/stale-state-1\tzookeeper-operator\tstale-state-1\t336'
  $'zk/stale-state-2\tzookeeper-operator\tstale-state-2\t455'
  $'zk/unobserved-state-1\tzookeeper-operator\tunobserved-state-1\t368'
  $'zk/indirect-1\tzookeeper-operator\tindirect-1\t278'
  $'rmq/stale-state-1\trabbitmq-operator\tstale-state-1\t358'
  $'rmq/stale-state-2\trabbitmq-operator\tstale-state-2\t329'
  $'rmq/unobserved-state-1\trabbitmq-operator\tunobserved-state-1\t512'
  $'rmq/intermediate-state-1\trabbitmq-operator\tintermediate-state-1\t233'
  $'cass/stale-state-1\tcass-operator\tstale-state-1\t580'
  $'cass/intermediate-state-1\tcass-operator\tintermediate-state-1\t842'
  $'cass/intermediate-state-2\tcass-operator\tintermediate-state-2\t428'
)

if [[ -n "$only_experiment" ]]; then
  selected_rows=()
  for row in "${rows[@]}"; do
    IFS=$'\t' read -r experiment _ <<<"$row"
    if [[ "$experiment" == "$only_experiment" ]]; then
      selected_rows+=("$row")
    fi
  done
  if [[ ${#selected_rows[@]} -eq 0 ]]; then
    echo "unknown experiment: $only_experiment" >&2
    exit 2
  fi
  rows=("${selected_rows[@]}")
fi

if [[ "$dry_run" == true ]]; then
  for row in "${rows[@]}"; do
    IFS=$'\t' read -r experiment controller bug paper_seconds <<<"$row"
    printf '%s\t(cd %q && %q reproduce_bugs.py -c %q -b %q -r %q)\n' \
      "$experiment" "$sieve_root" "$python" "$controller" "$bug" "$registry"
  done
  exit 0
fi

for command in docker helm kind kubectl go git jq "$python"; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "required command not found: $command" >&2
    exit 1
  }
done
if [[ ! -f "$sieve_root/sieve_config.json" ]]; then
  echo "missing $sieve_root/sieve_config.json" >&2
  echo 'create it with: {"workload_conditional_wait_timeout": 1000}' >&2
  exit 1
fi
if [[ "$(jq -r '.workload_conditional_wait_timeout // empty' \
  "$sieve_root/sieve_config.json")" != "1000" ]]; then
  echo "sieve_config.json must set workload_conditional_wait_timeout to 1000" >&2
  exit 1
fi
docker info >/dev/null
if kind get clusters 2>/dev/null | grep -qx 'kind'; then
  echo "refusing to run: a kind cluster named 'kind' already exists" >&2
  echo "Sieve would delete it; use a dedicated host or remove it intentionally" >&2
  exit 1
fi

delete_kind_cluster() {
  local reason="$1"
  if kind get clusters 2>/dev/null | grep -qx 'kind'; then
    echo "deleting kind cluster $reason" >&2
    kind delete cluster --name kind
  fi
}

cleanup_failed_run() {
  local exit_status=$?
  if [[ "$exit_status" -ne 0 ]]; then
    delete_kind_cluster "after unsuccessful Sieve run" || true
  fi
  return "$exit_status"
}
trap cleanup_failed_run EXIT

if [[ -e "$output_root" ]]; then
  echo "refusing to overwrite existing output: $output_root" >&2
  exit 1
fi
mkdir -p "$output_root/runs"
cp "$sieve_root/sieve_config.json" "$output_root/sieve_config.json"

tsv="$output_root/table6-sieve.tsv"
printf 'experiment\tpaper_sieve_s\tobserved_s\treproduced\tnumber_errors\tresult_json\n' >"$tsv"
failures=0

for row in "${rows[@]}"; do
  IFS=$'\t' read -r experiment controller bug paper_seconds <<<"$row"
  run_dir="$output_root/runs/${experiment//\//-}"
  mkdir -p "$run_dir"
  echo "running Sieve baseline: $experiment"
  set +e
  (
    cd "$sieve_root"
    "$python" reproduce_bugs.py -c "$controller" -b "$bug" -r "$registry"
  ) 2>&1 | tee "$run_dir/run.log"
  sieve_status=${PIPESTATUS[0]}
  set -e
  if [[ "$sieve_status" -ne 0 ]]; then
    echo "Sieve failed for $experiment" >&2
    exit "$sieve_status"
  fi

  stats_line="$(awk -F '\t' -v controller="$controller" -v bug="$bug" \
    '$1 == controller && $2 == bug { print; exit }' \
    "$sieve_root/bug_reproduction_stats.tsv")"
  if [[ -z "$stats_line" ]]; then
    echo "Sieve did not record stats for $controller/$bug" >&2
    exit 1
  fi
  reproduced="$(printf '%s\n' "$stats_line" | awk -F '\t' '{print $3}')"
  result_rel="$(printf '%s\n' "$stats_line" | awk -F '\t' '{print $4}')"
  result_source="$sieve_root/$result_rel"
  if [[ ! -f "$result_source" ]]; then
    echo "Sieve result is missing: $result_source" >&2
    exit 1
  fi
  result_copy="$run_dir/result.json"
  cp "$result_source" "$result_copy"
  cp -R "$sieve_root/sieve_test_results" "$run_dir/sieve_test_results"
  cp "$sieve_root/bug_reproduction_stats.tsv" "$run_dir/bug_reproduction_stats.tsv"

  duration="$(jq -er '[.. | objects | .duration? // empty] | last' "$result_copy")"
  number_errors="$(jq -er '[.. | objects | .number_errors? // empty] | last' "$result_copy")"
  printf '%s\t%s\t%.3f\t%s\t%s\t%s\n' \
    "$experiment" "$paper_seconds" "$duration" "$reproduced" \
    "$number_errors" "$result_copy" | tee -a "$tsv"
  if [[ "$reproduced" != "True" ]]; then
    failures=$((failures + 1))
    delete_kind_cluster "after $experiment reported reproduced=False"
  fi
done

echo "Sieve baseline summary written to $tsv"
if [[ "$failures" -ne 0 ]]; then
  echo "$failures Sieve row(s) did not report reproduced=True" >&2
  exit 1
fi
