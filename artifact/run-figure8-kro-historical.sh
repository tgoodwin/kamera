#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mode="${1:-focused}"
output_root="${2:-artifact-results/figure8-kro-${mode}-$(date +%Y%m%d-%H%M%S)}"
[[ "$output_root" = /* ]] || output_root="$PWD/$output_root"

if [[ "$mode" != "focused" && "$mode" != "full" ]]; then
  echo "usage: $0 [focused|full] [output-directory]" >&2
  exit 2
fi
if [[ -e "$output_root" ]]; then
  echo "refusing to overwrite existing output: $output_root" >&2
  exit 1
fi
if [[ -n "${KAMERA_AE_GOCACHE:-}" ]]; then
  export GOCACHE="$KAMERA_AE_GOCACHE"
fi
if [[ -n "${KAMERA_AE_GOMODCACHE:-}" ]]; then
  export GOMODCACHE="$KAMERA_AE_GOMODCACHE"
fi

deps_root="${KAMERA_AE_KRO_DEPS_DIR:-$repo_root/artifact-deps/figure8-kro}"
kamera_dir="${KAMERA_AE_HISTORICAL_KAMERA_DIR:-$deps_root/kamera-paper}"
kro_dir="${KAMERA_AE_KRO_DIR:-$deps_root/kro}"
kamera_sha="1c85e5b89fa46cc8470dbd63159d6640921fdeee"
kro_sha="c9320ee963f745637bb622f6b68853a870187d20"
kro_patch="$repo_root/artifact/figure8/kro-historical/kro-simulation.patch"
harness="$kamera_dir/examples/kro"
scenario_dir="$repo_root/artifact/figure8/kro-historical"

if [[ ! -f "$kamera_dir/go.mod" ]]; then
  echo "reconstructed Kamera source not found at $kamera_dir" >&2
  echo "run ./artifact/setup-figure8-kro-deps.sh first" >&2
  exit 1
fi
if [[ "$(git -C "$kamera_dir" rev-parse HEAD)" != "$kamera_sha" ]]; then
  echo "Kamera source is not at pinned commit $kamera_sha" >&2
  exit 1
fi
if [[ -n "$(git -C "$kamera_dir" status --short)" ]]; then
  echo "reconstructed Kamera source has unexpected changes" >&2
  exit 1
fi
if [[ ! -f "$kro_dir/go.mod" ]]; then
  echo "KRO source not found at $kro_dir" >&2
  echo "run ./artifact/setup-figure8-kro-deps.sh first" >&2
  exit 1
fi
if [[ "$(git -C "$kro_dir" rev-parse HEAD)" != "$kro_sha" ]]; then
  echo "KRO source is not at pinned commit $kro_sha" >&2
  exit 1
fi
if ! cmp -s <(git -C "$kro_dir" diff --binary --unified=0 --abbrev=8) "$kro_patch"; then
  echo "KRO source does not contain exactly the pinned paper-era adapter" >&2
  exit 1
fi

mkdir -p "$output_root/bin" "$output_root/build" \
  "$output_root/focused/dumps/reference" "$output_root/focused/dumps/rerun"
cp "$harness/go.mod" "$output_root/build/kro.mod"
cp "$harness/go.sum" "$output_root/build/kro.sum"
go mod edit -modfile="$output_root/build/kro.mod" \
  -replace="github.com/tgoodwin/kamera=$kamera_dir" \
  -replace="github.com/kubernetes-sigs/kro=$kro_dir"

echo "building pinned KRO-2 paper-snapshot harness"
(
  cd "$harness"
  go build -modfile="$output_root/build/kro.mod" -o "$output_root/bin/kro-historical" .
)

echo "running focused KRO-2 scenario"
: >"$output_root/focused/run.log"
(
  cd "$harness"
  "$output_root/bin/kro-historical" \
    --inputs "$scenario_dir/k2b-tuning-v1.json" \
    --output "$output_root/focused/dumps/reference" \
    --interactive=false \
    --closed-loop=false \
    --no-perturbations \
    --emit-stats
) >>"$output_root/focused/run.log" 2>&1
(
  cd "$harness"
  "$output_root/bin/kro-historical" \
    --inputs "$scenario_dir/k2b-tuning-v1.json" \
    --output "$output_root/focused/dumps/rerun" \
    --interactive=false \
    --closed-loop=false \
    --emit-stats
) >>"$output_root/focused/run.log" 2>&1

: >"$output_root/focused/campaign-metrics.txt"
focused_dumps=()
while IFS= read -r dump; do
  focused_dumps+=("$dump")
  go run "$repo_root/cmd/kamera" analyze campaign-metrics "$dump" \
    >>"$output_root/focused/campaign-metrics.txt"
done < <(find "$output_root/focused/dumps" -type f -name '*.jsonl' | sort)
if [[ "${#focused_dumps[@]}" -ne 2 ]]; then
  echo "expected focused reference and rerun dumps, found ${#focused_dumps[@]}" >&2
  exit 1
fi
focused_rerun=""
for dump in "${focused_dumps[@]}"; do
  if [[ "$dump" == */rerun/* ]]; then
    focused_rerun="$dump"
  fi
done
if [[ -z "$focused_rerun" ]]; then
  echo "the focused KRO-2 run did not produce a rerun dump" >&2
  exit 1
fi
python3 "$scenario_dir/check_outcome.py" "$focused_rerun" --json \
  >"$output_root/focused/outcome.json"

if [[ "$mode" == "full" ]]; then
  echo "running complete KRO-2 input matrix"
  mkdir -p "$output_root/exhaustive/dumps"
  (
    cd "$harness"
    "$output_root/bin/kro-historical" \
      --inputs "$scenario_dir/k2b-exhaustive.json" \
      --output "$output_root/exhaustive/dumps" \
      --interactive=false \
      --metrics-only-staleness \
      --emit-stats
  ) >"$output_root/exhaustive/run.log" 2>&1

  : >"$output_root/exhaustive/campaign-metrics.txt"
  exhaustive_count=0
  while IFS= read -r dump; do
    exhaustive_count=$((exhaustive_count + 1))
    go run "$repo_root/cmd/kamera" analyze campaign-metrics "$dump" \
      >>"$output_root/exhaustive/campaign-metrics.txt"
  done < <(find "$output_root/exhaustive/dumps" -type f -name '*.jsonl' | sort)
  if [[ "$exhaustive_count" -eq 0 ]]; then
    echo "the full run produced no dumps" >&2
    exit 1
  fi
  echo "$exhaustive_count" >"$output_root/exhaustive/dump-count.txt"
  metrics_csv="$output_root/exhaustive/dumps/staleness_metrics.csv"
  if [[ ! -f "$metrics_csv" ]]; then
    echo "the full run did not produce staleness_metrics.csv" >&2
    exit 1
  fi
  awk 'FNR > 1 { count++ } END { print count + 0 }' "$metrics_csv" \
    >"$output_root/exhaustive/staleness-trial-count.txt"
  python3 "$scenario_dir/summarize_exhaustive.py" \
    "$output_root/exhaustive/dumps" --json \
    >"$output_root/exhaustive/summary.json"
fi

cp "$scenario_dir/dependencies.json" "$output_root/dependencies.json"
cat "$output_root/focused/outcome.json"
echo "campaign metrics: $output_root/focused/campaign-metrics.txt"
if [[ "$mode" == "full" ]]; then
  cat "$output_root/exhaustive/summary.json"
  echo "complete-matrix metrics: $output_root/exhaustive/campaign-metrics.txt"
fi
echo "wrote KRO-2 results to $output_root"
