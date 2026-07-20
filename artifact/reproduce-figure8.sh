#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exhaustive_source="archived"
output_root=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exhaustive-source)
      exhaustive_source="${2:-}"
      shift 2
      ;;
    --output)
      output_root="${2:-}"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
usage: ./artifact/reproduce-figure8.sh [--exhaustive-source archived|fresh] [--output DIR]

  archived  Run fresh agent-selected simulations and extract exhaustive curves
            from the checked-in raw-evidence archives (standard AE path).
  fresh     Rerun every exhaustive campaign as well. KAR-12 is capped at two
            hours and all output completed before the cap is retained.
EOF
      exit 0
      ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ "$exhaustive_source" != "archived" && "$exhaustive_source" != "fresh" ]]; then
  echo "--exhaustive-source must be archived or fresh" >&2
  exit 2
fi
if [[ -z "$output_root" ]]; then
  output_root="artifact-results/figure8-${exhaustive_source}-$(date +%Y%m%d-%H%M%S)"
fi
[[ "$output_root" = /* ]] || output_root="$PWD/$output_root"
if [[ -e "$output_root" ]]; then
  echo "refusing to overwrite existing output: $output_root" >&2
  exit 1
fi
export MPLCONFIGDIR="${MPLCONFIGDIR:-${TMPDIR:-/tmp}/kamera-matplotlib-cache}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-${TMPDIR:-/tmp}/kamera-cache}"
mkdir -p "$MPLCONFIGDIR" "$XDG_CACHE_HOME"
if ! python3 -c 'import matplotlib' >/dev/null 2>&1; then
  echo "Figure 8 requires Python 3 and the pinned matplotlib dependency." >&2
  echo "See artifact/figure8/README.md for the virtual-environment command." >&2
  exit 1
fi

mkdir -p "$output_root"
"$repo_root/artifact/run-figure8-simulations.sh" "$output_root/simulations"

if [[ "$exhaustive_source" == "archived" ]]; then
  archive_dir="$repo_root/artifact/data/figure8/raw"
  python3 "$repo_root/artifact/figure8/verify_raw_archives.py" "$archive_dir"
  mkdir -p "$output_root/raw"
  for archive in kcp4 kro2 kar12; do
    tar -xzf "$archive_dir/$archive.tar.gz" -C "$output_root/raw"
  done
  kcp_exhaustive_log="$output_root/raw/kcp4/run.log"
  kro_exhaustive_dir="$output_root/raw/kro2"
  kar_exhaustive_dir="$output_root/raw/kar12"
else
  KAMERA_AE_FIGURE8_BIN_DIR="$output_root/simulations/bin" \
    "$repo_root/artifact/run-figure8-exhaustive.sh" all "$output_root/exhaustive"
  kcp_exhaustive_log="$output_root/exhaustive/kcp4/run.log"
  kro_exhaustive_dir="$output_root/exhaustive/kro2/exhaustive/dumps"
  kar_exhaustive_dir="$output_root/exhaustive/kar12"
fi

curves="$output_root/curves"
python3 "$repo_root/artifact/figure8/extract_curves.py" \
  --kcp-exhaustive-log "$kcp_exhaustive_log" \
  --kcp-agent-log "$output_root/simulations/kcp4/run.log" \
  --kro-exhaustive-dir "$kro_exhaustive_dir" \
  --kro-agent-log "$output_root/simulations/kro2/focused/run.log" \
  --kar-exhaustive-dir "$kar_exhaustive_dir" \
  --kar-agent-log "$output_root/simulations/kar12/run.log" \
  --output "$curves"

python3 "$repo_root/artifact/figure8/plot_figure8.py" \
  --curves "$curves" \
  --output "$output_root/figure8.pdf"

python3 "$repo_root/artifact/figure8/write_report.py" \
  --curves "$curves/curve-summary.json" \
  --simulations "$output_root/simulations/simulation-summary.json" \
  --exhaustive-source "$exhaustive_source" \
  --markdown "$output_root/figure8-report.md" \
  --tsv "$output_root/figure8-report.tsv"

cp "$repo_root/artifact/figure8/dependencies.json" "$output_root/dependencies.json"
if [[ "$exhaustive_source" == "archived" ]]; then
  cp "$repo_root/artifact/data/figure8/raw/manifest.json" "$output_root/raw-archive-manifest.json"
fi
printf 'Figure 8 written to: %s\n' "$output_root/figure8.pdf"
printf 'Numerical report written to: %s\n' "$output_root/figure8-report.md"
