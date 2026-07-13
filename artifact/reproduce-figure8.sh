#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_root="${1:-artifact-results/figure8-$(date +%Y%m%d-%H%M%S)}"
[[ "$output_root" = /* ]] || output_root="$PWD/$output_root"

if [[ -e "$output_root" ]]; then
  echo "refusing to overwrite existing output: $output_root" >&2
  exit 1
fi

if ! python3 -c 'import matplotlib' >/dev/null 2>&1; then
  echo "Figure 8 requires Python 3 and matplotlib." >&2
  echo "Install matplotlib in a virtual environment, then retry." >&2
  exit 1
fi

mkdir -p "$output_root"
data="$repo_root/artifact/data/figure8"
plot="$repo_root/scripts/plot_comparison.py"

python3 "$plot" \
  --runs "$data/kcp4-exhaustive.jsonl" "$data/kcp4-agent.jsonl" \
  --labels "Exhaustive" "Agent" \
  --offsets 0 68 \
  --milestones "Agent reproduces" 133 green "Exhaustive done" 1672 green \
  --legend-loc "lower right" \
  --figwidth 3.33 --figheight 0.96 \
  --xlim 1760 \
  --title "" \
  -o "$output_root/figure8a-kcp4.pdf"

python3 "$plot" \
  --runs "$data/kro2-exhaustive.jsonl" "$data/kro2-agent.jsonl" \
  --labels "Exhaustive" "Agent" \
  --milestones "Agent reproduces" 99 green "Exhaustive done" 374 green \
  --legend-loc "lower right" \
  --figwidth 3.33 --figheight 0.96 \
  --xlim 394 \
  --x-minutes \
  --annotate "Agent (102S, 279ms exec)" 99 51 55 \
  --title "" \
  -o "$output_root/figure8b-kro2.pdf"

python3 "$plot" \
  --runs "$data/kar12-exhaustive.jsonl" "$data/kar12-agent.jsonl" \
  --labels "Exhaustive" "Agent" \
  --milestones "Agent reproduces" 133 green "Timeout" 7200 red \
  --legend-loc "lower right" \
  --figwidth 3.33 --figheight 0.96 \
  --xlim 7600 \
  --annotate "Agent (1481S, 194ms exec)" 133 1481 30 \
  --title "" \
  -o "$output_root/figure8c-kar12.pdf"

printf 'panel\texperiment\tagent_reproduction_s\texhaustive_end_s\texhaustive_outcome\n' > "$output_root/figure8-summary.tsv"
printf 'a\tKCP-4\t133\t1672\tcompleted\n' >> "$output_root/figure8-summary.tsv"
printf 'b\tKRO-2\t99\t374\tcompleted\n' >> "$output_root/figure8-summary.tsv"
printf 'c\tKAR-12\t133\t7200\ttimed-out\n' >> "$output_root/figure8-summary.tsv"
cp "$data/manifest.json" "$output_root/data-manifest.json"

echo "wrote Figure 8 panels and summary to $output_root"
