#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_root="${1:-artifact-results/table6-$(date +%Y%m%d-%H%M%S)}"
[[ "$output_root" = /* ]] || output_root="$PWD/$output_root"

if [[ -e "$output_root" ]]; then
  echo "refusing to overwrite existing output: $output_root" >&2
  exit 1
fi
mkdir -p "$output_root"

experiments=(
  zk/stale-state-1
  zk/stale-state-2
  zk/unobserved-state-1
  zk/indirect-1
  rmq/stale-state-1
  rmq/stale-state-2
  rmq/unobserved-state-1
  rmq/intermediate-state-1
  cass/stale-state-1
  cass/intermediate-state-1
  cass/intermediate-state-2
)

tsv="$output_root/table6.tsv"
printf 'experiment\tpaper_kamera_ms\tobserved_ms\tstatus\tresult_json\n' >"$tsv"
for experiment in "${experiments[@]}"; do
  "$repo_root/artifact/run-experiment.sh" "$experiment" "$output_root" | tee -a "$tsv"
done

awk -F '\t' '
  BEGIN {
    print "# Table 6: perturbed simulation execution time"
    print ""
    print "Only the Kamera perturbed-run duration is measured. Sieve cluster time and any baseline run are excluded."
    print ""
    print "| Experiment | Paper Kamera (ms) | Observed (ms) | Observed / paper | Status |"
    print "|---|---:|---:|---:|---|"
  }
  NR > 1 {
    ratio = $3 / $2
    printf "| %s | %.0f | %.3f | %.2fx | %s |\n", $1, $2, $3, ratio, $4
    logsum += log(ratio)
    count++
  }
  END {
    print ""
    if (count > 0) printf "Geometric mean observed/paper ratio: %.2fx\n", exp(logsum/count)
  }
' "$tsv" >"$output_root/table6.md"

echo "wrote $output_root/table6.md"
