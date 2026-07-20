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
printf 'experiment\tpaper_kamera_ms\tpaper_sieve_s\tobserved_ms\tstatus\tresult_json\n' >"$tsv"
for experiment in "${experiments[@]}"; do
  "$repo_root/artifact/run-experiment.sh" "$experiment" "$output_root" | tee -a "$tsv"
done

awk -F '\t' '
  BEGIN {
    print "# Table 6: bug reproduction time"
    print ""
    print "Only the Kamera perturbed-run duration is freshly measured. Sieve times are the paper baselines and are not rerun."
    print ""
    print "| Controller | Bug | Sieve | Kamera | Speedup |"
    print "|---|---|---:|---:|---:|"
  }
  NR > 1 {
    split($1, experiment, "/")
    if (experiment[1] == "zk") controller = "ZooKeeper"
    else if (experiment[1] == "rmq") controller = "RabbitMQ"
    else if (experiment[1] == "cass") controller = "Cassandra"
    else controller = experiment[1]

    speedup = ($3 * 1000) / $4
    printf "| %s | %s | %.0f s | %.3f ms | %.0fx |\n", controller, experiment[2], $3, $4, speedup
    speedup_logsum += log(speedup)
    count++
  }
  END {
    if (count > 0) {
      printf "| **Geometric mean** |  |  |  | **%.0fx** |\n", exp(speedup_logsum/count)
    }
  }
' "$tsv" >"$output_root/table6.md"

printf '\nTable 6 reproduction summary\n\n'
cat "$output_root/table6.md"
printf '\nFull results written to: %s\n' "$output_root"
