# Figure 8 data packaging

Figure 8 compares archived exhaustive-exploration coverage with the first
agent-guided reproduction for KCP-4, KRO-2, and KAR-12. The standard artifact
workflow redraws the figure from checked-in coverage samples and does not
require access to an LLM.

The files under `artifact/data/figure8` contain only the four fields consumed
by `scripts/plot_comparison.py`: timestamp, total states, distinct states, and
resource states. Long curves are evenly reduced to at most 600 samples while
preserving their first and last samples. `manifest.json` records source hashes,
sample counts, and endpoints.

`prepare_data.py` is a maintainer tool for rebuilding the compact inputs from
the original logs. It is not needed by artifact evaluators. The source-log
provenance and original experiment commands are documented in
`experiments/coverage-curves/MANIFEST.md` and the project-specific experiment
notes.

## Rerunning the KRO-2 simulations

KRO-2 can additionally be rerun with the exact focused and exhaustive scenario
files used for the paper. The dependency setup makes a local clone of Kamera at
the first commit that captures the experiment's bounded-reference planner
fallback, checks out the upstream KRO base, and applies the five small
simulation-integration changes that existed in the author's experiment
checkout:

```bash
./artifact/setup-figure8-kro-deps.sh
./artifact/run-figure8-kro-historical.sh focused
```

The focused run is the fixed simulator execution selected by the agent. It
requires no model and writes a complete dump, a campaign-metrics report, and
`outcome.json`. Its historical depth-50 trace reaches the reported observable
outcome but ends at the configured maximum depth; the checker records zero
converged states and does not relabel the partial trace as converged.

The complete paper-era input matrix is optional:

```bash
./artifact/run-figure8-kro-historical.sh full
```

This runs the 15 entries in `k2b-exhaustive.json` with the experiment-time
`--metrics-only-staleness` mode. The expected layout is 30 full
reference/rerun dumps plus 1,680 lightweight staleness rows in
`staleness_metrics.csv`; the script applies `campaign-metrics` to every full
dump. It may take several minutes. The checked-in Figure 8 samples remain the
canonical representation of the recorded timing curve because the 99-second
agent milestone includes the historical model-inference interval, which cannot
be recreated by rerunning only the deterministic simulation.

The full workflow writes `exhaustive/summary.json` and succeeds only when the
run matches the archived campaign invariants: 30 dumps, 1,680 lightweight
trials, 54,418 total node visits, and 131 global resource states. Durations are
reported but deliberately not asserted because wall-clock time varies by host.

`kro-historical/dependencies.json` pins the commits, scenario hashes, adapter
hash, and the relationship to draft PR
[tgoodwin/kamera#83](https://github.com/tgoodwin/kamera/pull/83). That PR is a
post-paper schema-backed apply prototype. The historical rerun is evidence for
the paper snapshot; it is not a claim about that later implementation.

The archived March 30 log shows 112 derived staleness intervals for each input,
but the corresponding bounded-reference fallback was first committed in
`1c85e5b` on April 25. The manifest therefore records both the March 29 base
commit and this first reconstructible commit. This is evidence that the
experiment ran from a working tree whose planner change was committed later,
not that the April commit introduced a different experiment.
