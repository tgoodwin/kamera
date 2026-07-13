# Figure 8 data packaging

Figure 8 compares archived exhaustive-exploration coverage with the first
agent-guided reproduction for KCP-4, KRO-2, and KAR-12. The standard artifact
workflow redraws the figure from checked-in coverage samples; it does not rerun
the multi-hour exhaustive campaigns or require access to an LLM.

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
