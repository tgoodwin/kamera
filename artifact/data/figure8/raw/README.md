# Figure 8 raw exhaustive evidence

These checksummed archives are the default exhaustive-side inputs for
`artifact/reproduce-figure8.sh`:

- `kcp4.tar.gz` contains the surviving raw log and dump from the paper run;
- `kro2.tar.gz` contains a regenerated full matrix whose summary matches all
  paper invariants; and
- `kar12.tar.gz` contains a regenerated run stopped at the paper's two-hour
  limit, including all output completed before the cutoff.

`manifest.json` records each archive checksum and the checksum and size of
every member. The reproducer verifies the archives before extraction and then
derives plot-ready JSONL curves with `artifact/figure8/extract_curves.py`.
The plotter never consumes a precomputed curve as the exhaustive evidence.

The exact source, controller, adapter, and scenario pins are recorded in
`artifact/figure8/dependencies.json`. Evaluators can replace these archives
with fresh computation by running:

```bash
./artifact/reproduce-figure8.sh --exhaustive-source fresh
```
