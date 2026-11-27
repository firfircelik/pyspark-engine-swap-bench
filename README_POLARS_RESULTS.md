# README section draft: Polars Phase 2 results (Nov 2026)

Paste or adapt under the Polars / how-to-reproduce part of README.md. Not published. Do not push until approved.

---

## Polars line (Phase 2 protocol, November 2026)

The merged Polars runner lives in `scripts/bench2_polars.py` and the `polars)` case of `scripts/run_engine2.sh`. It must run from `venv-polars` (`polars==2.2.0` only), not `venv-sail` (commit 04eaba5). Export `BENCH_S3_KEY` and `BENCH_S3_SECRET` for MinIO conditions; the script reads those names directly.

A two-rep matrix at SF10 and SF30, matching the published DuckDB cells (local, MinIO loopback, toxiproxy 0 ms and 10 ms for TPC-H; loopback and 10 ms for ETL), was run on 27 November 2026 on the original study box under the 9 GiB cgroup cap. Raw JSONL, medians, hash checks and the E6 probe are in `results/polars/` (`TABLES.md` is the summary). SF100 was not run. Published Phase 2 totals already dropped rep 3, so this line used two reps.

Headline figures, median total TPC-H wall in seconds (n=2), with published Phase 2 comparators at SF30:

- SF30 local: Polars 60.975, DuckDB 48.412, Sail (no reorder) 49.584, Comet 4.2.0 default 237.253, Spark 4.2.0 default 386.568.
- SF30 MinIO loopback: Polars 72.550, DuckDB 55.330, Sail (reorder) 71.980.
- SF30 MinIO + 10 ms: Polars 86.597, DuckDB 98.582, Sail about 121-122.

All 22 TPC-H hashes, including ordered hashes, matched DuckDB at both scales. ETL E1-E5 digests matched. E6 matched row counts but not the digest: DuckDB `CAST(... AS DECIMAL(15,2))` rounds half-up and Polars SQL CAST rounds half-even, which disagreed on 11416 of 22812 half-cent ties at SF10 and 34153 of 68330 at SF30 (Polars always 0.01 lower on those ties). Peak tree RSS for Polars was 2.921 GB at SF10 and 6.486 GB at SF30, with no OOM and no spill files. That does not by itself prove Polars reads `memory.max`.

SF10 has a same-session DuckDB reference in `results/polars/sf10/` (other engines have no Phase 2 matrix at SF10). The first Polars SF10 loopback TPC-H rep (55.728 s) was a cold-cache outlier after the SF30 MinIO upload; rep 2 was 15.956 s.
