# Keep your PySpark code, swap the engine

A controlled single-node study of **Apache Spark 4.1.3 / 4.2.0, Apache DataFusion Comet 1.2.0, Sail 0.7.3 and DuckDB 2.0.1** on speed, correctness, object-store latency (S3A classic stream vs the Analytics Accelerator Library), table formats (Iceberg, Delta) and cost.

- Paper (20 pages, all tables, models, proofs and source locations): [`paper/paper.pdf`](paper/paper.pdf) (source: [`paper/paper.md`](paper/paper.md))
- Author: Firat Celik, Senior Data Platform Architect

## Research questions

- **RQ1 (speed).** With identical job code (Comet, Sail) or identical SQL (DuckDB), how does wall time compare to Spark on the same node, memory cap and data, and how certain are we given the number of repetitions?
- **RQ2 (object storage).** How does the S3A input stream (classic vs AAL) interact with request latency, and at what latency does AAL start to pay off?
- **RQ3 (correctness).** Where do results diverge from Spark, why, and can the divergence be predicted from first principles?
- **RQ4 (operations and cost).** What do table-format support, memory behavior and pricing imply for an engine choice?

## Headline results (TPC-H SF30, 22 queries, 9 GiB cgroup cap, n=2 per cell)

All 22 TPC-H results agreed across engines, S3A stream types and storage conditions (hashes over values normalized to two decimals; the oracle is cross-engine agreement).

| Comparison (A vs B) | Total time ratio A/B | 95% CI, engine-stratified Welch (or [min, max] of rep ratios) | Queries where both A runs beat both B runs |
|---|---|---|---|
| Sail (join reorder) vs Spark 4.2.0, MinIO loopback | 0.164 | 0.129 to 0.207 | 22/22 |
| DuckDB vs Spark 4.2.0, MinIO loopback (same SQL text) | 0.126 | 0.114 to 0.139 | 22/22 |
| Comet vs Spark 4.2.0, MinIO loopback | 0.640 | 0.548 to 0.747 | 18/22 |
| Spark 4.2.0 AAL vs classic, +10 ms latency | 0.758 | 0.723 to 0.794 | 22/22 |
| Spark 4.2.0 AAL vs classic, loopback | 1.009 | 0.963 to 1.057 | 5/22 (2/22 slower) |
| Spark 4.1.3 AAL vs classic, loopback | 1.176 | 1.087 to 1.272 | 0/22 (15/22 slower) |
| DuckDB vs Spark 4.2.0, SF100 (5 queries, local files) | 0.171 | [0.109, 0.240], no CI | 5/5 |

- Holm correction is applied across the ten comparisons of the paper's Table 1; all SF30 rows above except Spark 4.2.0 AAL on loopback stay below 0.05.
- AAL cut GET requests by 54.6% on Spark 4.2.0 and 40.2% on 4.1.3. A two-point interpolation between 0 and 10 ms of added latency puts the AAL break-even at -0.06 ms on Spark 4.2.0 (95% interval -1.9 to 1.3 ms) and 2.5 ms on 4.1.3 (-3.5 to 6.7 ms); linearity in latency is untested. Comet's native scan does not use the S3A stream, so AAL does not change its GETs.
- Correctness edge cases that TPC-H in UTC does not exercise: stale time zone data (one hour off in four probed zones on Sail; 23 of 70 probe rows in five of seven zones on Comet), a double-to-decimal cast on Sail that is one cent off on 631 of 683,346 rows (a binary64 model predicts exactly the same 631 rows; keys in `article/academic/out/f1_mismatch_orderkeys.csv`), `format_number` on DECIMAL through binary64, exponent strings to DECIMAL, a JSON permissive read that fails on decimal overflow, and an empty DataFrame write that loses the schema. Minimal repros: [`repro/minimal/`](repro/minimal).
- Operations: no `iceberg-spark-runtime-4.2` exists yet; the published Comet 1.2.0 jar does not accelerate Delta; Comet needs `MALLOC_ARENA_MAX=2` to stay inside the cap (with a speed cost we did not isolate); Sail's Q18 was OOM-killed in 3 of 8 SF30 attempts on local files (0 of 14 through MinIO) and in 6 of 6 at SF100.

## Limitations (read these before quoting a number)

- **n=2 repetitions per cell.** With n=2 the smallest exact two-sided Mann-Whitney p is 1/3, so no run-level rank test can be significant and no per-query comparison survives Holm correction. The intervals above pool run-to-run variance within each engine; a cell-only interval (one degree of freedom per side) still excludes 1 for Sail, DuckDB and AAL at +10 ms, but not for Comet or Spark 4.1.3 AAL. The paper lists which cells need 5 repetitions (about 7.1 box hours); they were not run.
- **Survivorship and protocol deviations.** Published tables use only attempts that completed without error or freeze. There were 15 query errors across all attempts (12 on Spark), one cell's rep 2 was a manual fourth attempt, the box was restored from a snapshot mid-matrix, the freeze rule changed mid-matrix, and rep 3 was dropped after stalls and errors. Paper Section 3.4; adding back the complete rep-3 runs changes no conclusion.
- **"AAL has no effect on loopback" is a failure to detect,** not evidence of no effect (the interval allows a 4% gain or a 6% loss).
- **SF10 pilot is not comparable** with SF30/SF100: a single run on MinIO loopback, without the memory cap and with DuckDB's file cache on. Scaling exponents are descriptive; SF30 fits largely in page cache and SF100 does not, and caches were not dropped.
- One node, no network shuffle: nothing here says anything about workloads where distributed Spark should win. MinIO behind a fixed toxiproxy delay is not S3. DuckDB ran SQL, not PySpark, and its ETL arithmetic is decimal where PySpark's is double. Cost figures multiply open-source Spark times by list prices; Databricks Runtime, Photon, the Fabric runtime and Fabric NEE were not measured.

## Repository layout

| Path | What |
|---|---|
| `scripts/` | Harness: `run_one.py` (cgroup placement, RSS sampler, freeze detector, timeouts), `run_engine2.sh` (per-engine launch), `orchestrate.py` (seeded shuffled matrix, resumable), `bench2_spark_api.py` (one PySpark file for Spark, Comet and Sail), `bench2_duckdb.py`, `verify_etl.py`, aggregation and finalize scripts, table-format and SF100 runners |
| `queries/` | The 22 TPC-H queries as run |
| `repro/` | Minimal correctness repros with their outputs on Spark 4.2.0 and Sail 0.7.3 |
| `probes/` | API compatibility probes |
| `results/<study>/` | Raw per-run logs: `queries.jsonl` (per-query wall time, S3 GET/HEAD counts, result hashes), `summary.jsonl`, `state.jsonl`, `plan.json`, RSS samples, `dropped_reps.json` |
| `results/final/`, `results/tablefmt/final/`, `results/sf100/final/` | Published tables (`TABLES.md`), long-format CSVs, hash checks, GET reductions, `f4_events.csv`, Comet fallback reports |
| `article/academic/analysis/` | `stats.py`, `models.py`, `ieee.py`, `f1_rows.py`, `deviations.py`, `tz.py`, `comet_mem.py`, `figs.py`; outputs in `out/`. Root from `ENGINE_SWAP_ROOT` (default: the checkout) |
| `article/` | Cost tables, migration checklist, figures and `make_figures.py` |
| `paper/` | `paper.md`, `paper.pdf`, figures |
| `VERSIONS.md` | Every version, engine setting and jar checksum |

Per-row query outputs, Parquet data, jars and engine logs are not included.

## How to reproduce

Everything was installed natively on one Linux VM; no Docker. Scripts assume the checkout lives at `/workspace/engine-swap` (the shell scripts accept `ROOT=...`; the Python scripts hard-code that path, so the simplest route is `ln -s $(pwd) /workspace/engine-swap`). Installs, data, jars and logs go under that root and are git-ignored.

1. **Install** (versions in `VERSIONS.md`): Temurin JDK 21 into `$ROOT/opt/`; Spark 4.1.3 and 4.2.0 binary tarballs into `$ROOT/opt/`, then add hadoop-aws, the AWS SDK bundle and AAL jars to each `jars/`; Comet, Iceberg and Delta jars into `$ROOT/dist/` (`scripts/fetch_tablefmt_jars.sh`); two Python 3.12 venvs, `venv-spark` (pyarrow, pandas, psutil) and `venv-sail` (`pysail==0.7.3`, `pyspark-client==4.2.0`, `duckdb==2.0.1`, pyarrow, pandas, psutil).
2. **Data:** `cargo install tpchgen-cli --version 4.0.0`, then `tpchgen-cli -s 30 --format=parquet --parts=24 --compression=SNAPPY --output-dir=$ROOT/data/tpch-sf30` (SF100: `-s 100 --parts=80`; SF1 for warmup).
3. **Object store:** build MinIO `RELEASE.2025-10-15T17-29-55Z` and `mc` from source into `$ROOT/opt/bin`, choose local credentials and `export BENCH_S3_KEY=... BENCH_S3_SECRET=...` (scripts read them from the environment), run `scripts/minio-start.sh` (127.0.0.1:9000), create bucket `tpch` and copy `data/tpch-sf30` and `data/tpch-sf1` into it with `mc cp --recursive`.
4. **Latency:** toxiproxy 2.13.0 server on 127.0.0.1:8474; `orchestrate.py` creates proxy `minio_lat` (127.0.0.1:9100 to 9000) and sets a 0 ms or 10 ms downstream `latency` toxic per condition.
5. **Memory cap:** `sudo mkdir /sys/fs/cgroup/engine-swap-bench && echo 9G | sudo tee .../memory.max && echo 0 | sudo tee .../memory.swap.max`. `run_one.py --cgroup` moves each job into it.
6. **Run:** `venv-sail/bin/python scripts/orchestrate.py` (Phase 2 matrix from `results/phase2/plan.json`, resumable from `state.jsonl`), then `scripts/aggregate2.py` and `scripts/finalize_tables.py`. Table formats: `scripts/write_tablefmt.sh`, `scripts/orchestrate_tablefmt.py`, `scripts/aggregate_tablefmt.py`. SF100: `scripts/run_sf100.sh`. Correctness repros: `repro/minimal/run_all.sh`.
7. **Analysis and paper:** from `article/academic/analysis/`, run `stats.py`, `deviations.py`, `models.py`, `ieee.py`, `tz.py`, `comet_mem.py`, `figs.py` in that order (`f1_rows.py` needs MinIO with the E6 ETL outputs and `BENCH_S3_KEY`/`BENCH_S3_SECRET`; `ieee.py` and `f1_rows.py` need `duckdb`). `figs.py` writes to `article/academic/figures/`; copy them to `paper/figures/`, then `pandoc paper/paper.md -o paper/paper.pdf --pdf-engine=tectonic --resource-path=paper`.

Run one benchmark at a time on an otherwise idle machine; the freeze detector reruns any attempt with a sampler gap over 2 s.

## Changelog

- **2026-11-26 (revision after technical review).** Totals now use engine-stratified Welch intervals instead of one pooled variance (the Spark 4.1.3 AAL slowdown on loopback is now supported at the total level); Holm across the ten headline comparisons; AAL-isolation and SF100 rows reported as [min, max] ratios only; post-hoc power removed; AAL break-even presented as a two-point interpolation with a wider interval; Sail SF~32 OOM extrapolation removed in favor of observed attempt counts; F1 checked row by row against archived engine outputs (631 of 631 rows match the binary64 model); new protocol-deviations and survivorship section with a rep-3 sensitivity analysis; corrected local-disk speed claim (DuckDB and Sail equal within noise), Comet time-zone count (5 of 7 zones), scaling labels, IEEE details (JDK 19+ dependency, cent resolution from 2^46), cost sources and assumptions, threats to validity and bibliography. `analysis/common.py` takes its root from `ENGINE_SWAP_ROOT`. New scripts: `f1_rows.py`, `deviations.py`, `tz.py`, `comet_mem.py`.
- **2026-11-26.** Initial public release.

## License

Code: MIT (see `LICENSE`). Measured results and the paper may be reused with attribution.

Contributions adding engines are welcome; see `CONTRIBUTING.md`.
