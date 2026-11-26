# Keep your PySpark code, swap the engine

A controlled single-node study of **Apache Spark 4.1.3 / 4.2.0, Apache DataFusion Comet 1.2.0, Sail 0.7.3 and DuckDB 2.0.1** on speed, correctness, object-store latency (S3A classic stream vs the Analytics Accelerator Library), table formats (Iceberg, Delta) and cost.

- Paper (15 pages, all tables, models, proofs and source locations): [`paper/paper.pdf`](paper/paper.pdf) (source: [`paper/paper.md`](paper/paper.md))
- Author: Firat Celik, Senior Data Platform Architect

## Research questions

- **RQ1 (speed).** With identical job code (Comet, Sail) or identical SQL (DuckDB), how does wall time compare to Spark on the same node, memory cap and data, and how certain are we given the number of repetitions?
- **RQ2 (object storage).** How does the S3A input stream (classic vs AAL) interact with request latency, and at what latency does AAL start to pay off?
- **RQ3 (correctness).** Where do results diverge from Spark, why, and can the divergence be predicted from first principles?
- **RQ4 (operations and cost).** What do table-format support, memory behavior and pricing imply for an engine choice?

## Headline results (TPC-H SF30, 22 queries, 9 GiB cgroup cap, n=2 per cell)

All 22 TPC-H results were hash-identical across engines, S3A stream types and storage conditions.

| Comparison (A vs B) | Total time ratio A/B | Pooled-variance 95% CI | Faster queries |
|---|---|---|---|
| Sail (join reorder) vs Spark 4.2.0, MinIO loopback | 0.164 | 0.137 to 0.195 | 22/22 |
| DuckDB vs Spark 4.2.0, MinIO loopback (same SQL) | 0.126 | 0.105 to 0.150 | 22/22 |
| Comet vs Spark 4.2.0, MinIO loopback | 0.640 | 0.536 to 0.763 | 21/22 |
| Spark 4.2.0 AAL vs classic, +10 ms latency | 0.758 | 0.635 to 0.904 | 22/22 |
| Spark 4.2.0 AAL vs classic, loopback | 1.009 | 0.846 to 1.204 | 9/22 |
| Spark 4.1.3 AAL vs classic, loopback | 1.176 | 0.986 to 1.403 | 1/22 |
| DuckDB vs Spark 4.2.0, SF100 (5 queries, local files) | 0.171 | 0.144 to 0.205 | 5/5 |

- AAL cut GET requests by 54.6% on Spark 4.2.0 and 40.2% on 4.1.3. A request-latency model puts the AAL break-even at -0.06 ms of added latency on Spark 4.2.0 (bootstrap -0.71 to 0.52 ms) and 2.45 ms on 4.1.3 (0.69 to 4.42 ms). Comet's native scan does not use the S3A stream, so AAL does not change its GETs.
- Correctness edge cases that TPC-H in UTC does not exercise: stale time zone data (one hour wrong in four zones on Sail and Comet), a double-to-decimal cast on Sail that is one cent off on 631 of 683,346 rows (reproduced exactly from IEEE 754 binary64 arithmetic), `format_number` on DECIMAL through binary64, exponent strings to DECIMAL, a JSON permissive read that fails on decimal overflow, and an empty DataFrame write that loses the schema. Minimal repros: [`repro/minimal/`](repro/minimal).
- Operations: no `iceberg-spark-runtime-4.2` exists yet; Comet 1.2.0 does not accelerate Delta; Comet needs `MALLOC_ARENA_MAX=2` to stay inside the cap; Sail's Q18 was OOM-killed at the 9 GiB cap at SF100 in 6 of 6 attempts (a power-law fit places the threshold near SF 32).

## Limitations (read these before quoting a number)

- **n=2 repetitions per cell.** A third repetition was dropped for operational reasons (repeated Spark stalls), not after looking at results. With n=2 the smallest exact two-sided Mann-Whitney p is 1/3, so no run-level rank test can be significant and no per-query comparison survives Holm correction. The intervals above use a pooled run-to-run variance (33 cells) and the query-level Wilcoxon test generalizes over queries, not runs. The paper lists which cells need 5 repetitions (about 7 box hours); they were not run.
- **"AAL has no effect on loopback" is a failure to detect,** not evidence of no effect (the interval allows a 15% gain or a 20% loss).
- **SF10 pilot is not comparable** with SF30/SF100: it was a single run on MinIO loopback, without the memory cap and with DuckDB's file cache on. The scaling exponents that use it are flagged as such; SF30 to SF100 is the more comparable pair.
- One node, no network shuffle: nothing here says anything about workloads where distributed Spark should win. MinIO behind a fixed toxiproxy delay is not S3. DuckDB ran SQL, not PySpark. Cost figures multiply open-source Spark times by list prices; Photon and Fabric NEE were not measured.

## Repository layout

| Path | What |
|---|---|
| `scripts/` | Harness: `run_one.py` (cgroup placement, RSS sampler, freeze detector, timeouts), `run_engine2.sh` (per-engine launch), `orchestrate.py` (seeded shuffled matrix, resumable), `bench2_spark_api.py` (one PySpark file for Spark, Comet and Sail), `bench2_duckdb.py`, `verify_etl.py`, aggregation and finalize scripts, table-format and SF100 runners |
| `queries/` | The 22 TPC-H queries as run |
| `repro/` | Minimal correctness repros with their outputs on Spark 4.2.0 and Sail 0.7.3 |
| `probes/` | API compatibility probes |
| `results/<study>/` | Raw per-run logs: `queries.jsonl` (per-query wall time, S3 GET/HEAD counts, result hashes), `summary.jsonl`, `state.jsonl`, `plan.json`, RSS samples, `dropped_reps.json` |
| `results/final/`, `results/tablefmt/final/`, `results/sf100/final/` | Published tables (`TABLES.md`), long-format CSVs, hash checks, GET reductions, `f4_events.csv`, Comet fallback reports |
| `article/academic/analysis/` | `stats.py`, `models.py`, `ieee.py`, `figs.py`; outputs in `out/` |
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
7. **Analysis and paper:** `python3 article/academic/analysis/{stats,models,ieee,figs}.py`, then `pandoc paper/paper.md -o paper/paper.pdf --pdf-engine=tectonic --resource-path=paper`.

Run one benchmark at a time on an otherwise idle machine; the freeze detector reruns any attempt with a sampler gap over 2 s.

## License

Code: MIT (see `LICENSE`). Measured results and the paper may be reused with attribution.

Contributions adding engines are welcome; see `CONTRIBUTING.md`.
