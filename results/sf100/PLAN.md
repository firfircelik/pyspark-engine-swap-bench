# SF100 scale-curve extension

Started 2026-11-26 10:58 Europe/Istanbul. Firat approved the reduced plan and the MinIO delete (10:51 TR).

## What is being measured

Local-file TPC-H at scale factor 100, five queries (Q1, Q3, Q6, Q12, Q18), two reps, four engines:

- DuckDB 2.0.1
- Sail 0.7.3 with join reorder on
- Comet 1.2.0 on Spark 4.2.0 (`comet420-classic`)
- Spark 4.2.0 (`spark420-classic`)

`classic` is an S3A stream type and does not apply to `file://`. The names follow the approved engine list. The size-curve table maps them to Phase 2 local `spark420-default` / `comet420-default`.

## Data

- Generator: `tpchgen-cli` 4.0.0, `parquet -s 100 --parts=80 --compression=SNAPPY`
- Path: `data/tpch-sf100` (39 GB). Partitioning style matches SF10 (8 parts) and SF30 (24 parts): parts scale with SF, nation/region stay at 1 file, lineitem part size ~324 MB (SF30 was ~316 MB).
- Warmup: `data/tpch-sf1`, untimed
- No MinIO

## Runner

Existing `scripts/run_engine2.sh`, `bench2_spark_api.py`, `bench2_duckdb.py`, `run_one.py`, `orchestrate.py sf100`.

- 9 GiB cgroup, swap 0
- Comet: `MALLOC_ARENA_MAX=2`, driver 2g + off-heap 3g
- Spark: driver 6g, local[8]
- GC between queries, 20-min no-progress stall kill, per-job Spark temp cleanup
- Job TIMEOUT=7200 s (backstop for an SF100 5-query session; stall kill remains 20 min)

## Job order (randomized, balanced, same seed scheme as Phase 2)

`random.Random(1000+rep).shuffle` per rep. Each rep contains each engine once.

- Rep 1: ['sail-reorder', 'comet420-classic', 'spark420-classic', 'duckdb']
- Rep 2: ['sail-reorder', 'comet420-classic', 'duckdb', 'spark420-classic']

Query order inside a job is always 1,3,6,12,18.

## Hash check

DuckDB `hash_set` from valid SF100 runs is the baseline. Other engines are compared to it (values normalized to 2 decimals).

## Size curve

SF10 from `results/pilot-sf10-r2`; SF30 from published `results/final/phase2_query_medians.csv` (Phase 2 local file, final ok attempts). Condition differences are flagged in the table, not hidden.

## Disk

Approved MinIO delete already done. Keep >15 GB free. Do not delete `data/` Parquet or `results/`. Memory errors and stalls are findings; do not tune.

## Timed-run data path (added 2026-11-26 11:07 TR)

Host snapshot sidecars (`*.parquet.box-store-snap-*`, owner `sand-host`) appeared in `data/tpch-sf100` and broke Spark/Sail directory listings. Canonical generator output stays there. Timed jobs used `TABLEBASE=file:///tmp/engine-swap-data/tpch-sf100` (copy on `/dev/vdc`, verified DuckDB row counts). `bench2_spark_api.py` now globs `*.parquet`. Pre-fix attempts are in `superseded.json`.
