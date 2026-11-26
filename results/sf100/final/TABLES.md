# SF100 extension results

Generated 2026-11-26 11:35:25 Europe/Istanbul. Numbers are read from on-disk result files. Nothing here is estimated.

## Data

- `tpchgen-cli` 4.0.0, `parquet -s 100 --parts=80 --compression=SNAPPY`, 121.111 s. Canonical files: `data/tpch-sf100` 39 GB (nation/region 1 file; other tables 80 parts; lineitem parts 324 MB).
- Timed runs read a copy at `/tmp/engine-swap-data/tpch-sf100` on `/dev/vdc` (not the workspace disk). Reason: host snapshot sidecars named `*.parquet.box-store-snap-*` appeared in the workspace directory and were picked up by Spark/Sail directory listings. `bench2_spark_api.py` was also changed to glob `*.parquet`, matching DuckDB. Warmup remains `data/tpch-sf1`.
- 9 GiB cgroup, swap 0. Comet `MALLOC_ARENA_MAX=2`. 20-min stall kill. Job TIMEOUT 7200 s. No stalls or timeouts occurred after the path fix.

## Hash check (SF100 `hash_set` vs DuckDB baseline)

Baseline (DuckDB `hash_set`):

| query | hash_set |
|---|---|
| Q1 | `44436d26ae1aaca4` |
| Q3 | `8431b1787d0bfc9e` |
| Q6 | `120f2e2641aca18e` |
| Q12 | `1c6cd5758b87b163` |
| Q18 | `0df1e1a9e59b48c8` |

| engine | rep | Q1 | Q3 | Q6 | Q12 | Q18 |
|---|---|---|---|---|---|---|
| duckdb | 1 | match | match | match | match | match |
| duckdb | 2 | match | match | match | match | match |
| sail-reorder | 1* | match* | match* | match* | match* | OOM / no result |
| sail-reorder | 2* | match* | match* | match* | match* | OOM / no result |
| comet420-classic | 1 | match | match | match | match | match |
| comet420-classic | 2 | match | match | match | match | match |
| spark420-classic | 1 | match | match | match | match | match |
| spark420-classic | 2 | match | match | match | match | match |

Every completed timing query (including Sail Q1/Q3/Q6/Q12 in sessions that then died on Q18) matched the DuckDB baseline. A `match*` cell is from an incomplete session.

## SF100 per-query wall time (seconds)

Complete 5-query sessions for DuckDB, Comet, and Spark 4.2 (n=2). Sail 0.7.3 with join reorder never finished Q18: all 6 post-fix attempts (3 per rep) were OOM-killed by the 9 GiB cgroup during Q18 (peak tree RSS 8.7–9.6 GB). Sail Q1/Q3/Q6/Q12 times below are the median of those 6 successful query executions, flagged with `*`. Config was not retuned.

| engine | Q1 median (min–max, n) | Q3 | Q6 | Q12 | Q18 | peak RSS GB median |
|---|---|---|---|---|---|---|
| duckdb | 13.822 (6.086–21.558, n=2) | 18.53 (10.077–26.984, n=2) | 2.103 (2.015–2.191, n=2) | 3.646 (3.044–4.247, n=2) | 22.662 (19.005–26.319, n=2) | 7.250 |
| sail-reorder* | 12.13 (11.312–24.647, n=6)* | 9.691 (8.491–17.806, n=6)* | 6.41 (6.159–9.045, n=6)* | 7.777 (7.285–10.561, n=6)* | OOM ×6 | 9.125 |
| comet420-classic | 21.195 (19.188–23.202, n=2) | 44.66 (44.462–44.857, n=2) | 7.139 (7.03–7.249, n=2) | 18.343 (17.941–18.745, n=2) | 77.051 (75.45–78.652, n=2) | 4.795 |
| spark420-classic | 105.422 (105.377–105.467, n=2) | 62.847 (56.296–69.399, n=2) | 8.748 (7.856–9.64, n=2) | 29.939 (29.67–30.207, n=2) | 147.352 (138.629–156.075, n=2) | 7.204 |

Raw times: `sf100_query_medians.csv` and `sf100_query_runs.csv`. DuckDB r1 was slower than r2 on Q1/Q3/Q18 (e.g. Q1 21.558 s vs 6.086 s); both are included in the median.

## SF100 5-query total (sum of medians, seconds)

| engine | n complete reps | sum of medians s |
|---|---|---|
| duckdb | 2 | 60.763 |
| sail-reorder | 0 | — (no complete 5-query run) |
| comet420-classic | 2 | 168.388 |
| spark420-classic | 2 | 354.308 |

## Failures, stalls, and memory events

Two distinct classes. Config was not retuned.

### 1. Host snapshot sidecars (superseded; not counted as engine results)

Before the path fix, Sail r1 a1–a3 and Comet r1 a1–a2 failed because the workspace data directory briefly contained `*.parquet.box-store-snap-*` files owned by `sand-host`. Spark: `FAILED_READ_FILE.FILE_NOT_EXIST`. Sail: `Invalid Parquet file. Corrupt footer` or `Object at location ...box-store-snap-... not found`. The Parquet dataset itself is valid (482 footers parse; DuckDB `lineitem` = 600,037,902). Those attempts are in `superseded.json` and were re-run from `/tmp/engine-swap-data/tpch-sf100`.

### 2. Sail Q18 OOM at the 9 GiB cap (finding, F3-style at SF100)

After the path fix, Sail 0.7.3 with join reorder completed Q1, Q3, Q6, and Q12 with hashes matching DuckDB, then the Spark Connect server was SIGKILL'd (`rc=-9`) during Q18 on all 6 attempts, with a cgroup `oom_kill` increment and peak tree RSS 8.7–9.6 GB. Same class of limit seen at SF30 (STATUS F3). No stalls and no job timeouts. DuckDB, Comet, and Spark 4.2 each have 2 complete valid reps and no post-fix query errors.

| id | attempt | status | q | wall_s | oom_this_job | error |
|---|---|---|---|---|---|---|
| r1-local-sail-reorder-tpch | 4 | qerror | Q18 | 6.609 | True | SERVER_DIED: Spark Connect server exited rc=-9 during the query (see summary cg_events: oom_kill) |
| r1-local-sail-reorder-tpch | 5 | qerror | Q18 | 4.853 | True | SERVER_DIED: Spark Connect server exited rc=-9 during the query (see summary cg_events: oom_kill) |
| r1-local-sail-reorder-tpch | 6 | qerror | Q18 | 4.869 | True | SERVER_DIED: Spark Connect server exited rc=-9 during the query (see summary cg_events: oom_kill) |
| r2-local-sail-reorder-tpch | 1 | qerror | Q18 | 6.589 | True | SERVER_DIED: Spark Connect server exited rc=-9 during the query (see summary cg_events: oom_kill) |
| r2-local-sail-reorder-tpch | 2 | qerror | Q18 | 6.117 | True | SERVER_DIED: Spark Connect server exited rc=-9 during the query (see summary cg_events: oom_kill) |
| r2-local-sail-reorder-tpch | 3 | qerror | Q18 | 5.834 | True | SERVER_DIED: Spark Connect server exited rc=-9 during the query (see summary cg_events: oom_kill) |

## Size curve: SF10 / SF30 / SF100 wall time (seconds)

This table is **not** a like-for-like speedup claim across all three scales. Conditions differ, and they are flagged in the last column and below.

- **SF10** (`results/pilot-sf10-r2`): MinIO loopback, not local file; Spark driver 8g; Comet 4g heap + 4g off-heap; no 9 GiB cgroup cap; single-run pilot (n=1). Query set was Q1, Q3, Q6, Q9, Q18 — **Q12 was not run**.
- **SF30** (`results/final/phase2_query_medians.csv`, cond=local): local Parquet, 9 GiB cgroup, Comet 2g+3g and `MALLOC_ARENA_MAX=2`, median of 2 valid reps. Timings come from **full 22-query sessions**, so Q12 and Q18 ran after many earlier queries (different memory and GC state than a 5-query SF100 session).
- **SF100** (this extension): local Parquet copied to `/tmp/engine-swap-data/tpch-sf100`, 9 GiB cgroup, 5-query sessions, median of 2 complete reps except Sail. `spark420-classic` / `comet420-classic` are the same local-file engines as Phase 2 `spark420-default` / `comet420-default` (S3A stream type does not apply to `file://`). Sail Q1/Q3/Q6/Q12 are medians from 6 incomplete sessions that OOM-killed on Q18; Sail Q18 has no successful time.
- Scale factors are 10 / 30 / 100 (3× then 3.33×). A perfectly linear scan would grow by those ratios; this is only a rough reference, not a model.

| engine | query | SF10 s (n=1, loopback pilot) | SF30 s (median, local, 22-query session) | SF100 s (median, local, 5-query session) | SF30/SF10 | SF100/SF30 |
|---|---|---|---|---|---|---|
| DuckDB 2.0.1 | Q1 | 0.752 | 8.576 | 13.822 | 11.400 | 1.610 |
| DuckDB 2.0.1 | Q3 | 0.821 | 9.630 | 18.530 | 11.730 | 1.920 |
| DuckDB 2.0.1 | Q6 | 0.139 | 0.494 | 2.103 | 3.550 | 4.260 |
| DuckDB 2.0.1 | Q12 | — | 0.733 | 3.646 | — | 4.970 |
| DuckDB 2.0.1 | Q18 | 0.445 | 2.146 | 22.662 | 4.820 | 10.560 |
| Sail 0.7.3 (reorder on) | Q1 | 0.931 | 5.308 | 12.130 | 5.700 | 2.290 |
| Sail 0.7.3 (reorder on) | Q3 | 0.837 | 2.867 | 9.691 | 3.430 | 3.380 |
| Sail 0.7.3 (reorder on) | Q6 | 0.451 | 0.910 | 6.410 | 2.020 | 7.040 |
| Sail 0.7.3 (reorder on) | Q12 | — | 2.234 | 7.777 | — | 3.480 |
| Sail 0.7.3 (reorder on) | Q18 | 4.677 | 6.691 | — | 1.430 | — |
| Comet 1.2.0 on Spark 4.2 | Q1 | 4.288 | 6.843 | 21.195 | 1.600 | 3.100 |
| Comet 1.2.0 on Spark 4.2 | Q3 | 4.591 | 13.159 | 44.660 | 2.870 | 3.390 |
| Comet 1.2.0 on Spark 4.2 | Q6 | 0.612 | 1.812 | 7.139 | 2.960 | 3.940 |
| Comet 1.2.0 on Spark 4.2 | Q12 | — | 5.374 | 18.343 | — | 3.410 |
| Comet 1.2.0 on Spark 4.2 | Q18 | 6.656 | 19.935 | 77.051 | 3.000 | 3.870 |
| Spark 4.2.0 | Q1 | 14.313 | 30.846 | 105.422 | 2.160 | 3.420 |
| Spark 4.2.0 | Q3 | 9.498 | 19.193 | 62.847 | 2.020 | 3.270 |
| Spark 4.2.0 | Q6 | 3.070 | 2.590 | 8.748 | 0.840 | 3.380 |
| Spark 4.2.0 | Q12 | — | 9.784 | 29.939 | — | 3.060 |
| Spark 4.2.0 | Q18 | 15.432 | 35.516 | 147.352 | 2.300 | 4.150 |

Full notes per row: `size_curve.csv`.

## File paths

- Canonical data: `/workspace/engine-swap/data/tpch-sf100`
- Timed-run copy: `/tmp/engine-swap-data/tpch-sf100` (ephemeral root disk; regenerate from the canonical copy if missing)
- Jobs: `/workspace/engine-swap/results/sf100/`
- This report: `/workspace/engine-swap/results/sf100/final/TABLES.md`
- CSVs: `sf100_attempts.csv`, `sf100_query_runs.csv`, `sf100_query_medians.csv`, `sf100_hash_check.csv`, `sf100_failures.csv`, `size_curve.csv`
- Plan: `/workspace/engine-swap/results/sf100/PLAN.md`
