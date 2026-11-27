# Polars Phase 2 line (generated from results/polars)

Protocol matches the published Phase 2 DuckDB line: 2 reps (published totals dropped rep 3), TPC-H Q1-Q22 plus ETL E1-E6, local / MinIO loopback / toxiproxy 0 ms / toxiproxy 10 ms (ETL: loopback and 10 ms only), fresh process per job, SF1 warmup, 9 GiB cgroup cap, seeded job order taken from the DuckDB cells in results/phase2/plan.json. Polars 2.2.0 from commit 04eaba5 in venv-polars. Medians are over complete runs, the same method as scripts/aggregate2.py (n=2; min and max are the two rep totals). Wall times are the sum of per-query wall_s, not process elapsed. SF30 comparators for DuckDB, Sail, Comet and Spark are the published Phase 2 figures in results/final/phase2_totals.csv, not re-run here. SF10 DuckDB figures are from a same-session reference run under this harness (other engines have no Phase 2 matrix at SF10).

Runs: 27 November 2026, 17:11-17:41 Europe/Istanbul, on the study box. All 36 jobs (12 Polars + 12 DuckDB at SF10, 12 Polars at SF30) finished status=ok on attempt 1. No query errors, freezes, timeouts or OOM kills.

## SF30 TPC-H total timed wall (median of complete runs, s; peak tree RSS GB median)

| config | local | loopback | proxy0 | proxy10 |
|---|---|---|---|---|
| polars | 60.975 (n=2/2, 3.751 GB) | 72.55 (n=2/2, 3.804 GB) | 79.446 (n=2/2, 4.385 GB) | 86.597 (n=2/2, 3.81 GB) |
| comet420-default | 237.253 (n=2/2, 4.884 GB) |  |  |  |
| duckdb | 48.412 (n=2/2, 5.436 GB) | 55.33 (n=2/2, 5.521 GB) | 70.162 (n=2/2, 5.518 GB) | 98.582 (n=2/2, 5.5 GB) |
| sail-noreorder | 49.584 (n=2/2, 9.276 GB) | 81.079 (n=2/2, 8.48 GB) | 116.448 (n=2/2, 8.479 GB) | 122.165 (n=2/2, 8.298 GB) |
| sail-reorder | 52.796 (n=2/2, 9.185 GB) | 71.98 (n=2/2, 8.693 GB) | 86.845 (n=2/2, 8.527 GB) | 121.165 (n=2/2, 8.362 GB) |
| spark413-default | 383.934 (n=2/2, 7.381 GB) |  |  |  |
| spark420-default | 386.568 (n=2/2, 7.41 GB) |  |  |  |
| comet420-analytics |  | 289.055 (n=2/2, 4.867 GB) | 268.277 (n=2/2, 4.982 GB) | 292.885 (n=2/2, 4.918 GB) |
| comet420-classic |  | 281.6 (n=2/2, 5.321 GB) | 278.144 (n=2/2, 5.069 GB) | 308.812 (n=2/2, 5.387 GB) |
| spark413-analytics |  | 510.628 (n=2/2, 7.64 GB) | 486.093 (n=2/2, 7.613 GB) | 621.88 (n=2/2, 7.643 GB) |
| spark413-classic |  | 434.267 (n=2/2, 7.553 GB) | 463.414 (n=2/2, 7.549 GB) | 691.361 (n=2/2, 7.506 GB) |
| spark420-analytics |  | 443.983 (n=2/2, 7.601 GB) | 494.802 (n=2/2, 7.567 GB) | 532.065 (n=2/2, 7.577 GB) |
| spark420-classic |  | 440.087 (n=2/2, 7.542 GB) | 495.861 (n=2/2, 7.536 GB) | 702.024 (n=2/2, 7.532 GB) |

## SF30 ETL total timed wall (median of complete runs, s; peak tree RSS GB median)

| config | local | loopback | proxy0 | proxy10 |
|---|---|---|---|---|
| polars |  | 19.704 (n=2/2, 6.407 GB) |  | 23.879 (n=2/2, 6.377 GB) |
| duckdb |  | 23.149 (n=2/2, 7.282 GB) |  | 23.584 (n=2/2, 7.295 GB) |
| sail-noreorder |  | 19.596 (n=2/2, 4.126 GB) |  | 26.241 (n=2/2, 4.157 GB) |
| sail-reorder |  | 20.05 (n=2/2, 4.2 GB) |  | 36.944 (n=2/2, 4.146 GB) |
| comet420-analytics |  | 71.498 (n=2/2, 4.905 GB) |  | 150.869 (n=2/2, 5.013 GB) |
| comet420-classic |  | 73.024 (n=2/2, 5.037 GB) |  | 143.161 (n=2/2, 5.094 GB) |
| spark413-analytics |  | 131.671 (n=2/2, 7.526 GB) |  | 219.089 (n=2/2, 7.488 GB) |
| spark413-classic |  | 119.225 (n=2/2, 7.463 GB) |  | 221.676 (n=2/2, 7.44 GB) |
| spark420-analytics |  | 112.642 (n=2/2, 7.415 GB) |  | 202.79 (n=2/2, 7.468 GB) |
| spark420-classic |  | 115.546 (n=2/2, 7.421 GB) |  | 229.351 (n=2/2, 7.509 GB) |

## SF10 TPC-H and ETL (same-session Polars vs DuckDB only)

Other engines were not run at SF10 under the Phase 2 protocol. The first Polars SF10 loopback TPC-H run (55.728 s) started immediately after an 11.29 GiB MinIO upload of SF30 and is a cold-cache outlier; rep 2 was 15.956 s. The median of 35.842 s should be read with both reps in mind.

| config | wl | local | loopback | proxy0 | proxy10 |
|---|---|---|---|---|---|
| polars | tpch | 21.792 (n=2/2, 1.681 GB; reps 29.835;13.749) | 35.842 (n=2/2, 2.179 GB; reps 55.728;15.956) | 17.899 (n=2/2, 2.111 GB; reps 15.878;19.92) | 17.801 (n=2/2, 2.068 GB; reps 17.482;18.121) |
| polars | etl |  | 6.06 (n=2/2, 2.633 GB; reps 5.171;6.949) |  | 8.495 (n=2/2, 2.425 GB; reps 10.33;6.659) |
| duckdb | tpch | 12.718 (n=2/2, 2.327 GB; reps 15.282;10.154) | 17.712 (n=2/2, 2.49 GB; reps 17.322;18.103) | 22.837 (n=2/2, 2.486 GB; reps 22.466;23.207) | 29.366 (n=2/2, 2.481 GB; reps 29.769;28.963) |
| duckdb | etl |  | 5.447 (n=2/2, 3.969 GB; reps 5.502;5.393) |  | 7.319 (n=2/2, 4.269 GB; reps 7.372;7.265) |

## TPC-H correctness vs DuckDB

All 22 queries, both scales, every condition and both reps: hash_set and hash_ordered match DuckDB. SF10 reference is the same-session DuckDB line; SF30 reference is published Phase 2 DuckDB.

| scale | queries matching hash_set | hash_ordered also equal |
|---|---|---|
| sf10 | 22/22 | 22/22 |
| sf30 | 22/22 | 22/22 |

## ETL correctness vs DuckDB

| scale | step | rows | digest |
|---|---|---|---|
| sf10 | E1_filter_agg | duck [14] / polars [14] | match |
| sf10 | E2_join_agg | duck [1000] / polars [1000] | match |
| sf10 | E3_window_latest | duck [999982] / polars [999982] | match |
| sf10 | E4_highcard_agg | duck [2000000] / polars [2000000] | match |
| sf10 | E5_rewrite_partitioned | duck [2275575] / polars [2275575] | match |
| sf10 | E6_upsert_antijoin_union | duck [2275575] / polars [2275575] | DIFF (same row count; see E6 probe) |
| sf30 | E1_filter_agg | duck [14] / polars [14] | match |
| sf30 | E2_join_agg | duck [1000] / polars [1000] | match |
| sf30 | E3_window_latest | duck [2999930] / polars [2999930] | match |
| sf30 | E4_highcard_agg | duck [6000000] / polars [6000000] | match |
| sf30 | E5_rewrite_partitioned | duck [6828731] / polars [6828731] | match |
| sf30 | E6_upsert_antijoin_union | duck [6828731] / polars [6828731] | DIFF (same row count; see E6 probe) |

## E6 half-cent probe (CAST(o_totalprice*1.1 AS DECIMAL(15,2)) on 1995 orders with o_orderkey % 10 = 0)

Every digest difference is a half-cent tie. DuckDB CAST rounds half-up; Polars SQL CAST rounds half-even (banker's). Non-tie values match. ANTI JOIN and UNION ALL BY NAME produced the same keys and statuses.

| scale | updated rows | half-cent ties | ties where modes agree | ties where Polars is 0.01 below DuckDB | Polars result even on all ties? |
|---|---|---|---|---|---|
| sf10 | 227841 | 22812 | 11396 | 11416 | yes (22812 of 22812) |
| sf30 | 683346 | 68330 | 34177 | 34153 | yes (68330 of 68330) |

SF10 example: 198194.75 * 1.1 = 218014.225; DuckDB 218014.23, Polars 218014.22. SF30 example: 293975.35 * 1.1 = 323372.885; DuckDB 323372.89, Polars 323372.88.

## Memory (9 GiB cgroup, swap 0)

Peak tree RSS is the trustworthy figure. cg_memory_peak was 9664139264 bytes on all 36 runs (identical, including DuckDB SF10), so memory.peak was not reset and cannot be used here. No run incremented cgroup oom or oom_kill. POLARS_OOC_SPILL_DIR was empty after the runs. Max Polars tree RSS: SF10 2.921 GB, SF30 6.486 GB (ETL); SF30 TPC-H max 4.931 GB. That is inside the 9 GiB cap with headroom and with no spill files, so this study does not show whether Polars reads memory.max. It only shows that Polars finished SF30 TPC-H and ETL under the cap without OOM.

