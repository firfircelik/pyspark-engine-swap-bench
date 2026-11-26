# Table-format extension plan (2026-11-26)

Question: does swapping the engine still work when the data is an Iceberg or Delta table instead of plain Parquet?

This is a small, justified follow-on to Phase 2 (plain Parquet on MinIO). It is not a second full 22-query x 3-rep x latency matrix.

## Pinned versions (to be confirmed by sha on disk)

| Piece | Pin | Why |
|---|---|---|
| Iceberg Spark runtime | `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0` (latest published as of 2026-11-26) | Official docs and Maven Central. Built against Spark 4.1.3. |
| Iceberg Spark 4.2 runtime | **none published** | `iceberg-spark-runtime-4.2_2.13` returns Maven 404. The 1.12.0 source tree has a Spark 4.2 module, but `dev/stage-binaries.sh` publishes 3.5, 4.0, 4.1 only. We will not build a private runtime. Spark 4.2 / Comet-on-4.2 Iceberg reads are a support-matrix finding (try the 4.1 runtime once, record the exact error if it fails). |
| Iceberg writer | Spark 4.1.3 + runtime 1.12.0 | Only published compatible Spark in this lab. |
| Delta Spark | `io.delta:delta-spark_4.2_2.13:4.4.1` and `delta-spark_4.1_2.13:4.4.1`, plus `delta-storage` / kernel transitives 4.4.1 | Latest Delta (2026-10-05). Artifacts exist for both Spark 4.1 and 4.2. |
| Delta writer | Spark 4.2.0 + delta-spark 4.4.1 | Latest Spark in the study that has a published Delta runtime. |
| Comet | already-used 1.2.0 on Spark 4.2; official `comet-spark-spark4.1_2.13:1.2.0` only if Iceberg on Spark 4.2 is blocked | Comet 1.2 docs: native Iceberg scan is on by default; tested with Hadoop catalog. |
| Sail | pysail 0.7.3 (unchanged) | Native `spark.read.format("iceberg"\|"delta").load(path)`. No Hadoop catalog provider; path load / `CREATE TABLE ... LOCATION` is the native path. |
| DuckDB | 2.0.1 + `iceberg` and `delta` core extensions from extensions.duckdb.org | `iceberg_scan` / `delta_scan` on object-store paths. No Hadoop catalog attach; path scan is documented. |
| Catalog / storage | Iceberg Hadoop catalog on MinIO (`s3a://tpch/iceberg`); path-based Delta (`s3a://tpch/delta/sf{1,30}/<table>`). No REST, HMS, or Docker. | Common denominator for Spark, DuckDB path scan, and Sail path load. |
| Table properties | Iceberg `format-version=2`, parquet + snappy, unpartitioned. Delta: default 4.4.1 writer settings (record protocol / features; do not silently disable them). | Same layout as the existing unpartitioned Parquet baseline. |

## Matrix (timing)

- Data: SF30 TPC-H, all 8 tables, already on disk and in MinIO as Parquet. Write once to Iceberg and once to Delta on MinIO. Also write SF1 for warmup (same format).
- Condition: MinIO loopback only. AAL / toxiproxy are out of scope (that question is already answered on Parquet).
- Queries: Q1, Q3, Q6, Q12, Q18. Same pilot set: scan+agg (Q1, Q6), joins (Q3, Q12), heavy agg/join (Q18). Enough to compare result hashes to the Phase 2 Parquet baseline and to get wall times. Not all 22.
- Reps: 2 fresh processes, 9 GiB cgroup cap, classic S3A stream, same `run_one.py` / `run_engine2.sh` harness.
- Engines in the timing matrix (if the SF1 probe passes): `duckdb`, `sail-noreorder`, `spark413-classic`, `spark420-classic`, `comet420-classic` (and `comet413-classic` for Iceberg if that is the only working Comet+Iceberg path).
- Hash check: `hash_set` against Phase 2 majority hashes (Q1 `dc9fb64947ffda53`, Q3 `03a4d982c46be77e`, Q6 `86f1051af17c11ba`, Q12 `7245c10c0d25d7c9`, Q18 `247ac87094aee3e6`). Wrong results are bug candidates under `repro/minimal-tablefmt/`. No upstream issues will be filed.
- Unsupported engine x format combinations are recorded with the exact error in the support matrix. They are not dropped silently, and they are not retried as timing jobs at SF30.

## Order of work

1. Download and checksum jars / DuckDB extensions. Record versions.
2. Write SF1 Iceberg (Spark 4.1.3) and SF1 Delta (Spark 4.2.0).
3. Probe every engine on SF1 (`count(*)` or Q6) and fill the support matrix.
4. Write SF30 Iceberg, then SF30 Delta. Keep >15 GB free. Do not delete existing data; only per-job Spark temp dirs.
5. Run the timing matrix, one job at a time, checkpointed under `results/tablefmt/`.
6. Aggregate: support matrix, timings vs Parquet medians, hash agreement, findings, paths.

## Disk

`/workspace` has ~58 GB free. SF30 Parquet on MinIO is 12 GB. Two more copies (~24 GB) leave ~34 GB, still above 15 GB. `minio-data/tpch/etl-scratch` is 3 GB leftover from Phase 2; left untouched unless Firat approves deletion.
