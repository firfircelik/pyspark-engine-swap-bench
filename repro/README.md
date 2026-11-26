# Reproducers (NOT filed upstream; report to Firat first)

## sail_compat_repro.py (inline data, no S3)
Spark:  JAVA_HOME=opt/jdk-21* opt/spark-4.2.0-bin-hadoop3/bin/spark-submit --master local[2] repro/sail_compat_repro.py
Sail:   venv-sail/bin/sail spark server --port 50051 &  then  venv-sail/bin/python repro/sail_compat_repro.py sc://127.0.0.1:50051
Outputs saved: sail_compat_spark420.out, sail_compat_sail073.out

| # | Spark 4.2.0 | Sail 0.7.3 | Upstream status (checked 2026-11-21) |
|---|---|---|---|
| R1 percentile_approx(decimal(15,2) 10,20,30,40, 0.5) | decimal(15,2) 20.00 | double 25.0 (interpolated, not a data value) | known: open PR lakehq/sail#2117; #2948 "fix: approx_percentile" merged 2026-11-05, after 0.7.3 (2026-10-30), effect unverified |
| R1b percentile_approx with array of percentages | array<decimal(15,2)> [10.00, 30.00] | AnalysisException: approx_percentile_cont signature mismatch | covered by open PR #2117 (#1391 closed Feb 2026, still fails in 0.7.3) |
| R2b summary() 25%/75% on 1,2,4 | 1.0 / 4.0 | 1.25 / 3.5 | same root cause as R1 |
| R2 describe() row order | count, mean, stddev, min, max | max, min, mean, stddev, count (values equal) | not found in a quick search |
| R3 date_trunc('month', DATE) type | timestamp | timestamp_ntz | known: open PR #2237 |

---
# Phase 2 minimal reproducers (repro/minimal/, NOT filed upstream)
Versions: Spark 4.2.0 (spark-4.2.0-bin-hadoop3), Temurin OpenJDK 21.0.13.1 (2026-11-17, lib/tzdb.dat = 2026f), pysail 0.7.3 (bundles chrono-tz 0.10.4 = tzdata 2025b, jiff 0.2.35), pyspark-client 4.2.0, Comet comet-spark-spark4.2_2.13-1.2.0.jar. Linux x86_64, local mode, no S3.
Run everything: `repro/minimal/run_all.sh` -> outputs in repro/minimal/out/<name>_{spark420,sail073,comet120}.out.
Single script: Spark `opt/spark-4.2.0-bin-hadoop3/bin/spark-submit --master local[2] repro/minimal/<f>.py`; Sail `venv-sail/bin/sail spark server --port 50051 &` then `cd repro/minimal && venv-sail/bin/python <f>.py sc://127.0.0.1:50051`.
Duplicate check 2026-11-21 ~21:40 TR: GitHub issue/PR search (links below) + grep of @sail-bug BDD scenarios in lakehq/sail main @3f38bd9 (2026-11-21) and PR branches #2633, #2707, #3024 (clone in tmp/sail-src).

| ID | File | What | Severity | Upstream |
|---|---|---|---|---|
| C1 | c1_sail_stale_tzdata.py | Sail local times use tzdata 2025b (chrono-tz 0.10.4 pinned in Cargo.lock, still on main); Havana/Edmonton/Yellowknife/Casablanca 1 h off vs Spark (JVM 2026f); session-zone to_timestamp/hour too. No warning. | High (silent wrong results for real current dates in 4 regions; grows with every tzdata release) | NEW. Searches: https://github.com/lakehq/sail/issues?q=chrono-tz , https://github.com/lakehq/sail/issues?q=tzdata , https://github.com/lakehq/sail/issues?q=Havana , ?q=Casablanca , ?q=Edmonton : no hits. Same class as apache/datafusion-comet#6331. |
| C4 | c4_sail_format_number_decimal.py | format_number(DECIMAL) goes through f64: 0.005 -> 0.01 (Spark 0.00, HALF_EVEN), 1.115 -> 1.11 (Spark 1.12), and values beyond 2^53 lose digits | Medium (silently wrong formatted money values) | NEW. https://github.com/lakehq/sail/issues?q=format_number : only coverage tracker #508 (marked done, PR #1324); no @sail-bug scenario. Source: crates/sail-function/src/scalar/string/format_number.rs (format_number_fixed, scalar_to_f64). |
| C7 | c7_sail_exponent_string_to_decimal.py | cast('1e3' as decimal(10,0)) errors "Cannot cast string '1e3' to value of Decimal128" (Spark: 1000) | Medium (pipelines that cast CSV/JSON string columns fail) | NEW. https://github.com/lakehq/sail/issues?q=cast+string+decimal+exponent , ?q=scientific+notation : no hits; #2707 (CAST divergences) does not cover it. |
| C8 | c8_sail_json_permissive_decimal_overflow.py | spark.read.json with decimal(5,2) schema: one overflowing value fails the whole read ("parse decimal overflow"); Spark PERMISSIVE gives NULL for that row | Medium (whole job fails on one bad record) | NEW. https://github.com/lakehq/sail/issues?q=PERMISSIVE , ?q=json+decimal : only CSV mode issues #2587/#2844 and from_json #3042 (different). |
| C5 | c5_sail_empty_dataframe_write.py | Empty DataFrame write creates nothing; read back gives struct<> with 0 rows and no error (Spark: schema-only part file, schema preserved) | Low-Medium | Read side KNOWN: lakehq/sail#3061 (fix PR #3074). Write side not found: https://github.com/lakehq/sail/issues?q=empty+parquet+write |
| Comet-tz | comet_tz_impact.py | Comet native project with session zone America/Havana: ts->string, hour, to_timestamp(string) 1 h off vs Spark (2025b vs 2026f). Probe over 7 zones: 23/70 rows differ (results/probes/comet_tz_v2.jsonl). | High impact, but KNOWN and warned | apache/datafusion-comet#6331 (closed 2026-09-30 via startup warning #6353). Not new; new part is quantified impact. |
| F1 | ../sail_double_to_decimal_cast.py | DOUBLE->DECIMAL cast rounds binary value (ETL E6: 631/683,346 rows 1 cent off) | High (money) | KNOWN: open PR lakehq/sail#3024 |
| C2/C3/C6 | ../sail_new_candidates.py | bround half-away + wrong type (and internal schema assertion in multi-column select), round(1.005D,2)=1.0, double->string not Java style | Medium | KNOWN: pinned @sail-bug in bround.feature / round.feature / cast_to_string.feature (PR #2633, main) |
