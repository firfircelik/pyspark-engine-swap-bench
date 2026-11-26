# Test before you swap the engine

A runbook for a platform architect who wants to keep existing PySpark code and change the engine underneath it (Spark, Comet, Sail, or DuckDB). Every item below comes from this study's measured findings. Nothing here is an upstream bug report, and no issue should be filed without explicit approval.

How to run the minimal reproducers, from the study root:

```
repro/minimal/run_all.sh
```

Or a single script, as documented in `repro/README.md`. Versions used for the reproducers: Spark 4.2.0, Temurin OpenJDK 21.0.13.1 (tzdata 2026f), pysail 0.7.3 (chrono-tz 0.10.4, tzdata 2025b), Comet 1.2.0. Phase 2 ran the same engines under a 9 GiB cgroup cap, with Spark driver 6g and `local[8]`, on TPC-H SF30 and the study ETL set. All 22 TPC-H query result hashes matched across engines in the published matrix. The failures below are the edge cases that a hash of a UTC TPC-H query will not catch.

There are 10 checks.

## 1. Sail stale timezone data (C1)

**What breaks.** Sail 0.7.3 converts session and explicit time zones with chrono-tz 0.10.4, which still ships IANA tzdata 2025b. Spark on this box uses the JVM tzdb from Temurin 21.0.13.1 (2026f). After 2025b, several zones changed. Havana, Edmonton, Yellowknife, and Casablanca come out one hour off versus Spark, with no warning. Istanbul is kept in the repro as a control that should still match. This is silent wrong local time for current and future dates in those regions, and the gap grows with every later tzdata release.

**How to test.** `repro/minimal/c1_sail_stale_tzdata.py`. Compare Sail to Spark on `from_utc_timestamp` for America/Havana (2026-11-21 18:00 UTC), America/Edmonton and America/Yellowknife (2027-01-15 18:00 UTC), Africa/Casablanca (2027-04-01 12:00 UTC), and Europe/Istanbul as a control. Also set `spark.sql.session.timeZone=America/Havana` and check `to_timestamp` / `hour` on a UTC instant.

**What we measured.** On Spark 4.2.0 with tzdata 2026f the expected local strings are 14:00 (Havana), 12:00 (Edmonton), 12:00 (Yellowknife), 12:00 (Casablanca), and 15:00 (Istanbul control), as recorded in the script header and `repro/README.md`. Sail is one hour off on the four changed zones and does not warn. Severity: high, because money and audit pipelines that bucket by local hour or civil date will be wrong without failing the job. Treated as new relative to Sail issue search on 2026-11-21; not filed.

**Mitigation.** Do not swap a workload onto Sail 0.7.3 if it uses civil time in zones that changed after tzdata 2025b, until Sail's bundled tzdata matches the JVM (or you have re-run C1 and any customer zones against Spark). Pin and record both tzdata versions in the release checklist. Compare a sample of production timestamps in the customer's zones, not only UTC. Prefer storing UTC and applying the zone only at the serving layer if you can. Do not file an upstream issue without approval.

## 2. Comet timezone impact (known, #6331)

**What breaks.** Comet's native projection uses its own tzdata (2025b in 1.2.0) rather than the JVM's 2026f. With session zone America/Havana, timestamp-to-string, `hour`, and `to_timestamp(string)` are one hour off versus Spark. Comet does emit a startup warning (upstream #6353). The class is known. The new part is the measured impact.

**How to test.** `repro/minimal/comet_tz_impact.py`. Probe the same functions across a set of zones and compare to Spark.

**What we measured.** 23 of 70 probe rows differed versus Spark (`results/probes/comet_tz_v2.jsonl`). Upstream: apache/datafusion-comet#6331, closed 2026-09-30 with the startup warning in #6353.

**Mitigation.** Treat the warning as a gate, not noise. If the customer session zone or any explicit zone changed between 2025b and the JVM tzdata you ship, run the probe and a sample of production rows against Spark before enabling Comet native projection on those columns. Keep a Spark-only path for timezone-sensitive jobs until the engine tzdata matches. Do not describe this as a new Comet bug; it is a measured impact of a known issue.

## 3. Sail DOUBLE to DECIMAL money rounding (F1, PR #3024)

**What breaks.** Casting a DOUBLE to DECIMAL on Sail rounds the binary value, not the decimal value a money column is supposed to have. A one-cent error on a subset of rows will not change the row count and may not trip a simple checksum if you only hash rounded aggregates.

**How to test.** `repro/sail_double_to_decimal_cast.py`. The script checks values such as 38771.645, 0.145, 1.005, 2.675, and -1.005, and the DataFrame pattern used in ETL step E6: `(decimal_price * lit(1.1)).cast('decimal(15,2)')`.

**What we measured.** In Phase 2 ETL step E6, 631 of 683,346 rows were one cent off versus Spark. This is known upstream (open PR lakehq/sail#3024). Severity: high for money.

**Mitigation.** Do not introduce a DOUBLE in a money path and then cast back to DECIMAL on Sail 0.7.3. Keep the type DECIMAL for prices, rates, and FX, or compare the full column to Spark (or to a decimal-exact engine) row by row, not only by count. Track PR #3024 before treating this as fixed. A platform swap test must include a customer-like "price * rate, then decimal(15,2)" step, which is what caught E6.

## 4. Sail `format_number` on DECIMAL (C4)

**What breaks.** Sail's `format_number` converts DECIMAL (and every input) to f64 and formats with Rust `{:.N}`. Spark formats DECIMAL with BigDecimal half-even. Displayed money strings can be silently wrong, including values beyond 2^53 that lose digits.

**How to test.** `repro/minimal/c4_sail_format_number_decimal.py`.

**What we measured.** Spark 4.2.0 expected strings: `0.00` for 0.005, `1.12` for 1.115, `2.68` for 2.675, and `12,345,678,901,234,567.89` for a 19,2 decimal beyond 2^53. Sail returns `0.01`, `1.11`, `2.67`, and a lossy rendering of the large value (`repro/README.md` and the script header). Severity: medium. Treated as new on 2026-11-21; not filed. Source path in Sail: `crates/sail-function/src/scalar/string/format_number.rs` (`format_number_fixed`, `scalar_to_f64`).

**Mitigation.** Do not use Sail `format_number` as the system of record for money display. Format in an application layer from a DECIMAL, or compare formatted strings to Spark on a fixture that includes half-even ties and integers bigger than 2^53. Do not file without approval.

## 5. Sail scientific-notation string to DECIMAL (C7)

**What breaks.** `cast('1e3' as decimal(10,0))` errors on Sail 0.7.3 (`Cannot cast string '1e3' to value of Decimal128`). Spark returns 1000. `try_cast` on Sail returns NULL for the same input, with no error. Pipelines that ingest CSV or JSON numeric strings in scientific notation can fail hard, or worse, drop values through `try_cast` and keep running.

**How to test.** `repro/minimal/c7_sail_exponent_string_to_decimal.py`. Cases: `'1e3'` to decimal(10,0), `'1.5E2'` to decimal(10,1), `'-2e-2'` to decimal(5,3), and `try_cast('1e3' as decimal(10,0))`.

**What we measured.** Spark 4.2.0 returns 1000, 150.0, and -0.020, and `try_cast` also returns 1000. Sail errors on `cast` and returns NULL on `try_cast`. Severity: medium. Treated as new on 2026-11-21; not filed.

**Mitigation.** Normalize scientific-notation strings to plain decimal text before the cast, or reject them in a validating parse. Do not "fix" a failing `cast` by switching to `try_cast` on Sail: that turns a job failure into silent NULLs. Include exponent strings in the swap test fixtures.

## 6. Sail JSON PERMISSIVE decimal overflow (C8)

**What breaks.** In Spark, `spark.read.json` with a `decimal(5,2)` schema and PERMISSIVE mode sets an overflowing value to NULL and keeps the rest of the file. On Sail 0.7.3, one overflowing decimal fails the whole read (`parse decimal overflow`). A single bad record becomes an outage.

**How to test.** `repro/minimal/c8_sail_json_permissive_decimal_overflow.py`.

**What we measured.** Spark returns NULL for the overflowing row and continues. Sail fails the entire read. Severity: medium. Related Sail issues for CSV mode and `from_json` are different; the JSON read-side overflow in PERMISSIVE mode was not found in a 2026-11-21 search and is not filed.

**Mitigation.** Do not assume PERMISSIVE means "bad decimals become NULL" on Sail 0.7.3. Pre-validate numeric JSON against the target precision, or isolate the read and treat a parse failure as a data-quality event, not a retryable engine crash. Run C8 (and a sample of dirty production JSON) before moving a PERMISSIVE ingestion job.

## 7. Sail empty DataFrame write (C5)

**What breaks.** Writing an empty DataFrame on Spark creates a schema-only part file, and a later read preserves the schema. On Sail 0.7.3 the write creates nothing. Reading back yields `struct<>` with 0 rows and no error. Downstream jobs that rely on the written schema (partition discovery, a declared table schema, or an empty-but-valid file as a success signal) will go wrong silently.

**How to test.** `repro/minimal/c5_sail_empty_dataframe_write.py`.

**What we measured.** Spark keeps the schema in a schema-only part file. Sail writes no file and the read-back schema is empty. Severity: low to medium. The read side is known (lakehq/sail#3061, fix PR #3074). The write side was not found in a 2026-11-21 search and is not filed.

**Mitigation.** Do not use "write an empty DataFrame" as the way to register a schema on Sail 0.7.3. Handle the empty-batch case in the job (skip, write an explicit success marker, or create the table/schema through a catalog API). After a write, assert that the expected path and schema exist, even when the row count is zero.

## 8. Comet memory and `MALLOC_ARENA_MAX=2` (#7108)

**What breaks.** Under the 9 GiB cgroup cap, Comet 1.2.0 with the default glibc allocator was OOM-killed on SF30 TPC-H Q18 even when configured heap plus off-heap was well under the cap. Process RSS ran 8.1 to 9.6 GB. The class is known (apache/datafusion-comet#7108: glibc keeps several GiB outside Comet's pool). The measured effect in this lab is new evidence, not a new diagnosis.

**How to test.** There is no one-line minimal script in `repro/minimal/` for the OOM itself (it needed SF30 Q18 under the 9 GiB cap). The diagnostic runs are in `results/comet-mem-diag/` (including `E_2g_3g_arena2_run{1,2,3}` with `mem_per_query.csv`). For a swap test on a customer cluster, run the real job under a cgroup or container memory limit that matches production, with and without `MALLOC_ARENA_MAX=2`, and record process-tree RSS, not only the Comet or JVM pool counter.

**What we measured.** Default glibc: peak RSS 8.1 to 9.6 GB, OOM at the 9 GiB cap on Q18 (configured 2g heap + 3g off-heap in the matrix; earlier 3g+4g also died). With `MALLOC_ARENA_MAX=2`, three consecutive SF30 TPC-H runs peaked at 5.4, 5.1, and 5.5 GB. That setting was adopted for all later Comet jobs in this study. Related upstream: #7344 (jemalloc default, open), #7342, #7304, #7635.

**Mitigation.** Set `MALLOC_ARENA_MAX=2` (or another allocator policy you have measured) on Comet processes before a production cap test. Monitor process RSS against the container limit, not only heap plus off-heap. Do not treat "configured 5 to 7 GiB" as "fits in 9 GiB" until you have seen the RSS peak on the real query mix. Budget the first swap test to include at least one Q18-class join-heavy query.

## 9. S3A analytics stream (AAL) settings

**What breaks.** Nothing correctness-related in this matrix: TPC-H hashes matched on classic and analytics. What breaks is the cost and runtime assumption that "turning on the S3 analytics stream will make it faster." AAL cuts GET counts on Spark and barely moves wall time when the store is local. With per-request latency it can be a large win on Spark 4.2.0, and a loss on Spark 4.1.3 loopback. Comet is unaffected, because the S3A stream type does not touch its scan.

**How to test.** No single minimal script. Use the published Phase 2 and AAL-isolation tables: `results/final/TABLES.md`, `results/final/F4.md`, `results/final/phase2_get_reduction.csv`, `results/final/phase2_stream_type_compare.csv`, and `results/final/aal_iso_medians.csv`. On a customer store, run the same queries with `fs.s3a.input.stream.type` set to `classic` and to `analytics`, record wall time and GET counts, and repeat with realistic request latency.

**What we measured.** Phase 2 TPC-H, median of two complete runs, sum of timed-query GetObject:

- Spark 4.1.3: about 40.2% fewer GETs with analytics (231,388 classic vs 138,370 loopback).
- Spark 4.2.0: about 54.5% fewer GETs (231,388 vs 105,029 loopback). The reduction is stable on loopback, proxy0, and proxy10.
- Comet 1.2.0: 0.0% (64,252 vs 64,252).

Wall time, median total:

- Loopback Spark 4.1.3: classic 434.267 s, analytics 510.628 s (analytics is slower, ratio 1.176).
- Loopback Spark 4.2.0: classic 440.087 s, analytics 443.983 s (essentially tied, ratio 1.009).
- Proxy +10 ms Spark 4.2.0: classic 702.024 s, analytics 532.065 s (analytics is 24% faster, ratio 0.758). Q6 in the isolation study went from 22.52 s to 12.859 s.
- Proxy +10 ms Spark 4.1.3: classic 691.361 s, analytics 621.88 s (analytics is 10% faster, ratio 0.900).

**Mitigation.** Do not default AAL on or off from a blog post. On a low-latency store, measure both streams; on Spark 4.1.3 in this lab, classic was the faster loopback setting. On a store with per-request latency, measure Spark 4.2.0 analytics first (that is where the GET reduction turned into wall time). Do not expect Comet 1.2.0 to benefit. Carry GET count and wall time, not only one of them, into the platform decision. The cost table shows what a 24% Spark 4.2.0 saving is worth under the priced rates, still excluding cluster startup.

## 10. Spark execution-memory limit (F4)

**What breaks.** Intermittent `SparkOutOfMemoryError [UNABLE_TO_ACQUIRE_MEMORY]` ("Unable to acquire 65536 bytes, got 0", SQLSTATE 53200) in `UnsafeExternalSorter.growPointerArrayIfNecessary`. This is characterized as a configuration limit of this lab (Spark `local[8]`, driver 6g, 9 GiB cgroup), not as a Spark bug. A retry often succeeds, which is a reliability problem for a scheduled customer job even when the root cause is a tight budget.

**How to test.** There is no minimal unit script. The event list and slices are in `results/final/F4.md` and `results/final/f4_events.csv`. On a customer cluster, run the swap with production-like driver/executor memory and shuffle partitions, and watch Q3, Q12, and Q18 (or the customer's equivalent join-and-sort queries) across more than one repetition. A single green run is not enough.

**What we measured.** 17 F4 events in total: 7 in Phase 2 (including dropped rep 3) and 10 in AAL isolation, all the same 64 KB acquisition failure. In published Phase 2 they concentrate on Q12 (4), Q18 (2), and Q3 (1), and more often on the analytics stream (6 vs 1 classic among Phase 2 events). In AAL isolation, all 10 recorded F4 OOMs were on Q3 with an analytics stream. Both Spark 4.1.3 and 4.2.0 appear. The error was not observed on Comet, Sail, or DuckDB query events in this matrix. Separately, 10 no-progress stalls (hangs, not the 64 KB error) and 3 long RSS-sampler gaps are listed in `F4.md`. Several stalls are on dropped rep 3; they are still operational evidence.

**Mitigation.** Treat F4 as a memory-budget limit, not a reason to abandon a swap and not a reason to call Spark broken. Raise or rebalance driver/executor execution memory, `spark.memory.fraction`, and `spark.sql.shuffle.partitions` in a controlled test (we did not retune those knobs in this study, so this file does not claim a measured fix). Require more than one successful repetition of the heavy queries. Do not use "retry until it passes" as the production control. Budget extra wall time and extra platform cost for the first weeks of a Spark swap. The published cost table uses only valid complete runs and does not add these failed or hung attempts as billed hours; a real operations calendar would.

## After the ten checks

If every check is green on the customer's data and zones, the remaining decision is cost, capacity, and operations (see `article/cost_table.md`). If any check is red, keep the current engine for that workload, or isolate the unsafe functions, until the engine version that fixes it is the one you will actually run.
