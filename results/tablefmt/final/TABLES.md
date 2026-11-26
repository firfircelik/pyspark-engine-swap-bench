# Table-format extension results

## Support matrix (SF1 probe, Q6)

| Engine | Iceberg | Delta |
|---|---|---|
| duckdb | read ok (hash 37fc3f716d28d660) | read ok (hash 37fc3f716d28d660) |
| sail-noreorder | read ok (hash 37fc3f716d28d660) | read ok (hash 37fc3f716d28d660) |
| spark413-classic | read ok (hash 37fc3f716d28d660) | read ok (hash 37fc3f716d28d660) |
| spark420-classic | error: `java.lang.IncompatibleClassChangeError: Found interface org.apache.spark.sql.connector.catalog.CatalogManager, but class was expected` | read ok (hash 37fc3f716d28d660) |
| comet413-classic | read ok (hash 37fc3f716d28d660) | read ok (hash 37fc3f716d28d660) |
| comet420-classic | error: `java.lang.IncompatibleClassChangeError: Found interface org.apache.spark.sql.connector.catalog.CatalogManager, but class was expected` | read ok (hash 37fc3f716d28d660) |

## Timing medians vs Phase 2 Parquet (loopback, SF30, Q1/Q3/Q6/Q12/Q18)

| Engine | Format | Q | n | median s | Parquet median s | ratio |
|---|---|---|---|---|---|---|
| comet413-classic | iceberg | Q1 | 2 | 5.974 | 7.564 | 0.79 |
| comet413-classic | iceberg | Q3 | 2 | 12.994 | 15.588 | 0.834 |
| comet413-classic | iceberg | Q6 | 2 | 1.532 | 2.724 | 0.562 |
| comet413-classic | iceberg | Q12 | 2 | 5.697 | 10.96 | 0.52 |
| comet413-classic | iceberg | Q18 | 2 | 20.038 | 28.235 | 0.71 |
| comet420-classic | delta | Q1 | 1 | 37.664 | 7.564 | 4.979 |
| comet420-classic | delta | Q3 | 1 | 17.866 | 15.588 | 1.146 |
| comet420-classic | delta | Q6 | 1 | 2.137 | 2.724 | 0.785 |
| comet420-classic | delta | Q12 | 1 | 9.002 | 10.96 | 0.821 |
| comet420-classic | delta | Q18 | 1 | 38.923 | 28.235 | 1.379 |
| duckdb | delta | Q1 | 2 | 2.425 | 2.732 | 0.887 |
| duckdb | delta | Q3 | 2 | 1.924 | 3.615 | 0.532 |
| duckdb | delta | Q6 | 2 | 0.657 | 0.81 | 0.81 |
| duckdb | delta | Q12 | 2 | 1.262 | 2.064 | 0.611 |
| duckdb | delta | Q18 | 2 | 2.91 | 4.56 | 0.638 |
| duckdb | iceberg | Q1 | 2 | 3.629 | 2.732 | 1.328 |
| duckdb | iceberg | Q3 | 2 | 2.235 | 3.615 | 0.618 |
| duckdb | iceberg | Q6 | 2 | 0.637 | 0.81 | 0.786 |
| duckdb | iceberg | Q12 | 2 | 1.588 | 2.064 | 0.769 |
| duckdb | iceberg | Q18 | 2 | 2.793 | 4.56 | 0.613 |
| sail-noreorder | delta | Q1 | 2 | 3.298 | 3.962 | 0.832 |
| sail-noreorder | delta | Q3 | 2 | 2.12 | 5.13 | 0.413 |
| sail-noreorder | delta | Q6 | 2 | 0.907 | 1.543 | 0.588 |
| sail-noreorder | delta | Q12 | 2 | 1.843 | 2.728 | 0.676 |
| sail-noreorder | delta | Q18 | 2 | 7.375 | 9.043 | 0.816 |
| sail-noreorder | iceberg | Q1 | 2 | 3.509 | 3.962 | 0.886 |
| sail-noreorder | iceberg | Q3 | 2 | 2.87 | 5.13 | 0.559 |
| sail-noreorder | iceberg | Q6 | 2 | 1.14 | 1.543 | 0.739 |
| sail-noreorder | iceberg | Q12 | 2 | 2.482 | 2.728 | 0.91 |
| sail-noreorder | iceberg | Q18 | 2 | 9.071 | 9.043 | 1.003 |
| spark413-classic | delta | Q1 | 2 | 30.809 | 33.709 | 0.914 |
| spark413-classic | delta | Q3 | 2 | 21.128 | 20.166 | 1.048 |
| spark413-classic | delta | Q6 | 2 | 2.216 | 6.112 | 0.362 |
| spark413-classic | delta | Q12 | 2 | 10.855 | 12.146 | 0.894 |
| spark413-classic | delta | Q18 | 2 | 40.364 | 42.559 | 0.948 |
| spark413-classic | iceberg | Q1 | 2 | 30.276 | 33.709 | 0.898 |
| spark413-classic | iceberg | Q3 | 2 | 18.847 | 20.166 | 0.935 |
| spark413-classic | iceberg | Q6 | 2 | 2.171 | 6.112 | 0.355 |
| spark413-classic | iceberg | Q12 | 2 | 10.6 | 12.146 | 0.873 |
| spark413-classic | iceberg | Q18 | 2 | 38.895 | 42.559 | 0.914 |
| spark420-classic | delta | Q1 | 2 | 31.725 | 36.363 | 0.872 |
| spark420-classic | delta | Q3 | 2 | 20.084 | 20.853 | 0.963 |
| spark420-classic | delta | Q6 | 2 | 2.377 | 5.644 | 0.421 |
| spark420-classic | delta | Q12 | 2 | 10.755 | 12.332 | 0.872 |
| spark420-classic | delta | Q18 | 2 | 40.921 | 41.323 | 0.99 |

## Five-query totals

| Engine | Format | n | median total s | min | max |
|---|---|---|---|---|---|
| comet413-classic | iceberg | 2 | 46.236 | 46.136 | 46.335 |
| comet420-classic | delta | 1 | 105.592 | 105.592 | 105.592 |
| duckdb | delta | 2 | 9.177 | 7.105 | 11.249 |
| duckdb | iceberg | 2 | 10.881 | 9.722 | 12.041 |
| sail-noreorder | delta | 2 | 15.543 | 14.813 | 16.274 |
| sail-noreorder | iceberg | 2 | 19.072 | 15.228 | 22.916 |
| spark413-classic | delta | 2 | 105.371 | 100.381 | 110.361 |
| spark413-classic | iceberg | 2 | 100.789 | 97.361 | 104.217 |
| spark420-classic | delta | 2 | 105.862 | 102.657 | 109.067 |

## Hash agreement vs Phase 2 Parquet hash_set

Compared 85 query results. hash_ok=85, mismatch_or_missing=0.
See `hash_issues.json` for details. Mismatches are bug candidates under `repro/minimal-tablefmt/`.
