# Contributing

PRs that add engines are welcome (for example Polars, Daft, chDB, DataFusion Python, Photon or Fabric NEE numbers from your own runs, or newer releases of the engines here). Corrections to the analysis are just as welcome.

Ground rules, so that results stay comparable:

1. Same workload: the 22 queries in `queries/` and the ETL steps E1 to E6, on `tpchgen-cli 4.0.0` data (SF30, 24 parts per table, Snappy).
2. Same harness: run through `scripts/run_one.py` so the job sits in the cgroup v2 cap (`memory.max=9G`, swap off), the RSS sampler and freeze detector run, and every query result is hashed.
3. Results must match the existing `hash_set` values per query. A mismatch is a finding: report it, do not tune it away.
4. Commit the per-run `queries.jsonl`, `summary.jsonl` and the engine settings you used; state your hardware. Do not commit Parquet data, jars or credentials.
5. Say how many repetitions you ran. Two is the minimum; five or more is what run-level tests need (see `paper/paper.pdf`, Section 3.3).

Bug reports about the engines themselves belong upstream, not here.
