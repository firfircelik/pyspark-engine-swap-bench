# Polars Phase 2 protocol used on 27 November 2026

- Harness: /workspace/engine-swap, merged runner from firfircelik/pyspark-engine-swap-bench commit 04eaba5, venv-polars with polars==2.2.0.
- Plan: DuckDB cells and order from results/phase2/plan.json, reps 1-2 only (published totals dropped rep 3). Conditions: local, loopback, proxy0, proxy10 for TPC-H; loopback and proxy10 for ETL.
- Orchestrator: scripts/orchestrate.py with pre-built results/polars/sf10/plan.json and results/polars/sf30/plan.json. Wrapper: scripts/run_polars_phase2.sh. Log: logs/orchestrate-polars.out.
- SF10 plan also includes a same-session DuckDB reference (12 jobs) so hashes and walls can be compared at that scale.
- Env: SF=sf10 then SF=sf30, BENCH_S3_KEY / BENCH_S3_SECRET, AWS_EC2_METADATA_DISABLED=true. Cgroup /sys/fs/cgroup/engine-swap-bench, memory.max=9G, swap 0.
- Kill rules as in the harness: orchestrate MAX_ATTEMPTS=3; run_one stall after 20 min with no queries.jsonl progress; TIMEOUT=1800 s. No retries were needed.
- MinIO started with scripts/minio-start.sh. SF30 was re-uploaded (11.29 GiB) because it had been removed from the bucket. toxiproxy was already running; orchestrate.py set the 0 ms or 10 ms toxic per job.
- SF100 not run.
