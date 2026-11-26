"""Untimed: read each ETL output with DuckDB, record row count, column types and an order-free digest (numerics rounded to 2 dp)."""
import argparse, json, sys, os
import duckdb
ap = argparse.ArgumentParser(); ap.add_argument("--etl-out", required=True); ap.add_argument("--out", required=True)
ap.add_argument("--label", required=True); ap.add_argument("--job", required=True)
a = ap.parse_args()
con = duckdb.connect(config={"extension_directory": "/workspace/engine-swap/opt/duckdb-ext", "autoinstall_known_extensions": False})
con.execute("LOAD httpfs"); con.execute("SET enable_external_file_cache=false")
con.execute(f"CREATE SECRET m (TYPE s3, KEY_ID '{os.environ['BENCH_S3_KEY']}', SECRET '{os.environ['BENCH_S3_SECRET']}', ENDPOINT '127.0.0.1:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')")
base = a.etl_out.replace("s3a://", "s3://").replace("file://", "")
STEPS = ["E1_filter_agg", "E2_join_agg", "E3_window_latest", "E4_highcard_agg", "E5_rewrite_partitioned", "E6_upsert_antijoin_union"]
with open(a.out, "a") as f:
    for s in STEPS:
        rec = {"job": a.job, "label": a.label, "step": s}
        try:
            src = f"read_parquet('{base}/{s}/**/*.parquet', hive_partitioning=true, union_by_name=true)"
            cols = con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()
            rec["schema"] = [(c[0], c[1]) for c in cols]
            exprs = []
            for name, typ, *_ in cols:
                t = typ.upper()
                if t.startswith(("DECIMAL", "DOUBLE", "FLOAT", "REAL")): exprs.append(f"round(\"{name}\"::DOUBLE, 2)")
                elif t.startswith(("DATE", "TIMESTAMP")): exprs.append(f"\"{name}\"::VARCHAR")
                elif name == "o_month": exprs.append(f"\"{name}\"::BIGINT")
                else: exprs.append(f"\"{name}\"")
            n, dg = con.execute(f"SELECT count(*), bit_xor(hash({', '.join(exprs)})) FROM {src}").fetchone()
            rec.update(rows=n, digest=str(dg), files=con.execute(f"SELECT count(*) FROM glob('{base}/{s}/**/*.parquet')").fetchone()[0])
        except Exception as e:
            rec["error"] = f"{type(e).__name__}: {str(e)[:500]}"
        f.write(json.dumps(rec) + "\n"); print(json.dumps(rec))
