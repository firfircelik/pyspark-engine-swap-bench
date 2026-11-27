"""Polars line: same SQL text as DuckDB for TPC-H and the ETL steps, run through pl.SQLContext on the streaming engine.
Fresh process per run; warmup separated. Polars reads the cgroup memory.max on its own (no memory knob is set)."""
import argparse, json, os, sys, time
sys.path.insert(0, os.path.dirname(__file__))
from common import *
ap = argparse.ArgumentParser()
ap.add_argument("--label", required=True); ap.add_argument("--base", required=True); ap.add_argument("--warmup-base", required=True)
ap.add_argument("--workload", choices=["tpch", "etl"], required=True); ap.add_argument("--out", required=True)
ap.add_argument("--etl-out"); ap.add_argument("--qdir", default="/workspace/engine-swap/queries"); ap.add_argument("--rowsdir", required=True)
ap.add_argument("--plansdir"); ap.add_argument("--threads", default="8")
ap.add_argument("--queries", default=",".join(str(i) for i in range(1, 23)))
ap.add_argument("--format", default="parquet", choices=["parquet"])
a = ap.parse_args()
os.environ.setdefault("POLARS_MAX_THREADS", a.threads)
os.environ.setdefault("POLARS_OOC_SPILL_DIR", "/workspace/engine-swap/tmp/polars")
os.makedirs(os.environ["POLARS_OOC_SPILL_DIR"], exist_ok=True)
os.makedirs(a.rowsdir, exist_ok=True)
out = open(a.out, "a")
def emit(rec):
    rec.update(label=a.label, pid=os.getpid(), base=a.base, workload=a.workload); out.write(json.dumps(rec)+"\n"); out.flush(); print(json.dumps(rec), flush=True)
t0 = time.time()
import polars as pl
port = os.environ.get("S3_PORT", "9000")
def storage_options(base):
    if not base.startswith("s3a://"):
        return None
    return {"aws_access_key_id": os.environ["BENCH_S3_KEY"], "aws_secret_access_key": os.environ["BENCH_S3_SECRET"],
            "aws_endpoint_url": f"http://127.0.0.1:{port}", "aws_region": "us-east-1", "aws_allow_http": "true"}
def p(path):
    return path.replace("s3a://", "s3://").replace("file://", "")
ctx = pl.SQLContext()
emit({"event": "session", "session_s": round(time.time()-t0, 3), "version": pl.__version__, "format": a.format,
      "threads": pl.thread_pool_size()})
def register(base):
    so = storage_options(base)
    for t in TABLES:
        ctx.register(t, pl.scan_parquet(f"{p(base)}/{t}/*.parquet", storage_options=so))
def run_sql_query(q):
    rows = []
    for st in load_statements(a.qdir, q):
        if is_ddl(st): ctx.execute(st)
        else: rows = ctx.execute(st).collect(engine="streaming").rows()
    return rows
tw = time.time(); register(a.warmup_base)
for q in (1, 6, 3):
    ts = time.time(); run_sql_query(q); emit({"event": "warmup", "q": q, "wall_s": round(time.time()-ts, 3), "error": None})
emit({"event": "warmup_total", "s": round(time.time()-tw, 3)})
c0 = minio_counters(); tr = time.time(); register(a.base)
emit({"event": "register", "s": round(time.time()-tr, 3), "s3": counter_delta(c0, minio_counters())})
# Same SQL as the ETL list in bench2_duckdb.py.
ETL = [
 ("E1_filter_agg", "SELECT l_shipmode, l_returnflag, count(*) AS n, sum(l_extendedprice*(1-l_discount)) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1995-01-01' AND l_returnflag IN ('R','A') GROUP BY ALL", None),
 ("E2_join_agg", "SELECT p_brand, p_container, count(*) AS n, sum(l_extendedprice*(1-l_discount)) AS revenue, avg(l_quantity) AS avg_qty FROM lineitem JOIN part ON l_partkey=p_partkey WHERE l_discount > 0.05 GROUP BY ALL", None),
 ("E3_window_latest", "SELECT o_custkey, o_orderkey, o_orderdate, o_totalprice FROM orders QUALIFY row_number() OVER (PARTITION BY o_custkey ORDER BY o_orderdate DESC, o_orderkey DESC) = 1", None),
 ("E4_highcard_agg", "SELECT l_partkey, count(*) AS n, sum(l_extendedprice*(1-l_discount)) AS revenue, min(l_shipdate) AS first_ship, max(l_shipdate) AS last_ship FROM lineitem GROUP BY l_partkey", None),
 ("E5_rewrite_partitioned", "SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, length(o_comment) AS comment_len, month(o_orderdate) AS o_month FROM orders WHERE year(o_orderdate) = 1995", "o_month"),
 ("E6_upsert_antijoin_union", "WITH upd AS (SELECT o_orderkey, o_custkey, 'U' AS o_orderstatus, CAST(o_totalprice*1.1 AS DECIMAL(15,2)) AS o_totalprice, o_orderdate FROM orders WHERE o_orderkey % 10 = 0 AND year(o_orderdate) = 1995) SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate FROM orders ANTI JOIN upd USING (o_orderkey) WHERE year(o_orderdate) = 1995 UNION ALL BY NAME SELECT * FROM upd", None),
]
if a.workload == "tpch":
    for q in [int(x) for x in a.queries.split(",")]:
        c0 = minio_counters(); ts = time.time(); err = None; rows = []
        try: rows = run_sql_query(q)
        except Exception as e: err = f"{type(e).__name__}: {str(e)[:1500]}"
        te = time.time()
        rec = {"event":"query","q":q,"start":ts,"end":te,"wall_s":round(te-ts,3),"rows":len(rows),"s3":counter_delta(c0, minio_counters()),"error":err}
        if not err:
            ho, hs, nrows = result_hashes(rows); rec.update(hash_ordered=ho, hash_set=hs)
            open(f"{a.rowsdir}/q{q}.txt","w").write("\n".join(nrows)+"\n")
        emit(rec)
else:
    so = storage_options(a.etl_out)
    for name, sql, part in ETL:
        c0 = minio_counters(); ts = time.time(); err = None
        try:
            lf = ctx.execute(sql)
            if part: target = pl.PartitionBy(f"{p(a.etl_out)}/{name}", key=part, include_key=False)
            else: target = f"{p(a.etl_out)}/{name}/data.parquet"
            lf.sink_parquet(target, storage_options=so, mkdir=True, engine="streaming")
        except Exception as e: err = f"{type(e).__name__}: {str(e)[:1500]}"
        te = time.time()
        emit({"event":"query","q":name,"start":ts,"end":te,"wall_s":round(te-ts,3),"s3":counter_delta(c0, minio_counters()),"error":err})
