"""Phase 2 DuckDB reference line: same SQL for TPC-H, SQL equivalents of the ETL DataFrame steps.
External file cache disabled; fresh process per run; warmup separated."""
import argparse, json, os, sys, time
sys.path.insert(0, os.path.dirname(__file__))
from common import *
import duckdb
ap = argparse.ArgumentParser()
ap.add_argument("--label", required=True); ap.add_argument("--base", required=True); ap.add_argument("--warmup-base", required=True)
ap.add_argument("--workload", choices=["tpch", "etl"], required=True); ap.add_argument("--out", required=True)
ap.add_argument("--etl-out"); ap.add_argument("--qdir", default="/workspace/engine-swap/queries"); ap.add_argument("--rowsdir", required=True)
ap.add_argument("--plansdir"); ap.add_argument("--memory-limit", default="7GB")
ap.add_argument("--queries", default=",".join(str(i) for i in range(1, 23)))
ap.add_argument("--format", default="parquet", choices=["parquet", "iceberg", "delta"])
a = ap.parse_args()
os.makedirs(a.rowsdir, exist_ok=True)
out = open(a.out, "a")
def emit(rec):
    rec.update(label=a.label, pid=os.getpid(), base=a.base, workload=a.workload); out.write(json.dumps(rec)+"\n"); out.flush(); print(json.dumps(rec), flush=True)
def p(path):
    return path.replace("s3a://", "s3://").replace("file://", "")
t0 = time.time()
con = duckdb.connect(config={"extension_directory": "/workspace/engine-swap/opt/duckdb-ext", "autoinstall_known_extensions": False})
con.execute("SET threads=8"); con.execute(f"SET memory_limit='{a.memory_limit}'")
con.execute("SET temp_directory='/workspace/engine-swap/tmp/duckdb'")
con.execute("SET enable_external_file_cache=false")
con.execute("LOAD httpfs")
if a.format == "iceberg":
    con.execute("LOAD avro")  # required by the 2.0.1 iceberg extension
    con.execute("LOAD iceberg")
elif a.format == "delta":
    con.execute("LOAD delta")
port = os.environ.get("S3_PORT", "9000")
con.execute(f"CREATE SECRET minio (TYPE s3, KEY_ID '{os.environ['BENCH_S3_KEY']}', SECRET '{os.environ['BENCH_S3_SECRET']}', ENDPOINT '127.0.0.1:{port}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')")
emit({"event": "session", "session_s": round(time.time()-t0, 3), "version": duckdb.__version__, "format": a.format})
def register(base):
    for t in TABLES:
        path = f"{p(base)}/{t}"
        if a.format == "parquet":
            con.execute(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet('{path}/*.parquet')")
        elif a.format == "iceberg":
            # Path scan of a Hadoop-catalog table. Version guessing lets us point at the table root
            # instead of a specific metadata.json (DuckDB 2.0 docs: required unless guessing is on).
            # Hadoop catalog writes metadata/version-hint.text; DuckDB uses that hint.
            # Globbing for the latest metadata.json is off by default (unsafe_enable_version_guessing).
            con.execute(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM iceberg_scan('{path}', allow_moved_paths=true)")
        else:
            con.execute(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM delta_scan('{path}')")
def run_sql_query(q):
    rows = []
    for st in load_statements(a.qdir, q):
        if is_ddl(st): con.execute(st)
        else: rows = con.execute(st).fetchall()
    return rows
tw = time.time(); register(a.warmup_base)
for q in (1, 6, 3):
    ts = time.time(); run_sql_query(q); emit({"event": "warmup", "q": q, "wall_s": round(time.time()-ts, 3), "error": None})
emit({"event": "warmup_total", "s": round(time.time()-tw, 3)})
c0 = minio_counters(); tr = time.time(); register(a.base)
emit({"event": "register", "s": round(time.time()-tr, 3), "s3": counter_delta(c0, minio_counters())})
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
    for name, sql, part in ETL:
        c0 = minio_counters(); ts = time.time(); err = None
        try:
            opts = f"FORMAT parquet, OVERWRITE_OR_IGNORE true" + (f", PARTITION_BY ({part})" if part else "")
            target = f"{p(a.etl_out)}/{name}" + ("" if part else "/data.parquet")
            con.execute(f"COPY ({sql}) TO '{target}' ({opts})")
        except Exception as e: err = f"{type(e).__name__}: {str(e)[:1500]}"
        te = time.time()
        emit({"event":"query","q":name,"start":ts,"end":te,"wall_s":round(te-ts,3),"s3":counter_delta(c0, minio_counters()),"error":err})
