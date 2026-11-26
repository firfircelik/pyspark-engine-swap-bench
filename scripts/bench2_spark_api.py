"""Phase 2: identical PySpark code for Spark 4.1.3/4.2.0, Comet 1.2.0 (spark-submit) and Sail 0.7.3 (--remote).
Warmup (SF1, untimed for results) is separated from timed queries. Workloads: tpch (22 queries) or etl (DataFrame API, Parquet writes)."""
import argparse, json, os, sys, time
sys.path.insert(0, os.path.dirname(__file__))
from common import *
ap = argparse.ArgumentParser()
ap.add_argument("--label", required=True); ap.add_argument("--base", required=True); ap.add_argument("--warmup-base", required=True)
ap.add_argument("--workload", choices=["tpch", "etl"], required=True); ap.add_argument("--out", required=True)
ap.add_argument("--etl-out"); ap.add_argument("--remote"); ap.add_argument("--qdir", default="/workspace/engine-swap/queries")
ap.add_argument("--rowsdir", required=True); ap.add_argument("--plansdir", required=True)
ap.add_argument("--queries", default=",".join(str(i) for i in range(1, 23)))
ap.add_argument("--format", default="parquet", choices=["parquet", "iceberg", "delta"])
a = ap.parse_args()
from pyspark.sql import SparkSession, functions as F, Window
t0 = time.time()
spark = SparkSession.builder.remote(a.remote).getOrCreate() if a.remote else SparkSession.builder.getOrCreate()
os.makedirs(a.rowsdir, exist_ok=True); os.makedirs(a.plansdir, exist_ok=True)
out = open(a.out, "a")
def emit(rec):
    rec.update(label=a.label, pid=os.getpid(), base=a.base, workload=a.workload)
    out.write(json.dumps(rec) + "\n"); out.flush(); print(json.dumps(rec), flush=True)
def dump_plan(df, name):
    if a.remote: return
    try: open(f"{a.plansdir}/{name}.txt", "w").write(df._jdf.queryExecution().executedPlan().toString())
    except Exception as e: print("plan dump failed", e)
def register(base):
    for t in TABLES:
        path = f"{base}/{t}"
        if a.format == "parquet":
            # glob *.parquet so host snapshot sidecars (*.parquet.box-store-snap-*) are not opened as data files
            spark.read.parquet(f"{path}/*.parquet").createOrReplaceTempView(t)
        elif a.format == "iceberg":
            spark.read.format("iceberg").load(path).createOrReplaceTempView(t)
        else:
            spark.read.format("delta").load(path).createOrReplaceTempView(t)
def run_sql_query(q):
    df = None
    for st in load_statements(a.qdir, q):
        if is_ddl(st): spark.sql(st).collect()
        else: df = spark.sql(st); rows = [tuple(r) for r in df.collect()]
    return df, rows
emit({"event": "session", "session_s": round(time.time() - t0, 3), "version": spark.version, "format": a.format})
# ---- warmup: SF1, Q1 + Q6 + Q3, same storage path type; not part of results
tw = time.time(); register(a.warmup_base)
for q in (1, 6, 3):
    ts = time.time(); err = None
    try: run_sql_query(q)
    except Exception as e: err = f"{type(e).__name__}: {str(e)[:500]}"
    emit({"event": "warmup", "q": q, "wall_s": round(time.time() - ts, 3), "error": err})
emit({"event": "warmup_total", "s": round(time.time() - tw, 3)})
c0 = minio_counters(); tr = time.time(); register(a.base)
emit({"event": "register", "s": round(time.time() - tr, 3), "s3": counter_delta(c0, minio_counters())})

def etl_steps():
    li = spark.table("lineitem"); od = spark.table("orders"); pt = spark.table("part")
    rev = F.col("l_extendedprice") * (1 - F.col("l_discount"))
    yield "E1_filter_agg", (li.filter((F.col("l_shipdate") >= F.lit("1995-01-01").cast("date")) & F.col("l_returnflag").isin("R", "A"))
        .groupBy("l_shipmode", "l_returnflag").agg(F.count("*").alias("n"), F.sum(rev).alias("revenue"))), None
    yield "E2_join_agg", (li.filter(F.col("l_discount") > 0.05).join(pt, F.col("l_partkey") == F.col("p_partkey"))
        .groupBy("p_brand", "p_container").agg(F.count("*").alias("n"), F.sum(rev).alias("revenue"), F.avg("l_quantity").alias("avg_qty"))), None
    w = Window.partitionBy("o_custkey").orderBy(F.col("o_orderdate").desc(), F.col("o_orderkey").desc())
    yield "E3_window_latest", (od.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
        .select("o_custkey", "o_orderkey", "o_orderdate", "o_totalprice")), None
    yield "E4_highcard_agg", (li.groupBy("l_partkey").agg(F.count("*").alias("n"), F.sum(rev).alias("revenue"),
        F.min("l_shipdate").alias("first_ship"), F.max("l_shipdate").alias("last_ship"))), None
    od95 = od.filter(F.year("o_orderdate") == 1995)
    yield "E5_rewrite_partitioned", (od95.withColumn("o_month", F.month("o_orderdate")).withColumn("comment_len", F.length("o_comment"))
        .select("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "comment_len", "o_month")), "o_month"
    upd = (od95.filter(F.col("o_orderkey") % 10 == 0).select("o_orderkey", "o_custkey", F.lit("U").alias("o_orderstatus"),
        (F.col("o_totalprice") * F.lit(1.1)).cast("decimal(15,2)").alias("o_totalprice"), "o_orderdate"))
    base = od95.select("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate")
    yield "E6_upsert_antijoin_union", base.join(upd.select("o_orderkey"), "o_orderkey", "left_anti").unionByName(upd), None

def untimed_cleanup():
    """Between queries, untimed: trigger a JVM GC so Spark's ContextCleaner removes shuffle files of finished queries
    (otherwise SF30 shuffle files pile up in spark.local.dir across 22 queries until the disk fills). No-op for Sail."""
    if a.remote: return
    try: spark._jvm.System.gc(); time.sleep(3)
    except Exception: pass

if a.workload == "tpch":
    for q in [int(x) for x in a.queries.split(",")]:
        emit({"event": "query_start", "q": q, "start": time.time()})
        c0 = minio_counters(); ts = time.time(); err = None; rows = []; df = None
        try: df, rows = run_sql_query(q)
        except Exception as e: err = f"{type(e).__name__}: {str(e)[:1500]}"
        te = time.time()
        rec = {"event": "query", "q": q, "start": ts, "end": te, "wall_s": round(te - ts, 3), "rows": len(rows),
               "s3": counter_delta(c0, minio_counters()), "error": err}
        if not err:
            ho, hs, nrows = result_hashes(rows); rec.update(hash_ordered=ho, hash_set=hs)
            open(f"{a.rowsdir}/q{q}.txt", "w").write("\n".join(nrows) + "\n")
            dump_plan(df, f"q{q}")
        emit(rec); df = None; untimed_cleanup()
        if a.remote and err and ("UNAVAILABLE" in err or "Connection refused" in err or "RETRIES_EXCEEDED" in err):
            # Spark Connect server died (e.g. OOM-killed): stop instead of burning ~10 min of client retries per query
            emit({"event": "server_dead", "q": q}); sys.exit(3)
else:
    for name, df, part in etl_steps():
        c0 = minio_counters(); ts = time.time(); err = None
        try:
            wr = df.write.mode("overwrite")
            if part: wr = wr.partitionBy(part)
            wr.parquet(f"{a.etl_out}/{name}")
        except Exception as e: err = f"{type(e).__name__}: {str(e)[:1500]}"
        te = time.time()
        emit({"event": "query", "q": name, "start": ts, "end": te, "wall_s": round(te - ts, 3), "s3": counter_delta(c0, minio_counters()), "error": err})
        dump_plan(df, name); untimed_cleanup()
spark.stop()
