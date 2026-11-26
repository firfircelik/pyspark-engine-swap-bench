"""Same PySpark code for Spark 4.1.3 / 4.2.0 / 4.2.0+Comet (spark-submit) and Sail (Spark Connect).
Usage: bench_spark_api.py --label X --base s3a://tpch/tpch-sf10 --queries 1,3,6,9,18 --out results/x.jsonl [--remote sc://localhost:50051]
"""
import argparse, json, os, sys, time, traceback
sys.path.insert(0, os.path.dirname(__file__))
from common import *

ap = argparse.ArgumentParser()
ap.add_argument("--label", required=True); ap.add_argument("--base", required=True)
ap.add_argument("--queries", default="1,3,6,9,18"); ap.add_argument("--out", required=True)
ap.add_argument("--remote"); ap.add_argument("--qdir", default="/workspace/engine-swap/queries")
ap.add_argument("--rowsdir", default="/workspace/engine-swap/results/rows")
a = ap.parse_args()

from pyspark.sql import SparkSession
t_start = time.time()
if a.remote:
    spark = SparkSession.builder.remote(a.remote).getOrCreate()
else:
    spark = SparkSession.builder.getOrCreate()
t_session = time.time() - t_start
os.makedirs(a.rowsdir, exist_ok=True)
out = open(a.out, "a")
def emit(rec):
    rec.update(label=a.label, pid=os.getpid(), base=a.base)
    out.write(json.dumps(rec) + "\n"); out.flush(); print(json.dumps(rec), flush=True)

emit({"event": "session", "session_s": round(t_session, 3), "version": spark.version})
c0 = minio_counters(); t0 = time.time()
for t in TABLES:
    spark.read.parquet(f"{a.base}/{t}/").createOrReplaceTempView(t)
emit({"event": "register", "s": round(time.time() - t0, 3), "s3": counter_delta(c0, minio_counters())})

for q in [int(x) for x in a.queries.split(",")]:
    sql = load_query(a.qdir, q)
    c0 = minio_counters(); ts = time.time(); err = None; rows = []
    try:
        df = spark.sql(sql)
        rows = [tuple(r) for r in df.collect()]
        if not a.remote:
            try:
                os.makedirs(f"{a.rowsdir}/../plans", exist_ok=True)
                open(f"{a.rowsdir}/../plans/{a.label}_q{q}.txt", "w").write(df._jdf.queryExecution().executedPlan().toString())
            except Exception as pe: print("plan dump failed", pe)
    except Exception as e:
        err = f"{type(e).__name__}: {str(e)[:2000]}"
    te = time.time()
    rec = {"event": "query", "q": q, "start": ts, "end": te, "wall_s": round(te - ts, 3),
           "rows": len(rows), "s3": counter_delta(c0, minio_counters()), "error": err}
    if not err:
        ho, hs, nrows = result_hashes(rows)
        rec.update(hash_ordered=ho, hash_set=hs)
        with open(f"{a.rowsdir}/{a.label}_q{q}.txt", "w") as f: f.write("\n".join(nrows) + "\n")
    emit(rec)
spark.stop()
