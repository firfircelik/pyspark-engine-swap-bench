"""Everyday PySpark API probe: each check runs on Spark (local) and Sail (connect); records ok/error and a result digest."""
import argparse, json, os, sys, traceback, hashlib
ap = argparse.ArgumentParser(); ap.add_argument("--remote"); ap.add_argument("--label", required=True)
ap.add_argument("--base", default="s3a://tpch/tpch-sf10"); ap.add_argument("--out", required=True)
a = ap.parse_args()
from pyspark.sql import SparkSession, functions as F, Window, types as T
spark = SparkSession.builder.remote(a.remote).getOrCreate() if a.remote else SparkSession.builder.getOrCreate()
li = spark.read.parquet(f"{a.base}/lineitem/").filter("l_orderkey <= 200000")
od = spark.read.parquet(f"{a.base}/orders/")
nat = spark.read.parquet(f"{a.base}/nation/")
def dig(rows): return hashlib.sha256("\n".join(sorted(str(tuple(r)) if hasattr(r, "__iter__") and not isinstance(r, str) else str(r) for r in rows)).encode()).hexdigest()[:12]
checks = {
 "window_row_number_dedup": lambda: li.withColumn("rn", F.row_number().over(Window.partitionBy("l_orderkey").orderBy(F.col("l_linenumber").desc()))).filter("rn=1").groupBy().count().collect(),
 "window_lag_running_sum": lambda: li.withColumn("s", F.sum("l_quantity").over(Window.partitionBy("l_suppkey").orderBy("l_orderkey","l_linenumber").rowsBetween(Window.unboundedPreceding, 0))).agg(F.sum("s")).collect(),
 "python_udf": lambda: li.select(F.udf(lambda s: s[:3], "string")("l_shipmode").alias("m")).distinct().collect(),
 "pandas_udf": lambda: li.select(F.pandas_udf(lambda s: s * 2, "double")(F.col("l_quantity").cast("double")).alias("x")).agg(F.sum("x")).collect(),
 "mapInPandas": lambda: li.select("l_quantity").mapInPandas(lambda it: (pdf[pdf.l_quantity > 25] for pdf in it), "l_quantity decimal(15,2)").groupBy().count().collect(),
 "applyInPandas": lambda: li.select("l_returnflag", F.col("l_quantity").cast("double").alias("q")).groupBy("l_returnflag").applyInPandas(lambda pdf: pdf.assign(q=pdf.q - pdf.q.mean()).head(1), "l_returnflag string, q double").collect(),
 "cache_count": lambda: [li.cache().count(), li.unpersist() and 0][:1],
 "broadcast_hint_join": lambda: li.join(F.broadcast(nat), li.l_suppkey % 25 == nat.n_nationkey).groupBy("n_name").count().collect(),
 "pivot": lambda: li.groupBy("l_returnflag").pivot("l_linestatus").agg(F.sum("l_quantity")).collect(),
 "explode_split": lambda: nat.select(F.explode(F.split("n_comment", " ")).alias("w")).groupBy().count().collect(),
 "approx_percentile": lambda: li.agg(F.percentile_approx("l_extendedprice", 0.5, 10000)).collect(),
 "count_distinct_approx": lambda: li.agg(F.approx_count_distinct("l_orderkey")).collect(),
 "date_ops": lambda: li.select(F.date_trunc("month", "l_shipdate").alias("m"), F.datediff("l_receiptdate","l_shipdate").alias("d")).groupBy("m").agg(F.avg("d")).collect(),
 "regexp_extract": lambda: nat.select(F.regexp_extract("n_comment", r"(\w+)ly", 1).alias("x")).collect(),
 "struct_json": lambda: nat.select(F.to_json(F.struct("n_name","n_regionkey")).alias("j")).select(F.from_json("j","n_name string, n_regionkey long").alias("s")).select("s.*").collect(),
 "dropDuplicates_subset": lambda: li.dropDuplicates(["l_orderkey"]).groupBy().count().collect(),
 "union_by_name": lambda: nat.select("n_name","n_nationkey").unionByName(nat.select("n_nationkey","n_name")).groupBy().count().collect(),
 "write_parquet_s3_overwrite_partitioned": lambda: (li.select("l_orderkey","l_returnflag","l_quantity").write.mode("overwrite").partitionBy("l_returnflag").parquet(f"s3a://tpch/probe-out/{a.label}/li"), spark.read.parquet(f"s3a://tpch/probe-out/{a.label}/li").groupBy("l_returnflag").count().collect())[1],
 "temp_view_sql_cte": lambda: (li.createOrReplaceTempView("li_p"), spark.sql("with t as (select l_returnflag f, sum(l_quantity) q from li_p group by 1) select * from t order by f").collect())[1],
 "rdd_access": lambda: nat.rdd.map(lambda r: r.n_name).take(3),
 "toPandas": lambda: [len(nat.toPandas())],
 "explain_string": lambda: [len(li._jdf.queryExecution().toString()) if hasattr(li, "_jdf") else li.explain(mode="formatted") or 1],
 "sample_seed": lambda: [li.sample(fraction=0.1, seed=42).count()],
 "describe": lambda: li.select("l_quantity").describe().collect(),
}
out = open(a.out, "a")
for name, fn in checks.items():
    try:
        r = fn(); rec = {"check": name, "ok": True, "digest": dig(r), "sample": str(r[:2])[:200]}
    except Exception as e:
        rec = {"check": name, "ok": False, "error": f"{type(e).__name__}: {str(e).splitlines()[0][:400] if str(e) else ''}"}
    rec["label"] = a.label; out.write(json.dumps(rec)+"\n"); out.flush(); print(json.dumps(rec), flush=True)
spark.stop()
