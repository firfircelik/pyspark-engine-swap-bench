"""Minimal reproducers: run with Spark classic (spark-submit) and with Sail (--remote sc://127.0.0.1:50051). Inline data, no S3."""
import sys, os
from pyspark.sql import SparkSession, functions as F
remote = sys.argv[1] if len(sys.argv) > 1 else None
spark = SparkSession.builder.remote(remote).getOrCreate() if remote else SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
print("ENGINE", "sail" if remote else "spark", spark.version)
df = spark.sql("select cast(x as decimal(15,2)) as p from values (10.00),(20.00),(30.00),(40.00) as t(x)")
r = df.agg(F.percentile_approx("p", 0.5, 10000).alias("m"))
print("R1 percentile_approx schema:", r.schema.simpleString(), "value:", r.collect()[0][0])
try:
    r = df.agg(F.percentile_approx("p", F.array(F.lit(0.25), F.lit(0.75)), 10000).alias("m"))
    print("R1b percentile_approx array schema:", r.schema.simpleString(), "value:", r.collect()[0][0])
except Exception as e: print("R1b ERROR", type(e).__name__, str(e)[:300])
d = spark.sql("select cast(x as decimal(15,2)) as q from values (1.00),(2.00),(4.00) as t(x)")
for nm, fn in [("R2 describe", lambda: d.describe()), ("R2b summary", lambda: d.summary())]:
    try: print(nm+":", [tuple(x) for x in fn().collect()])
    except Exception as e: print(nm, "ERROR", type(e).__name__, str(e)[:300])
t = spark.sql("select date'1993-10-17' as d")
r = t.select(F.date_trunc("month", "d").alias("m"))
print("R3 date_trunc schema:", r.schema.simpleString(), "value:", r.collect()[0][0], "as string:", r.select(F.col("m").cast("string")).collect()[0][0])
spark.stop()
