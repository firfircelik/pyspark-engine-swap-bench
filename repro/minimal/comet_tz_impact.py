"""Comet 1.2.0 native kernels use chrono-tz (tzdata 2025b); Spark uses the JVM tzdb (2026f here). Known upstream:
apache/datafusion-comet#6331 (closed by a startup warning, #6353). This shows the current real-world impact:
a Parquet timestamp read with session zone America/Havana. Expected (Spark): ts_str 2026-11-15 08:00:00, hour 8,
to_timestamp epoch 1794744000 (instant of '2026-11-15 08:00:00' Havana = 12:00 UTC)."""
import os, sys
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
comet = "CometPlugin" in spark.conf.get("spark.plugins", "")
print("ENGINE", "comet" if comet else "spark", spark.version)
d = "/workspace/engine-swap/tmp/repro_comet_tz_pq"
spark.conf.set("spark.sql.session.timeZone", "UTC")
if not os.path.exists(f"{d}/_SUCCESS"):
    spark.sql("select timestamp'2026-11-15 12:00:00' ts, '2026-11-15 08:00:00' s").repartition(1).write.mode("overwrite").parquet(d)
spark.conf.set("spark.sql.session.timeZone", "America/Havana")
df = spark.read.parquet(d).select(F.col("ts").cast("string").alias("ts_str"), F.hour("ts").alias("hr"),
        F.to_timestamp("s").cast("long").alias("str_to_ts_epoch"), F.col("ts").cast("timestamp_ntz").cast("string").alias("ntz"))
print("COMET_TZ |", [tuple(r) for r in df.collect()])
plan = df._jdf.queryExecution().executedPlan().toString()
print("NATIVE_PROJECT |", "CometProject" in plan)
spark.stop()
