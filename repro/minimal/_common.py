"""Shared harness: run with plain Spark (spark-submit file.py) or against a Sail Spark Connect server (python file.py sc://127.0.0.1:50051)."""
import sys
from pyspark.sql import SparkSession
def session():
    remote = sys.argv[1] if len(sys.argv) > 1 else None
    spark = SparkSession.builder.remote(remote).getOrCreate() if remote else SparkSession.builder.getOrCreate()
    eng = "sail" if remote else "spark"
    print(f"ENGINE {eng} (pyspark client {spark.version})")
    return spark, eng
def show(spark, tag, sql):
    try:
        df = spark.sql(sql); print(tag, "|", df.schema.simpleString(), "|", [tuple(r) for r in df.collect()])
    except Exception as e:
        print(tag, "| ERROR", type(e).__name__, str(e).splitlines()[0][:240])
