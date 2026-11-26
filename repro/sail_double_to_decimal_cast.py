"""Sail vs Spark: casting a DOUBLE to DECIMAL rounds differently at .xx5 values.
Found via ETL step E6 (o_totalprice * F.lit(1.1)).cast('decimal(15,2)'): 631 of 683,346 rows differ at SF30."""
import sys
from pyspark.sql import SparkSession, functions as F
remote = sys.argv[1] if len(sys.argv) > 1 else None
spark = SparkSession.builder.remote(remote).getOrCreate() if remote else SparkSession.builder.getOrCreate()
print("ENGINE", "sail" if remote else "spark", spark.version)
q = """select v, cast(v as decimal(15,2)) as d2, cast(v as decimal(10,1)) as d1, cast(v as string) as s
       from values (cast(38771.645 as double)), (cast(0.145 as double)), (cast(1.005 as double)), (cast(2.675 as double)), (cast(-1.005 as double)) as t(v)"""
for r in spark.sql(q).collect(): print("SQL", tuple(r))
from decimal import Decimal
df = spark.createDataFrame([(Decimal("35246.95"),)], "p decimal(15,2)")
r = df.select((F.col("p") * F.lit(1.1)).alias("x"), (F.col("p") * F.lit(1.1)).cast("decimal(15,2)").alias("y"))
print("DF schema", r.schema.simpleString(), "row", tuple(r.collect()[0]))
spark.stop()
