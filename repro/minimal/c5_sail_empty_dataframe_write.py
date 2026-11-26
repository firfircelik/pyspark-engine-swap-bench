"""C5: writing an empty DataFrame. Spark writes _SUCCESS + one schema-only Parquet file, so a later read returns the
schema (a int, b string, c decimal(5,2)) with 0 rows. Sail 0.7.3 creates no directory; reading the path back returns an
empty struct<> without error (read side = lakehq/sail#3061, fix PR #3074). Write side not found upstream."""
import os
from _common import session
spark, eng = session()
p = f"/workspace/engine-swap/tmp/repro_c5_{eng}"
spark.sql("select 1 as a, 'x' as b, cast(1.5 as decimal(5,2)) as c where false").write.mode("overwrite").parquet(p)
print("C5_after_write | exists", os.path.exists(p), "| files", sorted(os.listdir(p)) if os.path.exists(p) else None)
try:
    df = spark.read.parquet(p); print("C5_read_back |", df.schema.simpleString(), "| rows", df.count())
except Exception as e: print("C5_read_back | ERROR", type(e).__name__, str(e).splitlines()[0][:240])
spark.stop()
