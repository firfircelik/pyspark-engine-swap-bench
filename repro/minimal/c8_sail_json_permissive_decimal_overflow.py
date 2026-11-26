"""C8: spark.read.json with an explicit DECIMAL schema, default mode PERMISSIVE. A value that does not fit the decimal
must become NULL for that row (Spark); Sail 0.7.3 fails the whole read ('parse decimal overflow').
Expected (Spark 4.2.0): [(1, 12.50), (2, None), (3, None)]"""
import os
from _common import session
spark, eng = session()
d = f"/workspace/engine-swap/tmp/repro_c8_{eng}"; os.makedirs(d, exist_ok=True)
open(f"{d}/a.json", "w").write('{"k":1,"d":12.5}\n{"k":2,"d":99999999.99}\n{"k":3,"d":"abc"}\n')
for mode in ["PERMISSIVE (default)", "PERMISSIVE"]:
    try:
        r = spark.read.schema("k int, d decimal(5,2)")
        if mode == "PERMISSIVE": r = r.option("mode", "PERMISSIVE")
        print("C8", mode, "|", [tuple(x) for x in r.json(d).orderBy("k").collect()])
    except Exception as e: print("C8", mode, "| ERROR", type(e).__name__, str(e).splitlines()[0][:240])
spark.stop()
