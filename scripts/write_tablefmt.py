"""Write TPC-H parquet tables once as Iceberg (Spark 4.1.3) or Delta (Spark 4.2.0) on MinIO.

Usage (via write_tablefmt.sh): spark-submit ... write_tablefmt.py --format iceberg|delta --sf sf1|sf30
"""
import argparse, json, os, sys, time
sys.path.insert(0, os.path.dirname(__file__))
from common import TABLES

ap = argparse.ArgumentParser()
ap.add_argument("--format", required=True, choices=["iceberg", "delta"])
ap.add_argument("--sf", required=True, choices=["sf1", "sf30"])
ap.add_argument("--src", required=True, help="parquet root, e.g. file:///workspace/engine-swap/data/tpch-sf30")
ap.add_argument("--out", required=True, help="jsonl of write records")
a = ap.parse_args()

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
out = open(a.out, "a")

def emit(rec):
    rec.update(format=a.format, sf=a.sf, spark_version=spark.version, pid=os.getpid())
    out.write(json.dumps(rec, default=str) + "\n"); out.flush()
    print(json.dumps(rec, default=str), flush=True)

emit({"event": "session", "app": spark.sparkContext.appName if not getattr(spark, "_jsparkSession", None) or True else ""})

def props_map(ident):
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES {ident}").collect()
        return {r[0]: r[1] for r in rows}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

def count_and_schema(ident_or_df, is_df=False):
    df = ident_or_df if is_df else spark.table(ident_or_df)
    n = df.count()
    schema = df.schema.json()
    return n, schema

t_all = time.time()
written = []
for t in TABLES:
    src = f"{a.src}/{t}"
    ts = time.time()
    err = None
    rec = {"event": "write", "table": t, "src": src}
    try:
        df = spark.read.parquet(src)
        src_n = df.count()
        rec["src_rows"] = src_n
        if a.format == "iceberg":
            ident = f"ice.{a.sf}.{t}"
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS ice.{a.sf}")
            spark.sql(f"DROP TABLE IF EXISTS {ident}")
            (df.writeTo(ident)
               .tableProperty("write.format.default", "parquet")
               .tableProperty("write.parquet.compression-codec", "snappy")
               .tableProperty("format-version", "2")
               .create())
            n, schema = count_and_schema(ident)
            rec.update(ident=ident, location=f"s3a://tpch/iceberg/{a.sf}/{t}",
                       rows=n, schema=schema, tblproperties=props_map(ident))
        else:
            dest = f"s3a://tpch/delta/{a.sf}/{t}"
            (df.write.format("delta").mode("overwrite")
               .option("compression", "snappy")
               .save(dest))
            outdf = spark.read.format("delta").load(dest)
            n, schema = count_and_schema(outdf, is_df=True)
            # Delta detail / properties via SQL if the catalog is hooked up; also try DESCRIBE DETAIL on the path
            detail = {}
            try:
                drows = spark.sql(f"DESCRIBE DETAIL delta.`{dest}`").collect()
                if drows:
                    detail = drows[0].asDict()
                    # drop huge fields
                    for k in list(detail):
                        if k in ("partitionColumns", "properties") or k.endswith("Columns"):
                            continue
                        if isinstance(detail[k], (bytes, bytearray)):
                            detail[k] = str(detail[k])
            except Exception as e:
                detail = {"error": f"{type(e).__name__}: {e}"}
            rec.update(ident=dest, location=dest, rows=n, schema=schema, detail={
                k: detail.get(k) for k in ("format", "id", "name", "location", "createdAt", "lastModified",
                                            "partitionColumns", "numFiles", "sizeInBytes", "minReaderVersion",
                                            "minWriterVersion", "tableFeatures", "properties") if k in detail or True
            } if isinstance(detail, dict) else detail)
        if rec.get("rows") != src_n:
            rec["row_mismatch"] = True
    except Exception as e:
        err = f"{type(e).__name__}: {str(e)[:4000]}"
        rec["error"] = err
    rec["wall_s"] = round(time.time() - ts, 3)
    emit(rec)
    written.append(rec)
    if err:
        # keep going so we record every table; do not hide failures
        print(f"WRITE ERROR {t}: {err}", file=sys.stderr)

emit({"event": "write_total", "s": round(time.time() - t_all, 3),
      "ok": sum(1 for r in written if not r.get("error")),
      "failed": [r["table"] for r in written if r.get("error")],
      "row_mismatch": [r["table"] for r in written if r.get("row_mismatch")]})
spark.stop()
if any(r.get("error") or r.get("row_mismatch") for r in written):
    sys.exit(2)
