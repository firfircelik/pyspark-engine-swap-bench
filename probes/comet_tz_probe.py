"""Comet 1.2.0 native tz database (2025b) vs JVM tzdata (2026f). Writes UTC instants to Parquet (plain Spark),
then reads them back with a non-UTC session time zone and evaluates tz-sensitive expressions.
Run once with Comet and once without; compare. Usage: comet_tz_probe.py <label> <out.jsonl> <datadir>"""
import sys, json, os
from pyspark.sql import SparkSession, functions as F
label, outp, ddir = sys.argv[1], sys.argv[2], sys.argv[3]
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
# instants (UTC) around rule changes made in tzdata 2025c..2026f
inst = [
 ("havana_after_nov1", "2026-11-15 12:00:00"), ("havana_feb2027", "2027-02-01 12:00:00"),
 ("edmonton_after_nov1", "2026-11-15 12:00:00"), ("edmonton_jan2027", "2027-01-15 18:00:00"),
 ("casablanca_2026_12", "2026-12-15 12:00:00"), ("casablanca_2027_04", "2027-04-01 12:00:00"),
 ("chisinau_2023_03_26", "2023-03-26 00:30:00"), ("chisinau_2024_10_27", "2024-10-27 00:30:00"),
 ("saopaulo_1965_04_01", "1965-04-01 02:30:00"), ("control_2025_06", "2025-06-01 12:00:00"),
]
if not os.path.exists(f"{ddir}/_SUCCESS"):
    spark.createDataFrame([(n, s) for n, s in inst], "name string, s string").select("name", F.to_timestamp("s").alias("ts"), "s") \
        .repartition(1).write.mode("overwrite").parquet(ddir)
zones = ["America/Havana", "America/Edmonton", "America/Yellowknife", "Africa/Casablanca", "Europe/Chisinau", "America/Sao_Paulo", "UTC"]
out = open(outp, "a")
for z in zones:
    spark.conf.set("spark.sql.session.timeZone", z)
    df = spark.read.parquet(ddir).select("name",
        F.col("ts").cast("string").alias("ts_str"), F.hour("ts").alias("hr"), F.col("ts").cast("date").cast("string").alias("dt"),
        F.date_format("ts", "yyyy-MM-dd HH:mm XXX").alias("fmt"), F.date_trunc("day", "ts").cast("long").alias("trunc_day_epoch"),
        F.from_utc_timestamp("ts", z).cast("long").alias("from_utc_epoch"), F.to_utc_timestamp("ts", z).cast("long").alias("to_utc_epoch"),
        F.unix_timestamp("s").alias("unix_from_str"), F.to_timestamp("s").cast("long").alias("str_to_ts_epoch"),
        F.col("ts").cast("timestamp_ntz").cast("string").alias("ltz_to_ntz"))
    rows = df.collect()
    try: plan = df._jdf.queryExecution().executedPlan().toString()
    except Exception as e: plan = str(e)
    native = "CometProject" in plan
    for r in rows:
        out.write(json.dumps({"label": label, "zone": z, "native_project": native, **r.asDict()}) + "\n")
    open(f"{os.path.dirname(outp)}/{label}_{z.replace('/','_')}_plan.txt", "w").write(plan)
spark.stop()
