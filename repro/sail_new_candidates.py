"""Candidate new Sail 0.7.3 divergences from Spark 4.2.0. SQL only, inline literals, no files except C5.
Run: Spark classic (spark-submit) and Sail (--remote). Prints one line per check."""
import sys, os, shutil
from pyspark.sql import SparkSession
remote = sys.argv[1] if len(sys.argv) > 1 else None
spark = SparkSession.builder.remote(remote).getOrCreate() if remote else SparkSession.builder.getOrCreate()
eng = "sail" if remote else "spark"
spark.conf.set("spark.sql.session.timeZone", "UTC")
def show(tag, sql):
    try:
        df = spark.sql(sql); print(tag, "|", df.schema.simpleString(), "|", [tuple(r) for r in df.collect()])
    except Exception as e: print(tag, "| ERROR", type(e).__name__, str(e).splitlines()[0][:200])
print("ENGINE", eng, spark.version)
# C1: time zone rules from tzdata 2025c..2026f (from_utc_timestamp renders UTC instant as local wall time)
show("C1_tz_rules", """select
  cast(from_utc_timestamp(timestamp'2026-11-21 18:00:00', 'America/Havana') as string) havana_2026_11_21,
  cast(from_utc_timestamp(timestamp'2027-01-15 18:00:00', 'America/Edmonton') as string) edmonton_2027_01,
  cast(from_utc_timestamp(timestamp'2027-01-15 18:00:00', 'America/Yellowknife') as string) yellowknife_2027_01,
  cast(from_utc_timestamp(timestamp'2027-04-01 12:00:00', 'Africa/Casablanca') as string) casablanca_2027_04,
  cast(from_utc_timestamp(timestamp'2025-06-01 12:00:00', 'Europe/Istanbul') as string) control_istanbul""")
spark.conf.set("spark.sql.session.timeZone", "America/Havana")
show("C1b_session_tz_havana", "select cast(to_timestamp('2026-11-21T18:00:00Z') as string) local_str, hour(to_timestamp('2026-11-21T18:00:00Z')) h")
spark.conf.set("spark.sql.session.timeZone", "UTC")
# C2: bround is HALF_EVEN
show("C2_bround", "select bround(2.5) a, bround(3.5) b, bround(-2.5) c, bround(2.5D) d, bround(cast(2.5 as decimal(2,1))) e, bround(0.125, 2) f, bround(2.5F) g")
# C3: round on DOUBLE (Spark rounds the decimal representation half-up)
show("C3_round_double", "select round(1.005D, 2) a, round(2.675D, 2) b, round(0.145D, 2) c, round(1.5D) d, round(-0.5D) e")
# C4: format_number rounding mode
show("C4_format_number", "select format_number(cast(0.005 as decimal(5,3)), 2) a, format_number(0.125D, 2) b, format_number(2.5D, 0) c, format_number(cast(1.115 as decimal(5,3)), 2) d")
# C6: double -> string formatting
show("C6_double_to_string", "select cast(1e20D as string) a, cast(1.0E-5D as string) b, cast(123456789.0D as string) c, cast(1e7D as string) d, cast(0.001D as string) e, cast(100.0F as string) f, concat('x', 1e-4D) g")
# C7: string with exponent -> decimal
show("C7_exp_string_to_decimal", "select cast('1e3' as decimal(10,0)) a, cast('1.5E2' as decimal(10,1)) b, cast('-2e-2' as decimal(5,3)) c")
# C5: writing an empty DataFrame keeps the schema
p = f"/workspace/engine-swap/tmp/repro_empty_{eng}"
try:
    spark.sql("select 1 as a, 'x' as b, cast(1.5 as decimal(5,2)) as c where false").write.mode("overwrite").parquet(p)
    df = spark.read.parquet(p); print("C5_empty_write |", df.schema.simpleString(), "| rows", df.count(), "| files", sorted(os.listdir(p)))
except Exception as e: print("C5_empty_write | ERROR", type(e).__name__, str(e).splitlines()[0][:200])
# C8: JSON read, PERMISSIVE: value that overflows the decimal in the schema
pj = f"/workspace/engine-swap/tmp/repro_json_{eng}"
os.makedirs(pj, exist_ok=True); open(f"{pj}/a.json", "w").write('{"k":1,"d":12.5}\n{"k":2,"d":99999999.99}\n{"k":3,"d":"abc"}\n')
try:
    df = spark.read.schema("k int, d decimal(5,2)").json(pj); print("C8_json_permissive |", [tuple(r) for r in df.orderBy("k").collect()])
except Exception as e: print("C8_json_permissive | ERROR", type(e).__name__, str(e).splitlines()[0][:200])
spark.stop()
