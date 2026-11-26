"""F1 row identity: compare the archived E6 outputs (Sail vs Spark 4.2.0) row by row, and check that the
binary64 model predicts exactly the same o_orderkeys. Needs MinIO with the etl-scratch outputs and the SF30 data.
Writes out/f1_mismatch_orderkeys.csv and out/f1_rows.json."""
import os, json, math, duckdb
from decimal import Decimal, ROUND_HALF_UP, getcontext
from common import ROOT, OUT
getcontext().prec = 80
con = duckdb.connect(config={"extension_directory": f"{ROOT}/opt/duckdb-ext", "autoinstall_known_extensions": False})
con.execute("LOAD httpfs")
con.execute(f"CREATE SECRET m (TYPE s3, KEY_ID '{os.environ['BENCH_S3_KEY']}', SECRET '{os.environ['BENCH_S3_SECRET']}', ENDPOINT '127.0.0.1:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')")
def e6(cfg):
    return f"read_parquet('s3://tpch/etl-scratch/{cfg}/E6_upsert_antijoin_union/*.parquet')"
res = {}
# measured: updated rows (o_orderstatus='U') where Sail and Spark differ
meas = con.execute(f"""
  with s as (select o_orderkey, o_totalprice p from {e6('sail-noreorder')} where o_orderstatus='U'),
       k as (select o_orderkey, o_totalprice p from {e6('spark420-classic')} where o_orderstatus='U')
  select k.o_orderkey, cast(k.p as varchar), cast(s.p as varchar) from k join s using (o_orderkey) where k.p <> s.p order by 1""").fetchall()
res["updated_rows_spark"] = con.execute(f"select count(*) from {e6('spark420-classic')} where o_orderstatus='U'").fetchone()[0]
res["updated_rows_sail"] = con.execute(f"select count(*) from {e6('sail-noreorder')} where o_orderstatus='U'").fetchone()[0]
res["total_rows_spark"] = con.execute(f"select count(*) from {e6('spark420-classic')}").fetchone()[0]
res["total_rows_sail"] = con.execute(f"select count(*) from {e6('sail-noreorder')}").fetchone()[0]
res["non_updated_diffs"] = con.execute(f"""select count(*) from (select * from {e6('spark420-classic')} where o_orderstatus<>'U') k
   full join (select * from {e6('sail-noreorder')} where o_orderstatus<>'U') s using (o_orderkey)
   where k.o_totalprice is distinct from s.o_totalprice""").fetchone()[0]
res["measured_mismatches"] = len(meas)
res["measured_sail_lower_by_one_cent"] = sum(1 for _, k, s in meas if Decimal(k) - Decimal(s) == Decimal("0.01"))
# other engines vs Spark 4.2.0 on the updated rows
for cfg in ["comet420-classic", "spark413-classic", "duckdb", "sail-reorder"]:
    try:
        res[f"diffs_vs_spark420_{cfg}"] = con.execute(f"""select count(*) from (select o_orderkey,o_totalprice p from {e6('spark420-classic')} where o_orderstatus='U') k
          join (select o_orderkey,o_totalprice p from {e6(cfg)} where o_orderstatus='U') s using (o_orderkey) where k.p<>s.p""").fetchone()[0]
    except Exception as e:
        res[f"diffs_vs_spark420_{cfg}"] = f"error: {type(e).__name__}"
# model: Spark = shortest repr of binary64 product, HALF_UP; Sail = arrow-cast round(100*x) half away from zero
rows = con.execute(f"""select o_orderkey, cast(o_totalprice as varchar) from read_parquet('{ROOT}/data/tpch-sf30/orders/*.parquet')
   where year(o_orderdate)=1995 and o_orderkey % 10 = 0""").fetchall()
q = Decimal("0.01"); pred = []
for k, p in rows:
    x = float(Decimal(p)) * 1.1
    spark = Decimal(repr(x)).quantize(q, ROUND_HALF_UP)
    sail = Decimal(100.0 * x).quantize(Decimal(1), ROUND_HALF_UP).scaleb(-2)
    if sail != spark: pred.append((k, p, str(spark), str(sail)))
res["model_population"] = len(rows); res["model_mismatches"] = len(pred)
mk = {r[0] for r in meas}; pk = {r[0] for r in pred}
res["set_intersection"] = len(mk & pk); res["only_measured"] = len(mk - pk); res["only_model"] = len(pk - mk)
pv = {r[0]: (r[2], r[3]) for r in pred}
res["value_match_on_intersection"] = sum(1 for k, a, b in meas if k in pv and Decimal(pv[k][0]) == Decimal(a) and Decimal(pv[k][1]) == Decimal(b))
with open(f"{OUT}/f1_mismatch_orderkeys.csv", "w") as f:
    f.write("o_orderkey,o_totalprice_input,spark420_measured,sail073_measured,spark_model,sail_model\n")
    src = {k: p for k, p in rows}
    for k, a, b in meas:
        sm, lm = pv.get(k, ("", ""))
        f.write(f"{k},{src.get(k,'')},{a},{b},{sm},{lm}\n")
json.dump(res, open(f"{OUT}/f1_rows.json", "w"), indent=1); print(json.dumps(res, indent=1))
