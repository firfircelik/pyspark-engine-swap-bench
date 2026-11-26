"""(2c) Exact binary64 analysis for F1 (double -> decimal(15,2)) and C4 (format_number).
Reproduces the F1 row count from the SF30 data without running any engine."""
import json, sys, math
from decimal import Decimal, ROUND_HALF_UP, ROUND_HALF_EVEN, getcontext
import numpy as np
getcontext().prec = 80
OUT = "/workspace/engine-swap/article/academic/out"
res = {}

def exact(x): return str(Decimal(x))
def spark_cast(x, s=2):      # Spark: Decimal(double) via Scala BigDecimal.decimal(d) = Double.toString(d) (shortest repr), then HALF_UP
    return Decimal(repr(x)).quantize(Decimal(1).scaleb(-s), ROUND_HALF_UP)
def arrow_cast(x, s=2):      # arrow-cast float_to_decimal: (10^s * x) in binary64, f64::round (half away from zero)
    y = (10.0 ** s) * x
    r = math.floor(abs(y) + 0.5) * (1 if y >= 0 else -1)
    return Decimal(int(r)).scaleb(-s)
def exact_half_up(x, s=2): return Decimal(x).quantize(Decimal(1).scaleb(-s), ROUND_HALF_UP)

f1 = []
for v in [38771.645, 0.145, 1.005, 2.675, -1.005]:
    f1.append(dict(literal=repr(v), exact_binary64=exact(v), x100_binary64=exact(100.0 * v), spark=str(spark_cast(v)),
                   arrow_round=str(arrow_cast(v)), exact_half_up=str(exact_half_up(v))))
res["F1_examples"] = f1

c4 = []
for s in ["0.005", "1.115", "2.675"]:
    v = float(s)
    c4.append(dict(decimal=s, exact_binary64=exact(v), spark_half_even=str(Decimal(s).quantize(Decimal("0.01"), ROUND_HALF_EVEN)),
                   rust_fmt_exact_binary=str(Decimal(v).quantize(Decimal("0.01"), ROUND_HALF_EVEN))))
big = "12345678901234567.89"; vb = float(big)
c4.append(dict(decimal=big, exact_binary64=exact(vb), ulp=math.ulp(vb), spark_half_even=big,
               rust_fmt_exact_binary=str(Decimal(vb).quantize(Decimal("0.01")))))
res["C4_examples"] = c4
res["two_pow_53"] = 2 ** 53

# ---- F1 population: ETL E6 rows (orders in 1995 with o_orderkey % 10 == 0), SF30 ----
try:
    import duckdb
    con = duckdb.connect()
    rows = con.execute("""select o_orderkey, cast(o_totalprice as varchar) p from read_parquet('/workspace/engine-swap/data/tpch-sf30/orders/*.parquet')
                          where year(o_orderdate)=1995 and o_orderkey % 10 = 0""").fetchall()
    n = len(rows); diff_arrow = 0; diff_exact = 0; ex = []
    for k, p in rows:
        x = float(Decimal(p)) * 1.1            # Spark: decimal(15,2) -> double (nearest), times double 1.1
        sp = spark_cast(x); ar = arrow_cast(x); eh = exact_half_up(x)
        if ar != sp:
            diff_arrow += 1
            if len(ex) < 5: ex.append(dict(o_orderkey=k, o_totalprice=p, product_shortest=repr(x), product_exact=exact(x), spark=str(sp), arrow=str(ar)))
        if eh != sp: diff_exact += 1
    res["F1_population"] = dict(rows=n, mismatches_arrow_model=diff_arrow, mismatches_exact_half_up_model=diff_exact, measured_mismatches=631, examples=ex)
except Exception as e:
    res["F1_population"] = dict(error=str(e))
json.dump(res, open(f"{OUT}/ieee.json", "w"), indent=1, default=str)
print(json.dumps(res, indent=1, default=str))
