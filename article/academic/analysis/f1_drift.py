"""F1 drift: for every E6 updated row (orders of 1995 with o_orderkey % 10 = 0, SF30), the exact signed distance of
fl(100 * fl(p * 1.1)) to the nearest half-cent tie k + 0.5 (in cents). Needs data/tpch-sf30 (tpchgen-cli) and duckdb.
Writes out/f1_drift_hist.csv (log-spaced histogram of |distance|, split by side of the tie and measured mismatch) and out/f1_drift.json."""
import json
from decimal import Decimal, getcontext
import numpy as np, pandas as pd, duckdb
from common import ROOT, OUT
getcontext().prec = 80
prices = duckdb.connect().execute(f"""select o_orderkey, cast(o_totalprice as varchar) from read_parquet('{ROOT}/data/tpch-sf30/orders/*.parquet')
   where year(o_orderdate)=1995 and o_orderkey % 10 = 0""").fetchall()
mis = set(pd.read_csv(f"{OUT}/f1_mismatch_orderkeys.csv").o_orderkey)
dist = []; flag = []
for k, p in prices:
    x = float(Decimal(p)) * 1.1          # decimal(15,2) -> nearest double, times double 1.1
    y = Decimal(100.0 * x)               # exact value of fl(100 x), the quantity arrow-cast rounds
    dist.append(float(y - (y.to_integral_value(rounding="ROUND_FLOOR") + Decimal("0.5")))); flag.append(k in mis)
dist = np.array(dist); flag = np.array(flag); ad = np.abs(dist)
edges = np.logspace(-11, 0, 56)
# exact ties (distance 0) are counted in f1_drift.json, not binned
below = dist < 0; above = dist > 0
hist = lambda m: np.histogram(ad[m], edges)[0]
pd.DataFrame(dict(lo=edges[:-1], hi=edges[1:], below_mismatch=hist(below & flag), below_other=hist(below & ~flag),
                  above_other=hist(above & ~flag), above_mismatch=hist(above & flag))).to_csv(f"{OUT}/f1_drift_hist.csv", index=False)
mx = ad[flag].max()
res = dict(population=int(len(dist)), mismatches=int(flag.sum()), mismatch_all_below_tie=bool((dist[flag] < 0).all()),
           mismatch_absdist_min_cents=float(ad[flag].min()), mismatch_absdist_max_cents=float(mx),
           exact_ties=int((dist == 0).sum()), other_rows_with_absdist_le_mismatch_max=int(((ad <= mx) & ~flag).sum()),
           other_rows_below_tie_within_mismatch_max=int(((dist < 0) & (ad <= mx) & ~flag).sum()),
           rows_above_tie_within_1e_8_cents=int(((dist > 0) & (ad < 1e-8)).sum()), rows_below_tie_within_1e_8_cents=int(((dist < 0) & (ad < 1e-8)).sum()),
           rows_far_from_tie_ge_0_05_cents=int((ad >= 0.05).sum()))
json.dump(res, open(f"{OUT}/f1_drift.json", "w"), indent=1); print(json.dumps(res, indent=1))
