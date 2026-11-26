"""(2a) latency model, (2b) scaling + Sail RSS, (2d) cost model. Reads published results only."""
import json, math, glob, os
import numpy as np, pandas as pd
from common import *
out = {}
p2 = phase2(); t = p2[p2.wl == "tpch"].copy()
LAT = {"loopback": 0.0, "proxy0": 0.0, "proxy10": 0.010}

# ---------- (2a) latency model ----------
# Per query q and config c:  t = a + c_p * I_proxy + b * G * L   (L = added per-response latency, s; G = GET count)
# b = wall seconds per second of summed request latency = 1 / effective request concurrency.
def fit_cfg(df):
    X = np.column_stack([np.ones(len(df)), (df.cond != "loopback").astype(float), df.s3_get.values * df.cond.map(LAT).values])
    beta, *_ = np.linalg.lstsq(X, df.wall_s.values, rcond=None); return beta
def breakeven(dfA, dfC):
    a = fit_cfg(dfA); c = fit_cfg(dfC)
    GA = dfA.s3_get.median(); GC = dfC.s3_get.median()
    num = (a[0] + a[1]) - (c[0] + c[1]); den = c[2] * GC - a[2] * GA      # AAL wins when L > num/den (if den>0)
    return a, c, GA, GC, (num / den if den > 0 else np.nan), num, den
def boot_be(dfA, dfC, B=2000):
    vals = []
    for _ in range(B):
        parts = []
        for d in (dfA, dfC):
            s = []
            for (cond, q), g in d.groupby(["cond", "q"]):
                s.append(g.sample(len(g), replace=True, random_state=int(RNG.integers(1e9))))
            parts.append(pd.concat(s))
        vals.append(breakeven(*parts)[4])
    v = np.array(vals); return np.nanquantile(v, [0.025, 0.5, 0.975]), np.mean(np.isnan(v))
lat_rows = []
for ver in ["spark420", "spark413"]:
    A = t[t.cfg == f"{ver}-analytics"]; C = t[t.cfg == f"{ver}-classic"]
    # totals level: aggregate per (cond, rep)
    def agg(d): return d.groupby(["cond", "rep"]).agg(wall_s=("wall_s", "sum"), s3_get=("s3_get", "sum")).reset_index().assign(q="all")
    a, c, GA, GC, be, num, den = breakeven(agg(A), agg(C))
    ci, nanfrac = boot_be(agg(A), agg(C), 1000)
    lat_rows.append(dict(version=ver, q="TOTAL", a_aal=a[0], proxy_aal=a[1], b_aal=a[2], a_cl=c[0], proxy_cl=c[1], b_cl=c[2], G_aal=GA, G_cl=GC,
                         conc_aal=1 / a[2] if a[2] > 0 else np.nan, conc_cl=1 / c[2] if c[2] > 0 else np.nan,
                         gap_at_0ms_s=num, slope_gap_s_per_ms=den / 1000, breakeven_ms=be * 1000, be_lo_ms=ci[0] * 1000, be_hi_ms=ci[2] * 1000, boot_nan_frac=nanfrac))
    for q in sorted(A.q.unique(), key=int):
        aq, cq = A[A.q == q], C[C.q == q]
        a, c, GA, GC, be, num, den = breakeven(aq, cq)
        ci, nanfrac = boot_be(aq, cq, 300)
        lat_rows.append(dict(version=ver, q=q, a_aal=a[0], proxy_aal=a[1], b_aal=a[2], a_cl=c[0], proxy_cl=c[1], b_cl=c[2], G_aal=GA, G_cl=GC,
                             conc_aal=1 / a[2] if a[2] > 0 else np.nan, conc_cl=1 / c[2] if c[2] > 0 else np.nan,
                             gap_at_0ms_s=num, slope_gap_s_per_ms=den / 1000, breakeven_ms=be * 1000, be_lo_ms=ci[0] * 1000, be_hi_ms=ci[2] * 1000, boot_nan_frac=nanfrac))
L = pd.DataFrame(lat_rows); L.to_csv(f"{OUT}/latency_model.csv", index=False)
# goodness of fit at totals: predicted vs observed per condition
fit_rows = []
for ver in ["spark420", "spark413"]:
    for s in ["analytics", "classic"]:
        d = t[t.cfg == f"{ver}-{s}"].groupby(["cond", "rep"]).agg(wall_s=("wall_s", "sum"), s3_get=("s3_get", "sum")).reset_index()
        b = fit_cfg(d)
        for cond, g in d.groupby("cond"):
            pred = b[0] + b[1] * (cond != "loopback") + b[2] * g.s3_get.median() * LAT[cond]
            fit_rows.append(dict(cfg=f"{ver}-{s}", cond=cond, obs_median=g.wall_s.median(), pred=pred))
pd.DataFrame(fit_rows).to_csv(f"{OUT}/latency_model_fit.csv", index=False)

# ---------- (2b) scaling ----------
sc = pd.read_csv(f"{R}/sf100/final/size_curve.csv")
srows = []
for _, r in sc.iterrows():
    pts = [(sf, r[f"sf{sf}_s"]) for sf in (10, 30, 100) if pd.notna(r[f"sf{sf}_s"])]
    if len(pts) < 2: continue
    x = np.log([p[0] for p in pts]); y = np.log([p[1] for p in pts])
    b, la = np.polyfit(x, y, 1)
    se = np.nan
    if len(pts) == 3:
        res = y - (la + b * x); se = math.sqrt((res ** 2).sum() / 1 / ((x - x.mean()) ** 2).sum())
    b30 = math.log(r.sf100_s / r.sf30_s) / math.log(100 / 30) if pd.notna(r.sf100_s) and pd.notna(r.sf30_s) else np.nan
    srows.append(dict(engine=r.engine_id, q=r.q, n_points=len(pts), b_all=b, b_se_1df=se, a=math.exp(la), b_sf30_sf100=b30))
S = pd.DataFrame(srows); S.to_csv(f"{OUT}/scaling_fit.csv", index=False)

# Sail per-query peak RSS vs SF (Q18 and others)
def per_query_rss(qfile, rssfile, label=None):
    ev = [json.loads(l) for l in open(qfile)]
    rs = pd.read_csv(rssfile)
    out = {}
    starts = {e["q"]: e["start"] for e in ev if e.get("event") == "query_start" and (label is None or e.get("label") == label)}
    for e in ev:
        if e.get("event") == "query" and (label is None or e.get("label") == label):
            w = rs[(rs.t >= e["start"]) & (rs.t <= e["end"])]
            out[e["q"]] = (w.rss_bytes.max() / 1e9 if len(w) else np.nan, "ok" if not e.get("error") else "error")
            starts.pop(e["q"], None)
    for q, s in starts.items():   # started, never finished (server killed)
        w = rs[rs.t >= s]; out[q] = (w.rss_bytes.max() / 1e9 if len(w) else np.nan, "killed")
    return out
rss_rows = []
for d in sorted(glob.glob(f"{R}/phase2/local/tpch/sail-reorder/rep*")):
    if os.path.exists(f"{d}/queries.jsonl") and os.path.exists(f"{d}/rss.csv"):
        for q, (v, st) in per_query_rss(f"{d}/queries.jsonl", f"{d}/rss.csv").items():
            rss_rows.append(dict(sf=30, run=os.path.basename(d), q=q, peak_gb=v, status=st))
sup = set(json.load(open(f"{R}/sf100/superseded.json")))
for d in sorted(glob.glob(f"{R}/sf100/local/tpch/sail-reorder/rep*")):
    if d in sup: continue
    for q, (v, st) in per_query_rss(f"{d}/queries.jsonl", f"{d}/rss.csv").items():
        rss_rows.append(dict(sf=100, run=os.path.basename(d), q=q, peak_gb=v, status=st))
pf = f"{R}/pilot-sf10-r2/rss/sail-reorder.csv"
if os.path.exists(pf):
    for q, (v, st) in per_query_rss(f"{R}/pilot-sf10-r2/queries.jsonl", pf, label="sail-reorder").items():
        rss_rows.append(dict(sf=10, run="pilot", q=q, peak_gb=v, status=st))
RS = pd.DataFrame(rss_rows); RS.to_csv(f"{OUT}/sail_rss_per_query.csv", index=False)
q18 = RS[RS.q == 18]
ok18 = q18[q18.status == "ok"].groupby("sf").peak_gb.median()
cap = 9 * 2**30 / 1e9
oom = {}
if len(ok18) >= 2:
    x = np.log(ok18.index.values.astype(float)); y = np.log(ok18.values); b, la = np.polyfit(x, y, 1)
    oom = dict(points={int(k): float(v) for k, v in ok18.items()}, exponent=b, sf_at_cap=math.exp((math.log(cap) - la) / b), cap_gb=cap)
# also linear-in-SF model (RSS = r0 + r1*SF)
if len(ok18) >= 2:
    r1, r0 = np.polyfit(ok18.index.values.astype(float), ok18.values, 1); oom.update(linear_r0=r0, linear_r1=r1, linear_sf_at_cap=(cap - r0) / r1)
out["sail_q18_oom"] = oom

# ---------- (2d) cost model ----------
r_vm, r_dbx, r_fab = 0.388, 1.144, 0.88          # $/h, verified 2026-11-26 (article/cost_table.md)
t_duck, t_spark, t_sail = 55.33, 440.087, 71.98   # s, loopback SF30 medians
fixed_vm_month = r_vm * 720; fixed_f4_month = 4 * 0.22 * 720
def per_run(t_s, s_s, r): return (t_s + s_s) / 3600 * r
cost = dict(rates=dict(vm=r_vm, dbx=r_dbx, fabric=r_fab), fixed_vm_month=fixed_vm_month, fixed_f4_month=fixed_f4_month, grid=[])
for s in [0, 60, 180, 300, 600]:
    for name, r in [("databricks_jobs", r_dbx), ("fabric_payg", r_fab)]:
        s_bill = s if name == "databricks_jobs" else 0.0      # Fabric docs: cluster acquisition / context init not billed
        pr_ = per_run(t_spark, s_bill, r)
        cost["grid"].append(dict(platform=name, startup_s=s, billed_startup_s=s_bill, per_run=pr_,
                                 breakeven_runs_per_day_vs_alwayson_vm=fixed_vm_month / 30 / pr_,
                                 ratio_vs_ondemand_duckdb_vm=pr_ / per_run(t_duck, s, r_vm)))
cost["vm_capacity_runs_per_day_duckdb"] = 86400 / t_duck
out["cost"] = cost
json.dump(out, open(f"{OUT}/models.json", "w"), indent=1, default=float)
pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30)
print(L[L.q == "TOTAL"].round(4).to_string()); print(L[L.q != "TOTAL"][["version", "q", "G_aal", "G_cl", "conc_aal", "conc_cl", "gap_at_0ms_s", "breakeven_ms", "be_lo_ms", "be_hi_ms"]].round(2).to_string())
print(pd.DataFrame(fit_rows).round(1).to_string()); print(S.round(3).to_string()); print(RS.sort_values(["q", "sf"]).round(2).to_string()); print(json.dumps(out, indent=1, default=float))

# ---------- REVISION (2026-11-26, after technical review) ----------
# (a) Break-even L* as a two-point interpolation between the proxy0 and proxy10 means (the 3-parameter model is exactly
# identified, so L* depends only on these four cell means; linearity in L is untested). Interval under the Table 1
# noise model (engine-stratified log-total SD, uncertainty in sigma propagated by a scaled inverse chi-square draw).
NE = pd.read_csv(f"{OUT}/noise_by_engine_total.csv", index_col=0)
rev = {}
rng = np.random.default_rng(20261126)
for ver in ["spark420", "spark413"]:
    tot = t[t.cfg.str.startswith(ver)].groupby(["cfg", "cond", "rep"]).wall_s.sum().reset_index()
    m = tot.groupby(["cfg", "cond"]).wall_s.mean()
    n = tot.groupby(["cfg", "cond"]).wall_s.count()
    A0, A10, C0, C10 = m[(f"{ver}-analytics", "proxy0")], m[(f"{ver}-analytics", "proxy10")], m[(f"{ver}-classic", "proxy0")], m[(f"{ver}-classic", "proxy10")]
    AL, CL = m[(f"{ver}-analytics", "loopback")], m[(f"{ver}-classic", "loopback")]
    gap0 = A0 - C0; slope = ((C10 - C0) - (A10 - A0)) / 10.0
    Lstar = gap0 / slope if slope > 0 else np.nan
    s2, dof = NE.loc[ver, "var"], NE.loc[ver, "dof"]
    B = 200000
    sig = np.sqrt(s2 * dof / rng.chisquare(dof, B))
    def sim(mu, k): return mu * np.exp(rng.normal(0, 1, (B, k)) * sig[:, None]).mean(1)
    k = 2
    a0, a10, c0, c10 = sim(A0, k), sim(A10, k), sim(C0, k), sim(C10, k)
    sl = ((c10 - c0) - (a10 - a0)) / 10.0
    Ls = np.where(sl > 0, (a0 - c0) / sl, np.nan)
    # latency-hiding factor = summed added request latency / added wall time  (G * 10 ms / (t10 - t0))
    G = t[t.cfg.str.startswith(ver)].groupby(["cfg", "cond", "rep"]).s3_get.sum().groupby(["cfg", "cond"]).median()
    hide = lambda c: G[(c, "proxy10")] * 0.010 / (m[(c, "proxy10")] - m[(c, "proxy0")])
    per_q = L[(L.version == ver) & (L.q != "TOTAL")]
    rev[ver] = dict(mean_totals=dict(A_loop=AL, C_loop=CL, A_proxy0=A0, C_proxy0=C0, A_proxy10=A10, C_proxy10=C10),
                    gap_loopback_s=AL - CL, gap_proxy0_s=gap0, slope_gap_s_per_ms=slope, Lstar_ms=Lstar,
                    Lstar_ci_ms=np.nanquantile(Ls, [0.025, 0.975]).tolist(), Lstar_sim_no_breakeven_frac=float(np.isnan(Ls).mean()),
                    sigma_total=float(math.sqrt(s2)), sigma_dof=float(dof),
                    hiding_factor_aal=hide(f"{ver}-analytics"), hiding_factor_classic=hide(f"{ver}-classic"),
                    perq_n=int(len(per_q)), perq_breakeven_nan=int(per_q.breakeven_ms.isna().sum()),
                    perq_median_breakeven_ms_excl_nan=float(per_q.breakeven_ms.median()),
                    perq_aal_ahead_at_proxy0=int((per_q.gap_at_0ms_s < 0).sum()),
                    perq_aal_ahead_at_proxy10=int(((per_q.gap_at_0ms_s - per_q.slope_gap_s_per_ms * 10) < 0).sum()))
out["latency_revised"] = rev
# (b) cost: robustness of the per-run cost ratio to price error (each of r_D and r_V off by up to +/-50%)
base = (t_spark * r_dbx) / (t_duck * r_vm)
out["cost"]["ratio_dbx_vs_vm_zero_startup"] = base
out["cost"]["ratio_dbx_vs_vm_price_pm50_range"] = [base * 0.5 / 1.5, base * 1.5 / 0.5]
out["cost"]["ratio_fabric_vs_vm_zero_startup"] = (t_spark * r_fab) / (t_duck * r_vm)
out["cost"]["ratio_fabric_vs_vm_price_pm50_range"] = [out["cost"]["ratio_fabric_vs_vm_zero_startup"] / 3, out["cost"]["ratio_fabric_vs_vm_zero_startup"] * 3]
# (c) Sail Q18 attempts at SF30 and SF100 (status per attempt; cgroup memory.peak per attempt)
q18 = []
for d in sorted(glob.glob(f"{R}/phase2/*/tpch/sail-*/rep*")) + sorted(glob.glob(f"{R}/sf100/local/tpch/sail-reorder/rep*")):
    qf, sf_ = f"{d}/queries.jsonl", f"{d}/summary.jsonl"
    if not os.path.exists(qf): continue
    ev = [json.loads(l) for l in open(qf)]
    e18 = [e for e in ev if e.get("event") == "query" and e.get("q") == 18]
    sm = json.loads(open(sf_).readline()) if os.path.exists(sf_) and os.path.getsize(sf_) else {}
    err = (e18[0].get("error") or "") if e18 else ""
    status = "ok" if e18 and not err else ("oom/server died" if ("SERVER_DIED" in err or "SparkConnectGrpc" in err) else ("other error" if err else "not reached"))
    q18.append(dict(sf=100 if "/sf100/" in d else 30, cond=d.split("/")[-4], cfg=d.split("/")[-2], run=os.path.basename(d),
                    superseded=d in sup, q18=status, peak_tree_rss_gb=sm.get("peak_tree_rss_bytes", np.nan) / 1e9 if sm else np.nan,
                    cg_memory_peak_gb=sm.get("cg_memory_peak", np.nan) / 1e9 if sm else np.nan))
Q18 = pd.DataFrame(q18); Q18.to_csv(f"{OUT}/sail_q18_attempts.csv", index=False)
out["sail_q18_attempts"] = Q18[~Q18.superseded].groupby(["sf", "cond", "cfg", "q18"]).size().rename("n").reset_index().to_dict("records")
json.dump(out, open(f"{OUT}/models.json", "w"), indent=1, default=float)
print(json.dumps(rev, indent=1, default=float)); print(Q18.to_string()); print(out["cost"])
