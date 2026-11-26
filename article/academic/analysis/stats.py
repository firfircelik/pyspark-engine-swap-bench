"""(1) Bootstrap CIs, Mann-Whitney U + Cliff's delta + Holm, query-level Wilcoxon, power/sample size."""
import itertools, json, math
import numpy as np, pandas as pd
from scipy import stats
from statsmodels.stats.multitest import multipletests
from common import *

p2 = phase2(); ai = aal_iso(); tf = tablefmt(); s1 = sf100()

# ---------- per-query bootstrap CIs (mean of reps), all studies ----------
rows = []
def add(study, keys, g):
    x = g.wall_s.values; lo, hi = boot_ci(x)
    rows.append(dict(study=study, **keys, n=len(x), mean_s=x.mean(), median_s=np.median(x), min_s=x.min(), max_s=x.max(), boot_lo=lo, boot_hi=hi,
                     cv=(x.std(ddof=1) / x.mean() if len(x) > 1 else np.nan)))
for (c, w, cfg, q), g in p2.groupby(["cond", "wl", "cfg", "q"]): add("phase2", dict(cond=c, wl=w, cfg=cfg, q=q), g)
for (c, cfg, q), g in ai.groupby(["cond", "cfg", "q"]): add("aal-iso", dict(cond=c, wl="tpch", cfg=cfg, q=q), g)
for (cfg, f, q), g in tf.groupby(["cfg", "format", "q"]): add("tablefmt", dict(cond="loopback-" + f, wl="tpch", cfg=cfg, q=q), g)
for (cfg, q), g in s1.groupby(["cfg", "q"]): add("sf100", dict(cond="local-sf100", wl="tpch", cfg=cfg, q=q), g)
ci = pd.DataFrame(rows); ci.to_csv(f"{OUT}/ci_per_query.csv", index=False)

# ---------- totals: per-rep totals and stratified bootstrap ----------
def totals(df, keys):
    out = []
    for k, g in df.groupby(keys):
        per_rep = g.groupby("rep").wall_s.sum()
        nq = g.groupby("rep").q.nunique()
        per_rep = per_rep[nq == nq.max()]
        pt, lo, hi = strat_boot_total([gg.wall_s.values for _, gg in g.groupby("q")])
        out.append(dict(zip(keys, k if isinstance(k, tuple) else (k,)), n_reps=len(per_rep), rep_totals=";".join(f"{v:.1f}" for v in per_rep),
                        total_mean=pt, strat_boot_lo=lo, strat_boot_hi=hi))
    return pd.DataFrame(out)
tot = pd.concat([totals(p2, ["cond", "wl", "cfg"]).assign(study="phase2"),
                 totals(ai, ["cond", "cfg"]).assign(study="aal-iso", wl="tpch-q1369"),
                 totals(tf, ["format", "cfg"]).rename(columns={"format": "cond"}).assign(study="tablefmt", wl="tpch-5q"),
                 totals(s1[s1.cfg != "sail-reorder"], ["cfg"]).assign(study="sf100", cond="local", wl="tpch-5q")])
tot.to_csv(f"{OUT}/ci_totals.csv", index=False)

# ---------- noise model: pooled within-cell SD of log(time) ----------
def pooled_log_sd(df, keys):
    v = []; dof = 0
    for _, g in df.groupby(keys):
        x = np.log(g.wall_s.values)
        if len(x) > 1: v.append(x.var(ddof=1) * (len(x) - 1)); dof += len(x) - 1
    return math.sqrt(sum(v) / dof), dof
sd_q, dof_q = pooled_log_sd(p2[p2.wl == "tpch"], ["cond", "cfg", "q"])
# total-level log SD from per-rep totals
tl = []
for _, g in p2[p2.wl == "tpch"].groupby(["cond", "cfg"]):
    t = np.log(g.groupby("rep").wall_s.sum().values)
    if len(t) > 1: tl.append(t.var(ddof=1))
sd_tot = math.sqrt(np.mean(tl)); dof_tot = len(tl)
sd_by_engine = {}
for e in ["spark420", "spark413", "comet420", "sail", "duckdb"]:
    sub = p2[(p2.wl == "tpch") & p2.cfg.str.startswith(e)]
    sd_by_engine[e] = pooled_log_sd(sub, ["cond", "cfg", "q"])[0]

# ---------- critical comparisons ----------
def mwu(a, b):
    a = np.asarray(a); b = np.asarray(b)
    if len(a) < 1 or len(b) < 1: return np.nan
    return stats.mannwhitneyu(a, b, alternative="two-sided", method="exact").pvalue

def min_exact_p(n1, n2): return 2 / math.comb(n1 + n2, n1)

def needed_n_mwu(alpha):
    n = 2
    while min_exact_p(n, n) > alpha: n += 1
    return n

def needed_n_t(delta_log, sd, alpha, power=0.8):
    """Two-sample t on log time, equal n. Smallest n with power >= target (noncentral t)."""
    if sd == 0: return 2
    for n in range(2, 200):
        df = 2 * n - 2; nc = abs(delta_log) / (sd * math.sqrt(2 / n)); tc = stats.t.ppf(1 - alpha / 2, df)
        pw = 1 - stats.nct.cdf(tc, df, nc) + stats.nct.cdf(-tc, df, nc)
        if pw >= power: return n
    return 200

CRIT = [
    ("Sail(reorder) vs Spark 4.2 classic, SF30 loopback", p2, dict(cond="loopback", wl="tpch"), "sail-reorder", "spark420-classic"),
    ("Sail(noreorder) vs Spark 4.2 default, SF30 local", p2, dict(cond="local", wl="tpch"), "sail-noreorder", "spark420-default"),
    ("DuckDB vs Spark 4.2 classic, SF30 loopback", p2, dict(cond="loopback", wl="tpch"), "duckdb", "spark420-classic"),
    ("DuckDB vs Spark 4.2 default, SF30 local", p2, dict(cond="local", wl="tpch"), "duckdb", "spark420-default"),
    ("Comet vs Spark 4.2 classic, SF30 loopback", p2, dict(cond="loopback", wl="tpch"), "comet420-classic", "spark420-classic"),
    ("Spark 4.2 AAL vs classic, SF30 proxy10", p2, dict(cond="proxy10", wl="tpch"), "spark420-analytics", "spark420-classic"),
    ("Spark 4.2 AAL vs classic, SF30 loopback", p2, dict(cond="loopback", wl="tpch"), "spark420-analytics", "spark420-classic"),
    ("Spark 4.1.3 AAL vs classic, SF30 loopback", p2, dict(cond="loopback", wl="tpch"), "spark413-analytics", "spark413-classic"),
    ("Spark 4.2 AAL vs classic, aal-iso proxy10 (Q1,3,6,9)", ai, dict(cond="proxy10"), "B_420_aal_default", "A_420_classic"),
    ("DuckDB vs Spark 4.2, SF100", s1, {}, "duckdb", "spark420-classic"),
]
res = []; perq = []
for name, df, filt, a, b in CRIT:
    sub = df
    for k, v in filt.items(): sub = sub[sub[k] == v]
    A = sub[sub.cfg == a]; Bd = sub[sub.cfg == b]
    qs = sorted(set(A.q) & set(Bd.q), key=lambda s: (len(s), s))
    pv = []; cds = []; lr = []
    for q in qs:
        xa = A[A.q == q].wall_s.values; xb = Bd[Bd.q == q].wall_s.values
        p = mwu(xa, xb); cd = cliffs_delta(xa, xb); pv.append(p); cds.append(cd)
        lr.append(math.log(np.median(xa) / np.median(xb)))
        perq.append(dict(comparison=name, q=q, n_a=len(xa), n_b=len(xb), median_a=np.median(xa), median_b=np.median(xb),
                         ratio=np.median(xa) / np.median(xb), mwu_p=p, cliffs_delta=cd))
    holm = multipletests(pv, method="holm")[1] if qs else []
    for i, h in enumerate(holm): perq[len(perq) - len(qs) + i]["holm_p"] = h
    ta = A.groupby("rep").wall_s.sum().values; tb = Bd.groupby("rep").wall_s.sum().values
    # totals over common queries only
    ta = A[A.q.isin(qs)].groupby("rep").wall_s.sum().values; tb = Bd[Bd.q.isin(qs)].groupby("rep").wall_s.sum().values
    ptA, loA, hiA = strat_boot_total([A[A.q == q].wall_s.values for q in qs])
    ptB, loB, hiB = strat_boot_total([Bd[Bd.q == q].wall_s.values for q in qs])
    # ratio CI by stratified bootstrap
    B_ = 10000; ra = np.zeros(B_); rb = np.zeros(B_)
    for q in qs:
        xa = A[A.q == q].wall_s.values; xb = Bd[Bd.q == q].wall_s.values
        ra += xa[RNG.integers(0, len(xa), (B_, len(xa)))].mean(1); rb += xb[RNG.integers(0, len(xb), (B_, len(xb)))].mean(1)
    rlo, rhi = np.quantile(ra / rb, [0.025, 0.975])
    w = stats.wilcoxon(lr) if len(lr) >= 5 else None
    signs = sum(1 for v in lr if v < 0)
    d_log = math.log(ptA / ptB)
    sd_use = sd_tot
    res.append(dict(comparison=name, n_queries=len(qs), reps_a=len(ta), reps_b=len(tb), total_a=ptA, total_b=ptB, ratio=ptA / ptB,
                    ratio_strat_boot_lo=rlo, ratio_strat_boot_hi=rhi, mwu_total_p=mwu(ta, tb), cliffs_total=cliffs_delta(ta, tb),
                    min_possible_p=min_exact_p(len(ta), len(tb)) if len(ta) and len(tb) else np.nan,
                    perq_sig_after_holm=int(sum(h < 0.05 for h in holm)),
                    perq_median_cliffs=float(np.median(cds)) if cds else np.nan,
                    query_level_wilcoxon_p=(w.pvalue if w else np.nan), queries_faster=f"{signs}/{len(lr)}",
                    n_needed_mwu_total=needed_n_mwu(0.05), n_needed_mwu_perq_holm=needed_n_mwu(0.05 / max(len(qs), 1)),
                    n_needed_t_total=needed_n_t(d_log, sd_use, 0.05)))
R_ = pd.DataFrame(res); R_.to_csv(f"{OUT}/critical_comparisons.csv", index=False)
pd.DataFrame(perq).to_csv(f"{OUT}/critical_perquery_tests.csv", index=False)
json.dump(dict(pooled_log_sd_query=sd_q, dof_query=dof_q, pooled_log_sd_total=sd_tot, n_cells_total=dof_tot,
               log_sd_by_engine=sd_by_engine, min_exact_p_2v2=min_exact_p(2, 2), min_exact_p_3v3=min_exact_p(3, 3),
               min_exact_p_5v5=min_exact_p(5, 5), n_mwu_alpha05=needed_n_mwu(0.05), n_mwu_holm22=needed_n_mwu(0.05 / 22),
               n_mwu_holm4=needed_n_mwu(0.05 / 4)), open(f"{OUT}/noise.json", "w"), indent=1)
pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30)
print(json.dumps(json.load(open(f"{OUT}/noise.json")), indent=1))
print(R_.round(4).to_string())

# ---------- pooled-variance parametric test on log totals (borrows run noise from all 33 TPC-H cells) ----------
tcrit = stats.t.ppf(0.975, dof_tot)
par = []
for r in res:
    lr_ = math.log(r["ratio"]); se = sd_tot * math.sqrt(1 / max(r["reps_a"], 1) + 1 / max(r["reps_b"], 1))
    tval = lr_ / se; p = 2 * stats.t.sf(abs(tval), dof_tot)
    par.append(dict(comparison=r["comparison"], ratio=r["ratio"], pooled_lo=math.exp(lr_ - tcrit * se), pooled_hi=math.exp(lr_ + tcrit * se),
                    t=tval, df=dof_tot, p=p, n_for_80pct_power=needed_n_t(lr_, sd_tot, 0.05)))
P = pd.DataFrame(par); P.to_csv(f"{OUT}/critical_pooled_t.csv", index=False); print(P.round(4).to_string())

# ---------- 5-rep plan and box-hour estimate ----------
pr = pd.read_csv(f"{R}/final/phase2_process_runs.csv")
pr = pr[pr.status == "ok"]
PLAN = [("loopback", "tpch", c) for c in ["spark420-classic", "spark420-analytics", "spark413-classic", "spark413-analytics", "sail-reorder", "duckdb", "comet420-classic"]] + \
       [("proxy0", "tpch", c) for c in ["spark420-classic", "spark420-analytics"]] + \
       [("proxy10", "tpch", c) for c in ["spark420-classic", "spark420-analytics"]] + \
       [("local", "tpch", c) for c in ["spark420-default", "sail-noreorder", "duckdb"]]
plan = []
for c, w, cfg in PLAN:
    e = pr[(pr.cond == c) & (pr.wl == w) & (pr.cfg == cfg)].process_elapsed_s.median()
    plan.append(dict(cond=c, wl=w, cfg=cfg, have_reps=2, add_reps=3, median_process_s=e, add_hours=3 * e / 3600))
sfp = pd.read_csv(f"{R}/sf100/final/sf100_attempts.csv") if os.path.exists(f"{R}/sf100/final/sf100_attempts.csv") else None
PL = pd.DataFrame(plan); PL.to_csv(f"{OUT}/rep_plan.csv", index=False)
print(PL.round(2).to_string()); print("TOTAL add hours", PL.add_hours.sum().round(2))
if sfp is not None: print(sfp.columns.tolist()); print(sfp.head(3).to_string())
