import json, math
import numpy as np, pandas as pd, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt
from common import *
F = "/workspace/engine-swap/article/academic/figures"
plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False})
# A forest plot
c = pd.read_csv(f"{OUT}/critical_comparisons.csv"); p = pd.read_csv(f"{OUT}/critical_pooled_t.csv")
fig, ax = plt.subplots(figsize=(7.5, 4)); y = np.arange(len(c))[::-1]
ax.errorbar(c.ratio, y + 0.12, xerr=[c.ratio - c.ratio_strat_boot_lo, c.ratio_strat_boot_hi - c.ratio], fmt="o", ms=4, label="stratified bootstrap 95% CI")
ax.errorbar(p.ratio, y - 0.12, xerr=[p.ratio - p.pooled_lo, p.pooled_hi - p.ratio], fmt="s", ms=4, label="pooled-variance t 95% CI")
ax.axvline(1, color="k", lw=0.8); ax.set_xscale("log"); ax.set_yticks(y); ax.set_yticklabels(c.comparison, fontsize=7)
ax.set_xlabel("ratio of total wall time (A / B), log scale"); ax.legend(frameon=False, fontsize=7, loc="lower right")
fig.tight_layout(); fig.savefig(f"{F}/figA_forest.png", dpi=170)
# B latency model
p2 = phase2(); t = p2[p2.wl == "tpch"]; L = pd.read_csv(f"{OUT}/latency_model.csv"); Lt = L[L.q == "TOTAL"].set_index("version")
fig, axes = plt.subplots(1, 2, figsize=(8, 3.3))
for ax, ver, name in zip(axes, ["spark420", "spark413"], ["Spark 4.2.0", "Spark 4.1.3"]):
    r = Lt.loc[ver]; xs = np.linspace(0, 20, 50)
    for s, a, pr, b, G, col in [("classic", r.a_cl, r.proxy_cl, r.b_cl, r.G_cl, "C1"), ("AAL", r.a_aal, r.proxy_aal, r.b_aal, r.G_aal, "C0")]:
        ax.plot(xs, a + pr + b * G * xs / 1000, color=col, label=f"{s} model")
        cfg = f"{ver}-{'classic' if s == 'classic' else 'analytics'}"
        d = t[t.cfg == cfg].groupby(["cond", "rep"]).wall_s.sum().reset_index()
        for cond, lat in [("proxy0", 0), ("proxy10", 10)]:
            v = d[d.cond == cond].wall_s; ax.scatter([lat] * len(v), v, color=col, s=12)
    ax.axvline(max(r.breakeven_ms, 0), ls=":", color="gray"); ax.set_title(f"{name}: break-even {r.breakeven_ms:.2f} ms [{r.be_lo_ms:.2f}, {r.be_hi_ms:.2f}]", fontsize=8)
    ax.set_xlabel("added latency per response (ms)"); ax.set_ylabel("TPC-H SF30 total (s)"); ax.legend(frameon=False, fontsize=7)
fig.tight_layout(); fig.savefig(f"{F}/figB_latency_model.png", dpi=170)
# C scaling
sc = pd.read_csv(f"{R}/sf100/final/size_curve.csv")
fig, axes = plt.subplots(1, 4, figsize=(10, 2.8), sharey=True)
for ax, e in zip(axes, ["duckdb", "sail-reorder", "comet", "spark420"]):
    for q, g in sc[sc.engine_id == e].groupby("q"):
        xs = [sf for sf in (10, 30, 100) if pd.notna(g[f"sf{sf}_s"].iloc[0])]; ys = [g[f"sf{sf}_s"].iloc[0] for sf in xs]
        ax.plot(xs, ys, "o-", ms=3, label=f"Q{q}")
    ax.plot([10, 100], [0.3, 3], "k--", lw=0.6); ax.set_xscale("log"); ax.set_yscale("log"); ax.set_title(e, fontsize=8); ax.set_xlabel("SF")
axes[0].set_ylabel("median s (log)"); axes[0].legend(frameon=False, fontsize=6); fig.suptitle("t vs SF (dashed: slope 1). SF10/30/100 measured under different conditions", fontsize=8)
fig.tight_layout(); fig.savefig(f"{F}/figC_scaling.png", dpi=170)
# D Sail Q18 RSS
rs = pd.read_csv(f"{OUT}/sail_rss_per_query.csv"); q = rs[rs.q == 18]; m = json.load(open(f"{OUT}/models.json"))["sail_q18_oom"]
fig, ax = plt.subplots(figsize=(5, 3.2))
for st, mk, col in [("ok", "o", "C2"), ("error", "x", "C3")]:
    d = q[q.status == st]; ax.scatter(d.sf, d.peak_gb, marker=mk, color=col, label="Q18 completed" if st == "ok" else "Q18 OOM-killed")
xs = np.linspace(8, 120, 100); ax.plot(xs, m["points"]["10"] * (xs / 10) ** m["exponent"], "k--", lw=0.8, label=f"fit RSS ~ SF^{m['exponent']:.2f}")
ax.axhline(m["cap_gb"], color="gray", ls=":", label="9 GiB cap"); ax.axvline(m["sf_at_cap"], color="gray", ls="-.", lw=0.6)
ax.set_xscale("log"); ax.set_xlabel("TPC-H scale factor"); ax.set_ylabel("Sail tree peak RSS during Q18 (GB)"); ax.legend(frameon=False, fontsize=7)
ax.set_title(f"Predicted cap crossing at SF {m['sf_at_cap']:.0f}", fontsize=8); fig.tight_layout(); fig.savefig(f"{F}/figD_sail_rss.png", dpi=170)
print("ok")
