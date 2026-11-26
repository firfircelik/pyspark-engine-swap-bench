import json, math
import numpy as np, pandas as pd, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt
from common import *
F = f"{ROOT}/article/academic/figures"; os.makedirs(F, exist_ok=True)
plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False})
# A forest plot: engine-stratified Welch 95% CI (primary) for the SF30 rows; [min, max] ratio of rep totals for AAL-iso and SF100
c = pd.read_csv(f"{OUT}/table1_revised.csv"); bt = pd.read_csv(f"{OUT}/critical_comparisons.csv")
fig, ax = plt.subplots(figsize=(7.5, 4)); y = np.arange(len(c))[::-1]
s30 = c.strat_lo.notna()
ax.errorbar(c.ratio[s30], y[s30] + 0.12, xerr=[c.ratio[s30] - c.strat_lo[s30], c.strat_hi[s30] - c.ratio[s30]], fmt="o", ms=4, label="engine-stratified Welch 95% CI (primary)")
ax.errorbar(bt.ratio, y - 0.12, xerr=[bt.ratio - bt.ratio_strat_boot_lo, bt.ratio_strat_boot_hi - bt.ratio], fmt="s", ms=3, color="gray", alpha=0.6, label="stratified bootstrap (anti-conservative)")
ax.errorbar(c.ratio[~s30], y[~s30] + 0.12, xerr=[c.ratio[~s30] - c.min_ratio[~s30], c.max_ratio[~s30] - c.ratio[~s30]], fmt="D", ms=4, color="C3", label="[min, max] ratio of rep totals (no CI)")
ax.axvline(1, color="k", lw=0.8); ax.set_xscale("log"); ax.set_yticks(y); ax.set_yticklabels(c.comparison, fontsize=7)
ax.set_xlabel("ratio of total wall time (A / B), log scale"); ax.legend(frameon=False, fontsize=6.5, loc="lower right")
fig.tight_layout(); fig.savefig(f"{F}/figA_forest.png", dpi=170)
# B latency model
p2 = phase2(); t = p2[p2.wl == "tpch"]; L = pd.read_csv(f"{OUT}/latency_model.csv"); Lt = L[L.q == "TOTAL"].set_index("version"); LR = json.load(open(f"{OUT}/models.json"))["latency_revised"]
fig, axes = plt.subplots(1, 2, figsize=(8, 3.3))
for ax, ver, name in zip(axes, ["spark420", "spark413"], ["Spark 4.2.0", "Spark 4.1.3"]):
    r = Lt.loc[ver]; xs = np.linspace(0, 20, 50)
    for s, a, pr, b, G, col in [("classic", r.a_cl, r.proxy_cl, r.b_cl, r.G_cl, "C1"), ("AAL", r.a_aal, r.proxy_aal, r.b_aal, r.G_aal, "C0")]:
        ax.plot(xs, a + pr + b * G * xs / 1000, color=col, label=f"{s}: line through proxy0, proxy10")
        cfg = f"{ver}-{'classic' if s == 'classic' else 'analytics'}"
        d = t[t.cfg == cfg].groupby(["cond", "rep"]).wall_s.sum().reset_index()
        for cond, lat in [("proxy0", 0), ("proxy10", 10)]:
            v = d[d.cond == cond].wall_s; ax.scatter([lat] * len(v), v, color=col, s=12)
    lr = LR[ver]; ax.axvline(max(lr["Lstar_ms"], 0), ls=":", color="gray"); ax.set_title(f"{name}: L* {lr['Lstar_ms']:.2f} ms (two-point), 95% [{lr['Lstar_ci_ms'][0]:.1f}, {lr['Lstar_ci_ms'][1]:.1f}]", fontsize=8)
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
# D Sail Q18: observed tree peak RSS per attempt (no extrapolation)
Q = pd.read_csv(f"{OUT}/sail_q18_attempts.csv"); Q = Q[~Q.superseded & Q.peak_tree_rss_gb.notna() & (Q.peak_tree_rss_gb > 1)]
rs = pd.read_csv(f"{OUT}/sail_rss_per_query.csv"); p10 = rs[(rs.q == 18) & (rs.sf == 10)]
fig, ax = plt.subplots(figsize=(5.2, 3.2)); jit = {"local": -0.04, "loopback": 0.0, "proxy0": 0.02, "proxy10": 0.04}
for ok, mk, col, lab in [(True, "o", "C2", "Q18 completed"), (False, "x", "C3", "Q18 OOM-killed / server died")]:
    d = Q[(Q.q18 == "ok") == ok]; ax.scatter(d.sf * (1 + d.cond.map(jit)), d.peak_tree_rss_gb, marker=mk, color=col, label=lab)
ax.scatter(p10.sf, p10.peak_gb, marker="^", color="C2", label="SF10 pilot (no cap), Q18 window")
ax.axhline(9 * 2**30 / 1e9, color="gray", ls=":", label="9 GiB cap")
ax.set_xscale("log"); ax.set_xticks([10, 30, 100]); ax.set_xticklabels(["10", "30", "100"]); ax.set_xlabel("TPC-H scale factor")
ax.set_ylabel("Sail process-tree peak RSS (GB)"); ax.legend(frameon=False, fontsize=6.5, loc="lower right")
ax.set_title("Sail: observed peaks (session peak for SF30/SF100 attempts)", fontsize=8); fig.tight_layout(); fig.savefig(f"{F}/figD_sail_rss.png", dpi=170)
print("ok")
