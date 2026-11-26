"""Charts for the engine-swap article. Reads only the published CSVs."""
import pandas as pd, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
R = "/workspace/engine-swap/results"; O = "/workspace/engine-swap/article/figures"
plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})

# 1. TPC-H SF30 total wall time, MinIO loopback
t = pd.read_csv(f"{R}/final/phase2_totals.csv")
lb = t[(t.cond == "loopback") & (t.wl == "tpch")].set_index("cfg")["median_total_s"]
order = ["duckdb", "sail-reorder", "sail-noreorder", "comet420-classic", "comet420-analytics",
         "spark413-classic", "spark420-classic", "spark420-analytics", "spark413-analytics"]
labels = ["DuckDB 2.0.1", "Sail 0.7.3 (reorder)", "Sail 0.7.3 (no reorder)", "Comet 1.2.0 classic", "Comet 1.2.0 AAL",
          "Spark 4.1.3 classic", "Spark 4.2.0 classic", "Spark 4.2.0 AAL", "Spark 4.1.3 AAL"]
vals = [lb[c] for c in order]
colors = ["#1b9e77"] * 3 + ["#7570b3"] * 2 + ["#d95f02"] * 4
fig, ax = plt.subplots(figsize=(8, 4.2))
b = ax.barh(labels[::-1], vals[::-1], color=colors[::-1])
for rect, v in zip(b, vals[::-1]):
    ax.text(v + 5, rect.get_y() + rect.get_height() / 2, f"{v:.0f} s", va="center")
ax.set_xlabel("Total wall time, 22 TPC-H queries, seconds (median of 2 runs)")
ax.set_title("TPC-H SF30, MinIO loopback, 8 cores, 9 GiB cap", fontsize=11)
ax.set_xlim(0, max(vals) * 1.15); fig.tight_layout(); fig.savefig(f"{O}/fig1_tpch_sf30_totals.png", dpi=160)

# 2. AAL: GET reduction vs wall-time change, Spark 4.2.0 / 4.1.3 / Comet, by condition
g = pd.read_csv(f"{R}/final/phase2_get_reduction.csv")
tp = t[t.wl == "tpch"].set_index(["cond", "cfg"])["median_total_s"]
conds = ["loopback", "proxy0", "proxy10"]; cl = {"loopback": "loopback", "proxy0": "proxy +0 ms", "proxy10": "proxy +10 ms"}
engines = [("spark420", "Spark 4.2.0"), ("spark413", "Spark 4.1.3"), ("comet420", "Comet 1.2.0")]
fig, axes = plt.subplots(1, 2, figsize=(10, 4))
w = 0.25
for i, (e, name) in enumerate(engines):
    red = [g[(g.cond == c) & (g.engine == e)].GET_reduction_pct.iloc[0] for c in conds]
    chg = [(tp[(c, f"{e}-analytics")] / tp[(c, f"{e}-classic")] - 1) * 100 for c in conds]
    x = [j + (i - 1) * w for j in range(3)]
    axes[0].bar(x, red, w, label=name); axes[1].bar(x, chg, w, label=name)
    for xx, v in zip(x, chg): axes[1].text(xx, v + (1 if v >= 0 else -3), (f"{v:+.0f}%" if abs(v) >= 0.5 else "0%"), ha="center", fontsize=8)
for a in axes: a.set_xticks(range(3)); a.set_xticklabels([cl[c] for c in conds])
axes[0].set_ylabel("% fewer S3 GET requests with AAL"); axes[0].set_title("AAL cuts GETs everywhere (not on Comet)")
axes[1].set_ylabel("% change in TPC-H wall time, AAL vs classic"); axes[1].set_title("...but only saves time with latency")
axes[1].axhline(0, color="black", lw=0.8); axes[0].legend(frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=3); axes[0].set_ylim(0, 65); axes[1].set_ylim(-30, 22)
fig.tight_layout(); fig.savefig(f"{O}/fig2_aal_get_vs_time.png", dpi=160)

# 3. SF100 per query
m = pd.read_csv(f"{R}/sf100/final/sf100_query_medians.csv")
qs = [1, 3, 6, 12, 18]; eng = [("duckdb", "DuckDB 2.0.1"), ("sail-reorder", "Sail 0.7.3*"), ("comet420-classic", "Comet 1.2.0"), ("spark420-classic", "Spark 4.2.0")]
fig, ax = plt.subplots(figsize=(9, 4.2)); w = 0.2
for i, (c, name) in enumerate(eng):
    ys = []
    for q in qs:
        r = m[(m.cfg == c) & (m.q == q)]
        ys.append(float(r.median_s.iloc[0]) if len(r) and pd.notna(r.median_s.iloc[0]) else 0.0)
    x = [j + (i - 1.5) * w for j in range(len(qs))]
    ax.bar(x, ys, w, label=name)
    if c == "sail-reorder": ax.text(x[-1], 4, "OOM\nx6", ha="center", fontsize=8, color="#c00")
ax.set_xticks(range(len(qs))); ax.set_xticklabels([f"Q{q}" for q in qs])
ax.set_ylabel("seconds (median)"); ax.set_title("TPC-H SF100 (600M lineitem rows), local Parquet, 9 GiB cap")
ax.legend(frameon=False); ax.text(0, -0.16, "* Sail Q1-Q12: median of 6 executions in sessions that later died on Q18 (OOM at the 9 GiB cap)", transform=ax.transAxes, fontsize=8)
fig.tight_layout(); fig.savefig(f"{O}/fig3_sf100_per_query.png", dpi=160)
print("ok")
