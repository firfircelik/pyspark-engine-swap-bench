"""Extra figures for the DEV.to article. Reads only published result files and analysis outputs (no benchmark runs).
Run f1_drift.py first (it needs the SF30 orders Parquet and duckdb); this script needs pandas and matplotlib.
Writes article/academic/figures/devto_*.png and out/devto_figs.json (every number quoted from these figures)."""
import json, os, glob, collections
from decimal import Decimal, getcontext
import numpy as np, pandas as pd, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt
from matplotlib.colors import TwoSlopeNorm
from common import ROOT, R, OUT, phase2
getcontext().prec = 80
F = f"{ROOT}/article/academic/figures"; os.makedirs(F, exist_ok=True)
plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
                     "grid.alpha": 0.25, "axes.titlesize": 10, "axes.titleweight": "bold", "legend.frameon": False,
                     "savefig.bbox": "tight", "figure.dpi": 100})
COL = {"spark420": "#4C72B0", "spark413": "#8172B2", "comet420": "#DD8452", "sail": "#55A868", "duckdb": "#C44E52"}
ENG = lambda c: next(e for e in COL if c.startswith(e))
NICE = {"spark420": "Spark 4.2.0", "spark413": "Spark 4.1.3", "comet420": "Comet 1.2.0", "sail": "Sail 0.7.3", "duckdb": "DuckDB 2.0.1"}
info = {}
def save(fig, name): fig.savefig(f"{F}/{name}.png", dpi=170); plt.close(fig)
p2 = phase2(); tp = p2[p2.wl == "tpch"].copy(); tp["qi"] = tp.q.astype(int)

# 1 per-query ratio heatmap
pq = pd.read_csv(f"{OUT}/critical_perquery_tests.csv", dtype={"q": str})
rows = [("Sail (reorder) / Spark 4.2, loopback", "Sail(reorder) vs Spark 4.2 classic, SF30 loopback"),
        ("Sail (no reorder) / Spark 4.2, local", "Sail(noreorder) vs Spark 4.2 default, SF30 local"),
        ("DuckDB / Spark 4.2, loopback", "DuckDB vs Spark 4.2 classic, SF30 loopback"),
        ("DuckDB / Spark 4.2, local", "DuckDB vs Spark 4.2 default, SF30 local"),
        ("Comet / Spark 4.2, loopback", "Comet vs Spark 4.2 classic, SF30 loopback"),
        ("AAL / classic, Spark 4.2, +10 ms", "Spark 4.2 AAL vs classic, SF30 proxy10"),
        ("AAL / classic, Spark 4.2, loopback", "Spark 4.2 AAL vs classic, SF30 loopback"),
        ("AAL / classic, Spark 4.1.3, loopback", "Spark 4.1.3 AAL vs classic, SF30 loopback")]
M = np.array([[pq[(pq.comparison == c) & (pq.q == str(q))].ratio.iloc[0] for q in range(1, 23)] for _, c in rows])
fig, ax = plt.subplots(figsize=(10, 3.6)); ax.grid(False)
im = ax.imshow(np.log2(M), cmap="RdBu_r", norm=TwoSlopeNorm(0, -4.5, 1), aspect="auto")
for i in range(M.shape[0]):
    for j in range(M.shape[1]):
        ax.text(j, i, f"{M[i, j]:.2f}", ha="center", va="center", fontsize=5.6, color="white" if abs(np.log2(M[i, j])) > 2.2 else "black")
ax.set_xticks(range(22)); ax.set_xticklabels([f"Q{q}" for q in range(1, 23)], fontsize=7)
ax.set_yticks(range(len(rows))); ax.set_yticklabels([r[0] for r in rows], fontsize=7.5)
cb = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.01); cb.set_label("ratio (log2 color)", fontsize=7)
cb.set_ticks(np.log2([1/16, 1/8, 1/4, 1/2, 1, 2])); cb.set_ticklabels(["1/16", "1/8", "1/4", "1/2", "1", "2"])
ax.set_title("Per-query median time ratio, TPC-H SF30 (blue: A faster, red: A slower)")
save(fig, "devto_perquery_heatmap")
info["heatmap_min_max"] = {r[0]: [float(M[i].min()), float(M[i].max())] for i, r in enumerate(rows)}

# 2 GET count vs wall time per query, three object-store conditions
fig, axes = plt.subplots(1, 3, figsize=(11, 3.6), sharex=True, sharey=True)
med = tp.groupby(["cond", "cfg", "qi"]).agg(wall=("wall_s", "median"), get=("s3_get", "median")).reset_index()
for ax, cond in zip(axes, ["loopback", "proxy0", "proxy10"]):
    d = med[med.cond == cond]
    for cfg, g in d.groupby("cfg"):
        e = ENG(cfg); g = g[g["get"] > 0]
        ax.scatter(g["get"], g.wall, s=12, alpha=0.75, color=COL[e], marker="^" if "analytics" in cfg else "o",
                   label=NICE[e] + {"analytics": " AAL", "classic": " classic", "reorder": " reorder", "noreorder": " no reorder"}.get(cfg.split("-")[-1], ""))
    ax.set_xscale("log"); ax.set_yscale("log"); ax.set_title({"loopback": "MinIO loopback", "proxy0": "proxy, +0 ms", "proxy10": "proxy, +10 ms"}[cond])
    ax.set_xlabel("GET requests per query (median of reps)")
axes[0].set_ylabel("wall time per query (s)")
h, l = axes[0].get_legend_handles_labels(); fig.legend(h, l, loc="lower center", ncol=5, fontsize=7, bbox_to_anchor=(0.5, -0.12))
fig.suptitle("GET requests against wall time per TPC-H query (circle: classic or native, triangle: AAL)", fontsize=10, fontweight="bold")
save(fig, "devto_get_vs_time")
info["duckdb_get_zero_queries"] = int((med[(med.cfg == "duckdb")]["get"] == 0).sum())

# 3 observed totals at +0 and +10 ms for every engine configuration, line through the means
tot = tp[tp.cond.isin(["proxy0", "proxy10"])].groupby(["cfg", "cond", "rep"]).wall_s.sum().reset_index()
fig, ax = plt.subplots(figsize=(7.5, 4.2)); slopes = {}
for cfg, g in tot.groupby("cfg"):
    e = ENG(cfg); m0 = g[g.cond == "proxy0"].wall_s.mean(); m10 = g[g.cond == "proxy10"].wall_s.mean()
    slopes[cfg] = dict(mean_proxy0_s=m0, mean_proxy10_s=m10, slope_s_per_ms=(m10 - m0) / 10)
    ls = "--" if "analytics" in cfg else (":" if "noreorder" in cfg else "-")
    ax.plot([0, 10], [m0, m10], ls, color=COL[e], lw=1.8, label=f"{cfg}: {(m10 - m0) / 10:+.1f} s per ms")
    for cond, x in [("proxy0", 0), ("proxy10", 10)]:
        ax.scatter([x] * (g.cond == cond).sum(), g[g.cond == cond].wall_s, color=COL[e], s=14, zorder=3)
ax.set_xticks([0, 10]); ax.set_xlim(-1, 11); ax.set_ylim(0, None)
ax.set_xlabel("added latency per response (ms)"); ax.set_ylabel("TPC-H SF30 total (s)")
ax.set_title("Latency sensitivity: rep totals and line through the means"); ax.legend(fontsize=7, loc="center left", bbox_to_anchor=(1.01, 0.5))
save(fig, "devto_latency_all_engines"); info["latency_slopes"] = slopes

# 4 SF100 per-query bars
sm = pd.read_csv(f"{R}/sf100/final/sf100_query_medians.csv")
eng = [("duckdb", "DuckDB 2.0.1", COL["duckdb"]), ("sail-reorder", "Sail 0.7.3 (Q18 OOM)", COL["sail"]),
       ("comet420-classic", "Comet 1.2.0", COL["comet420"]), ("spark420-classic", "Spark 4.2.0", COL["spark420"])]
qs = [1, 3, 6, 12, 18]; fig, ax = plt.subplots(figsize=(8, 3.8)); w = 0.2
for k, (cfg, name, col) in enumerate(eng):
    d = sm[sm.cfg == cfg].set_index("q")
    vals = [d.median_s.get(q, np.nan) for q in qs]
    lo = [d.median_s.get(q, np.nan) - d.min_s.get(q, np.nan) for q in qs]; hi = [d.max_s.get(q, np.nan) - d.median_s.get(q, np.nan) for q in qs]
    xs = np.arange(len(qs)) + (k - 1.5) * w
    ax.bar(xs, vals, w, color=col, label=name, yerr=[lo, hi], capsize=2, error_kw=dict(lw=0.7))
    for x, v, q in zip(xs, vals, qs):
        if np.isnan(v): ax.text(x, 2.5, "OOM in\n6 of 6", ha="center", va="bottom", fontsize=7, color=col, fontweight="bold")
ax.set_yscale("log"); ax.set_ylim(1.5, None); ax.set_xticks(range(len(qs))); ax.set_xticklabels([f"Q{q}" for q in qs])
ax.set_ylabel("median seconds (log), whiskers: min and max"); ax.legend(fontsize=7, ncol=2)
ax.set_title("TPC-H SF100, local Parquet, 9 GiB cap"); save(fig, "devto_sf100_bars")

# 5 attempt outcomes per engine (Phase 2, reps 1 to 3)
ab = pd.read_csv(f"{OUT}/dev_attempts_by_engine.csv")
ab["lab"] = ab.engine.map(NICE) + " " + ab.wl.str.upper()
ab = ab.sort_values(["wl", "engine"], ascending=[False, True])
fig, ax = plt.subplots(figsize=(8, 3.8)); left = np.zeros(len(ab))
for st, col in [("ok", "#55A868"), ("frozen", "#8C8C8C"), ("qerror", "#DD8452"), ("timeout", "#8172B2"), ("failed", "#C44E52")]:
    ax.barh(ab.lab, ab[st], left=left, color=col, label=st); left += ab[st].values
for y, (n, o) in enumerate(zip(ab.attempts, ab.ok)): ax.text(n + 0.3, y, f"{o}/{n} ok", va="center", fontsize=7)
ax.set_xlabel("attempts"); ax.legend(ncol=5, fontsize=7, loc="lower right"); ax.grid(axis="y", visible=False)
ax.set_title("Phase 2 attempt outcomes per engine (all attempts, reps 1 to 3)"); ax.invert_yaxis()
save(fig, "devto_attempt_outcomes")

# 6 Sail RSS over time, SF30 local, no reorder: OOM-killed vs successful Q18
CAP = 9 * 2**30 / 1e9
fig, ax = plt.subplots(figsize=(8, 3.6)); rss_info = {}
for run, col, lab in [("rep1_a1", "#C44E52", "rep1 a1: Q18 OOM-killed"), ("rep1_a2", "#55A868", "rep1 a2: Q18 completed")]:
    d = f"{R}/phase2/local/tpch/sail-noreorder/{run}"
    rs = pd.read_csv(f"{d}/rss.csv"); qj = [json.loads(l) for l in open(f"{d}/queries.jsonl")]
    q18 = next(r for r in qj if r.get("event") == "query_start" and r.get("q") == 18)["start"]
    ax.plot(rs.t - q18, rs.rss_bytes / 1e9, color=col, lw=1.3, label=lab)
    rss_info[run] = dict(peak_gb=float(rs.rss_bytes.max() / 1e9), samples=int(len(rs)))
ax.axhline(CAP, color="k", ls=":", lw=1); ax.text(-20, CAP - 0.45, "cgroup memory.max = 9 GiB (9.66 GB)", fontsize=7)
ax.axvline(0, color="gray", lw=0.8); ax.text(0.3, 0.5, "Q18 starts", fontsize=7, color="gray")
ax.set_xlabel("seconds relative to Q18 start"); ax.set_ylabel("process-tree RSS (GB)")
ax.text(11, 1.15, "server restarted\nfor Q19 to Q22", fontsize=7, color="#C44E52")
ax.set_title("Sail 0.7.3, SF30 local files, no join reorder: same config, two outcomes"); ax.legend(fontsize=7, loc="upper left")
save(fig, "devto_sail_rss_timeline"); info["sail_rss_timeline"] = rss_info

# 7 monthly cost against runs per day
cost = json.load(open(f"{OUT}/models.json"))["cost"]; rt = cost["rates"]; tS = 440.087; tK = 55.33
N = np.linspace(1, 120, 240); fig, ax = plt.subplots(figsize=(8, 4))
for s, ls in [(0, "-"), (180, "--"), (600, ":")]:
    ax.plot(N, 30 * N * (tS + s) / 3600 * rt["dbx"], ls, color=COL["spark420"], label=f"Databricks Jobs, Spark time, billed startup {s} s")
ax.plot(N, 30 * N * tS / 3600 * rt["fabric"], color=COL["comet420"], label="Fabric on-demand, Spark time (startup not billed)")
ax.axhline(cost["fixed_vm_month"], color=COL["duckdb"], lw=1.6, label=f"always-on F8s_v2 VM, DuckDB ({cost['fixed_vm_month']:.2f} USD/month)")
ax.plot(N, 30 * N * tK / 3600 * rt["vm"], color=COL["duckdb"], ls="-.", lw=1, label="per-run VM, DuckDB time (no boot billed)")
for g in cost["grid"]:
    if g["platform"] == "databricks_jobs" and g["startup_s"] in (0, 180, 600):
        n = g["breakeven_runs_per_day_vs_alwayson_vm"]; ax.plot(n, cost["fixed_vm_month"], "o", color=COL["spark420"]); ax.annotate(f"{n:.1f}/day", (n, cost["fixed_vm_month"]), textcoords="offset points", xytext=(4, 6), fontsize=7)
fb = next(g for g in cost["grid"] if g["platform"] == "fabric_payg")["breakeven_runs_per_day_vs_alwayson_vm"]
ax.plot(fb, cost["fixed_vm_month"], "o", color=COL["comet420"]); ax.annotate(f"{fb:.1f}/day", (fb, cost["fixed_vm_month"]), textcoords="offset points", xytext=(4, -12), fontsize=7)
ax.set_xlabel("runs per day (TPC-H SF30 workload)"); ax.set_ylabel("USD per month"); ax.legend(fontsize=6.8, loc="upper left")
ax.set_title("Cost model: list prices times measured wall time (Azure West Europe, 2026-11-26)"); save(fig, "devto_cost_breakeven")

# 8 F1 drift (histogram written by f1_drift.py, which needs the SF30 orders Parquet and duckdb)
H = pd.read_csv(f"{OUT}/f1_drift_hist.csv"); FD = json.load(open(f"{OUT}/f1_drift.json")); wdt = H.hi - H.lo
fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.8), sharey=True)
a1.bar(H.lo, H.below_other, width=wdt, align="edge", color="#8C8C8C", alpha=0.8, label=f"other rows (n={int(H.below_other.sum()):,})")
a1.bar(H.lo, H.below_mismatch, width=wdt, align="edge", color="#C44E52", label=f"Sail one cent lower (n={int(H.below_mismatch.sum())})")
a2.bar(H.lo, H.above_other, width=wdt, align="edge", color="#8C8C8C", alpha=0.8, label=f"other rows (n={int(H.above_other.sum()):,})")
for ax, t in [(a1, "below the tie: fl(100 x) < k + 0.5"), (a2, "above the tie: fl(100 x) > k + 0.5")]:
    ax.set_xscale("log"); ax.set_yscale("log"); ax.set_title(t, fontsize=9); ax.legend(fontsize=7.5, loc="upper center")
    ax.set_xlabel("distance to the half-cent tie, in cents (log)")
a1.set_ylabel("updated E6 rows (log)")
fig.suptitle(f"F1: all 631 one-cent rows sit within {FD['mismatch_absdist_max_cents']:.1e} cents below a tie; {FD['exact_ties']:,} exact ties not shown", fontsize=10, fontweight="bold")
save(fig, "devto_f1_drift"); info["f1_drift"] = FD

# 9 Comet time zone differences by zone
rows_ = [json.loads(l) for l in open(f"{R}/probes/comet_tz_v2.jsonl")]
tzs = json.load(open(f"{OUT}/comet_tz_summary.json"))
per = collections.Counter(r["zone"] for r in rows_ if r["label"] == "spark420")
zones = sorted(per, key=lambda z: -tzs["zones_with_differences"].get(z, 0))
dif = [tzs["zones_with_differences"].get(z, 0) for z in zones]; totz = [per[z] for z in zones]
fig, ax = plt.subplots(figsize=(7.5, 3.4))
ax.bar(zones, totz, color="#DDDDDD", label="probe rows"); ax.bar(zones, dif, color=COL["comet420"], label="rows differing from Spark 4.2.0")
for i, (d_, t_) in enumerate(zip(dif, totz)): ax.text(i, t_ + 0.2, f"{d_}/{t_}", ha="center", fontsize=8)
ax.set_ylabel("rows"); ax.set_xticks(range(len(zones))); ax.set_xticklabels(zones, rotation=20, ha="right", fontsize=8)
ax.set_title(f"Comet 1.2.0 native tzdata 2025b vs JVM 2026f: {tzs['rows_differing']} of {tzs['probe_rows']} rows differ"); ax.legend(fontsize=7)
save(fig, "devto_comet_tz_by_zone"); info["comet_tz_rows_per_zone"] = dict(zip(zones, totz))
json.dump(info, open(f"{OUT}/devto_figs.json", "w"), indent=1, default=float); print(json.dumps(info, indent=1, default=float))
