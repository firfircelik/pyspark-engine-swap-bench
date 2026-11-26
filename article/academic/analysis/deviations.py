"""Protocol deviations and survivorship (Phase 2): attempt/failure table, query errors, realized vs planned order,
retry factor, and a rep-3 sensitivity analysis using the complete rep-3 runs that exist on disk. No new runs."""
import json, math, os, collections
import numpy as np, pandas as pd
from scipy import stats
from common import ROOT, R, OUT

S = [json.loads(l) for l in open(f"{R}/phase2/state.jsonl")]
st = pd.DataFrame(S)
st["engine"] = st.cfg.str.extract(r"^(spark420|spark413|comet420|sail|duckdb)")[0]
plan = json.load(open(f"{R}/phase2/plan.json"))

# attempts by rep and status
att = st.pivot_table(index="rep", columns="status", values="id", aggfunc="count", fill_value=0)
att["attempts"] = att.sum(axis=1); att.to_csv(f"{OUT}/dev_attempts_by_rep.csv")
# attempts by engine x workload (all reps) and not-ok counts
st["not_ok"] = st.status != "ok"
eng = st.groupby(["engine", "wl"]).agg(attempts=("id", "count"), ok=("status", lambda s: (s == "ok").sum()),
      frozen=("status", lambda s: (s == "frozen").sum()), qerror=("status", lambda s: (s == "qerror").sum()),
      timeout=("status", lambda s: (s == "timeout").sum()), failed=("status", lambda s: (s == "failed").sum()),
      jobs=("id", "nunique")).reset_index()
eng.to_csv(f"{OUT}/dev_attempts_by_engine.csv", index=False)
# query errors (all attempts incl. dropped rep 3)
qe = pd.read_csv(f"{R}/final/phase2_query_errors.csv")
qe["engine"] = qe.cfg.str.extract(r"^(spark420|spark413|comet420|sail|duckdb)")[0]
qe["rep"] = qe.job.str.extract(r"^r(\d)")[0].astype(int)
qsum = qe.groupby(["engine", "kind"]).size().rename("n").reset_index(); qsum.to_csv(f"{OUT}/dev_query_errors.csv", index=False)

# retry factor for the 5-rep plan: attempts per job that ended with a valid run, Spark TPC-H, reps 1-2
spk = st[(st.wl == "tpch") & st.engine.isin(["spark420", "spark413"]) & st.rep.isin([1, 2])]
per_job = spk.groupby("id").agg(n=("attempt", "count"), ok=("status", lambda s: (s == "ok").any()))
retry = per_job.n.sum() / per_job.ok.sum()

# realized vs planned order: first-attempt start time (first query start) vs the seeded plan order
def t0(od):
    p = os.path.join(od.replace("/workspace/engine-swap", ROOT), "queries.jsonl")
    if not os.path.exists(p): return np.nan
    for l in open(p):
        e = json.loads(l)
        if "start" in e: return e["start"]
    return np.nan
st["t0"] = st.outdir.map(t0)
first = st.sort_values("attempt").drop_duplicates("id", keep="first").dropna(subset=["t0"]).sort_values("t0").reset_index(drop=True)
order = {}
for rep in (1, 2, 3):
    p_ids = [j["id"] for j in plan if j["rep"] == rep]
    r_ids = [i for i in first.id if i in set(p_ids)]
    pos = {i: k for k, i in enumerate(p_ids)}
    rp = [pos[i] for i in r_ids]
    tau = stats.kendalltau(rp, range(len(rp))).statistic if len(rp) > 2 else np.nan
    # jobs started later than a job planned after them
    late = sum(1 for k in range(len(rp)) if any(rp[k2] > rp[k] for k2 in range(k)))
    order[rep] = dict(planned=len(p_ids), started=len(r_ids), kendall_tau=float(tau), jobs_started_after_a_later_planned_job=late)
reps_seq = list(first.rep)
inter = sum(1 for k in range(len(reps_seq)) if any(reps_seq[k2] > reps_seq[k] for k2 in range(k)))
# rep-3 sensitivity: complete valid rep-3 TPC-H runs
def load_run(outdir):
    rows = []
    for l in open(os.path.join(outdir, "queries.jsonl")):
        e = json.loads(l)
        if e.get("event") == "query" and e.get("error") is None: rows.append((str(e["q"]), e["wall_s"]))
    return rows
r3 = st[(st.rep == 3) & (st.status == "ok") & (st.wl == "tpch")]
long = pd.read_csv(f"{R}/final/phase2_query_runs_long.csv", dtype={"q": str}); long = long[long.error.isna()]
add = []
for _, r in r3.iterrows():
    od = r.outdir.replace("/workspace/engine-swap", ROOT)
    rows = load_run(od)
    if len({q for q, _ in rows}) == 22:
        add += [dict(cond=r.cond, wl="tpch", cfg=r.cfg, rep=3, q=q, wall_s=w) for q, w in rows]
r3df = pd.DataFrame(add)
r3cells = sorted({(a["cond"], a["cfg"]) for a in add})
allr = pd.concat([long[["cond", "wl", "cfg", "rep", "q", "wall_s"]], r3df])
CMP = [("Sail(reorder) vs Spark 4.2 classic, loopback", "loopback", "sail-reorder", "spark420-classic"),
       ("Sail(noreorder) vs Spark 4.2 default, local", "local", "sail-noreorder", "spark420-default"),
       ("DuckDB vs Spark 4.2 classic, loopback", "loopback", "duckdb", "spark420-classic"),
       ("DuckDB vs Spark 4.2 default, local", "local", "duckdb", "spark420-default"),
       ("Comet vs Spark 4.2 classic, loopback", "loopback", "comet420-classic", "spark420-classic"),
       ("Spark 4.2 AAL vs classic, proxy10", "proxy10", "spark420-analytics", "spark420-classic"),
       ("Spark 4.2 AAL vs classic, loopback", "loopback", "spark420-analytics", "spark420-classic"),
       ("Spark 4.1.3 AAL vs classic, loopback", "loopback", "spark413-analytics", "spark413-classic")]
sens = []
for name, c, a, b in CMP:
    for label, d in [("reps 1-2", long), ("reps 1-3 (complete rep-3 runs added)", allr)]:
        A = d[(d.cond == c) & (d.wl == "tpch") & (d.cfg == a)]; B = d[(d.cond == c) & (d.wl == "tpch") & (d.cfg == b)]
        ra = A.groupby("rep").wall_s.sum(); rb = B.groupby("rep").wall_s.sum()
        ma = A.groupby("q").wall_s.mean().sum(); mb = B.groupby("q").wall_s.mean().sum()
        sens.append(dict(comparison=name, data=label, reps_a=len(ra), reps_b=len(rb), ratio=ma / mb,
                         min_ratio=ra.min() / rb.max(), max_ratio=ra.max() / rb.min()))
SE = pd.DataFrame(sens); SE.to_csv(f"{OUT}/dev_rep3_sensitivity.csv", index=False)
import re, datetime as dt
from zoneinfo import ZoneInfo
TZ = ZoneInfo("Europe/Istanbul")
# logged first starts (progress.log) vs plan
plan_pos = {j["id"]: k for k, j in enumerate(plan)}; seen = []
for l in open(f"{R}/phase2/progress.log"):
    m = re.search(r"START (\S+) attempt (\d+)", l)
    if m and m.group(2) == "1" and m.group(1) not in seen: seen.append(m.group(1))
rp = [plan_pos[i] for i in seen if i in plan_pos]
allstarts = [(m.group(1), int(m.group(2))) for m in (re.search(r"START (\S+) attempt (\d+)", l) for l in open(f"{R}/phase2/progress.log")) if m]
retry_immediate = sum(1 for k, (i, a) in enumerate(allstarts) if a > 1 and allstarts[k - 1][0] == i)
retry_later = sum(1 for k, (i, a) in enumerate(allstarts) if a > 1 and allstarts[k - 1][0] != i)
inversions = sum(1 for a in range(len(rp)) for b in range(a + 1, len(rp)) if rp[a] > rp[b])
# published runs relative to protocol events (times Europe/Istanbul)
pr = pd.read_csv(f"{R}/final/phase2_process_runs.csv"); pr["t0"] = pr.outdir.map(lambda o: t0(os.path.join(ROOT, o)))
ev = {"comet_arena_adopted": dt.datetime(2026, 11, 21, 23, 5, tzinfo=TZ), "box_restore": dt.datetime(2026, 11, 22, 1, 41, tzinfo=TZ),
      "freeze_rule_refined": dt.datetime(2026, 11, 22, 4, 35, tzinfo=TZ)}
events = {k: dict(at=str(v), published_runs_before=int((pr.t0 < v.timestamp()).sum()),
                  comet_published_runs_before=int(((pr.t0 < v.timestamp()) & pr.cfg.str.startswith("comet")).sum())) for k, v in ev.items()}
summary = dict(progress_log_first_starts=len(seen), progress_log_order_inversions_vs_plan=inversions, progress_log_starts=len(allstarts), retries_run_immediately=retry_immediate, retries_run_later=retry_later, published_runs=len(pr),
               published_window=[str(dt.datetime.fromtimestamp(pr.t0.min(), TZ)), str(dt.datetime.fromtimestamp(pr.t0.max(), TZ))],
               protocol_events=events, attempts_total=len(st), attempts_by_status=st.status.value_counts().to_dict(),
               query_errors_total=len(qe), query_errors_by_engine=qe.engine.value_counts().to_dict(),
               query_errors_spark_oom=int(((qe.engine.isin(["spark420", "spark413"])) & (qe.kind == "SparkOutOfMemoryError")).sum()),
               manual_extra_attempts=st[st.get("manual_extra_attempt", False) == True].id.tolist(),
               spark_tpch_retry_factor_reps12=retry, spark_tpch_jobs_reps12=int(len(per_job)),
               order=order, first_attempts_started_before_an_earlier_rep_finished=inter,
               rep3_complete_tpch_cells=[f"{c}/{g}" for c, g in r3cells])
json.dump(summary, open(f"{OUT}/deviations.json", "w"), indent=1, default=float)
print(att.to_string()); print(eng.to_string()); print(qsum.to_string()); print(json.dumps(summary, indent=1, default=float)); print(SE.round(3).to_string())
