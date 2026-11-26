"""Aggregate Phase 2 + AAL isolation into charts-ready CSVs and markdown tables.
Uses only the final ok attempt of each job (superseded attempts ignored). Medians over reps; n reported.
Output: results/final/*.csv + results/final/TABLES.md"""
import json, os, csv, statistics, collections, glob
ROOT = "/workspace/engine-swap"; OUT = f"{ROOT}/results/final"; os.makedirs(OUT, exist_ok=True)

def dropped_reps(name):
    """Rep numbers dropped from published totals (see results/<name>/dropped_reps.json)."""
    p = f"{ROOT}/results/{name}/dropped_reps.json"
    if not os.path.exists(p): return set()
    d = json.load(open(p))
    return set(d.get("dropped_reps") or [])

def final_jobs(name, include_dropped=False):
    P = f"{ROOT}/results/{name}"
    if not os.path.exists(f"{P}/state.jsonl"): return {}, []
    sup = set(json.load(open(f"{P}/superseded.json"))) if os.path.exists(f"{P}/superseded.json") else set()
    drop = set() if include_dropped else dropped_reps(name)
    att = collections.defaultdict(list)
    for l in open(f"{P}/state.jsonl"):
        d = json.loads(l)
        if d.get("rep") in drop: continue
        if d["outdir"] not in sup: att[d["id"]].append(d)
    fin = {}
    for k, v in att.items():
        last = dict(v[-1])
        if last["status"] == "ok" and last.get("query_errors"): last["status"] = "qerror"   # legacy records before 2026-11-22 00:05
        if last["status"] in ("ok", "failed", "timeout") or len(v) >= 3: fin[k] = last   # exhausted retries: keep last attempt, errors flagged
    return fin, [d for v in att.values() for d in v]

def readq(outdir):
    p = f"{outdir}/queries.jsonl"
    return [json.loads(l) for l in open(p)] if os.path.exists(p) else []
def summ(outdir):
    """One line per process segment (Sail runs restart the server after it dies): peak = max, elapsed/cpu = sum."""
    try: segs = [json.loads(l) for l in open(f"{outdir}/summary.jsonl").read().strip().splitlines()]
    except Exception: return {}
    s = dict(segs[-1]); s["segments"] = len(segs)
    s["peak_tree_rss_bytes"] = max(x.get("peak_tree_rss_bytes", 0) for x in segs)
    for k in ("elapsed_s", "cpu_user_s", "cpu_sys_s"): s[k] = sum(x.get(k, 0) or 0 for x in segs)
    s["cg_events_before"] = segs[0].get("cg_events_before"); s["server_died"] = [x.get("server_died") for x in segs if x.get("server_died")]
    return s
def med(xs): return round(statistics.median(xs), 3) if xs else None
def wcsv(name, rows, fields=None):
    if not rows: return
    fields = fields or list(rows[0].keys())
    with open(f"{OUT}/{name}", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore"); w.writeheader(); w.writerows(rows)

def collect(name):
    fin, allatt = final_jobs(name)
    long, runs = [], []
    for j in fin.values():
        qs = readq(j["outdir"]); s = summ(j["outdir"])
        cfg = j.get("variant", j["cfg"])
        tq = [q for q in qs if q["event"] == "query"]
        for q in tq:
            s3 = q.get("s3") or {}
            long.append({"study": name, "cond": j["cond"], "wl": j["wl"], "cfg": cfg, "rep": j["rep"], "q": q["q"], "wall_s": q["wall_s"],
                         "rows": q.get("rows"), "error": (q.get("error") or "")[:160].replace("\n", " "), "hash_set": q.get("hash_set", ""),
                         "s3_get": s3.get("GetObject", 0), "s3_head": s3.get("HeadObject", 0), "s3_list": s3.get("ListObjectsV2", 0),
                         "s3_put": s3.get("PutObject", 0) + s3.get("PutObjectPart", 0), "s3_copy": s3.get("CopyObject", 0),
                         "s3_bytes_sent": s3.get("_sent_bytes", 0)})
        ok_all = j["status"] == "ok" and tq and not any(q.get("error") for q in tq)
        runs.append({"study": name, "cond": j["cond"], "wl": j["wl"], "cfg": cfg, "rep": j["rep"], "status": j["status"],
                     "n_queries": len(tq), "n_errors": sum(1 for q in tq if q.get("error")),
                     "total_wall_s": round(sum(q["wall_s"] for q in tq), 3) if ok_all else None,
                     "process_elapsed_s": s.get("elapsed_s"), "peak_tree_rss_gb": round(s.get("peak_tree_rss_bytes", 0) / 1e9, 3),
                     "cpu_s": round(s.get("cpu_user_s", 0) + s.get("cpu_sys_s", 0), 1),
                     "oom_kill": ("oom_kill 1" in s.get("cg_events_after", "")) and s.get("cg_events_before") != s.get("cg_events_after"),
                     "attempts": sum(1 for a in allatt if a["id"] == j["id"]), "segments": s.get("segments", 1), "server_died": len(s.get("server_died", [])), "outdir": j["outdir"].replace(ROOT + "/", "")})
    return long, runs

long, runs = collect("phase2")
wcsv("phase2_query_runs_long.csv", long); wcsv("phase2_process_runs.csv", runs)
# per-query medians
g = collections.defaultdict(list)
for r in long:
    if not r["error"]: g[(r["cond"], r["wl"], r["cfg"], r["q"])].append(r)
qmed = []
for (cond, wl, cfg, q), rs in sorted(g.items(), key=lambda x: (x[0][0], x[0][1], x[0][2], str(x[0][3]))):
    w = [x["wall_s"] for x in rs]
    qmed.append({"cond": cond, "wl": wl, "cfg": cfg, "q": q, "n": len(rs), "median_s": med(w), "min_s": min(w), "max_s": max(w),
                 "median_s3_get": med([x["s3_get"] for x in rs]), "median_s3_bytes": med([x["s3_bytes_sent"] for x in rs])})
wcsv("phase2_query_medians.csv", qmed)
# totals
gt = collections.defaultdict(list)
for r in runs: gt[(r["cond"], r["wl"], r["cfg"])].append(r)
tot = []
for (cond, wl, cfg), rs in sorted(gt.items()):
    t = [x["total_wall_s"] for x in rs if x["total_wall_s"] is not None]
    tot.append({"cond": cond, "wl": wl, "cfg": cfg, "n_runs": len(rs), "n_complete": len(t), "median_total_s": med(t),
                "min_total_s": min(t) if t else None, "max_total_s": max(t) if t else None,
                "median_peak_rss_gb": med([x["peak_tree_rss_gb"] for x in rs]), "max_peak_rss_gb": max(x["peak_tree_rss_gb"] for x in rs),
                "median_cpu_s": med([x["cpu_s"] for x in rs]), "oom_runs": sum(1 for x in rs if x["oom_kill"]),
                "failed_runs": sum(1 for x in rs if x["status"] != "ok"), "query_errors": sum(x["n_errors"] for x in rs)})
wcsv("phase2_totals.csv", tot)
# stream-type comparison (same engine version, classic vs analytics)
tl = {(t["cond"], t["wl"], t["cfg"]): t["median_total_s"] for t in tot}
sc = []
for cond in ["loopback", "proxy0", "proxy10"]:
    for wl in ["tpch", "etl"]:
        for eng in ["spark413", "spark420", "comet420"]:
            c, a = tl.get((cond, wl, f"{eng}-classic")), tl.get((cond, wl, f"{eng}-analytics"))
            if c and a: sc.append({"cond": cond, "wl": wl, "engine": eng, "classic_s": c, "analytics_s": a, "analytics_over_classic": round(a / c, 3)})
wcsv("phase2_stream_type_compare.csv", sc)
# result hash check (TPC-H): distinct normalized result hashes per query across all engines/conds/reps
hq = collections.defaultdict(lambda: collections.defaultdict(list))
for r in long:
    if r["wl"] == "tpch" and not r["error"]: hq[r["q"]][r["hash_set"]].append(f'{r["cfg"]}@{r["cond"]}')
hc = []
for q in sorted(hq, key=int):
    hs = hq[q]; major = max(hs, key=lambda h: len(hs[h]))
    others = {h: sorted(set(v)) for h, v in hs.items() if h != major}
    hc.append({"q": q, "distinct_hashes": len(hs), "majority_hash": major, "n_majority": len(hs[major]),
               "minority": json.dumps(others)[:1000]})
wcsv("phase2_hash_check.csv", hc)
# ETL verification digests
ev = []
for r in runs:
    if r["wl"] != "etl": continue
    p = f"{ROOT}/{r['outdir']}/etl_verify.jsonl"
    if os.path.exists(p):
        for l in open(p):
            d = json.loads(l)
            ev.append({"cond": r["cond"], "cfg": r["cfg"], "rep": r["rep"], "step": d["step"], "rows": d.get("rows"), "digest": d.get("digest"),
                       "files": d.get("files"), "schema": " ".join(f"{a}:{b}" for a, b in d.get("schema", []))})
wcsv("phase2_etl_verify.csv", ev)
evs = collections.defaultdict(lambda: collections.defaultdict(set))
for e in ev: evs[e["step"]][e["digest"]].add(e["cfg"])
etl_match = []
for step in sorted(evs):
    ds = evs[step]; ref = next((d for d, c in ds.items() if "spark420-classic" in c), None)
    for d, cs in ds.items():
        etl_match.append({"step": step, "digest": d, "matches_spark420": d == ref, "configs": " ".join(sorted(cs))})
wcsv("phase2_etl_digest_match.csv", etl_match)
# every query error over ALL attempts (incl. frozen / re-run ones): failure-rate finding
fin_all, allatt = final_jobs("phase2", include_dropped=True)
qe = []
for a in allatt:
    for q in readq(a["outdir"]):
        if q["event"] == "query" and q.get("error"):
            e = q["error"]; kind = ("SparkOutOfMemoryError" if "OutOfMemory" in e or "UNABLE_TO_ACQUIRE_MEMORY" in e else
                    "NoSpaceLeft" if "No space left" in e else "FAILED_READ_FILE" if "FAILED_READ_FILE" in e else e.split(":")[0][:60])
            qe.append({"job": a["id"], "attempt": a["attempt"], "attempt_status": a["status"], "cfg": a["cfg"], "cond": a["cond"], "wl": a["wl"],
                       "q": q["q"], "kind": kind, "frozen_attempt": a["status"] == "frozen", "error": e[:300].replace("\n", " ")})
wcsv("phase2_query_errors.csv", qe)
# warmup (separate)
wu = []
for j in final_jobs("phase2")[0].values():
    for q in readq(j["outdir"]):
        if q["event"] == "warmup": wu.append({"cond": j["cond"], "wl": j["wl"], "cfg": j["cfg"], "rep": j["rep"], "q": q["q"], "wall_s": q["wall_s"]})
wcsv("phase2_warmup.csv", wu)
# AAL isolation
along, aruns = collect("aal-iso")
wcsv("aal_iso_query_runs_long.csv", along)
ga = collections.defaultdict(list)
for r in along:
    if not r["error"]: ga[(r["cond"], r["cfg"], r["q"])].append(r)
aal = []
for (cond, v, q), rs in sorted(ga.items(), key=lambda x: (x[0][0], x[0][1], str(x[0][2]))):
    aal.append({"cond": cond, "variant": v, "q": q, "n": len(rs), "median_s": med([x["wall_s"] for x in rs]),
                "median_s3_get": med([x["s3_get"] for x in rs]), "median_s3_head": med([x["s3_head"] for x in rs]),
                "median_gb_sent": round(med([x["s3_bytes_sent"] for x in rs]) / 1e9, 3), "hash_set": "|".join(sorted({x["hash_set"] for x in rs}))})
wcsv("aal_iso_medians.csv", aal)
# Markdown tables
drop = dropped_reps("phase2")
drop_note = f" Published totals exclude rep {sorted(drop)} (see results/phase2/dropped_reps.json)." if drop else ""
L = ["# Phase 2 tables (generated by scripts/aggregate2.py)", "", f"Process runs: {len(runs)}; query rows: {len(long)}.{drop_note}", ""]
for wl in ["tpch", "etl"]:
    L += [f"## Total timed wall time, {wl} (median of complete runs, s; peak RSS GB median)", "", "| config | " + " | ".join(["local", "loopback", "proxy0", "proxy10"]) + " |", "|---|---|---|---|---|"]
    cfgs = sorted({t["cfg"] for t in tot if t["wl"] == wl})
    for c in cfgs:
        cells = []
        for cond in ["local", "loopback", "proxy0", "proxy10"]:
            t = next((x for x in tot if x["cfg"] == c and x["cond"] == cond and x["wl"] == wl), None)
            cells.append("" if not t else (f'{t["median_total_s"]} (n={t["n_complete"]}/{t["n_runs"]}, {t["median_peak_rss_gb"]} GB)' if t["median_total_s"] else f'incomplete {t["n_complete"]}/{t["n_runs"]}'))
        L.append(f"| {c} | " + " | ".join(cells) + " |")
    L.append("")
L += ["## classic vs analytics (AAL) stream", "", "| cond | wl | engine | classic s | analytics s | ratio |", "|---|---|---|---|---|---|"]
L += [f'| {s["cond"]} | {s["wl"]} | {s["engine"]} | {s["classic_s"]} | {s["analytics_s"]} | {s["analytics_over_classic"]} |' for s in sc]
L += ["", "## TPC-H result hash check", "", "| q | distinct hashes | minority (config@cond) |", "|---|---|---|"]
L += [f'| {h["q"]} | {h["distinct_hashes"]} | {h["minority"][:300]} |' for h in hc]
L += ["", "## AAL isolation (median over reps)", "", "| cond | variant | q | n | median s | GETs | GB sent |", "|---|---|---|---|---|---|---|"]
L += [f'| {a["cond"]} | {a["variant"]} | {a["q"]} | {a["n"]} | {a["median_s"]} | {a["median_s3_get"]} | {a["median_gb_sent"]} |' for a in aal]
open(f"{OUT}/TABLES.md", "w").write("\n".join(L) + "\n")
print("wrote", sorted(os.listdir(OUT)))

# F4 characterization, valid-reps, GET-reduction addendum
import runpy
runpy.run_path(f"{ROOT}/scripts/finalize_tables.py", run_name="__main__")
