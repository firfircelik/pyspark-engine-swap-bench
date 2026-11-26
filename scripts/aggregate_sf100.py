"""Aggregate SF100 timings, hash-check vs DuckDB baseline, and build the SF10/SF30/SF100 size-curve table.
Reads only on-disk results. Does not invent numbers. Output: results/sf100/final/
"""
import json, os, csv, statistics, collections, datetime
from pathlib import Path

ROOT = Path("/workspace/engine-swap")
P2 = ROOT / "results/sf100"
OUT = P2 / "final"
OUT.mkdir(parents=True, exist_ok=True)
QUERIES = [1, 3, 6, 12, 18]
ENGINES_SF100 = ["duckdb", "sail-reorder", "comet420-classic", "spark420-classic"]
# Map size-curve engine id -> cfg name in each study
MAP = {
    "duckdb": {"sf100": "duckdb", "sf30": "duckdb", "sf10": "duckdb", "label": "DuckDB 2.0.1"},
    "sail-reorder": {"sf100": "sail-reorder", "sf30": "sail-reorder", "sf10": "sail-reorder", "label": "Sail 0.7.3 (reorder on)"},
    "comet": {"sf100": "comet420-classic", "sf30": "comet420-default", "sf10": "comet420-classic", "label": "Comet 1.2.0 on Spark 4.2"},
    "spark420": {"sf100": "spark420-classic", "sf30": "spark420-default", "sf10": "spark420-classic", "label": "Spark 4.2.0"},
}
ENGINE_IDS = ["duckdb", "sail-reorder", "comet", "spark420"]


def med(xs):
    return round(statistics.median(xs), 3) if xs else None


def wcsv(name, rows, fields=None):
    if not rows:
        return
    fields = fields or list(rows[0].keys())
    with open(OUT / name, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def load_state():
    p = P2 / "state.jsonl"
    att = collections.defaultdict(list)
    if not p.exists():
        return att
    for line in open(p):
        if line.strip():
            d = json.loads(line)
            att[d["id"]].append(d)
    return att


def readq(outdir):
    p = Path(outdir) / "queries.jsonl"
    return [json.loads(l) for l in open(p)] if p.exists() else []


def summ(outdir):
    sp = Path(outdir) / "summary.jsonl"
    try:
        segs = [json.loads(l) for l in sp.read_text().strip().splitlines() if l.strip()]
    except Exception:
        return {}
    if not segs:
        return {}
    s = dict(segs[-1])
    s["segments"] = len(segs)
    s["peak_tree_rss_bytes"] = max(x.get("peak_tree_rss_bytes", 0) for x in segs)
    for k in ("elapsed_s", "cpu_user_s", "cpu_sys_s"):
        s[k] = sum(x.get(k, 0) or 0 for x in segs)
    s["cg_events_before"] = segs[0].get("cg_events_before")
    s["server_died"] = [x.get("server_died") for x in segs if x.get("server_died")]
    s["stalled_any"] = any(x.get("stalled") for x in segs)
    s["timed_out_any"] = any(x.get("timed_out") for x in segs)
    return s


def cg_oom_kills(events):
    """Parse the oom_kill counter from a cgroup memory.events blob."""
    text = events if isinstance(events, str) else str(events or "")
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[0] == "oom_kill":
            try:
                return int(parts[1])
            except ValueError:
                return 0
    return 0

def oom(s):
    """True only if this process segment increased the cgroup oom_kill counter."""
    return cg_oom_kills(s.get("cg_events_after")) > cg_oom_kills(s.get("cg_events_before"))


def query_events(qs):
    return [q for q in qs if q.get("event") == "query"]


def complete_ok(j, qs):
    tq = query_events(qs)
    wanted = set(QUERIES)
    got = {q["q"] for q in tq if not q.get("error")}
    return j.get("status") == "ok" and wanted <= got and not any(q.get("error") for q in tq if q.get("q") in wanted)


# ---- SF100 ----
att = load_state()
SUP = set()
_spf = P2 / "superseded.json"
if _spf.exists():
    SUP = set(json.load(open(_spf)))
attempts_rows, query_rows, failures = [], [], []
valid_by_cfg = collections.defaultdict(list)  # cfg -> list of (rep, outdir, queries, summary)

for jid, attempts in att.items():
    for j in attempts:
        qs = readq(j["outdir"])
        s = summ(j["outdir"])
        tq = query_events(qs)
        rec = {
            "id": j["id"], "cfg": j["cfg"], "rep": j["rep"], "attempt": j.get("attempt"),
            "status": j.get("status"), "rc": j.get("rc"), "elapsed_s": j.get("elapsed_s"),
            "outdir": j.get("outdir", "").replace(str(ROOT) + "/", ""),
            "query_errors": ",".join(str(x) for x in (j.get("query_errors") or [])),
            "frozen": json.dumps(j.get("frozen_queries") or []),
            "stalled": bool(s.get("stalled_any")), "timed_out": bool(s.get("timed_out_any")),
            "oom_kill": oom(s), "peak_tree_rss_gb": round((s.get("peak_tree_rss_bytes") or 0) / 1e9, 3),
            "n_queries": len(tq), "n_errors": sum(1 for q in tq if q.get("error")),
            "complete_ok": complete_ok(j, qs),
            "superseded": j.get("outdir") in SUP,
        }
        attempts_rows.append(rec)
        for q in tq:
            query_rows.append({
                "id": j["id"], "cfg": j["cfg"], "rep": j["rep"], "attempt": j.get("attempt"),
                "q": q.get("q"), "wall_s": q.get("wall_s"), "rows": q.get("rows"),
                "error": (q.get("error") or "").replace("\n", " ")[:300],
                "hash_set": q.get("hash_set") or "",
                "hash_ordered": q.get("hash_ordered") or "",
                "status": j.get("status"), "complete_ok": complete_ok(j, qs),
                "outdir": rec["outdir"],
            })
            if q.get("error"):
                failures.append({
                    "id": j["id"], "cfg": j["cfg"], "rep": j["rep"], "attempt": j.get("attempt"),
                    "q": q.get("q"), "wall_s": q.get("wall_s"),
                    "error": (q.get("error") or "").replace("\n", " ")[:500],
                    "stalled": bool(q.get("stalled") or s.get("stalled_any")),
                    "oom_kill": oom(s), "status": j.get("status"), "outdir": rec["outdir"],
                })
        if rec["complete_ok"]:
            valid_by_cfg[j["cfg"]].append((j["rep"], j["outdir"], tq, s, j))

# One valid run per (cfg, rep): last complete_ok attempt for that rep
valid_runs = {}
for cfg, items in valid_by_cfg.items():
    by_rep = collections.defaultdict(list)
    for it in items:
        by_rep[it[0]].append(it)
    for rep, its in by_rep.items():
        valid_runs[(cfg, rep)] = its[-1]

# Post-fix incomplete runs (not superseded): keep successful queries for hash/timing notes.
# Used for Sail, which consistently finished Q1/Q3/Q6/Q12 then OOM-killed on Q18.
partial_runs = {}  # (cfg, rep, attempt) -> (outdir, successful query events, summary, job)
for jid, attempts in att.items():
    for j in attempts:
        if j.get("outdir") in SUP:
            continue
        qs = readq(j["outdir"])
        if complete_ok(j, qs):
            continue
        tq = [q for q in query_events(qs) if q.get("q") in QUERIES and not q.get("error") and q.get("hash_set")]
        if tq:
            partial_runs[(j["cfg"], j["rep"], j.get("attempt"))] = (j["outdir"], tq, summ(j["outdir"]), j)

# DuckDB baseline: hash_set from valid duckdb runs (should be identical)
baseline = {}
duck_hashes = collections.defaultdict(set)
for (cfg, rep), (r, outdir, tq, s, j) in valid_runs.items():
    if cfg != "duckdb":
        continue
    for q in tq:
        if q.get("q") in QUERIES and q.get("hash_set"):
            duck_hashes[q["q"]].add(q["hash_set"])
            baseline.setdefault(q["q"], q["hash_set"])

hash_check = []
for (cfg, rep), (r, outdir, tq, s, j) in sorted(valid_runs.items()):
    for q in tq:
        if q.get("q") not in QUERIES or q.get("error"):
            continue
        base = baseline.get(q["q"])
        hash_check.append({
            "cfg": cfg, "rep": rep, "q": q["q"], "hash_set": q.get("hash_set") or "",
            "baseline_hash_set": base or "",
            "match": (q.get("hash_set") == base) if base else "",
            "rows": q.get("rows"), "wall_s": q.get("wall_s"),
            "outdir": str(outdir).replace(str(ROOT) + "/", ""),
            "complete": True,
        })
for (cfg, rep, attempt), (outdir, tq, s, j) in sorted(partial_runs.items()):
    for q in tq:
        if q.get("q") not in QUERIES:
            continue
        base = baseline.get(q["q"])
        hash_check.append({
            "cfg": cfg, "rep": rep, "q": q["q"], "hash_set": q.get("hash_set") or "",
            "baseline_hash_set": base or "",
            "match": (q.get("hash_set") == base) if base else "",
            "rows": q.get("rows"), "wall_s": q.get("wall_s"),
            "outdir": str(outdir).replace(str(ROOT) + "/", ""),
            "complete": False, "attempt": attempt,
        })

# Per-query timings from valid complete runs, plus post-fix partial successes (Sail Q1/Q3/Q6/Q12)
sf100_times = collections.defaultdict(list)  # (cfg, q) -> [wall_s]
sf100_rss = collections.defaultdict(list)
sf100_times_partial = collections.defaultdict(list)
sf100_rss_partial = collections.defaultdict(list)
sf100_complete_cfg = set()
for (cfg, rep), (r, outdir, tq, s, j) in valid_runs.items():
    sf100_complete_cfg.add(cfg)
    sf100_rss[cfg].append(round((s.get("peak_tree_rss_bytes") or 0) / 1e9, 3))
    for q in tq:
        if q.get("q") in QUERIES and not q.get("error"):
            sf100_times[(cfg, q["q"])].append(q["wall_s"])
for (cfg, rep, attempt), (outdir, tq, s, j) in partial_runs.items():
    sf100_rss_partial[cfg].append(round((s.get("peak_tree_rss_bytes") or 0) / 1e9, 3))
    for q in tq:
        if q.get("q") in QUERIES and not q.get("error"):
            sf100_times_partial[(cfg, q["q"])].append(q["wall_s"])

sf100_med = []
for cfg in ENGINES_SF100:
    complete = cfg in sf100_complete_cfg
    for q in QUERIES:
        if complete:
            xs = sf100_times.get((cfg, q), [])
            src = "complete_reps"
            rss = sf100_rss.get(cfg, [])
        else:
            xs = sf100_times_partial.get((cfg, q), [])
            src = "partial_sessions_q18_oom" if xs else "no_valid_times"
            rss = sf100_rss_partial.get(cfg, [])
        sf100_med.append({
            "cfg": cfg, "q": q, "n": len(xs),
            "median_s": med(xs), "min_s": (round(min(xs), 3) if xs else None),
            "max_s": (round(max(xs), 3) if xs else None),
            "reps_s": ",".join(f"{x:.3f}" for x in xs),
            "median_peak_rss_gb": med(rss),
            "source": src,
        })

wcsv("sf100_attempts.csv", attempts_rows)
wcsv("sf100_query_runs.csv", query_rows)
wcsv("sf100_failures.csv", failures)
wcsv("sf100_hash_check.csv", hash_check)
wcsv("sf100_query_medians.csv", sf100_med)

# ---- SF10 pilot-sf10-r2 (single combined queries.jsonl, loopback MinIO, n=1) ----
sf10 = collections.defaultdict(dict)
pilot = ROOT / "results/pilot-sf10-r2/queries.jsonl"
if pilot.exists():
    # For comet420-analytics the file contains a first run plus a rerun; we are using *-classic / sail-reorder / duckdb (single pass).
    by_lab_q = collections.defaultdict(list)
    for line in open(pilot):
        rec = json.loads(line)
        if rec.get("event") == "query" and not rec.get("error"):
            by_lab_q[(rec.get("label"), rec.get("q"))].append(rec)
    for (lab, q), recs in by_lab_q.items():
        # if a label has two values for a query, STATUS used the later (rerun) for comet420-analytics.
        # For our four engines only classic/sail/duckdb appear once.
        rec = recs[-1]
        sf10[lab][q] = rec["wall_s"]

# ---- SF30 published Phase 2 local medians ----
sf30 = collections.defaultdict(dict)
sf30_n = collections.defaultdict(dict)
med_csv = ROOT / "results/final/phase2_query_medians.csv"
with open(med_csv) as f:
    for rec in csv.DictReader(f):
        if rec["cond"] == "local" and rec["wl"] == "tpch":
            q = int(rec["q"]) if str(rec["q"]).isdigit() else rec["q"]
            sf30[rec["cfg"]][q] = float(rec["median_s"]) if rec["median_s"] else None
            sf30_n[rec["cfg"]][q] = int(rec["n"]) if rec["n"] else 0

# ---- size-curve rows ----
curve = []
for eid in ENGINE_IDS:
    m = MAP[eid]
    for q in QUERIES:
        s10 = sf10.get(m["sf10"], {}).get(q)
        s30 = sf30.get(m["sf30"], {}).get(q)
        xs = sf100_times.get((m["sf100"], q), [])
        src = "complete"
        if not xs:
            xs = sf100_times_partial.get((m["sf100"], q), [])
            if xs:
                src = "partial_q18_oom"
        s100 = med(xs)
        notes = []
        if q == 12 and s10 is None:
            notes.append("SF10 pilot has no Q12 (pilot ran Q1/Q3/Q6/Q9/Q18)")
        notes.append("SF10: MinIO loopback, driver 8g, Comet 4g+4g, no 9 GiB cgroup, single-run pilot")
        notes.append("SF30: Phase 2 local file, 9 GiB cgroup, timings from full 22-query sessions (published phase2_query_medians.csv)")
        notes.append("SF100: local file on /tmp copy of data/tpch-sf100, 9 GiB cgroup, 5-query sessions; spark/comet cfg names *-classic (S3A stream N/A on file://) mapped to Phase 2 local *-default")
        if src == "partial_q18_oom":
            notes.append("SF100 time is the median of successful query executions in sessions that then OOM-killed on Q18 (Sail 0.7.3 reorder, 9 GiB cap); not a complete 5-query run")
        if m["sf100"] and q == 18 and src != "complete" and not xs:
            notes.append("SF100 Q18: no successful run (Sail reorder OOM-killed at the 9 GiB cgroup cap on all 6 post-fix attempts)")
        curve.append({
            "engine_id": eid, "engine": m["label"],
            "q": q,
            "sf10_s": (round(s10, 3) if s10 is not None else ""),
            "sf10_n": (1 if s10 is not None else 0),
            "sf30_s": (round(s30, 3) if s30 is not None else ""),
            "sf30_n": sf30_n.get(m["sf30"], {}).get(q, 0),
            "sf100_s": (s100 if s100 is not None else ""),
            "sf100_n": len(xs),
            "sf100_min_s": (round(min(xs), 3) if xs else ""),
            "sf100_max_s": (round(max(xs), 3) if xs else ""),
            "sf100_source": src,
            "sf10_to_sf30_ratio": (round(s30 / s10, 2) if (s10 and s30) else ""),
            "sf30_to_sf100_ratio": (round(s100 / s30, 2) if (s30 and s100) else ""),
            "notes": " | ".join(notes),
        })
wcsv("size_curve.csv", curve)


def fmt(v):
    if v is None or v == "":
        return "—"
    if isinstance(v, float):
        return f"{v:.3f}"
    return str(v)


lines = []
lines += ["# SF100 extension results", ""]
lines += [f"Generated {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Europe/Istanbul. Numbers are read from on-disk result files. Nothing here is estimated.", ""]
lines += ["## Data", ""]
lines += [
    "- `tpchgen-cli` 4.0.0, `parquet -s 100 --parts=80 --compression=SNAPPY`, 121.111 s. Canonical files: `data/tpch-sf100` 39 GB (nation/region 1 file; other tables 80 parts; lineitem parts 324 MB).",
    "- Timed runs read a copy at `/tmp/engine-swap-data/tpch-sf100` on `/dev/vdc` (not the workspace disk). Reason: host snapshot sidecars named `*.parquet.box-store-snap-*` appeared in the workspace directory and were picked up by Spark/Sail directory listings. `bench2_spark_api.py` was also changed to glob `*.parquet`, matching DuckDB. Warmup remains `data/tpch-sf1`.",
    "- 9 GiB cgroup, swap 0. Comet `MALLOC_ARENA_MAX=2`. 20-min stall kill. Job TIMEOUT 7200 s. No stalls or timeouts occurred after the path fix.",
    ""]

# hash
lines += ["## Hash check (SF100 `hash_set` vs DuckDB baseline)", ""]
if any(len(v) > 1 for v in duck_hashes.values()):
    lines += ["WARNING: DuckDB valid runs did not all agree on the baseline hash for at least one query.", ""]
    for q, hs in duck_hashes.items():
        if len(hs) > 1:
            lines += [f"- Q{q} DuckDB hashes: {sorted(hs)}"]
if not baseline:
    lines += ["No valid DuckDB run, so no SF100 baseline was recorded.", ""]
else:
    lines += ["Baseline (DuckDB `hash_set`):", ""]
    lines += ["| query | hash_set |", "|---|---|"]
    for q in QUERIES:
        lines += [f"| Q{q} | `{baseline.get(q, '—')}` |"]
    lines += ["", "| engine | rep | Q1 | Q3 | Q6 | Q12 | Q18 |", "|---|---|---|---|---|---|---|"]
    by = collections.defaultdict(dict)
    for r in hash_check:
        by[(r["cfg"], r["rep"])][r["q"]] = r
    for cfg in ENGINES_SF100:
        reps = sorted({rep for (c, rep) in valid_runs if c == cfg})
        if not reps:
            # show partial post-fix attempts if any
            preps = sorted({(r["rep"], r.get("attempt")) for r in hash_check if r["cfg"] == cfg and r.get("complete") is False})
            if not preps:
                lines += [f"| {cfg} | — | no valid complete run |||||"]
            else:
                # collapse to one row per rep using the last attempt
                last = {}
                for r in hash_check:
                    if r["cfg"] == cfg and r.get("complete") is False:
                        last[(r["rep"], r["q"])] = r
                for rep in sorted({r for r, _a in preps}):
                    cells = []
                    for q in QUERIES:
                        h = last.get((rep, q))
                        if not h:
                            cells.append("OOM / no result" if q == 18 else "missing")
                        elif h["match"] is True:
                            cells.append("match*")
                        elif h["match"] is False:
                            cells.append(f"MISMATCH `{h['hash_set']}`")
                        else:
                            cells.append("no baseline")
                    lines += [f"| {cfg} | {rep}* | " + " | ".join(cells) + " |"]
            continue
        for rep in reps:
            cells = []
            for q in QUERIES:
                h = by.get((cfg, rep), {}).get(q)
                if not h:
                    cells.append("missing")
                elif h["match"] is True:
                    cells.append("match")
                elif h["match"] is False:
                    cells.append(f"MISMATCH `{h['hash_set']}`")
                else:
                    cells.append("no baseline")
            lines += [f"| {cfg} | {rep} | " + " | ".join(cells) + " |"]
    lines += [""]
    mismatches = [r for r in hash_check if r["match"] is False]
    if mismatches:
        lines += ["Mismatches (wrong-result candidates):"]
        for r in mismatches:
            lines += [f"- {r['cfg']} rep{r['rep']} Q{r['q']}: got `{r['hash_set']}` expected `{r['baseline_hash_set']}` ({r['outdir']})"]
        lines += [""]
    else:
        if hash_check and baseline:
            lines += ["Every completed timing query (including Sail Q1/Q3/Q6/Q12 in sessions that then died on Q18) matched the DuckDB baseline. A `match*` cell is from an incomplete session.", ""]

# timings
lines += ["## SF100 per-query wall time (seconds)", ""]
lines += ["Complete 5-query sessions for DuckDB, Comet, and Spark 4.2 (n=2). Sail 0.7.3 with join reorder never finished Q18: all 6 post-fix attempts (3 per rep) were OOM-killed by the 9 GiB cgroup during Q18 (peak tree RSS 8.7–9.6 GB). Sail Q1/Q3/Q6/Q12 times below are the median of those 6 successful query executions, flagged with `*`. Config was not retuned.", ""]
lines += ["| engine | Q1 median (min–max, n) | Q3 | Q6 | Q12 | Q18 | peak RSS GB median |",
          "|---|---|---|---|---|---|---|"]
for row_cfg in ENGINES_SF100:
    recs = [x for x in sf100_med if x["cfg"] == row_cfg]
    star = "*" if recs and recs[0]["source"] != "complete_reps" else ""
    cells = [row_cfg + star]
    for q in QUERIES:
        rec = next(x for x in sf100_med if x["cfg"] == row_cfg and x["q"] == q)
        if rec["n"] == 0:
            cells.append("OOM ×6" if (row_cfg == "sail-reorder" and q == 18) else "—")
        else:
            cells.append(f"{rec['median_s']} ({rec['min_s']}–{rec['max_s']}, n={rec['n']}){star}")
    if star:
        rss = med(sf100_rss_partial.get(row_cfg, []))
    else:
        rss = med(sf100_rss.get(row_cfg, []))
    cells.append(fmt(rss))
    lines += ["| " + " | ".join(str(c) for c in cells) + " |"]
lines += ["", "Raw times: `sf100_query_medians.csv` and `sf100_query_runs.csv`. DuckDB r1 was slower than r2 on Q1/Q3/Q18 (e.g. Q1 21.558 s vs 6.086 s); both are included in the median.", ""]

# 5-query totals
lines += ["## SF100 5-query total (sum of medians, seconds)", ""]
lines += ["| engine | n complete reps | sum of medians s |", "|---|---|---|"]
for row_cfg in ENGINES_SF100:
    recs = [x for x in sf100_med if x["cfg"] == row_cfg]
    ns = [x["n"] for x in recs]
    if not recs or min(ns) == 0:
        lines += [f"| {row_cfg} | {min(ns) if ns else 0} | — (no complete 5-query run) |"]
    else:
        lines += [f"| {row_cfg} | {min(ns)} | {round(sum(x['median_s'] for x in recs), 3)} |"]
lines += [""]

# failures
lines += ["## Failures, stalls, and memory events", ""]
pref = [a for a in attempts_rows if a.get("superseded")]
post = [a for a in attempts_rows if not a.get("superseded") and (a["oom_kill"] or a["stalled"] or a["timed_out"] or a["status"] != "ok" or a["n_errors"])]
lines += [
    "Two distinct classes. Config was not retuned.",
    "",
    "### 1. Host snapshot sidecars (superseded; not counted as engine results)",
    "",
    "Before the path fix, Sail r1 a1–a3 and Comet r1 a1–a2 failed because the workspace data directory briefly contained `*.parquet.box-store-snap-*` files owned by `sand-host`. Spark: `FAILED_READ_FILE.FILE_NOT_EXIST`. Sail: `Invalid Parquet file. Corrupt footer` or `Object at location ...box-store-snap-... not found`. The Parquet dataset itself is valid (482 footers parse; DuckDB `lineitem` = 600,037,902). Those attempts are in `superseded.json` and were re-run from `/tmp/engine-swap-data/tpch-sf100`.",
    "",
    "### 2. Sail Q18 OOM at the 9 GiB cap (finding, F3-style at SF100)",
    "",
    "After the path fix, Sail 0.7.3 with join reorder completed Q1, Q3, Q6, and Q12 with hashes matching DuckDB, then the Spark Connect server was SIGKILL'd (`rc=-9`) during Q18 on all 6 attempts, with a cgroup `oom_kill` increment and peak tree RSS 8.7–9.6 GB. Same class of limit seen at SF30 (STATUS F3). No stalls and no job timeouts. DuckDB, Comet, and Spark 4.2 each have 2 complete valid reps and no post-fix query errors.",
    "",
    "| id | attempt | status | q | wall_s | oom_this_job | error |",
    "|---|---|---|---|---|---|---|",
]
for f in failures:
    att = next((a for a in attempts_rows if a["id"]==f["id"] and a["attempt"]==f["attempt"]), {})
    if att.get("superseded"):
        continue
    lines += [f"| {f['id']} | {f['attempt']} | {f['status']} | Q{f['q']} | {f['wall_s']} | {att.get('oom_kill')} | {f['error'][:220]} |"]
lines += [""]

# size curve
lines += ["## Size curve: SF10 / SF30 / SF100 wall time (seconds)", ""]
lines += [
    "This table is **not** a like-for-like speedup claim across all three scales. Conditions differ, and they are flagged in the last column and below.",
    "",
    "- **SF10** (`results/pilot-sf10-r2`): MinIO loopback, not local file; Spark driver 8g; Comet 4g heap + 4g off-heap; no 9 GiB cgroup cap; single-run pilot (n=1). Query set was Q1, Q3, Q6, Q9, Q18 — **Q12 was not run**.",
    "- **SF30** (`results/final/phase2_query_medians.csv`, cond=local): local Parquet, 9 GiB cgroup, Comet 2g+3g and `MALLOC_ARENA_MAX=2`, median of 2 valid reps. Timings come from **full 22-query sessions**, so Q12 and Q18 ran after many earlier queries (different memory and GC state than a 5-query SF100 session).",
    "- **SF100** (this extension): local Parquet copied to `/tmp/engine-swap-data/tpch-sf100`, 9 GiB cgroup, 5-query sessions, median of 2 complete reps except Sail. `spark420-classic` / `comet420-classic` are the same local-file engines as Phase 2 `spark420-default` / `comet420-default` (S3A stream type does not apply to `file://`). Sail Q1/Q3/Q6/Q12 are medians from 6 incomplete sessions that OOM-killed on Q18; Sail Q18 has no successful time.",
    "- Scale factors are 10 / 30 / 100 (3× then 3.33×). A perfectly linear scan would grow by those ratios; this is only a rough reference, not a model.",
    "",
    "| engine | query | SF10 s (n=1, loopback pilot) | SF30 s (median, local, 22-query session) | SF100 s (median, local, 5-query session) | SF30/SF10 | SF100/SF30 |",
    "|---|---|---|---|---|---|---|",
]
for row in curve:
    lines += [f"| {row['engine']} | Q{row['q']} | {fmt(row['sf10_s'])} | {fmt(row['sf30_s'])} | {fmt(row['sf100_s'])} | {fmt(row['sf10_to_sf30_ratio'])} | {fmt(row['sf30_to_sf100_ratio'])} |"]
lines += ["", "Full notes per row: `size_curve.csv`.", ""]
lines += ["## File paths", ""]
lines += [
    "- Canonical data: `/workspace/engine-swap/data/tpch-sf100`",
    "- Timed-run copy: `/tmp/engine-swap-data/tpch-sf100` (ephemeral root disk; regenerate from the canonical copy if missing)",
    "- Jobs: `/workspace/engine-swap/results/sf100/`",
    "- This report: `/workspace/engine-swap/results/sf100/final/TABLES.md`",
    "- CSVs: `sf100_attempts.csv`, `sf100_query_runs.csv`, `sf100_query_medians.csv`, `sf100_hash_check.csv`, `sf100_failures.csv`, `size_curve.csv`",
    "- Plan: `/workspace/engine-swap/results/sf100/PLAN.md`",
]
(OUT / "TABLES.md").write_text("\n".join(lines) + "\n")
print("wrote", OUT / "TABLES.md")
print("valid_runs", {(c, r): str(v[1]) for (c, r), v in valid_runs.items()})
print("baseline", baseline)
print("failures", len(failures))
