"""Build the table-format support matrix, timings vs Parquet, and hash check from results/tablefmt/."""
import csv, json, os, glob, collections, re, shutil

ROOT = "/workspace/engine-swap"
P = f"{ROOT}/results/tablefmt"
FINAL = f"{P}/final"
os.makedirs(FINAL, exist_ok=True)

BASELINE_HASH = {
    1: "dc9fb64947ffda53",
    3: "03a4d982c46be77e",
    6: "86f1051af17c11ba",
    12: "7245c10c0d25d7c9",
    18: "247ac87094aee3e6",
}
PARQUET_MEDIAN = {}  # (cfg_key, q) -> median_s from phase2 loopback tpch
# Map tablefmt cfg to phase2 parquet cfg for timing comparison
PARQUET_CFG = {
    "duckdb": "duckdb",
    "sail-noreorder": "sail-noreorder",
    "spark413-classic": "spark413-classic",
    "spark420-classic": "spark420-classic",
    "comet420-classic": "comet420-classic",
    "comet413-classic": "comet420-classic",  # no 4.1 Comet in Phase 2; compare to study-default Comet on parquet
}

def load_parquet_medians():
    path = f"{ROOT}/results/final/phase2_query_medians.csv"
    with open(path) as f:
        for row in csv.DictReader(f):
            if row["cond"] == "loopback" and row["wl"] == "tpch" and row["q"] in {"1", "3", "6", "12", "18"}:
                PARQUET_MEDIAN[(row["cfg"], int(row["q"]))] = float(row["median_s"])

def first_error_from_log(outdir):
    log = os.path.join(outdir, "engine.log")
    if not os.path.exists(log):
        return None
    text = open(log, errors="replace").read()
    # Prefer a Java fatal error line
    m = re.search(r"(java\.lang\.\w+(?:Error|Exception): [^\n]+)", text)
    if m:
        return m.group(1)
    m = re.search(r"(_duckdb\.\w+|Binder Error|Invalid Input Error|Catalog Error):[^\n]+", text)
    if m:
        return m.group(0).strip()
    # last traceback message
    lines = [l.strip() for l in text.splitlines() if "Error" in l or "Exception" in l]
    return lines[-1][:500] if lines else None

def load_probe():
    rows = json.load(open(f"{P}/probe/probe.json"))
    out = []
    for r in rows:
        rec = dict(r)
        if not rec.get("ok") and not rec.get("query_error"):
            rec["register_or_session_error"] = first_error_from_log(rec["outdir"] if not rec.get("retry_of") else rec.get("retry_of", rec["outdir"]))
            # if this is a successful retry, first_error is historical only
            if rec.get("ok"):
                rec.pop("register_or_session_error", None)
        if not rec.get("ok"):
            rec["error_text"] = rec.get("query_error") or rec.get("register_or_session_error") or first_error_from_log(rec["outdir"])
        out.append(rec)
    return out

def load_timing_runs():
    runs = []
    state_path = f"{P}/state.jsonl"
    if not os.path.exists(state_path):
        return runs
    for line in open(state_path):
        job = json.loads(line)
        outdir = job["outdir"]
        qpath = os.path.join(outdir, "queries.jsonl")
        queries = []
        if os.path.exists(qpath):
            for l in open(qpath):
                r = json.loads(l)
                if r.get("event") == "query" and r.get("q") in BASELINE_HASH:
                    want = BASELINE_HASH[r["q"]]
                    r["hash_ok"] = (not r.get("error")) and r.get("hash_set") == want
                    r["baseline_hash"] = want
                    queries.append(r)
        summ = {}
        sp = os.path.join(outdir, "summary.jsonl")
        if os.path.exists(sp):
            try:
                summ = json.loads(open(sp).read().strip().splitlines()[-1])
            except Exception:
                pass
        runs.append({"job": job, "queries": queries, "summary": summ})
    return runs

def median(xs):
    xs = sorted(xs)
    n = len(xs)
    if not n:
        return None
    if n % 2:
        return xs[n // 2]
    return (xs[n // 2 - 1] + xs[n // 2]) / 2

def main():
    load_parquet_medians()
    probe = load_probe()
    # support matrix
    with open(f"{FINAL}/support_matrix.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["cfg", "format", "read_ok", "rc", "elapsed_s", "hash_set", "error"])
        w.writeheader()
        for r in probe:
            w.writerow({
                "cfg": r["cfg"], "format": r["format"], "read_ok": r.get("ok"),
                "rc": r.get("rc"), "elapsed_s": r.get("elapsed_s"), "hash_set": r.get("hash_set"),
                "error": (r.get("error_text") or r.get("first_error") or r.get("query_error") or "") if not r.get("ok") else "",
            })
    runs = load_timing_runs()
    # per-query long table (valid ok attempts only)
    long_rows = []
    hash_issues = []
    for run in runs:
        job = run["job"]
        # A run is timing-valid if all 5 subset queries hash-match the Parquet baseline,
        # even if the wrapper recorded status=failed (seen on comet413 Iceberg r1: summary rc=0, exit 1).
        q_ok = {q["q"]: q.get("hash_ok") for q in run["queries"]}
        complete = all(q_ok.get(q) for q in BASELINE_HASH)
        if not complete:
            for q in run["queries"]:
                if q.get("error") or not q.get("hash_ok"):
                    hash_issues.append({**job, "q": q.get("q"), "error": q.get("error"), "got": q.get("hash_set"), "want": q.get("baseline_hash")})
            continue
        for q in run["queries"]:
            if q.get("error"):
                hash_issues.append({**job, "q": q["q"], "error": q["error"], "got": q.get("hash_set"), "want": q.get("baseline_hash")})
                continue
            if not q.get("hash_ok"):
                hash_issues.append({**job, "q": q["q"], "error": None, "got": q.get("hash_set"), "want": q.get("baseline_hash")})
            cfg, fmt, qi = job["cfg"], job["format"], q["q"]
            pcfg = PARQUET_CFG.get(cfg, cfg)
            pmed = PARQUET_MEDIAN.get((pcfg, qi))
            s3 = q.get("s3") or {}
            long_rows.append({
                "cfg": cfg, "format": fmt, "rep": job["rep"], "attempt": job["attempt"],
                "q": qi, "wall_s": q["wall_s"], "rows": q.get("rows"),
                "hash_set": q.get("hash_set"), "hash_ok": q.get("hash_ok"),
                "s3_get": s3.get("GetObject") or s3.get("GET") or "",
                "parquet_median_s": pmed,
                "ratio_vs_parquet": (round(q["wall_s"] / pmed, 3) if pmed else ""),
                "parquet_cfg_compared": pcfg,
                "outdir": job["outdir"],
                "peak_rss_gb": round((run["summary"].get("peak_tree_rss_bytes") or 0) / 1e9, 3),
            })
    with open(f"{FINAL}/query_runs.csv", "w", newline="") as f:
        fields = ["cfg","format","rep","attempt","q","wall_s","rows","hash_set","hash_ok","s3_get","parquet_median_s","ratio_vs_parquet","parquet_cfg_compared","outdir","peak_rss_gb"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(long_rows)
    # medians per cfg, format, q (only hash_ok rows)
    groups = collections.defaultdict(list)
    for r in long_rows:
        if r["hash_ok"]:
            groups[(r["cfg"], r["format"], r["q"])].append(r["wall_s"])
    med_rows = []
    for (cfg, fmt, q), xs in sorted(groups.items()):
        pcfg = PARQUET_CFG.get(cfg, cfg)
        pmed = PARQUET_MEDIAN.get((pcfg, q))
        med = median(xs)
        med_rows.append({
            "cfg": cfg, "format": fmt, "q": q, "n": len(xs),
            "median_s": round(med, 3) if med is not None else "",
            "min_s": round(min(xs), 3), "max_s": round(max(xs), 3),
            "parquet_median_s": pmed if pmed is not None else "",
            "ratio_vs_parquet": (round(med / pmed, 3) if (med is not None and pmed) else ""),
            "parquet_cfg_compared": pcfg,
        })
    with open(f"{FINAL}/query_medians.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(med_rows[0].keys()) if med_rows else ["cfg"])
        w.writeheader(); w.writerows(med_rows)
    # totals (sum of the 5 queries per run)
    totals = collections.defaultdict(list)
    per_run = collections.defaultdict(list)
    for r in long_rows:
        if r["hash_ok"]:
            per_run[(r["cfg"], r["format"], r["rep"], r["attempt"])].append(r["wall_s"])
    for (cfg, fmt, rep, att), xs in per_run.items():
        if len(xs) == 5:
            totals[(cfg, fmt)].append(sum(xs))
    tot_rows = []
    for (cfg, fmt), xs in sorted(totals.items()):
        tot_rows.append({
            "cfg": cfg, "format": fmt, "n_complete": len(xs),
            "median_total_s": round(median(xs), 3),
            "min_total_s": round(min(xs), 3),
            "max_total_s": round(max(xs), 3),
        })
    with open(f"{FINAL}/totals.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["cfg","format","n_complete","median_total_s","min_total_s","max_total_s"])
        w.writeheader(); w.writerows(tot_rows)
    if hash_issues:
        with open(f"{FINAL}/hash_issues.json", "w") as f:
            json.dump(hash_issues, f, indent=2, default=str)
    # markdown summary (numbers only from files)
    lines = ["# Table-format extension results", ""]
    lines.append("## Support matrix (SF1 probe, Q6)")
    lines.append("")
    lines.append("| Engine | Iceberg | Delta |")
    lines.append("|---|---|---|")
    cfgs = ["duckdb", "sail-noreorder", "spark413-classic", "spark420-classic", "comet413-classic", "comet420-classic"]
    by = {(r["cfg"], r["format"]): r for r in probe}
    def cell(cfg, fmt):
        r = by.get((cfg, fmt))
        if not r:
            return "not probed"
        if r.get("ok"):
            return f"read ok (hash {r.get('hash_set')})"
        err = (r.get("error_text") or r.get("first_error") or r.get("query_error") or f"rc={r.get('rc')}")
        return f"error: `{err}`"
    for cfg in cfgs:
        lines.append(f"| {cfg} | {cell(cfg,'iceberg')} | {cell(cfg,'delta')} |")
    lines += ["", "## Timing medians vs Phase 2 Parquet (loopback, SF30, Q1/Q3/Q6/Q12/Q18)", ""]
    if med_rows:
        lines.append("| Engine | Format | Q | n | median s | Parquet median s | ratio |")
        lines.append("|---|---|---|---|---|---|---|")
        for r in med_rows:
            lines.append(f"| {r['cfg']} | {r['format']} | Q{r['q']} | {r['n']} | {r['median_s']} | {r['parquet_median_s']} | {r['ratio_vs_parquet']} |")
    lines += ["", "## Five-query totals", ""]
    if tot_rows:
        lines.append("| Engine | Format | n | median total s | min | max |")
        lines.append("|---|---|---|---|---|---|")
        for r in tot_rows:
            lines.append(f"| {r['cfg']} | {r['format']} | {r['n_complete']} | {r['median_total_s']} | {r['min_total_s']} | {r['max_total_s']} |")
    hash_ok = all(r["hash_ok"] for r in long_rows) if long_rows else False
    lines += ["", "## Hash agreement vs Phase 2 Parquet hash_set", ""]
    n_ok = sum(1 for r in long_rows if r["hash_ok"])
    n_bad = sum(1 for r in long_rows if not r["hash_ok"])
    lines.append(f"Compared {n_ok + n_bad} query results. hash_ok={n_ok}, mismatch_or_missing={n_bad}.")
    if hash_issues:
        lines.append("See `hash_issues.json` for details. Mismatches are bug candidates under `repro/minimal-tablefmt/`.")
    open(f"{FINAL}/TABLES.md", "w").write("\n".join(lines) + "\n")
    print(f"wrote {FINAL}/ TABLES.md support_matrix.csv query_runs.csv query_medians.csv totals.csv")
    print(f"timing rows={len(long_rows)} hash_issues={len(hash_issues)} disk_free_gb={shutil.disk_usage(ROOT).free/1e9:.1f}")

if __name__ == "__main__":
    main()
