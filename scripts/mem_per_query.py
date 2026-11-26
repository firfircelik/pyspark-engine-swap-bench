"""Peak process-tree RSS per timed query from rss.csv + queries.jsonl. Usage: mem_per_query.py <outdir>... ; prints a row per run and writes <outdir>/mem_per_query.csv"""
import sys, json, csv
for d in sys.argv[1:]:
    rss = [(float(t), int(v)) for t, v in csv.reader(open(f"{d}/rss.csv")) if t[:1].isdigit()]
    qs = [x for x in map(json.loads, open(f"{d}/queries.jsonl")) if x["event"] == "query"]
    try: summ = json.loads(open(f"{d}/summary.jsonl").read().strip().splitlines()[-1])
    except Exception: summ = {}
    rows = []
    for x in qs:
        m = max([v for t, v in rss if x["start"] <= t <= x["end"]] or [0])
        rows.append({"q": x["q"], "wall_s": x["wall_s"], "peak_rss_gb": round(m / 1e9, 3), "error": bool(x.get("error"))})
    with open(f"{d}/mem_per_query.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["q", "wall_s", "peak_rss_gb", "error"]); w.writeheader(); w.writerows(rows)
    print(d.split("results/")[-1], "rc", summ.get("rc"), "peak", round(summ.get("peak_tree_rss_bytes", 0) / 1e9, 2), "nq", len(qs),
          "sum_wall", round(sum(r["wall_s"] for r in rows), 1), "|", " ".join(f"{r['q']}:{r['peak_rss_gb']:.1f}" for r in rows))
