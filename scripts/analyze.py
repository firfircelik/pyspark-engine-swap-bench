import json, csv, sys, os, collections
tag = sys.argv[1]; D = f"/workspace/engine-swap/results/{tag}"
Q = [json.loads(l) for l in open(f"{D}/queries.jsonl")]
S = {}
for l in open(f"{D}/summary.jsonl"):
    s = json.loads(l); S[s["label"]] = s   # last wins
# latest run per label (labels may be rerun; keep last pid)
last_pid = {}
for r in Q: last_pid[r["label"]] = r["pid"] if r["event"] == "session" else last_pid.get(r["label"], r["pid"])
Q = [r for r in Q if r["pid"] == last_pid[r["label"]]]
def rss(label):
    p = f"{D}/rss/{label}.csv"
    return [(float(x["t"]), int(x["rss_bytes"])) for x in csv.DictReader(open(p))] if os.path.exists(p) else []
ref = collections.defaultdict(collections.Counter)
for r in Q:
    if r["event"] == "query" and not r["error"]: ref[r["q"]][r["hash_set"]] += 1
print("| config | Q | wall s | rows | result hash | match | GET | HEAD | LIST | MB sent | peak RSS GB (during query) | pause flag |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|")
for r in Q:
    if r["event"] != "query": continue
    samp = rss(r["label"]); win = [b for t, b in samp if r["start"] - 0.2 <= t <= r["end"] + 0.2]
    ts = [t for t, b in samp if r["start"] - 1 <= t <= r["end"] + 1]
    gap = max([b - a for a, b in zip(ts, ts[1:])], default=0)
    maj = ref[r["q"]].most_common(1)[0][0] if ref[r["q"]] else None
    s3 = r["s3"]
    print(f"| {r['label']} | Q{r['q']} | {r['wall_s']:.2f} | {r['rows']} | {r.get('hash_set','ERR')} | {'yes' if r.get('hash_set')==maj else ('ERROR' if r['error'] else 'NO')} | {int(s3.get('GetObject',0))} | {int(s3.get('HeadObject',0))} | {int(s3.get('ListObjectsV2',0))} | {s3.get('_sent_bytes',0)/1e6:.0f} | {max(win)/1e9 if win else 0:.2f} | {'PAUSE %.0fs'%gap if gap>2 else ''} |")
print()
print("| config | process elapsed s | peak tree RSS GB | CPU user+sys s |")
print("|---|---|---|---|")
for k, s in S.items():
    print(f"| {k} | {s['elapsed_s']} | {s['peak_tree_rss_bytes']/1e9:.2f} | {s['cpu_user_s']+s['cpu_sys_s']:.0f} |")
