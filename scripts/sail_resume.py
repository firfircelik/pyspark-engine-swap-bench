"""After a run whose Spark Connect server died: record the in-flight query as an error (SERVER_DIED) and print the
remaining TPC-H queries (comma list) so run_engine2.sh can restart the server and continue. Prints nothing when done.
Usage: sail_resume.py <queries.jsonl> <summary.jsonl> <full comma list>"""
import sys, json
qf, sf, full = sys.argv[1], sys.argv[2], [int(x) for x in sys.argv[3].split(",")]
recs = [json.loads(l) for l in open(qf)]
summ = json.loads(open(sf).read().strip().splitlines()[-1])
done = {r["q"] for r in recs if r["event"] == "query"}
starts = [r for r in recs if r["event"] == "query_start" and r["q"] not in done]
if summ.get("server_died") and starts:
    s = starts[-1]; t = summ["server_died"]["t"]
    rec = {"event": "query", "q": s["q"], "start": s["start"], "end": t, "wall_s": round(t - s["start"], 3), "rows": 0, "s3": {},
           "error": f"SERVER_DIED: Spark Connect server exited rc={summ['server_died']['server_rc']} during the query (see summary cg_events: oom_kill)",
           "label": s.get("label"), "pid": s.get("pid"), "base": s.get("base"), "workload": s.get("workload")}
    open(qf, "a").write(json.dumps(rec) + "\n"); done.add(s["q"])
print(",".join(str(q) for q in full if q not in done) if summ.get("server_died") else "")
