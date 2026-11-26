#!/bin/bash
cd /workspace/engine-swap
python3 - <<'PY'
import json,collections,datetime,os,shutil
for name in ["phase2","aal-iso"]:
    P=f"results/{name}"
    if not os.path.exists(f"{P}/state.jsonl"): print(name,"not started"); continue
    plan=json.load(open(f"{P}/plan.json")); sup=set(json.load(open(f"{P}/superseded.json"))) if os.path.exists(f"{P}/superseded.json") else set()
    done={}; allst=collections.Counter()
    for l in open(f"{P}/state.jsonl"):
        d=json.loads(l); allst[d["status"]]+=1
        if d["outdir"] in sup: continue
        if d["status"] == "ok" and d.get("query_errors"): d["status"] = "qerror"
        if d["status"] in ("ok","failed","timeout"): done[d["id"]]=d
    c=collections.Counter(d["status"] for d in done.values())
    print(name, f"done {len(done)}/{len(plan)}", dict(c), "attempts", dict(allst))
    for d in done.values():
        if d["status"]!="ok": print("   NOT OK:", d["id"], d["status"], d.get("query_errors"))
print("disk free GB", round(shutil.disk_usage("/workspace").free/1e9,1))
PY
tail -3 results/phase2/progress.log; [ -f results/aal-iso/progress.log ] && tail -2 results/aal-iso/progress.log; cat logs/chain.out 2>/dev/null | tail -2
pgrep -f "scripts/orchestrate.py" >/dev/null && echo "orchestrator alive" || echo "orchestrator NOT running"
ls tmp/spark
