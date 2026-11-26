"""Phase 2 orchestrator. Checkpointed: every finished attempt is appended to results/phase2/state.jsonl; restarting skips done jobs.
Pause: touch /workspace/engine-swap/PAUSE (checked between jobs). Stop: touch /workspace/engine-swap/STOP."""
import json, os, random, subprocess, time, csv, shutil, sys
ROOT = "/workspace/engine-swap"
NAME = sys.argv[1] if len(sys.argv) > 1 else "phase2"   # other names need a pre-built results/<name>/plan.json
P2 = f"{ROOT}/results/{NAME}"; os.makedirs(P2, exist_ok=True)
STATE = f"{P2}/state.jsonl"; PLAN = f"{P2}/plan.json"; LOG = f"{P2}/progress.log"
REPS = 3; MAX_ATTEMPTS = 3; FREEZE_GAP = 2.0
STREAMS = ["spark413-classic", "spark413-analytics", "spark420-classic", "spark420-analytics", "comet420-classic", "comet420-analytics", "sail-noreorder", "sail-reorder", "duckdb"]
LOCAL = ["spark413-default", "spark420-default", "comet420-default", "sail-noreorder", "sail-reorder", "duckdb"]
def log(m):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {m}"; print(line, flush=True); open(LOG, "a").write(line + "\n")
if not os.path.exists(PLAN):
    plan = []
    for r in range(1, REPS + 1):
        jobs = [("local", c, "tpch") for c in LOCAL] + [(cond, c, "tpch") for cond in ("loopback", "proxy0", "proxy10") for c in STREAMS] \
             + [(cond, c, "etl") for cond in ("loopback", "proxy10") for c in STREAMS]
        random.Random(1000 + r).shuffle(jobs)
        plan += [{"id": f"r{r}-{cond}-{c}-{wl}", "rep": r, "cond": cond, "cfg": c, "wl": wl} for cond, c, wl in jobs]
    json.dump(plan, open(PLAN, "w"), indent=1)
plan = json.load(open(PLAN))
def load_state():
    st = {}
    if os.path.exists(STATE):
        for l in open(STATE):
            r = json.loads(l); st.setdefault(r["id"], []).append(r)
    return st
def toxi(*args): return subprocess.run([f"{ROOT}/opt/bin/toxiproxy-cli", *args], capture_output=True, text=True)
def ensure_toxiproxy():
    if subprocess.run(["pgrep", "-f", "toxiproxy-server -host 127.0.0.1 -port 8474"], capture_output=True).returncode != 0:
        subprocess.Popen(f"exec {ROOT}/opt/bin/toxiproxy-server -host 127.0.0.1 -port 8474 >> {ROOT}/logs/toxiproxy-phase2.log 2>&1", shell=True, start_new_session=True)
        time.sleep(1.5)
    toxi("create", "-l", "127.0.0.1:9100", "-u", "127.0.0.1:9000", "minio_lat")
def set_latency(ms):
    toxi("toxic", "remove", "-n", "lat", "minio_lat")
    if ms: toxi("toxic", "add", "-n", "lat", "-t", "latency", "-a", f"latency={ms}", "-d", "minio_lat")
    lst = toxi("inspect", "minio_lat").stdout
    return lst.strip().replace("\n", " ")[:300]
def freeze_check(outdir):
    try:
        ts = [float(x["t"]) for x in csv.DictReader(open(f"{outdir}/rss.csv"))]
    except Exception: return []
    gaps = [(a, b - a) for a, b in zip(ts, ts[1:]) if b - a > FREEZE_GAP]
    flagged = []
    for l in open(f"{outdir}/queries.jsonl") if os.path.exists(f"{outdir}/queries.jsonl") else []:
        r = json.loads(l)
        if r["event"] != "query": continue
        for g0, g in gaps:
            # flag only gaps that can move the timing: > FREEZE_GAP and (> 10 s or > 5% of the query's wall time)
            if g0 < r["end"] and g0 + g > r["start"] and (g > 10 or g > 0.05 * r["wall_s"]): flagged.append({"q": r["q"], "gap_s": round(g, 1)})
    return flagged
def disk_free_gb(): return shutil.disk_usage(ROOT).free / 1e9
def mem_avail_gb():
    for l in open("/proc/meminfo"):
        if l.startswith("MemAvailable"): return int(l.split()[1]) / 1e6
SUPF = f"{P2}/superseded.json"  # attempts kept on disk as evidence but re-run (e.g. config re-sized)
ensure_toxiproxy()
while True:
    st = load_state(); pending = []
    SUPERSEDED = set(json.load(open(SUPF))) if os.path.exists(SUPF) else set()
    for j in plan:
        att = [a for a in st.get(j["id"], []) if a.get("outdir") not in SUPERSEDED]
        # a job whose process exited ok but had query errors ("qerror") is re-run like a frozen one (max attempts);
        # every attempt stays in state.jsonl and the errors are reported (results/final/phase2_query_errors.csv)
        if any(a["status"] in ("ok", "failed", "timeout") and not (a["status"] == "ok" and a.get("query_errors")) for a in att): continue
        if len(att) >= MAX_ATTEMPTS: continue
        pending.append(j)
    if not pending: log("ALL JOBS DONE"); break
    # keep rep order: first pending of the lowest rep, frozen reruns naturally fall behind fresh jobs of the same rep.
    # Disk-aware: Spark/Comet TPC-H at SF30 needs ~16 GB transient shuffle space in tmp/spark, so such a job is skipped
    # (not reordered permanently) while free disk is below its need; Comet jobs also wait while COMET_HOLD exists.
    while os.path.exists(f"{ROOT}/PAUSE"): time.sleep(10)
    if os.path.exists(f"{ROOT}/STOP"): log("STOP file found, exiting"); break
    def need_gb(j):
        if j["wl"] == "tpch" and j["cfg"].startswith(("spark", "comet")): return 22
        return 10
    def runnable(j):
        if j["cfg"].startswith("comet") and os.path.exists(f"{ROOT}/COMET_HOLD"): return False
        return disk_free_gb() >= need_gb(j)
    cand = [x for x in pending if runnable(x)]
    if not cand:
        log(f"no runnable job (disk {disk_free_gb():.1f} GB, COMET_HOLD={os.path.exists(ROOT + '/COMET_HOLD')}), waiting"); time.sleep(300); continue
    j = cand[0]
    if mem_avail_gb() < 10.5:   # VM balloon may hold memory after a box restore / idle period
        r = subprocess.run([sys.executable, f"{ROOT}/scripts/mem_pump.py", "11"], capture_output=True, text=True)
        log(f"mem pump: avail now {r.stdout.strip()} GB")
    waited = 0
    while mem_avail_gb() < 9.5 and waited < 600: time.sleep(15); waited += 15
    ensure_toxiproxy()
    tox = set_latency(10 if j["cond"] == "proxy10" else 0)
    attempt = len(st.get(j["id"], [])) + 1
    outdir = f"{P2}/{j['cond']}/{j['wl']}/{j.get('variant', j['cfg'])}/rep{j['rep']}_a{attempt}"
    if os.path.exists(outdir): outdir += f"_{int(time.time())}"
    log(f"START {j['id']} attempt {attempt} memavail={mem_avail_gb():.1f}GB disk={disk_free_gb():.1f}GB")
    t0 = time.time()
    env = dict(os.environ, **j.get("env", {}))
    rc = subprocess.run(["bash", f"{ROOT}/scripts/run_engine2.sh", j["cfg"], j["cond"], j["wl"], outdir], capture_output=True, text=True, env=env).returncode
    summ = {}
    try: summ = json.loads(open(f"{outdir}/summary.jsonl").read().strip().splitlines()[-1])
    except Exception: pass
    fl = freeze_check(outdir)
    errs = []
    if os.path.exists(f"{outdir}/queries.jsonl"):
        for l in open(f"{outdir}/queries.jsonl"):
            r = json.loads(l)
            if r["event"] == "query" and r.get("error"): errs.append(r["q"])
    status = "frozen" if fl else ("timeout" if summ.get("timed_out") else (("qerror" if errs else "ok") if rc == 0 else "failed"))
    rec = {"id": j["id"], **j, "attempt": attempt, "status": status, "rc": rc, "outdir": outdir, "elapsed_s": round(time.time() - t0, 1),
           "frozen_queries": fl, "query_errors": errs, "balloon": {k: v for k, v in (l.split() for l in open("/proc/vmstat")) if k.startswith("balloon_")}, "toxiproxy": tox, "mem_avail_start_gb": round(mem_avail_gb(), 1)}
    open(STATE, "a").write(json.dumps(rec) + "\n")
    log(f"END {j['id']} status={status} rc={rc} {rec['elapsed_s']}s errors={errs} frozen={fl}")
set_latency(0)
