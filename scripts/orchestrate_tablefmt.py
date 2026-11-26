"""Checkpointed SF30 timing matrix for Iceberg/Delta. Skips cells the SF1 probe marked unsupported."""
import json, os, random, subprocess, time, csv, shutil, sys
sys.path.insert(0, os.path.dirname(__file__))
from tablefmt_env import job_env

ROOT = "/workspace/engine-swap"
P2 = f"{ROOT}/results/tablefmt"
os.makedirs(P2, exist_ok=True)
STATE = f"{P2}/state.jsonl"
PLAN = f"{P2}/plan.json"
LOG = f"{P2}/progress.log"
PROBE = f"{ROOT}/results/tablefmt/probe/probe.json"
# Parquet SF30 majority hash_set from results/final/phase2_hash_check.csv
BASELINE_HASH = {
    1: "dc9fb64947ffda53",
    3: "03a4d982c46be77e",
    6: "86f1051af17c11ba",
    12: "7245c10c0d25d7c9",
    18: "247ac87094aee3e6",
}
REPS = 2
MAX_ATTEMPTS = 2
FREEZE_GAP = 2.0

def log(m):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {m}"
    print(line, flush=True)
    open(LOG, "a").write(line + "\n")

# Timing matrix: every engine that can natively (or, for Comet+Delta, via Spark fallback) read the format,
# using the Spark line that actually has a published runtime. Probe-only cells stay in the support matrix:
# spark420/comet420 + Iceberg fail (no published Iceberg 4.2 runtime); comet413+Delta works but is redundant
# with comet420+Delta, which is the study default Comet.
PRIMARY = {
    ("duckdb", "iceberg"), ("sail-noreorder", "iceberg"), ("spark413-classic", "iceberg"), ("comet413-classic", "iceberg"),
    ("duckdb", "delta"), ("sail-noreorder", "delta"), ("spark413-classic", "delta"), ("spark420-classic", "delta"),
    ("comet420-classic", "delta"),
}

def supported():
    """Return PRIMARY cells that also passed the SF1 probe."""
    if not os.path.exists(PROBE):
        raise SystemExit(f"missing {PROBE}; run probe_tablefmt.py first")
    ok = set()
    for r in json.load(open(PROBE)):
        if r.get("ok"):
            ok.add((r["cfg"], r["format"]))
    missing = PRIMARY - ok
    if missing:
        raise SystemExit(f"primary cells failed the SF1 probe: {sorted(missing)}")
    return PRIMARY

if not os.path.exists(PLAN):
    ok = supported()
    jobs = []
    cfgs_fmts = sorted(ok)
    for r in range(1, REPS + 1):
        pair = list(cfgs_fmts)
        random.Random(20261126 + r).shuffle(pair)
        for cfg, fmt in pair:
            jobs.append({
                "id": f"r{r}-loopback-{cfg}-{fmt}",
                "rep": r, "cond": "loopback", "cfg": cfg, "wl": "tpch",
                "format": fmt, "variant": f"{cfg}-{fmt}",
                "env": job_env(cfg, fmt, sf_base="sf30"),
            })
    json.dump(jobs, open(PLAN, "w"), indent=1)
    log(f"plan written: {len(jobs)} jobs from {len(ok)} supported cells")

plan = json.load(open(PLAN))

def load_state():
    st = {}
    if os.path.exists(STATE):
        for l in open(STATE):
            r = json.loads(l)
            st.setdefault(r["id"], []).append(r)
    return st

def freeze_check(outdir):
    try:
        ts = [float(x["t"]) for x in csv.DictReader(open(f"{outdir}/rss.csv"))]
    except Exception:
        return []
    gaps = [(a, b - a) for a, b in zip(ts, ts[1:]) if b - a > FREEZE_GAP]
    flagged = []
    path = f"{outdir}/queries.jsonl"
    if not os.path.exists(path):
        return []
    for l in open(path):
        r = json.loads(l)
        if r["event"] != "query":
            continue
        for g0, g in gaps:
            if g0 < r["end"] and g0 + g > r["start"] and (g > 10 or g > 0.05 * r["wall_s"]):
                flagged.append({"q": r["q"], "gap_s": round(g, 1)})
    return flagged

def disk_free_gb():
    return shutil.disk_usage(ROOT).free / 1e9

def mem_avail_gb():
    for l in open("/proc/meminfo"):
        if l.startswith("MemAvailable"):
            return int(l.split()[1]) / 1e6

while True:
    st = load_state()
    pending = []
    for j in plan:
        att = st.get(j["id"], [])
        if any(a["status"] in ("ok", "failed", "timeout") and not (a["status"] == "ok" and a.get("query_errors")) for a in att):
            continue
        if len(att) >= MAX_ATTEMPTS:
            continue
        pending.append(j)
    if not pending:
        log("ALL JOBS DONE")
        break
    while os.path.exists(f"{ROOT}/PAUSE"):
        time.sleep(10)
    if os.path.exists(f"{ROOT}/STOP"):
        log("STOP file found, exiting")
        break
    if disk_free_gb() < 16:
        log(f"disk {disk_free_gb():.1f} GB below 16 GB floor, waiting")
        time.sleep(300)
        continue
    if mem_avail_gb() < 10.5:
        r = subprocess.run([sys.executable, f"{ROOT}/scripts/mem_pump.py", "11"], capture_output=True, text=True)
        log(f"mem pump: avail now {r.stdout.strip()} GB")
    waited = 0
    while mem_avail_gb() < 9.5 and waited < 600:
        time.sleep(15)
        waited += 15
    j = pending[0]
    attempt = len(st.get(j["id"], [])) + 1
    outdir = f"{P2}/loopback/tpch/{j['variant']}/rep{j['rep']}_a{attempt}"
    if os.path.exists(outdir):
        outdir += f"_{int(time.time())}"
    log(f"START {j['id']} attempt {attempt} memavail={mem_avail_gb():.1f}GB disk={disk_free_gb():.1f}GB")
    t0 = time.time()
    env = dict(os.environ, **j.get("env", {}))
    rc = subprocess.run(
        ["bash", f"{ROOT}/scripts/run_engine2.sh", j["cfg"], j["cond"], j["wl"], outdir],
        capture_output=True, text=True, env=env,
    ).returncode
    summ = {}
    try:
        summ = json.loads(open(f"{outdir}/summary.jsonl").read().strip().splitlines()[-1])
    except Exception:
        pass
    fl = freeze_check(outdir)
    errs = []
    hash_mismatch = []
    if os.path.exists(f"{outdir}/queries.jsonl"):
        for l in open(f"{outdir}/queries.jsonl"):
            r = json.loads(l)
            if r["event"] == "query" and r.get("error"):
                errs.append(r["q"])
            if r["event"] == "query" and not r.get("error") and r.get("q") in BASELINE_HASH:
                if r.get("hash_set") != BASELINE_HASH[r["q"]]:
                    hash_mismatch.append({"q": r["q"], "got": r.get("hash_set"), "want": BASELINE_HASH[r["q"]]})
    status = "frozen" if fl else ("timeout" if summ.get("timed_out") else (("qerror" if errs else "ok") if rc == 0 else "failed"))
    rec = {
        "id": j["id"], **{k: j[k] for k in ("rep", "cond", "cfg", "wl", "format", "variant") if k in j},
        "attempt": attempt, "status": status, "rc": rc, "outdir": outdir,
        "elapsed_s": round(time.time() - t0, 1),
        "frozen_queries": fl, "query_errors": errs, "hash_mismatch": hash_mismatch,
        "mem_avail_start_gb": round(mem_avail_gb(), 1),
    }
    open(STATE, "a").write(json.dumps(rec) + "\n")
    log(f"END {j['id']} status={status} rc={rc} {rec['elapsed_s']}s errors={errs} frozen={fl} hash_mismatch={hash_mismatch}")
