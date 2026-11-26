"""One extra attempt (a4) for r2-loopback-spark420-analytics-tpch.
Do not start orchestrate.py phase2: dropped rep-3 jobs would be launched."""
import csv, json, os, subprocess, sys, time
ROOT = "/workspace/engine-swap"
P2 = f"{ROOT}/results/phase2"
LOG = f"{P2}/progress.log"
FREEZE_GAP = 2.0
JOB = {
    "id": "r2-loopback-spark420-analytics-tpch",
    "rep": 2, "cond": "loopback", "cfg": "spark420-analytics", "wl": "tpch",
}
OUTDIR = f"{P2}/loopback/tpch/spark420-analytics/rep2_a4"
if os.path.exists(OUTDIR):
    OUTDIR += f"_{int(time.time())}"

def log(m):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {m}"
    print(line, flush=True)
    open(LOG, "a").write(line + "\n")

def toxi(*args):
    return subprocess.run([f"{ROOT}/opt/bin/toxiproxy-cli", *args], capture_output=True, text=True)

def ensure_proxy_zero():
    if subprocess.run(["pgrep", "-f", "toxiproxy-server -host 127.0.0.1 -port 8474"], capture_output=True).returncode != 0:
        subprocess.Popen(f"exec {ROOT}/opt/bin/toxiproxy-server -host 127.0.0.1 -port 8474 >> {ROOT}/logs/toxiproxy-phase2.log 2>&1", shell=True, start_new_session=True)
        time.sleep(1.5)
    toxi("create", "-l", "127.0.0.1:9100", "-u", "127.0.0.1:9000", "minio_lat")
    toxi("toxic", "remove", "-n", "lat", "minio_lat")
    return toxi("inspect", "minio_lat").stdout.strip().replace("\n", " ")[:300]

def freeze_check(outdir):
    try:
        ts = [float(x["t"]) for x in csv.DictReader(open(f"{outdir}/rss.csv"))]
    except Exception:
        return []
    gaps = [(a, b - a) for a, b in zip(ts, ts[1:]) if b - a > FREEZE_GAP]
    flagged = []
    path = f"{outdir}/queries.jsonl"
    for l in open(path) if os.path.exists(path) else []:
        r = json.loads(l)
        if r["event"] != "query":
            continue
        for g0, g in gaps:
            if g0 < r["end"] and g0 + g > r["start"] and (g > 10 or g > 0.05 * r["wall_s"]):
                flagged.append({"q": r["q"], "gap_s": round(g, 1)})
    return flagged

def mem_avail_gb():
    for l in open("/proc/meminfo"):
        if l.startswith("MemAvailable"):
            return int(l.split()[1]) / 1e6

def main():
    tox = ensure_proxy_zero()
    log(f"START {JOB['id']} attempt 4 (manual extra after aal-iso) memavail={mem_avail_gb():.1f}GB outdir={OUTDIR}")
    t0 = time.time()
    env = dict(os.environ)
    env["TIMEOUT"] = env.get("TIMEOUT") or "1800"
    # no EXTRA_CONF / QARG: all 22 TPC-H queries, default AAL analytics
    rc = subprocess.run(["bash", f"{ROOT}/scripts/run_engine2.sh", JOB["cfg"], JOB["cond"], JOB["wl"], OUTDIR],
                        capture_output=True, text=True, env=env).returncode
    summ = {}
    try:
        summ = json.loads(open(f"{OUTDIR}/summary.jsonl").read().strip().splitlines()[-1])
    except Exception:
        pass
    fl = freeze_check(OUTDIR)
    errs = []
    if os.path.exists(f"{OUTDIR}/queries.jsonl"):
        for l in open(f"{OUTDIR}/queries.jsonl"):
            r = json.loads(l)
            if r["event"] == "query" and r.get("error"):
                errs.append(r["q"])
    stalled = bool(summ.get("stalled")) or any(
        (json.loads(l).get("error") == "STALLED_NO_PROGRESS")
        for l in open(f"{OUTDIR}/queries.jsonl")
        if l.strip() and os.path.exists(f"{OUTDIR}/queries.jsonl")
    ) if os.path.exists(f"{OUTDIR}/queries.jsonl") else False
    status = "frozen" if fl else ("timeout" if summ.get("timed_out") else (("qerror" if errs else "ok") if rc == 0 else "failed"))
    rec = {
        "id": JOB["id"], **JOB, "attempt": 4, "status": status, "rc": rc, "outdir": OUTDIR,
        "elapsed_s": round(time.time() - t0, 1),
        "frozen_queries": fl, "query_errors": errs,
        "balloon": {k: v for k, v in (l.split() for l in open("/proc/vmstat")) if k.startswith("balloon_")},
        "toxiproxy": tox, "mem_avail_start_gb": round(mem_avail_gb(), 1),
        "manual_extra_attempt": True, "stalled": stalled,
    }
    open(f"{P2}/state.jsonl", "a").write(json.dumps(rec) + "\n")
    log(f"END {JOB['id']} status={status} rc={rc} {rec['elapsed_s']}s errors={errs} frozen={fl} stalled={stalled}")
    sys.exit(0 if status == "ok" else 1)

if __name__ == "__main__":
    main()
