"""One extra comet420-delta SF30 timing attempt after the extraClassPath colon fix."""
import json, os, subprocess, time, sys
sys.path.insert(0, "/workspace/engine-swap/scripts")
from tablefmt_env import job_env
from orchestrate_tablefmt import freeze_check, BASELINE_HASH, P2, STATE

cfg, fmt = "comet420-classic", "delta"
env = dict(os.environ)
env.update(job_env(cfg, fmt, sf_base="sf30"))
stamp = int(time.time())
outdir = f"{P2}/loopback/tpch/{cfg}-{fmt}/rep1_a4_classpathfix"
if os.path.exists(outdir):
    outdir += f"_{stamp}"
print("START", outdir, flush=True)
t0 = time.time()
rc = subprocess.run(
    ["bash", "/workspace/engine-swap/scripts/run_engine2.sh", cfg, "loopback", "tpch", outdir],
    env=env,
).returncode
summ = {}
try:
    summ = json.loads(open(f"{outdir}/summary.jsonl").read().strip().splitlines()[-1])
except Exception:
    pass
fl = freeze_check(outdir)
errs, hash_mismatch = [], []
if os.path.exists(f"{outdir}/queries.jsonl"):
    for l in open(f"{outdir}/queries.jsonl"):
        r = json.loads(l)
        if r.get("event") == "query" and r.get("error"):
            errs.append(r["q"])
        if r.get("event") == "query" and not r.get("error") and r.get("q") in BASELINE_HASH:
            if r.get("hash_set") != BASELINE_HASH[r["q"]]:
                hash_mismatch.append({"q": r["q"], "got": r.get("hash_set"), "want": BASELINE_HASH[r["q"]]})
status = "frozen" if fl else ("timeout" if summ.get("timed_out") else (("qerror" if errs else "ok") if rc == 0 else "failed"))
# If wrapper rc != 0 but all 5 queries hash-matched and summary rc is 0, still record the wrapper rc and the query evidence.
rec = {
    "id": "r1-loopback-comet420-classic-delta", "rep": 1, "cond": "loopback",
    "cfg": cfg, "wl": "tpch", "format": fmt, "variant": f"{cfg}-{fmt}",
    "attempt": 4, "status": status, "rc": rc, "outdir": outdir,
    "elapsed_s": round(time.time() - t0, 1), "frozen_queries": fl,
    "query_errors": errs, "hash_mismatch": hash_mismatch,
    "classpath_fix": True, "manual_extra_attempt": True,
    "summary_rc": summ.get("rc"),
}
open(STATE, "a").write(json.dumps(rec) + "\n")
print(json.dumps(rec), flush=True)
if os.path.exists(f"{outdir}/queries.jsonl"):
    for l in open(f"{outdir}/queries.jsonl"):
        r = json.loads(l)
        if r.get("event") == "query":
            print("Q", r.get("q"), "wall", r.get("wall_s"), "hash", r.get("hash_set"),
                  "err", (r.get("error") or "")[:200], flush=True)
if status != "ok" or errs or hash_mismatch:
    sys.exit(2)
