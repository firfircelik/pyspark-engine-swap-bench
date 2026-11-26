"""SF1 read probe for every engine x format. Records exact errors. One job at a time."""
import json, os, subprocess, sys, time, shutil
sys.path.insert(0, os.path.dirname(__file__))
from tablefmt_env import job_env

ROOT = "/workspace/engine-swap"
OUT = f"{ROOT}/results/tablefmt/probe"
os.makedirs(OUT, exist_ok=True)
LOG = f"{OUT}/probe.log"
# spark420+iceberg uses the unpublished-4.2 gap: we still try the 4.1 runtime once.
# comet420+iceberg: same. comet413+iceberg is the fair native-scan path if 4.2 fails.
CFGS = [
    "duckdb",
    "sail-noreorder",
    "spark413-classic",
    "spark420-classic",
    "comet420-classic",
    "comet413-classic",
]
FMTS = ["iceberg", "delta"]

def log(m):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {m}"
    print(line, flush=True)
    open(LOG, "a").write(line + "\n")

def main():
    results = []
    for fmt in FMTS:
        for cfg in CFGS:
            env = dict(os.environ)
            env.update(job_env(cfg, fmt, sf_base="sf1"))
            env["QARG"] = "--queries 6"  # single scan+agg probe
            env["WARMUP_BASE"] = env["TABLEBASE"]  # warmup and timed path are both SF1
            env["TIMEOUT"] = "900"
            outdir = f"{OUT}/{fmt}/{cfg}"
            if os.path.exists(outdir):
                # keep prior attempt as evidence, write a new stamp dir
                outdir = f"{outdir}_{int(time.time())}"
            os.makedirs(outdir, exist_ok=True)
            log(f"START probe {cfg} {fmt} disk={shutil.disk_usage(ROOT).free/1e9:.1f}GB")
            t0 = time.time()
            rc = subprocess.run(
                ["bash", f"{ROOT}/scripts/run_engine2.sh", cfg, "loopback", "tpch", outdir],
                env=env, capture_output=True, text=True,
            ).returncode
            qerr = None; hash_set = None; rows = None; wall = None
            qpath = f"{outdir}/queries.jsonl"
            if os.path.exists(qpath):
                for line in open(qpath):
                    r = json.loads(line)
                    if r.get("event") == "query" and r.get("q") == 6:
                        qerr = r.get("error")
                        hash_set = r.get("hash_set")
                        rows = r.get("rows")
                        wall = r.get("wall_s")
            rec = {
                "cfg": cfg, "format": fmt, "rc": rc, "elapsed_s": round(time.time() - t0, 1),
                "outdir": outdir, "query_error": qerr, "hash_set": hash_set, "rows": rows, "wall_s": wall,
                "ok": rc == 0 and not qerr and hash_set is not None,
            }
            # Parquet SF1 Q6 hash is different from SF30; we only check the probe ran, not SF30 hashes.
            results.append(rec)
            open(f"{OUT}/probe.jsonl", "a").write(json.dumps(rec) + "\n")
            log(f"END probe {cfg} {fmt} ok={rec['ok']} rc={rc} err={qerr} hash={hash_set} {rec['elapsed_s']}s")
    json.dump(results, open(f"{OUT}/probe.json", "w"), indent=2)
    log("ALL PROBES DONE")

if __name__ == "__main__":
    main()
