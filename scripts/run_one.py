"""Launch optional server + client as children, sample process-tree RSS every 0.2s.
run_one.py --label L --rss-out f.csv [--server "cmd"] [--server-wait-port 50051] -- client cmd...
"""
import argparse, os, subprocess, sys, time, threading, socket, resource, json, shlex
import psutil
ap = argparse.ArgumentParser()
ap.add_argument("--label", required=True); ap.add_argument("--rss-out", required=True)
ap.add_argument("--server"); ap.add_argument("--server-wait-port", type=int)
ap.add_argument("--summary-out", required=True)
ap.add_argument("--cgroup"); ap.add_argument("--timeout", type=int, default=0)
ap.add_argument("cmd", nargs=argparse.REMAINDER)
a = ap.parse_args()
cmd = a.cmd[1:] if a.cmd and a.cmd[0] == "--" else a.cmd
me = psutil.Process()
def cg_read(name):
    try: return open(f"{a.cgroup}/{name}").read()
    except Exception: return ""
if a.cgroup:
    subprocess.run(["sudo", "sh", "-c", f"echo 0 > {a.cgroup}/memory.peak 2>/dev/null; echo {os.getpid()} > {a.cgroup}/cgroup.procs"], check=True)
    oom0 = cg_read("memory.events")
stop = False; peak = [0]
def sampler():
    with open(a.rss_out, "w") as f:
        f.write("t,rss_bytes\n")
        while not stop:
            tot = 0
            for p in me.children(recursive=True):
                try: tot += p.memory_info().rss
                except psutil.Error: pass
            peak[0] = max(peak[0], tot)
            f.write(f"{time.time():.3f},{tot}\n"); f.flush()
            time.sleep(0.2)
th = threading.Thread(target=sampler, daemon=True); th.start()
t0 = time.time(); srv = None
if a.server:
    try:
        socket.create_connection(("127.0.0.1", a.server_wait_port), 0.5).close()
        sys.exit(f"port {a.server_wait_port} already in use before server start; refusing to run")
    except OSError: pass
    srv = subprocess.Popen("exec " + a.server, shell=True, stdout=open(f"{a.rss_out}.server.log","w"), stderr=subprocess.STDOUT, start_new_session=True)
    for _ in range(300):
        try: socket.create_connection(("127.0.0.1", a.server_wait_port), 0.5).close(); break
        except OSError: time.sleep(0.2)
timed_out = False; server_died = None; stalled = False
STALL_PROGRESS_S = 1200  # 20 min with no queries.jsonl progress: treat as stall (F4 evidence), do not wait for the job timeout
qpath = os.path.join(os.path.dirname(os.path.abspath(a.rss_out)), "queries.jsonl")

def last_progress(t0):
    last = t0
    try:
        last = max(last, os.path.getmtime(qpath))
        for line in open(qpath):
            if not line.strip():
                continue
            r = json.loads(line)
            for k in ("end", "start"):
                if k in r:
                    try: last = max(last, float(r[k]))
                    except (TypeError, ValueError): pass
    except Exception:
        pass
    return last

def mark_stalled(t0):
    inflight = None
    try:
        for line in open(qpath):
            if not line.strip():
                continue
            r = json.loads(line)
            if r.get("event") == "query_start":
                inflight = r
            elif r.get("event") == "query" and inflight and r.get("q") == inflight.get("q"):
                inflight = None
    except Exception:
        pass
    te = time.time()
    ts = (inflight or {}).get("start", t0)
    rec = {"event": "query", "q": (inflight or {}).get("q", "unknown"), "start": ts, "end": te,
           "wall_s": round(te - ts, 3), "error": "STALLED_NO_PROGRESS", "stalled": True, "label": a.label}
    os.makedirs(os.path.dirname(qpath) or ".", exist_ok=True)
    with open(qpath, "a") as f:
        f.write(json.dumps(rec) + "\n")

def kill_tree(rc_if_wait_hangs=-9):
    for p in me.children(recursive=True):
        try: p.kill()
        except psutil.Error: pass
    try:
        cli.wait(30)
    except Exception:
        try: cli.kill()
        except Exception: pass
        try: cli.wait(10)
        except Exception: pass
    return cli.poll() if cli.poll() is not None else rc_if_wait_hangs

cli = subprocess.Popen(cmd); tstart = time.time()
while True:
    rc = cli.poll()
    if rc is not None: break
    if time.time() - last_progress(tstart) > STALL_PROGRESS_S:
        stalled = True
        try: mark_stalled(tstart)
        except Exception: pass
        # rc=0 + query error => orchestrator records qerror and retries (like a freeze)
        kill_tree()
        rc = 0
        break
    if a.timeout and time.time() - tstart > a.timeout:
        timed_out = True
        rc = kill_tree(-9)
        if rc is None or rc == 0:
            rc = -9
        break
    if srv and srv.poll() is not None:
        # server process exited under a running client (e.g. OOM-killed): stop the client now instead of letting it retry
        server_died = {"server_rc": srv.returncode, "t": time.time()}
        for p in psutil.Process(cli.pid).children(recursive=True) + [psutil.Process(cli.pid)]:
            try: p.terminate()
            except psutil.Error: pass
        try: cli.wait(20)
        except subprocess.TimeoutExpired: cli.kill(); cli.wait()
        rc = 4; break
    time.sleep(0.5)
if srv:
    import signal
    try: os.killpg(srv.pid, signal.SIGTERM)
    except ProcessLookupError: pass
    try: srv.wait(30)
    except subprocess.TimeoutExpired: os.killpg(srv.pid, signal.SIGKILL); srv.wait()
stop = True; th.join()
ru = resource.getrusage(resource.RUSAGE_CHILDREN)
s = {"label": a.label, "rc": rc, "elapsed_s": round(time.time()-t0,3), "peak_tree_rss_bytes": peak[0],
     "timed_out": timed_out, "stalled": stalled, "server_died": server_died,
     "cg_memory_peak": int(cg_read("memory.peak") or 0) if a.cgroup else None,
     "cg_memory_max": cg_read("memory.max").strip() if a.cgroup else None,
     "cg_events_before": oom0 if a.cgroup else None, "cg_events_after": cg_read("memory.events") if a.cgroup else None,
     "cpu_user_s": round(ru.ru_utime,1), "cpu_sys_s": round(ru.ru_stime,1), "ru_maxrss_kb_largest_child": ru.ru_maxrss}
open(a.summary_out, "a").write(json.dumps(s)+"\n"); print(json.dumps(s))
sys.exit(rc)
