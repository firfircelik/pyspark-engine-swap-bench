"""Install DuckDB 2.0.1 iceberg and delta extensions into the local ext dir (https repo)."""
import duckdb, os, json, time
EXT = "/workspace/engine-swap/opt/duckdb-ext"
os.makedirs(EXT, exist_ok=True)
out = "/workspace/engine-swap/results/tablefmt/duckdb-ext-install.json"
rec = {"started": time.strftime("%Y-%m-%d %H:%M:%S"), "version": duckdb.__version__, "ext_dir": EXT}
con = duckdb.connect(config={"extension_directory": EXT, "autoinstall_known_extensions": False})
# Prefer the official HTTPS repository (the HTTP one returned 500 during Phase 1).
try:
    con.execute("SET custom_extension_repository='https://extensions.duckdb.org'")
except Exception as e:
    rec["set_repo_error"] = f"{type(e).__name__}: {e}"
for name in ("avro", "iceberg", "delta"):
    t0 = time.time()
    try:
        con.execute(f"INSTALL {name}")
        con.execute(f"LOAD {name}")
        rec[name] = {"ok": True, "s": round(time.time() - t0, 3)}
    except Exception as e:
        rec[name] = {"ok": False, "s": round(time.time() - t0, 3), "error": f"{type(e).__name__}: {e}"}
# list files
files = []
for root, _, fnames in os.walk(EXT):
    for f in fnames:
        p = os.path.join(root, f)
        files.append({"path": p, "bytes": os.path.getsize(p)})
rec["files"] = files
json.dump(rec, open(out, "w"), indent=2)
print(json.dumps(rec, indent=2))
if not rec.get("iceberg", {}).get("ok") or not rec.get("delta", {}).get("ok"):
    raise SystemExit(2)
