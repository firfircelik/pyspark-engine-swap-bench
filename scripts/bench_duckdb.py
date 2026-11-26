import argparse, json, os, sys, time
sys.path.insert(0, os.path.dirname(__file__))
from common import *
import duckdb
ap = argparse.ArgumentParser()
ap.add_argument("--label", required=True); ap.add_argument("--base", required=True)
ap.add_argument("--queries", default="1,3,6,9,18"); ap.add_argument("--out", required=True)
ap.add_argument("--qdir", default="/workspace/engine-swap/queries"); ap.add_argument("--rowsdir", required=True)
ap.add_argument("--memory-limit", default=None)
a = ap.parse_args()
os.makedirs(a.rowsdir, exist_ok=True)
out = open(a.out, "a")
def emit(rec):
    rec.update(label=a.label, pid=os.getpid(), base=a.base); out.write(json.dumps(rec)+"\n"); out.flush(); print(json.dumps(rec), flush=True)
con = duckdb.connect(config={'extension_directory': '/workspace/engine-swap/opt/duckdb-ext', 'autoinstall_known_extensions': False})
con.execute("SET threads=8")
if a.memory_limit: con.execute(f"SET memory_limit='{a.memory_limit}'")
base = a.base.replace("s3a://", "s3://")
if base.startswith("s3://"):
    con.execute("LOAD httpfs")
    port = os.environ.get("S3_PORT", "9000")
    con.execute(f"CREATE SECRET minio (TYPE s3, KEY_ID '{os.environ['BENCH_S3_KEY']}', SECRET '{os.environ['BENCH_S3_SECRET']}', ENDPOINT '127.0.0.1:{port}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')")
emit({"event": "session", "version": duckdb.__version__})
c0 = minio_counters(); t0 = time.time()
for t in TABLES:
    con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{base}/{t}/*.parquet')")
emit({"event": "register", "s": round(time.time()-t0,3), "s3": counter_delta(c0, minio_counters())})
for q in [int(x) for x in a.queries.split(",")]:
    sql = load_query(a.qdir, q); c0 = minio_counters(); ts = time.time(); err=None; rows=[]
    try: rows = con.execute(sql).fetchall()
    except Exception as e: err = f"{type(e).__name__}: {str(e)[:2000]}"
    te = time.time()
    rec = {"event":"query","q":q,"start":ts,"end":te,"wall_s":round(te-ts,3),"rows":len(rows),"s3":counter_delta(c0, minio_counters()),"error":err}
    if not err:
        ho, hs, nrows = result_hashes(rows); rec.update(hash_ordered=ho, hash_set=hs)
        open(f"{a.rowsdir}/{a.label}_q{q}.txt","w").write("\n".join(nrows)+"\n")
    emit(rec)
