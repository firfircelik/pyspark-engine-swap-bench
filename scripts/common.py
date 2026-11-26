import hashlib, json, re, time, urllib.request, datetime, decimal, math

TABLES = ["customer","lineitem","nation","orders","part","partsupp","region","supplier"]
METRICS_URL = "http://127.0.0.1:9000/minio/metrics/v3/api/requests"

def minio_counters():
    try:
        txt = urllib.request.urlopen(METRICS_URL, timeout=5).read().decode()
    except Exception as e:
        return {"error": str(e)}
    out = {}
    for line in txt.splitlines():
        m = re.match(r'minio_api_requests_total\{name="([^"]+)"[^}]*\} ([0-9.e+]+)', line)
        if m: out[m.group(1)] = float(m.group(2))
        m = re.match(r'minio_api_requests_traffic_sent_bytes\{[^}]*\} ([0-9.e+]+)', line)
        if m: out["_sent_bytes"] = float(m.group(1))
    return out

def counter_delta(a, b):
    if "error" in a or "error" in b: return {"error": a.get("error") or b.get("error")}
    keys = set(a) | set(b)
    d = {k: b.get(k, 0) - a.get(k, 0) for k in keys}
    return {k: v for k, v in d.items() if v}

def norm(v):
    if v is None: return "NULL"
    if isinstance(v, bool): return str(v)
    if isinstance(v, (int,)): return str(v)
    if isinstance(v, (float, decimal.Decimal)):
        f = float(v)
        if math.isnan(f): return "NaN"
        return f"{f:.2f}"
    if isinstance(v, (datetime.date, datetime.datetime)): return v.isoformat()[:10] if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime) else v.isoformat()
    return str(v).rstrip()

def result_hashes(rows):
    """rows: list of tuples. Returns ordered hash, unordered hash, normalized rows."""
    nrows = ["|".join(norm(x) for x in r) for r in rows]
    h_ord = hashlib.sha256("\n".join(nrows).encode()).hexdigest()[:16]
    h_set = hashlib.sha256("\n".join(sorted(nrows)).encode()).hexdigest()[:16]
    return h_ord, h_set, nrows

def load_query(qdir, q):
    sql = open(f"{qdir}/q{q}.sql").read()
    sql = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--")).strip().rstrip(";")
    return sql


def load_statements(qdir, q):
    """Split a query file into statements; the result is the one SELECT that is not DDL."""
    sql = open(f"{qdir}/q{q}.sql").read()
    sql = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
    return [x.strip() for x in sql.split(";") if x.strip()]

def is_ddl(stmt):
    return stmt.lower().lstrip().startswith(("create", "drop"))
