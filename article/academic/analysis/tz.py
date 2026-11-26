"""Comet 1.2.0 vs Spark 4.2.0 time-zone probe summary (results/probes/comet_tz_v2.jsonl)."""
import json, collections
from common import R, OUT
rows = [json.loads(l) for l in open(f"{R}/probes/comet_tz_v2.jsonl")]
by = collections.defaultdict(dict)
for r in rows: by[(r["zone"], r["name"])][r["label"]] = r
F = ["hr", "dt", "fmt", "trunc_day_epoch", "from_utc_epoch", "to_utc_epoch", "unix_from_str", "str_to_ts_epoch", "ltz_to_ntz"]
zones = collections.Counter(); allz = set(); diff = 0
for (z, n), v in by.items():
    allz.add(z)
    if any(v["spark420"].get(f) != v["comet120"].get(f) for f in F): diff += 1; zones[z] += 1
res = dict(source="results/probes/comet_tz_v2.jsonl", probe_rows=len(by), rows_differing=diff, zones=sorted(allz),
           zones_with_differences=dict(zones), n_zones=len(allz), n_zones_differing=len(zones))
json.dump(res, open(f"{OUT}/comet_tz_summary.json", "w"), indent=1); print(res)
