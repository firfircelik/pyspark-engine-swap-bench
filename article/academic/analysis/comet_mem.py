"""Comet memory diagnostics summary (results/comet-mem-diag): peak tree RSS per diagnostic run, and per-query wall time
of MALLOC_ARENA_MAX=2 (E runs) relative to the default allocator with the same 2g+3g budget (C2). Diagnostic runs only."""
import json, glob, os
import pandas as pd, numpy as np
from common import R, OUT
D = f"{R}/comet-mem-diag"; rows = []; qt = []
for d in sorted(glob.glob(f"{D}/*/")):
    name = os.path.basename(d.rstrip("/")); s = f"{d}summary.jsonl"
    sm = json.loads(open(s).readline()) if os.path.exists(s) else {}
    rows.append(dict(run=name, peak_tree_rss_gb=sm.get("peak_tree_rss_bytes", np.nan) / 1e9 if sm else np.nan, rc=sm.get("rc")))
    q = f"{d}queries.jsonl"
    if os.path.exists(q):
        for l in open(q):
            e = json.loads(l)
            if e.get("event") == "query" and not e.get("error"): qt.append(dict(run=name, q=e["q"], wall_s=e["wall_s"]))
RS = pd.DataFrame(rows); QT = pd.DataFrame(qt)
E = QT[QT.run.str.startswith("E_")].groupby("q").wall_s.median(); C2 = QT[QT.run == "C2_2g_3g_all"].set_index("q").wall_s
ratio = (E / C2).dropna()
res = dict(peaks=RS.to_dict("records"), arena2_vs_default_C2_perq_ratio=dict(n=int(len(ratio)), median=float(ratio.median()),
           min=float(ratio.min()), max=float(ratio.max()), by_query={int(k): round(float(v), 3) for k, v in ratio.items()}),
           note="diagnostic runs; the arena runs each had one box freeze, so the timing ratio is indicative only")
json.dump(res, open(f"{OUT}/comet_mem_summary.json", "w"), indent=1); print(json.dumps(res, indent=1))
