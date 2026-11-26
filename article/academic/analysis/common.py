"""Shared loaders. Only reads published result files; no benchmark runs."""
import json, glob, os
import numpy as np, pandas as pd
# ROOT: env var ENGINE_SWAP_ROOT, else the checkout root (this file is article/academic/analysis/common.py).
from pathlib import Path
ROOT = os.environ.get("ENGINE_SWAP_ROOT", str(Path(__file__).resolve().parents[3]))
R = f"{ROOT}/results"; OUT = os.environ.get("ENGINE_SWAP_OUT", f"{ROOT}/article/academic/out")
RNG = np.random.default_rng(20261126)

def phase2():
    d = pd.read_csv(f"{R}/final/phase2_query_runs_long.csv", dtype={"q": str})
    return d[d.error.isna()].copy()

def aal_iso():
    d = pd.read_csv(f"{R}/final/aal_iso_query_runs_long.csv", dtype={"q": str})
    return d[d.error.isna()].copy()

def tablefmt():
    d = pd.read_csv(f"{R}/tablefmt/final/query_runs.csv", dtype={"q": str})
    return d[d.hash_ok == True].copy()

def sf100():
    d = pd.read_csv(f"{R}/sf100/final/sf100_query_runs.csv", dtype={"q": str})
    sup = set(json.load(open(f"{R}/sf100/superseded.json")))
    d = d[~d.outdir.map(lambda p: os.path.join(ROOT, p) in sup or p in sup)]
    return d[d.error.isna() & d.hash_set.notna()].copy()

def boot_ci(x, stat=np.mean, B=10000, alpha=0.05):
    x = np.asarray(x, float)
    if len(x) < 2: return (np.nan, np.nan)
    idx = RNG.integers(0, len(x), (B, len(x)))
    s = stat(x[idx], axis=1)
    return tuple(np.quantile(s, [alpha / 2, 1 - alpha / 2]))

def strat_boot_total(groups, B=10000, alpha=0.05):
    """Total = sum over queries; each query's run time resampled from its own reps (assumes
    independent run noise per query). Returns (point=sum of per-query means, lo, hi)."""
    arrs = [np.asarray(g, float) for g in groups]
    tot = np.zeros(B)
    for a in arrs:
        tot += a[RNG.integers(0, len(a), (B, len(a)))].mean(axis=1)
    return sum(a.mean() for a in arrs), *np.quantile(tot, [alpha / 2, 1 - alpha / 2])

def cliffs_delta(a, b):
    a = np.asarray(a, float); b = np.asarray(b, float)
    gt = (a[:, None] > b[None, :]).sum(); lt = (a[:, None] < b[None, :]).sum()
    return (gt - lt) / (len(a) * len(b))
