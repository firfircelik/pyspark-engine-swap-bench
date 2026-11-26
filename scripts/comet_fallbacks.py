"""Comet fallback inventory: per workload/step, non-Comet operators in the dumped physical plans and the
[COMET: reason] annotations logged by spark.comet.explain.fallback.enabled. Output results/final/comet_fallbacks*.csv"""
import glob, re, os, csv, collections
ROOT = "/workspace/engine-swap"; OUT = f"{ROOT}/results/final"; os.makedirs(OUT, exist_ok=True)
WRAP = {"AdaptiveSparkPlan", "ColumnarToRow", "CometColumnarToRow", "RowToColumnar", "InputAdapter", "WholeStageCodegen", "ReusedExchange",
        "Subquery", "SubqueryBroadcast", "ReusedSubquery", "CometSparkToColumnar", "AQEShuffleRead", "ShuffleQueryStage", "BroadcastQueryStage", "TableCacheQueryStage", "ResultQueryStage", "LogicalQueryStage", "EmptyRelation"}
ops = collections.defaultdict(lambda: collections.Counter()); reasons = collections.defaultdict(collections.Counter)
dirs = [d for d in glob.glob(f"{ROOT}/results/phase2/*/*/comet420-*/rep*") if os.path.isdir(d)]
for d in dirs:
    wl = d.split("/")[-3]
    for p in glob.glob(f"{d}/plans/*.txt"):
        step = os.path.basename(p)[:-4]
        for line in open(p):
            m = re.match(r"^[\s:|+\-]*(\*\(\d+\)\s*)?([A-Za-z][A-Za-z0-9]*)", line)
            if not m: continue
            op = m.group(2)
            if op.startswith("Comet") or op in WRAP: continue
            ops[(wl, step)][op] += 1
    log = f"{d}/engine.log"
    if os.path.exists(log):
        for line in open(log, errors="replace"):
            for r in re.findall(r"([A-Za-z]+) \[COMET: ([^\]]+)\]", line):
                reasons[wl][f"{r[0]}: {r[1][:200]}"] += 1
rows = [{"workload": wl, "step_or_query": s, "non_comet_operator": op, "occurrences_in_plans": n} for (wl, s), c in sorted(ops.items()) for op, n in c.items()]
with open(f"{OUT}/comet_fallbacks_plans.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["workload", "step_or_query", "non_comet_operator", "occurrences_in_plans"]); w.writeheader(); w.writerows(rows)
rr = [{"workload": wl, "reason": r, "log_lines": n} for wl, c in reasons.items() for r, n in c.most_common()]
with open(f"{OUT}/comet_fallbacks_reasons.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["workload", "reason", "log_lines"]); w.writeheader(); w.writerows(rr)
print("runs", len(dirs)); [print(r) for r in rr[:30]]
print("non-comet ops by step:"); [print(r) for r in rows[:60]]
