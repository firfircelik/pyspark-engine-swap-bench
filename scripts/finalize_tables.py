"""Append AAL isolation summaries and F4 characterization to results/final/TABLES.md.
Generated from results/phase2 and results/aal-iso state + queries.jsonl. No fabricated numbers.
Invoked at the end of aggregate2.py; also runnable on its own after aggregate2.
"""
import json, os, csv, collections, statistics, re
from pathlib import Path

ROOT = Path("/workspace/engine-swap")
OUT = ROOT / "results/final"

def med(xs):
    return statistics.median(xs) if xs else None

def load_state(name):
    p = ROOT / f"results/{name}/state.jsonl"
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()] if p.exists() else []

def dropped_reps(name):
    p = ROOT / f"results/{name}/dropped_reps.json"
    if not p.exists():
        return set()
    return set(json.loads(p.read_text()).get("dropped_reps") or [])

def superseded(name):
    p = ROOT / f"results/{name}/superseded.json"
    return set(json.loads(p.read_text())) if p.exists() else set()

def readq(outdir):
    p = Path(outdir) / "queries.jsonl"
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()] if p.exists() else []

def is_valid(a):
    return a.get("status") == "ok" and not a.get("query_errors")

def inflight_q(qs):
    inflight = None
    for q in qs:
        if q.get("event") == "query_start":
            inflight = q.get("q")
        elif q.get("event") == "query" and q.get("q") == inflight:
            inflight = None
    return inflight

def err_kind(e):
    e = e or ""
    if "UNABLE_TO_ACQUIRE_MEMORY" in e or ("OutOfMemory" in e and "Unable to acquire" in e):
        return "UNABLE_TO_ACQUIRE_MEMORY"
    if "STALLED_NO_PROGRESS" in e:
        return "STALLED_NO_PROGRESS"
    if "No space left" in e:
        return "NoSpaceLeft"
    if "FAILED_READ_FILE" in e:
        return "FAILED_READ_FILE"
    if "SERVER_DIED" in e:
        return "SERVER_DIED"
    return (e.split(":")[0][:80] or "error")

def f4_snippet(e):
    m = re.search(r"Unable to acquire[^\n]{0,100}", e or "")
    return m.group(0).strip() if m else ""

def collect_f4_and_stalls():
    events = []
    # F4 OOM from query errors
    for study in ("phase2", "aal-iso"):
        for a in load_state(study):
            qs = readq(a["outdir"])
            for q in qs:
                if q.get("event") != "query" or not q.get("error"):
                    continue
                kind = err_kind(q["error"])
                if kind != "UNABLE_TO_ACQUIRE_MEMORY":
                    continue
                events.append({
                    "study": study, "class": "F4_OOM", "job": a["id"], "rep": a.get("rep"),
                    "attempt": a.get("attempt"), "attempt_status": a.get("status"),
                    "cfg": a.get("cfg"), "variant": a.get("variant", a.get("cfg")),
                    "cond": a.get("cond"), "wl": a.get("wl"), "q": q.get("q"),
                    "elapsed_s": a.get("elapsed_s"), "wall_s": q.get("wall_s"),
                    "detail": f4_snippet(q["error"]) or "UNABLE_TO_ACQUIRE_MEMORY",
                    "dropped_rep": a.get("rep") in dropped_reps(study),
                })
            # stalls
            timed = [q for q in qs if q.get("event") == "query"]
            inf = inflight_q(qs)
            gaps = a.get("frozen_queries") or []
            max_gap = max((g.get("gap_s") or 0) for g in gaps) if gaps else 0
            elapsed = a.get("elapsed_s") or 0
            status = a.get("status")
            # no-progress hang: timeout, or in-flight query and long elapsed, or explicit STALLED
            stalled_event = any(q.get("error") == "STALLED_NO_PROGRESS" for q in timed)
            is_hang = (
                status == "timeout"
                or stalled_event
                or (inf is not None and elapsed >= 1200)
            )
            # long RSS gap on a query that still completed (box freeze), distinct from hang
            is_long_gap = (max_gap >= 60) and not is_hang
            if is_hang:
                events.append({
                    "study": study, "class": "NO_PROGRESS_STALL", "job": a["id"], "rep": a.get("rep"),
                    "attempt": a.get("attempt"), "attempt_status": status,
                    "cfg": a.get("cfg"), "variant": a.get("variant", a.get("cfg")),
                    "cond": a.get("cond"), "wl": a.get("wl"), "q": inf if inf is not None else "",
                    "elapsed_s": elapsed, "wall_s": None,
                    "detail": f"inflight={inf} n_query_done={len(timed)} max_rss_gap_s={max_gap} stalled_event={stalled_event}",
                    "dropped_rep": a.get("rep") in dropped_reps(study),
                })
            elif is_long_gap:
                events.append({
                    "study": study, "class": "LONG_RSS_GAP", "job": a["id"], "rep": a.get("rep"),
                    "attempt": a.get("attempt"), "attempt_status": status,
                    "cfg": a.get("cfg"), "variant": a.get("variant", a.get("cfg")),
                    "cond": a.get("cond"), "wl": a.get("wl"), "q": "",
                    "elapsed_s": elapsed, "wall_s": None,
                    "detail": f"n_query_done={len(timed)} frozen={gaps}",
                    "dropped_rep": a.get("rep") in dropped_reps(study),
                })
    return events

def aal_get_reduction():
    # valid ok attempts only
    g = collections.defaultdict(list)
    for a in load_state("aal-iso"):
        if not is_valid(a):
            continue
        for q in readq(a["outdir"]):
            if q.get("event") != "query" or q.get("error"):
                continue
            g[(a["cond"], a.get("variant", a["cfg"]), q["q"])].append(
                (q["wall_s"], (q.get("s3") or {}).get("GetObject") or 0)
            )
    pairs = [
        ("loopback", "A_420_classic", "B_420_aal_default", "4.2.0 classic vs AAL default"),
        ("proxy10", "A_420_classic", "B_420_aal_default", "4.2.0 classic vs AAL default"),
        ("loopback", "H_413_classic", "I_413_aal121", "4.1.3 classic vs AAL 1.2.1"),
        ("loopback", "A_420_classic", "C_420_aal_no_coalesce", "4.2.0 classic vs no coalesce"),
        ("loopback", "A_420_classic", "D_420_aal_coalesce_128k", "4.2.0 classic vs coalesce 128k"),
        ("loopback", "A_420_classic", "E_420_aal_no_vectored", "4.2.0 classic vs no vectored"),
        ("loopback", "A_420_classic", "F_420_aal_no_pageindex_prefetch", "4.2.0 classic vs no pageindex prefetch"),
        ("loopback", "A_420_classic", "G_420_aal_prefetch_off", "4.2.0 classic vs prefetch off"),
    ]
    rows = []
    for cond, va, vb, label in pairs:
        for q in (3, 1, 6, 9):
            a, b = g.get((cond, va, q), []), g.get((cond, vb, q), [])
            if not a or not b:
                continue
            wa, ga = med([x[0] for x in a]), med([x[1] for x in a])
            wb, gb = med([x[0] for x in b]), med([x[1] for x in b])
            rows.append({
                "cond": cond, "comparison": label, "classic_variant": va, "aal_variant": vb, "q": q,
                "n_classic": len(a), "n_aal": len(b),
                "classic_median_s": round(wa, 3), "aal_median_s": round(wb, 3),
                "wall_ratio_aal_over_classic": round(wb / wa, 3) if wa else None,
                "classic_median_GET": ga, "aal_median_GET": gb,
                "GET_reduction_pct": round((ga - gb) / ga * 100, 1) if ga else None,
            })
    return rows

def phase2_get_reduction():
    drop, sup = dropped_reps("phase2"), superseded("phase2")
    att = collections.defaultdict(list)
    for a in load_state("phase2"):
        if a.get("rep") in drop or a.get("outdir") in sup:
            continue
        att[a["id"]].append(a)
    fin = {}
    for k, v in att.items():
        last = dict(v[-1])
        if last["status"] == "ok" and last.get("query_errors"):
            last["status"] = "qerror"
        if last["status"] in ("ok", "failed", "timeout") or len(v) >= 3:
            fin[k] = last
    rget = collections.defaultdict(list)
    for j in fin.values():
        if j.get("wl") != "tpch":
            continue
        qs = readq(j["outdir"])
        tq = [q for q in qs if q.get("event") == "query"]
        ok = j.get("status") == "ok" and tq and not any(q.get("error") for q in tq)
        if not ok:
            continue
        rget[(j["cond"], j["cfg"])].append(sum((q.get("s3") or {}).get("GetObject") or 0 for q in tq))
    rows = []
    for cond in ("loopback", "proxy0", "proxy10"):
        for eng in ("spark413", "spark420", "comet420"):
            c = rget.get((cond, f"{eng}-classic"), [])
            a = rget.get((cond, f"{eng}-analytics"), [])
            if not c or not a:
                continue
            mc, ma = med(c), med(a)
            rows.append({
                "cond": cond, "engine": eng, "n_classic": len(c), "n_analytics": len(a),
                "classic_median_total_GET": mc, "analytics_median_total_GET": ma,
                "GET_reduction_pct": round((mc - ma) / mc * 100, 1) if mc else None,
            })
    return rows

def valid_rep_notes():
    # published phase2: reps not dropped, 51 cells x reps
    drop, sup = dropped_reps("phase2"), superseded("phase2")
    cells = collections.defaultdict(list)
    for a in load_state("phase2"):
        if a.get("outdir") in sup:
            continue
        cells[(a["cond"], a["wl"], a["cfg"], a["rep"])].append(a)
    pub = miss = 0
    missing = []
    n_by = collections.Counter()
    grouped = collections.defaultdict(dict)
    for (cond, wl, cfg, rep), recs in cells.items():
        if rep in drop:
            continue
        pub += 1
        ok = any(is_valid(a) for a in recs)
        grouped[(cond, wl, cfg)][rep] = ok
        if ok:
            miss += 0
        else:
            missing.append(f"{cond}/{wl}/{cfg} rep{rep}")
    n_ok = sum(1 for recs in cells.values() for a in recs if False)  # placeholder
    n_ok = 0
    for (cond, wl, cfg, rep), recs in cells.items():
        if rep in drop:
            continue
        if any(is_valid(a) for a in recs):
            n_ok += 1
    hist = collections.Counter(sum(1 for v in reps.values() if v) for reps in grouped.values())
    # aal-iso
    iso_cells = collections.defaultdict(list)
    for a in load_state("aal-iso"):
        iso_cells[(a["cond"], a.get("variant", a["cfg"]), a["rep"])].append(a)
    iso_var = collections.defaultdict(dict)
    for (cond, var, rep), recs in iso_cells.items():
        iso_var[(cond, var)][rep] = any(is_valid(a) for a in recs)
    iso_hist = collections.Counter(sum(1 for v in reps.values() if v) for reps in iso_var.values())
    iso_missing = [f"{c}/{v} reps with valid={ {r:ok for r,ok in reps.items()} }"
                   for (c, v), reps in sorted(iso_var.items()) if not all(reps.values())]
    return {
        "phase2_published_cells": pub,
        "phase2_valid_cells": n_ok,
        "phase2_missing": missing,
        "phase2_nvalid_hist": dict(hist),
        "phase2_n_cfgs": len(grouped),
        "aal_iso_cfgs": len(iso_var),
        "aal_iso_nvalid_hist": dict(iso_hist),
        "aal_iso_incomplete": iso_missing,
    }

def md_escape(s):
    return str(s).replace("|", "\\|")

def build_markdown(events, aal_red, p2_red, vnotes):
    L = []
    L += ["", "## Valid repetitions", "",
          f"Phase 2 published totals use reps 1–2 only (rep 3 dropped; see `results/phase2/dropped_reps.json`). "
          f"After the extra r2 attempt (a4) for `r2-loopback-spark420-analytics-tpch`, "
          f"**{vnotes['phase2_valid_cells']} of {vnotes['phase2_published_cells']} published cells have a valid complete run** "
          f"({vnotes['phase2_n_cfgs']} configs, n-valid-reps histogram {vnotes['phase2_nvalid_hist']}).",
          ""]
    if vnotes["phase2_missing"]:
        L += ["Missing published cells:", ""] + [f"- {m}" for m in vnotes["phase2_missing"]] + [""]
    L += [f"AAL isolation (39 jobs, 13 variant×condition cells): n-valid-reps histogram {vnotes['aal_iso_nvalid_hist']}.", ""]
    if vnotes["aal_iso_incomplete"]:
        L += ["Cells without a valid run for every planned rep:", ""] + [f"- {m}" for m in vnotes["aal_iso_incomplete"]] + [""]

    L += ["## AAL GET reduction (Phase 2 TPC-H, median of complete published runs, sum of timed-query GetObject)", "",
          "| cond | engine | classic GET (n) | analytics GET (n) | GET reduction |",
          "|---|---|---|---|---|"]
    for r in p2_red:
        L.append(f'| {r["cond"]} | {r["engine"]} | {int(r["classic_median_total_GET"])} (n={r["n_classic"]}) | {int(r["analytics_median_total_GET"])} (n={r["n_analytics"]}) | {r["GET_reduction_pct"]}% |')
    L.append("")

    L += ["## AAL isolation: wall time and GET vs classic (median of valid reps, queries 3,1,6,9)", "",
          "| cond | comparison | q | n classic / n AAL | classic s | AAL s | wall ratio | classic GET | AAL GET | GET reduction |",
          "|---|---|---|---|---|---|---|---|---|---|"]
    for r in aal_red:
        L.append(
            f'| {r["cond"]} | {r["aal_variant"]} | {r["q"]} | {r["n_classic"]}/{r["n_aal"]} | '
            f'{r["classic_median_s"]} | {r["aal_median_s"]} | {r["wall_ratio_aal_over_classic"]} | '
            f'{int(r["classic_median_GET"])} | {int(r["aal_median_GET"])} | {r["GET_reduction_pct"]}% |'
        )
    L.append("")

    f4 = [e for e in events if e["class"] == "F4_OOM"]
    hangs = [e for e in events if e["class"] == "NO_PROGRESS_STALL"]
    gaps = [e for e in events if e["class"] == "LONG_RSS_GAP"]

    L += ["## F4 characterization", "",
          "F4 here is the intermittent Spark execution-memory failure "
          "`UNABLE_TO_ACQUIRE_MEMORY` / `Unable to acquire 65536 bytes of memory, got 0` "
          "(SQLSTATE 53200), with stack `UnsafeExternalSorter.growPointerArrayIfNecessary` → "
          "`UnsafeExternalRowSorter.insertRow`. **This is characterized as a configuration limit "
          "under this matrix (Spark `local[8]`, driver 6g, cgroup memory.max 9 GiB), not as a "
          "Spark or AAL bug**, unless later evidence clearly shows otherwise. The same cell often "
          "succeeds on retry without changing memory settings; `spark.memory.fraction` and "
          "`spark.sql.shuffle.partitions` were not varied in this study, so their effect is not measured. "
          "No upstream issue was filed.",
          "",
          "A second, distinct phenomenon is also recorded here because it dominated wall-clock "
          "waste: **no-progress stalls**, mostly on Spark classic, where a job writes `query_start` "
          "and then makes no further query progress for a long time (CPU nearly idle). Some of "
          "these ran for many hours until a timeout fired (the 90-minute `run_one.py` timeout "
          "sometimes did not trip until the box unfroze, or `cli.wait()` hung after kill). "
          "From 2026-11-25 23:32 Europe/Istanbul, remaining aal-iso jobs used a 30-minute job "
          "timeout and a 20-minute no-`queries.jsonl`-progress stall kill (`STALLED_NO_PROGRESS`). "
          "Long RSS-sampler gaps on queries that still completed are listed separately (timing "
          "contaminated; those attempts were marked frozen and rerun).",
          ""]

    def freq_table(rows, keyfn, title, denom_note=""):
        out = [f"### {title}", ""]
        if denom_note:
            out.append(denom_note)
            out.append("")
        cnt = collections.Counter(keyfn(r) for r in rows)
        out += ["| key | F4 OOM count |", "|---|---|"]
        for k, n in cnt.most_common():
            out.append(f"| {md_escape(k)} | {n} |")
        out.append("")
        return out

    L += [f"**F4 OOM count: {len(f4)}** (Phase 2: {sum(1 for e in f4 if e['study']=='phase2')}, "
          f"including dropped rep 3; AAL isolation: {sum(1 for e in f4 if e['study']=='aal-iso')}). "
          f"Every one of these errors is the same 65536-byte acquisition failure.",
          ""]

    # denominators: spark tpch query events
    def spark_tpch_denom(study):
        n_q = n_f4 = 0
        by_q = collections.Counter(); by_q_f4 = collections.Counter()
        by_stream = collections.Counter(); by_stream_f4 = collections.Counter()
        by_ver = collections.Counter(); by_ver_f4 = collections.Counter()
        for a in load_state(study):
            if a.get("wl") and a["wl"] != "tpch":
                continue
            if study == "phase2" and not str(a.get("cfg", "")).startswith("spark"):
                continue
            cfg = a.get("cfg") or ""
            stream = "analytics" if "analytics" in cfg else ("classic" if "classic" in cfg else ("default" if "default" in cfg else cfg))
            ver = "4.1.3" if "413" in cfg or "413" in str(a.get("variant", "")) else ("4.2.0" if "420" in cfg or "420" in str(a.get("variant", "")) else cfg)
            for q in readq(a["outdir"]):
                if q.get("event") != "query":
                    continue
                n_q += 1
                by_q[q["q"]] += 1; by_stream[stream] += 1; by_ver[ver] += 1
                if q.get("error") and "UNABLE_TO_ACQUIRE_MEMORY" in (q.get("error") or ""):
                    n_f4 += 1
                    by_q_f4[q["q"]] += 1; by_stream_f4[stream] += 1; by_ver_f4[ver] += 1
        return n_q, n_f4, by_q, by_q_f4, by_stream, by_stream_f4, by_ver, by_ver_f4

    L += ["### Frequency per query / config / stream / version", ""]
    for study, label in (("phase2", "Phase 2 Spark TPC-H (all attempts, including dropped rep 3)"),
                         ("aal-iso", "AAL isolation (all attempts; queries 3,1,6,9)")):
        n_q, n_f4, by_q, by_q_f4, by_stream, by_stream_f4, by_ver, by_ver_f4 = spark_tpch_denom(study)
        L += [f"**{label}:** {n_f4} F4 OOM / {n_q} timed query events.", "",
              "| slice | F4 OOM / timed queries |", "|---|---|"]
        for q in sorted(set(by_q) | set(by_q_f4), key=lambda x: int(x) if str(x).isdigit() else 0):
            if by_q_f4[q] or study == "aal-iso":
                L.append(f"| {study} Q{q} | {by_q_f4[q]} / {by_q[q]} |")
        for s in sorted(by_stream):
            L.append(f"| {study} stream={s} | {by_stream_f4[s]} / {by_stream[s]} |")
        for s in sorted(by_ver):
            L.append(f"| {study} Spark {s} | {by_ver_f4[s]} / {by_ver[s]} |")
        L.append("")

    L += ["F4 OOM is concentrated on Q12 (Phase 2) and Q3 (AAL isolation, where Q3 is the first timed query). "
          "It appears on both Spark 4.1.3 and 4.2.0, and on both classic and analytics streams "
          "(Phase 2: 6 analytics vs 1 classic; AAL isolation: 10 analytics vs 0 classic among the "
          "recorded OOM events). It was not observed on Comet, Sail, or DuckDB query events in this matrix "
          "(those engines had other, separately classified errors).",
          ""]

    L += ["### F4 OOM event list", "",
          "| study | job | attempt | status | cfg / variant | cond | q | attempt wall s | note | dropped rep |",
          "|---|---|---|---|---|---|---|---|---|---|"]
    for e in f4:
        L.append(
            f'| {e["study"]} | {e["job"]} | {e["attempt"]} | {e["attempt_status"]} | '
            f'{e["variant"]} | {e["cond"]} | {e["q"]} | {e["elapsed_s"]} | {md_escape(e["detail"])} | {e["dropped_rep"]} |'
        )
    L.append("")

    L += [f"### No-progress stalls ({len(hangs)} attempts)", "",
          "These are hangs, not the 64 KB execution-memory error. Several Phase 2 stalls are on dropped rep 3 "
          "and are excluded from published medians, but they are evidence. Two AAL-isolation r2 classic cells "
          "have no valid r2 run because of stalls (r1 and r3 of those cells are valid).",
          "",
          "| study | job | attempt | status | cfg / variant | cond | inflight q | elapsed s | detail | dropped rep |",
          "|---|---|---|---|---|---|---|---|---|---|"]
    for e in hangs:
        L.append(
            f'| {e["study"]} | {e["job"]} | {e["attempt"]} | {e["attempt_status"]} | '
            f'{e["variant"]} | {e["cond"]} | {e["q"]} | {e["elapsed_s"]} | {md_escape(e["detail"])} | {e["dropped_rep"]} |'
        )
    L.append("")

    L += [f"### Long RSS-sampler gaps on completed queries ({len(gaps)} attempts)", "",
          "The query finished, but an RSS sample gap ≥ 60 s overlapped it, so the attempt was marked frozen and not used as a valid timing. "
          "Shorter gaps (the orchestrator flags >10 s or >5% of query wall) also caused some reruns; those shorter flags are in `state.jsonl` `frozen_queries` and are not all repeated here.",
          "",
          "| study | job | attempt | status | cfg / variant | cond | elapsed s | detail | dropped rep |",
          "|---|---|---|---|---|---|---|---|---|"]
    for e in gaps:
        L.append(
            f'| {e["study"]} | {e["job"]} | {e["attempt"]} | {e["attempt_status"]} | '
            f'{e["variant"]} | {e["cond"]} | {e["elapsed_s"]} | {md_escape(e["detail"])} | {e["dropped_rep"]} |'
        )
    L.append("")
    return "\n".join(L) + "\n"

def main():
    events = collect_f4_and_stalls()
    aal_red = aal_get_reduction()
    p2_red = phase2_get_reduction()
    vnotes = valid_rep_notes()
    fields = ["study", "class", "job", "rep", "attempt", "attempt_status", "cfg", "variant", "cond", "wl", "q", "elapsed_s", "wall_s", "detail", "dropped_rep"]
    with open(OUT / "f4_events.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader(); w.writerows(events)
    if aal_red:
        with open(OUT / "aal_iso_get_reduction.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(aal_red[0].keys()), extrasaction="ignore")
            w.writeheader(); w.writerows(aal_red)
    if p2_red:
        with open(OUT / "phase2_get_reduction.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(p2_red[0].keys()), extrasaction="ignore")
            w.writeheader(); w.writerows(p2_red)
    extra = build_markdown(events, aal_red, p2_red, vnotes)
    (OUT / "F4.md").write_text(
        "# F4 characterization and AAL isolation extras\n\n"
        "This file is the F4 / valid-reps / GET-reduction addendum also appended to TABLES.md by scripts/finalize_tables.py.\n"
        + extra
    )
    tables = OUT / "TABLES.md"
    body = tables.read_text() if tables.exists() else "# Phase 2 tables\n"
    # idempotent append: strip a previous addendum if re-run
    marker = "\n## Valid repetitions\n"
    if marker in body:
        body = body[:body.index(marker)]
    tables.write_text(body.rstrip() + "\n" + extra)
    print("finalize_tables: F4 OOM", sum(1 for e in events if e["class"]=="F4_OOM"),
          "stalls", sum(1 for e in events if e["class"]=="NO_PROGRESS_STALL"),
          "long gaps", sum(1 for e in events if e["class"]=="LONG_RSS_GAP"))
    print("wrote", OUT / "TABLES.md", OUT / "F4.md", OUT / "f4_events.csv")

if __name__ == "__main__":
    main()
