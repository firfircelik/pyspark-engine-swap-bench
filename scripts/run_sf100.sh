#!/bin/bash
# Resume-safe SF100 orchestrator + aggregate. Safe to re-run.
set -u
ROOT=${ROOT:-/workspace/engine-swap}
LOG=$ROOT/logs/orchestrate-sf100.out
PROG=$ROOT/results/sf100/progress.log
mkdir -p $ROOT/logs $ROOT/results/sf100
if ! grep -q "ALL JOBS DONE" "$PROG" 2>/dev/null; then
  /usr/bin/python3 $ROOT/scripts/orchestrate.py sf100 >> "$LOG" 2>&1
fi
if grep -q "ALL JOBS DONE" "$PROG" 2>/dev/null; then
  /usr/bin/python3 $ROOT/scripts/aggregate_sf100.py >> $ROOT/logs/aggregate-sf100.out 2>&1
  echo "aggregate rc=$?"
else
  echo "orchestrator exited without ALL JOBS DONE; not aggregating" >&2
  exit 2
fi
