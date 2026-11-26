#!/bin/bash
# 3 consecutive Comet 1.2.0 SF30 TPC-H runs (loopback, analytics stream), 9 GiB cap, driver 2g + off-heap 3g, MALLOC_ARENA_MAX=2
cd /workspace/engine-swap
while pgrep -f "scripts/run_engine2.sh" >/dev/null; do sleep 10; done
for i in 1 2 3; do
  COMET_MALLOC_ARENA_MAX=2 scripts/run_engine2.sh comet420-analytics loopback tpch results/comet-mem-diag/E_2g_3g_arena2_run$i
  echo "$(date +%T) run$i done" 
done
echo ALLDONE
