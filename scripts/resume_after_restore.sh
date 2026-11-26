#!/bin/bash
# Idempotent: bring the benchmark infrastructure back after a box reboot/restore, then (re)start orchestrator + chain.
cd /workspace/engine-swap
CG=/sys/fs/cgroup/engine-swap-bench
[ -d $CG ] || { sudo -n mkdir -p $CG; echo 9G | sudo -n tee $CG/memory.max >/dev/null; echo 0 | sudo -n tee $CG/memory.swap.max >/dev/null; }
mkdir -p tmp/spark dist
[ -f dist/comet-spark-spark4.2_2.13-1.2.0.jar ] || curl -sSfL -o dist/comet-spark-spark4.2_2.13-1.2.0.jar https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark4.2_2.13/1.2.0/comet-spark-spark4.2_2.13-1.2.0.jar
pgrep -f "opt/bin/minio server" >/dev/null || { scripts/minio-start.sh; sleep 4; }
pgrep -f "while true; do date" >/dev/null || (setsid nohup bash -c 'while true; do date +%s.%N; sleep 1; done' >> logs/heartbeat.log 2>&1 &)
python3 scripts/mem_pump.py 11
if ! pgrep -f "scripts/orchestrate.py" >/dev/null; then
  if grep -q "ALL JOBS DONE" results/phase2/progress.log; then setsid nohup venv-sail/bin/python scripts/orchestrate.py aal-iso >> logs/orchestrate-aal-iso.out 2>&1 &
  else setsid nohup venv-sail/bin/python scripts/orchestrate.py >> logs/orchestrate.out 2>&1 & fi
fi
sleep 2; pgrep -f chain_aal_iso2.sh >/dev/null || (setsid nohup scripts/chain_aal_iso2.sh > /dev/null 2>&1 &)
sleep 2; pgrep -af "minio server|orchestrate|chain_aal|toxiproxy-server" | cut -c1-100
