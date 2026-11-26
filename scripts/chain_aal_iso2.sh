#!/bin/bash
# Robust chain: waits until phase2 progress.log says ALL JOBS DONE (survives orchestrator restarts), then runs the AAL isolation plan once.
cd /workspace/engine-swap
exec 9>logs/chain_aal_iso.lock; flock -n 9 || { echo "$(date '+%F %T') another chain holds the lock, exiting" >> logs/chain.out; exit 0; }
echo "$(date '+%F %T') chain2 armed (pid $$), waiting for phase2 ALL JOBS DONE" >> logs/chain.out
until grep -q "ALL JOBS DONE" results/phase2/progress.log; do sleep 60; done
while pgrep -f "scripts/orchestrate.py$" >/dev/null || pgrep -f "orchestrate.py phase2" >/dev/null; do sleep 10; done
if [ -f results/aal-iso/progress.log ] && grep -q "ALL JOBS DONE" results/aal-iso/progress.log; then echo "$(date '+%F %T') aal-iso already done" >> logs/chain.out; exit 0; fi
echo "$(date '+%F %T') phase2 done, starting aal-iso" >> logs/chain.out
venv-sail/bin/python scripts/orchestrate.py aal-iso >> logs/orchestrate-aal-iso.out 2>&1
echo "$(date '+%F %T') aal-iso orchestrator exited" >> logs/chain.out
