#!/bin/bash
# Waits for the phase2 orchestrator to finish (ALL JOBS DONE), then runs the AAL isolation plan. One benchmark at a time.
cd /workspace/engine-swap
while pgrep -f "scripts/orchestrate.py$" >/dev/null || pgrep -f "scripts/orchestrate.py phase2" >/dev/null; do sleep 60; done
if grep -q "ALL JOBS DONE" results/phase2/progress.log; then
  echo "$(date '+%F %T') phase2 done, starting aal-iso" >> logs/chain.out
  venv-sail/bin/python scripts/orchestrate.py aal-iso >> logs/orchestrate-aal-iso.out 2>&1
  echo "$(date '+%F %T') aal-iso exited" >> logs/chain.out
else echo "$(date '+%F %T') phase2 orchestrator exited without ALL JOBS DONE; aal-iso not started" >> logs/chain.out; fi
