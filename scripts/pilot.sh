#!/bin/bash
# Phase 1 pilot: one run per config, SF10 on MinIO loopback. Sequential (one benchmark at a time).
cd /workspace/engine-swap
ss -ltn | grep -q ":50051 " && { echo "port 50051 busy, abort"; exit 1; }
TAG=${1:-pilot-sf10}
for C in spark413-classic spark413-analytics spark420-classic spark420-analytics comet420-classic comet420-analytics sail-noreorder sail-reorder duckdb; do
  sync; echo "$(date +%T) start $C"
  scripts/run_engine.sh $C s3a://tpch/tpch-sf10 1,3,6,9,18 $TAG
done
echo "$(date +%T) PILOT DONE"
