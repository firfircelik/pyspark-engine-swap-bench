#!/bin/bash
# Latency-arm pilot: MinIO behind toxiproxy (port 9100). Arms: 0 ms (proxy control) and 10 ms downstream latency. One run each.
cd /workspace/engine-swap
ss -ltn | grep -q ":50051 " && { echo "port 50051 busy"; exit 1; }
opt/bin/toxiproxy-server -host 127.0.0.1 -port 8474 > logs/toxiproxy.log 2>&1 & TP=$!
sleep 1; opt/bin/toxiproxy-cli create -l 127.0.0.1:9100 -u 127.0.0.1:9000 minio_lat
export S3_PORT=9100
for LAT in 0 10; do
  opt/bin/toxiproxy-cli toxic remove -n lat minio_lat >/dev/null 2>&1
  [ $LAT -gt 0 ] && opt/bin/toxiproxy-cli toxic add -n lat -t latency -a latency=$LAT -d minio_lat
  for C in spark420-classic spark420-analytics comet420-analytics sail-noreorder duckdb; do
    echo "$(date +%T) lat=$LAT start $C"
    scripts/run_engine.sh $C s3a://tpch/tpch-sf10 1,6,9 pilot-sf10-proxy${LAT}ms
  done
done
kill $TP; echo "$(date +%T) LATENCY PILOT DONE"
