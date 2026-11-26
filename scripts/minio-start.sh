#!/bin/bash
cd /workspace/engine-swap
export MINIO_ROOT_USER=${BENCH_S3_KEY:?export BENCH_S3_KEY} MINIO_ROOT_PASSWORD=${BENCH_S3_SECRET:?export BENCH_S3_SECRET} MINIO_PROMETHEUS_AUTH_TYPE=public MINIO_BROWSER=off
nohup opt/bin/minio server --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 minio-data > logs/minio.log 2>&1 &
echo $! > logs/minio.pid
