#!/bin/bash
# run_engine.sh <config> <base> <queries> <run_tag>
# configs: spark413-classic spark413-analytics spark420-classic spark420-analytics comet420-classic comet420-analytics sail-noreorder sail-reorder duckdb
set -u
ROOT=${ROOT:-/workspace/engine-swap}
CFG=$1; BASE=$2; QS=$3; TAG=$4
export JAVA_HOME=$(ls -d $ROOT/opt/jdk-21*) PATH=$(ls -d $ROOT/opt/jdk-21*)/bin:$PATH
OUT=$ROOT/results/$TAG; mkdir -p $OUT/rss $OUT/logs
LABEL=$CFG
S3A="--conf spark.hadoop.fs.s3a.endpoint=http://127.0.0.1:${S3_PORT:-9000} --conf spark.hadoop.fs.s3a.path.style.access=true
 --conf spark.hadoop.fs.s3a.access.key=${BENCH_S3_KEY:?export BENCH_S3_KEY} --conf spark.hadoop.fs.s3a.secret.key=${BENCH_S3_SECRET:?export BENCH_S3_SECRET}
 --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false --conf spark.hadoop.fs.s3a.endpoint.region=us-east-1
 --conf spark.ui.enabled=false --conf spark.sql.session.timeZone=UTC"
case $CFG in
  spark413-*) SH=$ROOT/opt/spark-4.1.3-bin-hadoop3;;
  spark420-*|comet420-*) SH=$ROOT/opt/spark-4.2.0-bin-hadoop3;;
esac
case $CFG in
  *-classic) STREAM="--conf spark.hadoop.fs.s3a.input.stream.type=classic";;
  *-analytics) STREAM="--conf spark.hadoop.fs.s3a.input.stream.type=analytics";;
  *-default) STREAM="";;
esac
RUN="$ROOT/venv-sail/bin/python $ROOT/scripts/run_one.py --label $LABEL --rss-out $OUT/rss/$LABEL.csv --summary-out $OUT/summary.jsonl"
case $CFG in
  spark*)
    export PYSPARK_PYTHON=$ROOT/venv-spark/bin/python PYSPARK_DRIVER_PYTHON=$ROOT/venv-spark/bin/python
    $RUN -- $SH/bin/spark-submit --master 'local[8]' --driver-memory 8g $S3A $STREAM \
      $ROOT/scripts/bench_spark_api.py --label $LABEL --base $BASE --queries $QS --out $OUT/queries.jsonl --rowsdir $OUT/rows > $OUT/logs/$LABEL.log 2>&1;;
  comet*)
    export PYSPARK_PYTHON=$ROOT/venv-spark/bin/python PYSPARK_DRIVER_PYTHON=$ROOT/venv-spark/bin/python
    CJ=$ROOT/dist/comet-spark-spark4.2_2.13-1.2.0.jar
    $RUN -- $SH/bin/spark-submit --master 'local[8]' --driver-memory 4g $S3A $STREAM \
      --jars $CJ --conf spark.driver.extraClassPath=$CJ \
      --conf spark.plugins=org.apache.spark.CometPlugin \
      --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
      --conf spark.comet.shuffle.enabled=true \
      --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=4g \
      --conf spark.comet.explain.fallback.enabled=true \
      $ROOT/scripts/bench_spark_api.py --label $LABEL --base $BASE --queries $QS --out $OUT/queries.jsonl --rowsdir $OUT/rows > $OUT/logs/$LABEL.log 2>&1;;
  sail-*)
    export AWS_ACCESS_KEY_ID=${BENCH_S3_KEY:?export BENCH_S3_KEY} AWS_SECRET_ACCESS_KEY=${BENCH_S3_SECRET:?export BENCH_S3_SECRET} AWS_ENDPOINT=http://127.0.0.1:${S3_PORT:-9000} AWS_REGION=us-east-1 AWS_ALLOW_HTTP=true
    [ $CFG = sail-reorder ] && export SAIL_OPTIMIZER__ENABLE_JOIN_REORDER=true || export SAIL_OPTIMIZER__ENABLE_JOIN_REORDER=false
    $RUN --server "$ROOT/venv-sail/bin/sail spark server --port 50051" --server-wait-port 50051 -- \
      $ROOT/venv-sail/bin/python $ROOT/scripts/bench_spark_api.py --remote sc://127.0.0.1:50051 --label $LABEL --base $BASE --queries $QS --out $OUT/queries.jsonl --rowsdir $OUT/rows > $OUT/logs/$LABEL.log 2>&1;;
  duckdb)
    $RUN -- $ROOT/venv-sail/bin/python $ROOT/scripts/bench_duckdb.py --label $LABEL --base $BASE --queries $QS --out $OUT/queries.jsonl --rowsdir $OUT/rows > $OUT/logs/$LABEL.log 2>&1;;
esac
echo "rc=$? $CFG"
