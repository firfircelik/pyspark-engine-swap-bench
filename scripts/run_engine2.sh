#!/bin/bash
# Phase 2 single job. Usage: run_engine2.sh <cfg> <cond> <workload> <outdir>
# cond: local | loopback | proxy0 | proxy10   (proxy* -> S3 port 9100 via toxiproxy; toxic set by orchestrator)
set -u
ROOT=${ROOT:-/workspace/engine-swap}; CFG=$1; COND=$2; WL=$3; OUT=$4
# per-run Spark scratch dir; removed after the run so a SIGKILLed JVM (OOM) cannot leave shuffle files behind
JOBTMP=$ROOT/tmp/spark/job-$$-$(date +%s); mkdir -p $JOBTMP
SF=${SF:-sf30}; CAP=${CAP:-/sys/fs/cgroup/engine-swap-bench}; TIMEOUT=${TIMEOUT:-1800}  # 2026-11-25 23:32 TR: lowered from 5400; aal-iso healthy jobs are 2-4 min. run_one also stall-kills after 20 min with no queries.jsonl progress.
export JAVA_HOME=$(ls -d $ROOT/opt/jdk-21*) ; export PATH=$JAVA_HOME/bin:$PATH
mkdir -p $OUT
case $COND in
  local) BASE=file://$ROOT/data/tpch-$SF; WBASE=file://$ROOT/data/tpch-sf1; ETL=file://$ROOT/tmp/etl-scratch/$CFG; export S3_PORT=9000;;
  loopback) BASE=s3a://tpch/tpch-$SF; WBASE=s3a://tpch/tpch-sf1; ETL=s3a://tpch/etl-scratch/$CFG; export S3_PORT=9000;;
  proxy*) BASE=s3a://tpch/tpch-$SF; WBASE=s3a://tpch/tpch-sf1; ETL=s3a://tpch/etl-scratch/$CFG; export S3_PORT=9100;;
esac
[ -n "${TABLEBASE:-}" ] && BASE=$TABLEBASE
[ -n "${WARMUP_BASE:-}" ] && WBASE=$WARMUP_BASE
FORMAT=${FORMAT:-parquet}
S3A="--conf spark.hadoop.fs.s3a.endpoint=http://127.0.0.1:$S3_PORT --conf spark.hadoop.fs.s3a.path.style.access=true
 --conf spark.hadoop.fs.s3a.access.key=${BENCH_S3_KEY:?export BENCH_S3_KEY} --conf spark.hadoop.fs.s3a.secret.key=${BENCH_S3_SECRET:?export BENCH_S3_SECRET}
 --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false --conf spark.hadoop.fs.s3a.endpoint.region=us-east-1
 --conf spark.ui.enabled=false --conf spark.sql.session.timeZone=UTC --conf spark.local.dir=$JOBTMP --conf spark.driver.extraJavaOptions=-Djava.io.tmpdir=$JOBTMP
 --conf spark.hadoop.fs.s3a.committer.name=directory --conf spark.hadoop.fs.s3a.buffer.dir=$ROOT/tmp/spark
 ${EXTRA_CONF:-}"
COMET_JAR=${COMET_JAR:-$ROOT/dist/comet-spark-spark4.2_2.13-1.2.0.jar}
case $CFG in
  spark413-*) SH=$ROOT/opt/spark-4.1.3-bin-hadoop3;;
  spark420-*) SH=$ROOT/opt/spark-4.2.0-bin-hadoop3;;
  comet413-*) SH=$ROOT/opt/spark-4.1.3-bin-hadoop3; COMET_JAR=${COMET_JAR_413:-$ROOT/dist/comet-spark-spark4.1_2.13-1.2.0.jar};;
  comet420-*) SH=$ROOT/opt/spark-4.2.0-bin-hadoop3;;
esac
TFJARS=""
if [ -n "${TABLEFMT_JARS:-}" ]; then TFJARS="--jars $TABLEFMT_JARS"; fi
case $CFG in *-classic) STREAM="--conf spark.hadoop.fs.s3a.input.stream.type=classic";; *-analytics) STREAM="--conf spark.hadoop.fs.s3a.input.stream.type=analytics";; *) STREAM="";; esac
COMMON="--label $CFG --base $BASE --warmup-base $WBASE --workload $WL --out $OUT/queries.jsonl --rowsdir $OUT/rows --plansdir $OUT/plans --etl-out $ETL --format $FORMAT ${QARG:-}"
RUN="$ROOT/venv-sail/bin/python $ROOT/scripts/run_one.py --label $CFG --rss-out $OUT/rss.csv --summary-out $OUT/summary.jsonl --cgroup $CAP --timeout $TIMEOUT"
export PYSPARK_PYTHON=$ROOT/venv-spark/bin/python PYSPARK_DRIVER_PYTHON=$ROOT/venv-spark/bin/python
case $CFG in
  spark*) $RUN -- $SH/bin/spark-submit --master 'local[8]' --driver-memory 6g $S3A $STREAM $TFJARS ${TABLEFMT_PACKAGES:-} $ROOT/scripts/bench2_spark_api.py $COMMON > $OUT/engine.log 2>&1;;
  comet*) export MALLOC_ARENA_MAX=${COMET_MALLOC_ARENA_MAX:-2}  # adopted 2026-11-21 23:05 TR: default glibc arenas OOM at 9 GiB (F2)
    CJ=$COMET_JAR
    if [ -n "${TABLEFMT_JARS:-}" ]; then
      CJARS=$CJ,$TABLEFMT_JARS
      # --jars is comma-separated; extraClassPath is a JVM classpath (colon on Linux).
      # A comma here hid the Comet jar from native shuffle (SpillSorter classpath error on Delta Q3/Q12/Q18).
      CPATH=$CJ:$(echo "$TABLEFMT_JARS" | tr ',' ':')
    else
      CJARS=$CJ; CPATH=$CJ
    fi
    $RUN -- $SH/bin/spark-submit --master 'local[8]' --driver-memory ${COMET_HEAP:-2g} $S3A $STREAM --jars $CJARS --conf spark.driver.extraClassPath=$CPATH --conf spark.executor.extraClassPath=$CPATH \
      --conf spark.plugins=org.apache.spark.CometPlugin --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
      --conf spark.comet.shuffle.enabled=true --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=${COMET_OFFHEAP:-3g} ${COMET_EXTRA:-} \
      --conf spark.comet.explain.fallback.enabled=true $ROOT/scripts/bench2_spark_api.py $COMMON > $OUT/engine.log 2>&1;;
  sail-*) export AWS_ACCESS_KEY_ID=${BENCH_S3_KEY:?export BENCH_S3_KEY} AWS_SECRET_ACCESS_KEY=${BENCH_S3_SECRET:?export BENCH_S3_SECRET} AWS_ENDPOINT=http://127.0.0.1:$S3_PORT AWS_REGION=us-east-1 AWS_ALLOW_HTTP=true
    [ $CFG = sail-reorder ] && export SAIL_OPTIMIZER__ENABLE_JOIN_REORDER=true || export SAIL_OPTIMIZER__ENABLE_JOIN_REORDER=false
    $RUN --server "$ROOT/venv-sail/bin/sail spark server --port 50051" --server-wait-port 50051 -- \
      $ROOT/venv-sail/bin/python $ROOT/scripts/bench2_spark_api.py --remote sc://127.0.0.1:50051 $COMMON > $OUT/engine.log 2>&1
    # server died mid-run (e.g. OOM-killed): record the in-flight query as SERVER_DIED, restart server, run the rest (max 4 more segments)
    if [ $WL = tpch ]; then
      FULL=$(echo "${QARG:-}" | grep -oP '(?<=--queries )\S+'); FULL=${FULL:-$(seq -s, 1 22)}
      for SEG in 2 3 4 5; do
        REM=$($ROOT/venv-sail/bin/python $ROOT/scripts/sail_resume.py $OUT/queries.jsonl $OUT/summary.jsonl $FULL)
        [ -z "$REM" ] && break
        sleep 5
        $ROOT/venv-sail/bin/python $ROOT/scripts/run_one.py --label $CFG --rss-out $OUT/rss_seg$SEG.csv --summary-out $OUT/summary.jsonl --cgroup $CAP --timeout $TIMEOUT \
          --server "$ROOT/venv-sail/bin/sail spark server --port 50051" --server-wait-port 50051 -- \
          $ROOT/venv-sail/bin/python $ROOT/scripts/bench2_spark_api.py --remote sc://127.0.0.1:50051 $COMMON --queries $REM >> $OUT/engine.log 2>&1
        tail -n +2 $OUT/rss_seg$SEG.csv >> $OUT/rss.csv
      done
    fi;;
  duckdb) $RUN -- $ROOT/venv-sail/bin/python $ROOT/scripts/bench2_duckdb.py $COMMON > $OUT/engine.log 2>&1;;
esac
RC=$?
[ -n "$JOBTMP" ] && case "$JOBTMP" in $ROOT/tmp/spark/job-*) rm -rf -- "$JOBTMP";; esac
if [ $WL = etl ]; then $ROOT/venv-sail/bin/python $ROOT/scripts/verify_etl.py --etl-out $ETL --out $OUT/etl_verify.jsonl --label $CFG --job $OUT > $OUT/verify.log 2>&1; fi
echo "rc=$RC $CFG $COND $WL"
exit $RC
