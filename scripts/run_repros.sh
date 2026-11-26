#!/bin/bash
ROOT=${ROOT:-/workspace/engine-swap}; cd $ROOT
export JAVA_HOME=$(ls -d $ROOT/opt/jdk-21*) PYSPARK_PYTHON=$ROOT/venv-spark/bin/python PYSPARK_DRIVER_PYTHON=$ROOT/venv-spark/bin/python
R=$ROOT/repro
$ROOT/opt/spark-4.2.0-bin-hadoop3/bin/spark-submit --master 'local[2]' $R/sail_new_candidates.py 2>/dev/null | grep -E "^(ENGINE|C[0-9])" > $R/sail_new_candidates_spark420.out
(exec $ROOT/venv-sail/bin/sail spark server --port 50051 > /dev/null 2>&1) & SP=$!; sleep 3
$ROOT/venv-sail/bin/python $R/sail_new_candidates.py sc://127.0.0.1:50051 2>&1 | grep -E "^(ENGINE|C[0-9])" > $R/sail_new_candidates_sail073.out
kill $SP; wait $SP 2>/dev/null
P=$ROOT/results/probes; CJ=$ROOT/dist/comet-spark-spark4.2_2.13-1.2.0.jar
$ROOT/opt/spark-4.2.0-bin-hadoop3/bin/spark-submit --master 'local[2]' $ROOT/probes/comet_tz_probe.py spark420 $P/comet_tz_v2.jsonl $ROOT/tmp/tzdata_pq > $P/comet_tz_v2_spark.log 2>&1
$ROOT/opt/spark-4.2.0-bin-hadoop3/bin/spark-submit --master 'local[2]' --jars $CJ --conf spark.driver.extraClassPath=$CJ --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=2g --conf spark.comet.explain.fallback.enabled=true \
  $ROOT/probes/comet_tz_probe.py comet120 $P/comet_tz_v2.jsonl $ROOT/tmp/tzdata_pq > $P/comet_tz_v2_comet.log 2>&1
echo REPROS DONE
