#!/bin/bash
# Runs API probe v2 (Spark 4.2.0 vs Sail 0.7.3) and Comet tz probe. Not timed; must not overlap benchmark jobs.
ROOT=${ROOT:-/workspace/engine-swap}; cd $ROOT
export JAVA_HOME=$(ls -d $ROOT/opt/jdk-21*) PYSPARK_PYTHON=$ROOT/venv-spark/bin/python PYSPARK_DRIVER_PYTHON=$ROOT/venv-spark/bin/python
R=$ROOT/results/probes; mkdir -p $R $ROOT/tmp/probe2
for TZ_ in Europe/Istanbul America/Havana; do
  T=$(echo $TZ_ | tr / _)
  (cd $ROOT/tmp/probe2 && $ROOT/opt/spark-4.2.0-bin-hadoop3/bin/spark-submit --master 'local[4]' --driver-memory 3g --conf spark.sql.warehouse.dir=$ROOT/tmp/probe2/wh-spark-$T \
     $ROOT/probes/api_probe2.py --label spark420 --tz $TZ_ --out $R/api_probe2.jsonl > $R/api_probe2_spark_$T.log 2>&1)
  (cd $ROOT/tmp/probe2 && exec $ROOT/venv-sail/bin/sail spark server --port 50051 > $R/sail_server_$T.log 2>&1) & SP=$!
  sleep 3
  $ROOT/venv-sail/bin/python $ROOT/probes/api_probe2.py --remote sc://127.0.0.1:50051 --label sail073 --tz $TZ_ --out $R/api_probe2.jsonl > $R/api_probe2_sail_$T.log 2>&1
  kill $SP; wait $SP 2>/dev/null
done
if [ ! -s $R/comet_tz.jsonl ]; then
CJ=$ROOT/dist/comet-spark-spark4.2_2.13-1.2.0.jar
$ROOT/opt/spark-4.2.0-bin-hadoop3/bin/spark-submit --master 'local[2]' $ROOT/probes/comet_tz_probe.py spark420 $R/comet_tz.jsonl $ROOT/tmp/tzdata_pq > $R/comet_tz_spark.log 2>&1
$ROOT/opt/spark-4.2.0-bin-hadoop3/bin/spark-submit --master 'local[2]' --jars $CJ --conf spark.driver.extraClassPath=$CJ --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=2g --conf spark.comet.explain.fallback.enabled=true \
  $ROOT/probes/comet_tz_probe.py comet120 $R/comet_tz.jsonl $ROOT/tmp/tzdata_pq > $R/comet_tz_comet.log 2>&1
fi
echo PROBES DONE
