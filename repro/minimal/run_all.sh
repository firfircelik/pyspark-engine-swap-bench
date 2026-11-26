#!/bin/bash
# Runs every minimal reproducer on Spark 4.2.0 (JVM), Sail 0.7.3 (Spark Connect) and, for comet_tz_impact, Comet 1.2.0.
ROOT=${ROOT:-/workspace/engine-swap}; M=$ROOT/repro/minimal; cd $M
export JAVA_HOME=$(ls -d $ROOT/opt/jdk-21*) PYSPARK_PYTHON=$ROOT/venv-spark/bin/python PYSPARK_DRIVER_PYTHON=$ROOT/venv-spark/bin/python
SS=$ROOT/opt/spark-4.2.0-bin-hadoop3/bin/spark-submit; CJ=$ROOT/dist/comet-spark-spark4.2_2.13-1.2.0.jar
mkdir -p out
for f in c1_sail_stale_tzdata c4_sail_format_number_decimal c7_sail_exponent_string_to_decimal c8_sail_json_permissive_decimal_overflow c5_sail_empty_dataframe_write; do
  $SS --master 'local[2]' --py-files _common.py $f.py 2>/dev/null | grep -E "^(ENGINE|C[0-9])" > out/${f}_spark420.out
done
(exec $ROOT/venv-sail/bin/sail spark server --port 50051 > out/sail_server.log 2>&1) & SP=$!; sleep 4
for f in c1_sail_stale_tzdata c4_sail_format_number_decimal c7_sail_exponent_string_to_decimal c8_sail_json_permissive_decimal_overflow c5_sail_empty_dataframe_write; do
  $ROOT/venv-sail/bin/python $f.py sc://127.0.0.1:50051 2>&1 | grep -E "^(ENGINE|C[0-9])" > out/${f}_sail073.out
done
kill $SP; wait $SP 2>/dev/null
$SS --master 'local[2]' comet_tz_impact.py 2>/dev/null | grep -E "^(ENGINE|COMET|NATIVE)" > out/comet_tz_impact_spark420.out
$SS --master 'local[2]' --jars $CJ --conf spark.driver.extraClassPath=$CJ --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=1g comet_tz_impact.py > out/comet_tz_impact_comet120.full.log 2>&1
grep -E "^(ENGINE|COMET|NATIVE)" out/comet_tz_impact_comet120.full.log > out/comet_tz_impact_comet120.out
grep -i "timezone database" out/comet_tz_impact_comet120.full.log | head -3 > out/comet_tz_impact_comet120_warning.out
echo DONE
