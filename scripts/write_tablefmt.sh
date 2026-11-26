#!/bin/bash
# Write SF1 or SF30 parquet tables as Iceberg (Spark 4.1.3) or Delta (Spark 4.2.0) on MinIO.
# Usage: write_tablefmt.sh iceberg|delta sf1|sf30
set -euo pipefail
ROOT=${ROOT:-/workspace/engine-swap}
FMT=${1:?format iceberg|delta}
SF=${2:?sf1|sf30}
OUTDIR=$ROOT/results/tablefmt/write/${FMT}-${SF}
mkdir -p "$OUTDIR" $ROOT/tmp/spark
JOBTMP=$ROOT/tmp/spark/write-$$-$(date +%s); mkdir -p "$JOBTMP"
export JAVA_HOME=$(ls -d $ROOT/opt/jdk-21*); export PATH=$JAVA_HOME/bin:$PATH
export PYSPARK_PYTHON=$ROOT/venv-spark/bin/python PYSPARK_DRIVER_PYTHON=$ROOT/venv-spark/bin/python
CAP=${CAP:-/sys/fs/cgroup/engine-swap-bench}
TIMEOUT=${TIMEOUT:-7200}
SRC=file://$ROOT/data/tpch-$SF
S3A="--conf spark.hadoop.fs.s3a.endpoint=http://127.0.0.1:9000
 --conf spark.hadoop.fs.s3a.path.style.access=true
 --conf spark.hadoop.fs.s3a.access.key=${BENCH_S3_KEY:?export BENCH_S3_KEY}
 --conf spark.hadoop.fs.s3a.secret.key=${BENCH_S3_SECRET:?export BENCH_S3_SECRET}
 --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
 --conf spark.hadoop.fs.s3a.endpoint.region=us-east-1
 --conf spark.ui.enabled=false
 --conf spark.sql.session.timeZone=UTC
 --conf spark.local.dir=$JOBTMP
 --conf spark.driver.extraJavaOptions=-Djava.io.tmpdir=$JOBTMP
 --conf spark.hadoop.fs.s3a.committer.name=directory
 --conf spark.hadoop.fs.s3a.buffer.dir=$ROOT/tmp/spark
 --conf spark.jars.ivy=$ROOT/opt/ivy"

if [ "$FMT" = iceberg ]; then
  SH=$ROOT/opt/spark-4.1.3-bin-hadoop3
  JARS="--jars $ROOT/dist/iceberg-spark-runtime-4.1_2.13-1.12.0.jar"
  EXT="--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
   --conf spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog
   --conf spark.sql.catalog.ice.type=hadoop
   --conf spark.sql.catalog.ice.warehouse=s3a://tpch/iceberg
   --conf spark.sql.catalog.ice.io-impl=org.apache.iceberg.hadoop.HadoopFileIO"
else
  SH=$ROOT/opt/spark-4.2.0-bin-hadoop3
  JARS="--jars $ROOT/dist/delta-spark_4.2_2.13-4.4.1.jar,$ROOT/dist/delta-storage-4.4.1.jar,$ROOT/dist/delta-kernel-api-4.4.1.jar,$ROOT/dist/delta-kernel-defaults-4.4.1.jar,$ROOT/dist/delta-kernel-unitycatalog-4.4.1.jar"
  EXT="--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
fi

# Honor the same 9 GiB cap as the benchmark runs.
$ROOT/venv-sail/bin/python $ROOT/scripts/run_one.py --label write-$FMT-$SF \
  --rss-out $OUTDIR/rss.csv --summary-out $OUTDIR/summary.jsonl --cgroup $CAP --timeout $TIMEOUT -- \
  $SH/bin/spark-submit --master 'local[8]' --driver-memory 6g $S3A $JARS $EXT \
    $ROOT/scripts/write_tablefmt.py --format $FMT --sf $SF --src $SRC --out $OUTDIR/write.jsonl \
  > $OUTDIR/engine.log 2>&1
RC=$?
rm -rf -- "$JOBTMP"
echo "rc=$RC write $FMT $SF -> $OUTDIR"
exit $RC
