"""Shared env fragments for Iceberg/Delta jobs (Spark/Comet jars + conf)."""
ROOT = "/workspace/engine-swap"
DIST = f"{ROOT}/dist"
ICEBERG_JAR = f"{DIST}/iceberg-spark-runtime-4.1_2.13-1.12.0.jar"
DELTA_COMMON = ",".join([
    f"{DIST}/delta-storage-4.4.1.jar",
    f"{DIST}/delta-kernel-api-4.4.1.jar",
    f"{DIST}/delta-kernel-defaults-4.4.1.jar",
    f"{DIST}/delta-kernel-unitycatalog-4.4.1.jar",
])
DELTA_JARS = {
    "4.1": f"{DIST}/delta-spark_4.1_2.13-4.4.1.jar,{DELTA_COMMON}",
    "4.2": f"{DIST}/delta-spark_4.2_2.13-4.4.1.jar,{DELTA_COMMON}",
}
ICEBERG_CONF = " ".join([
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog",
    "--conf spark.sql.catalog.ice.type=hadoop",
    "--conf spark.sql.catalog.ice.warehouse=s3a://tpch/iceberg",
    "--conf spark.sql.catalog.ice.io-impl=org.apache.iceberg.hadoop.HadoopFileIO",
])
DELTA_CONF = " ".join([
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
])
# Spark line used by each cfg
SPARK_LINE = {
    "spark413-classic": "4.1",
    "spark420-classic": "4.2",
    "comet413-classic": "4.1",
    "comet420-classic": "4.2",
}

def job_env(cfg, fmt, sf_base="sf30"):
    env = {
        "FORMAT": fmt,
        "TABLEBASE": f"s3a://tpch/{'iceberg' if fmt == 'iceberg' else 'delta'}/{sf_base}",
        "WARMUP_BASE": f"s3a://tpch/{'iceberg' if fmt == 'iceberg' else 'delta'}/sf1",
        "QARG": "--queries 1,3,6,12,18",
    }
    if cfg.startswith(("spark", "comet")):
        if fmt == "iceberg":
            env["TABLEFMT_JARS"] = ICEBERG_JAR
            env["EXTRA_CONF"] = ICEBERG_CONF
        else:
            line = SPARK_LINE[cfg]
            env["TABLEFMT_JARS"] = DELTA_JARS[line]
            env["EXTRA_CONF"] = DELTA_CONF
    return env
