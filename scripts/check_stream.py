import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
jvm = spark._jvm; conf = spark._jsc.hadoopConfiguration()
p = jvm.org.apache.hadoop.fs.Path("s3a://tpch/tpch-sf10/nation/nation.1.parquet")
fs = p.getFileSystem(conf)
st = fs.open(p)
w = st.getWrappedStream()
buf = bytearray(4)
st.seek(0); n = st.read(0, jvm.java.lang.reflect.Array.newInstance(jvm.java.lang.Byte.TYPE, 4), 0, 4)
print("STREAMCHECK version", spark.version, "conf=", conf.get("fs.s3a.input.stream.type"), "class=", w.getClass().getName(), "read=", n)
try:
    print("STREAMCHECK stats", str(w.getIOStatistics())[:600])
except Exception as e: print("STREAMCHECK nostats", e)
st.close(); spark.stop()
