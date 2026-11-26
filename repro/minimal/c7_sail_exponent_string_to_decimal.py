"""C7: Sail 0.7.3 cannot cast a string in scientific notation to DECIMAL (errors); Spark parses it (also try_cast).
Expected (Spark 4.2.0): 1000, 150.0, -0.020"""
from _common import session, show
spark, eng = session()
show(spark, "C7_cast", "select cast('1e3' as decimal(10,0)) a, cast('1.5E2' as decimal(10,1)) b, cast('-2e-2' as decimal(5,3)) c")
show(spark, "C7_try_cast", "select try_cast('1e3' as decimal(10,0)) a")
spark.stop()
