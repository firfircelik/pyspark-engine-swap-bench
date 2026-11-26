"""C4: Sail 0.7.3 format_number converts DECIMAL (and every input) to f64 and formats with Rust {:.N}
(crates/sail-function/src/scalar/string/format_number.rs). Spark formats DECIMAL exactly with BigDecimal HALF_EVEN.
Expected (Spark 4.2.0): '0.00', '1.12', '2.68', '12,345,678,901,234,567.89'"""
from _common import session, show
spark, eng = session()
show(spark, "C4_format_number", """select
  format_number(cast(0.005 as decimal(5,3)), 2) tie_half_even,
  format_number(cast(1.115 as decimal(5,3)), 2) tie_binary_below,
  format_number(cast(2.675 as decimal(5,3)), 2) tie_binary_below2,
  format_number(cast('12345678901234567.89' as decimal(19,2)), 2) beyond_2_pow_53""")
spark.stop()
