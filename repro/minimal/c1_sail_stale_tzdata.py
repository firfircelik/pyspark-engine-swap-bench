"""C1: Sail 0.7.3 uses chrono-tz 0.10.4 (IANA tzdata 2025b) for session/explicit time zones; Spark uses the JVM tzdb
(Temurin 21.0.13 ships 2026f). Rule changes after 2025b give silently different local times (1 h), no warning.
Expected (Spark 4.2.0, JDK tzdata 2026f): 14:00, 12:00, 12:00, 12:00, 15:00 / session Havana: 14:00, 14
Changes used: Cuba keeps UTC-4 after 2026-11-01 (2026f); Alberta and NWT stay on UTC-6 (2026c/d); Morocco permanent UTC+0 (2026c)."""
from _common import session, show
spark, eng = session()
spark.conf.set("spark.sql.session.timeZone", "UTC")
show(spark, "C1_from_utc_timestamp", """select
  cast(from_utc_timestamp(timestamp'2026-11-21 18:00:00', 'America/Havana') as string) havana,
  cast(from_utc_timestamp(timestamp'2027-01-15 18:00:00', 'America/Edmonton') as string) edmonton,
  cast(from_utc_timestamp(timestamp'2027-01-15 18:00:00', 'America/Yellowknife') as string) yellowknife,
  cast(from_utc_timestamp(timestamp'2027-04-01 12:00:00', 'Africa/Casablanca') as string) casablanca,
  cast(from_utc_timestamp(timestamp'2025-06-01 12:00:00', 'Europe/Istanbul') as string) control_istanbul""")
spark.conf.set("spark.sql.session.timeZone", "America/Havana")
show(spark, "C1_session_tz_havana", "select cast(to_timestamp('2026-11-21T18:00:00Z') as string) local_str, hour(to_timestamp('2026-11-21T18:00:00Z')) h")
spark.stop()
