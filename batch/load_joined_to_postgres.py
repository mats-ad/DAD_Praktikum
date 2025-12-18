import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.utils import AnalysisException

# ---------- Paths ----------
LAKE_ROOT = os.getenv("LAKE_ROOT", "/data/lake")
JOINED_PATH = os.getenv("JOINED_PATH", os.path.join(LAKE_ROOT, "joined", "daily"))

# ---------- Postgres JDBC ----------
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "gabi")
PG_USER = os.getenv("PG_USER", "gabi")
PG_PASSWORD = os.getenv("PG_PASSWORD", "gabi_pw")

PG_URL = os.getenv("PG_URL", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")

PG_TABLE = os.getenv("PG_TABLE", "joined_daily")
PG_STAGE = os.getenv("PG_STAGE_TABLE", f"{PG_TABLE}__staging")

INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "300"))  # default 5 min


def exec_sql_in_postgres(spark: SparkSession, sql: str) -> None:
    """
    Execute SQL via JDBC using Spark's JVM (no psql needed).
    """
    jvm = spark._jvm

    props = jvm.java.util.Properties()
    props.setProperty("user", PG_USER)
    props.setProperty("password", PG_PASSWORD)

    conn = jvm.java.sql.DriverManager.getConnection(PG_URL, props)
    try:
        stmt = conn.createStatement()
        try:
            stmt.execute(sql)
        finally:
            stmt.close()
    finally:
        conn.close()


def run_once(spark: SparkSession) -> None:
    # 1) Read parquet
    try:
        df = spark.read.parquet(JOINED_PATH)
    except AnalysisException:
        print(f"[WAIT] Parquet not found yet: {JOINED_PATH}")
        return

    # 2) Normalize schema
    df = (
        df.withColumn("day", to_date(col("day")))
        .select("ticker", "day", "price_day_last", "avg_sentiment", "mentions")
    )

    # 3) Write into staging (overwrite is OK here)
    (
        df.write.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", PG_STAGE)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    # 4) Atomic-ish swap into target without dropping target table
    #    - lock target to avoid readers seeing half state
    #    - truncate and insert
    swap_sql = f"""
    BEGIN;

      -- ensure staging exists (Spark creates it anyway)
      -- keep target table (no DROP!), because views depend on it

      LOCK TABLE {PG_TABLE} IN EXCLUSIVE MODE;

      TRUNCATE TABLE {PG_TABLE};

      INSERT INTO {PG_TABLE} (ticker, day, price_day_last, avg_sentiment, mentions)
      SELECT ticker, day, price_day_last, avg_sentiment, mentions
      FROM {PG_STAGE};

    COMMIT;
    """

    exec_sql_in_postgres(spark, swap_sql)
    print("[OK] joined_daily refreshed via staging + truncate/insert (no drop)")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("LoadJoinedDailyToPostgresLoop")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    while True:
        try:
            run_once(spark)
        except Exception as e:
            # nicht crashen -> loop soll weiterlaufen
            print(f"[ERR] run_once failed: {e}")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()