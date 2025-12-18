from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, max as spark_max

IN_PATH = "data/lake/prices/speed"
OUT_PATH = "data/lake/prices/batch_daily"

def main():
    spark = SparkSession.builder.appName("PricesBatchDaily").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(IN_PATH)

    df = (
        df.withColumn("ingest_ts", to_timestamp(col("ingest_ts")))
          .where(
              col("symbol").isNotNull() &
              col("price").isNotNull() &
              col("ingest_ts").isNotNull()
          )
          .withColumn("day", to_date(col("ingest_ts")))
    )

    df_a = df.alias("a")

    ts_max = (
        df.groupBy("symbol", "day")
          .agg(spark_max("ingest_ts").alias("max_ingest_ts"))
          .alias("b")
    )

    last_rows = (
        df_a.join(
            ts_max,
            (col("a.symbol") == col("b.symbol")) &
            (col("a.day") == col("b.day")) &
            (col("a.ingest_ts") == col("b.max_ingest_ts")),
            "inner"
        )
        .select(
            col("a.symbol").alias("symbol"),
            col("a.day").alias("day"),
            col("a.price").alias("price_day_last")
        )
    )

    (
        last_rows
        .write
        .mode("overwrite")
        .parquet(OUT_PATH)
    )

    print("[OK] prices_batch_daily written")
    spark.stop()

if __name__ == "__main__":
    main()