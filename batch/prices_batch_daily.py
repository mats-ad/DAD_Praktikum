from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, max as smax

IN_PATH = "data/lake/prices/speed"
OUT_PATH = "data/lake/prices/batch_daily"

def main():
    spark = SparkSession.builder.appName("PricesBatchDaily").getOrCreate()
    df = spark.read.parquet(IN_PATH)

    out = (
        df.withColumn("day", to_date(col("ingest_ts")))
          .groupBy("symbol", "day")
          .agg(smax("price").alias("price_day_last"))
    )

    out.write.mode("overwrite").parquet(OUT_PATH)
    spark.stop()

if __name__ == "__main__":
    main()