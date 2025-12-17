from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

IN_PATH = "data/lake/social/reddit/speed"
OUT_PATH = "data/lake/social/reddit/batch_daily"

def main():
    spark = SparkSession.builder.appName("SocialBatchDaily").getOrCreate()
    df = spark.read.parquet(IN_PATH)

    out = (
        df.withColumn("day", to_date(col("created_ts")))
          .groupBy("ticker", "day")
          .count()
          .withColumnRenamed("count", "mentions")
    )

    out.write.mode("overwrite").parquet(OUT_PATH)
    spark.stop()

if __name__ == "__main__":
    main()