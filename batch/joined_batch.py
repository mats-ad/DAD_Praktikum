# batch/joined_daily.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

LAKE_ROOT = os.getenv("LAKE_ROOT", "data/lake")

PRICES = os.path.join(LAKE_ROOT, "prices", "batch_daily")
SOCIAL = os.path.join(LAKE_ROOT, "social", "reddit", "batch_daily")
OUT = os.path.join(LAKE_ROOT, "joined", "daily")

def main():
    spark = SparkSession.builder.appName("JoinedDaily").getOrCreate()

    prices = spark.read.parquet(PRICES)
    social = spark.read.parquet(SOCIAL)

    # prices: symbol -> ticker
    prices = prices.withColumnRenamed("symbol", "ticker")

    joined = (
        prices.join(social, on=["ticker", "day"], how="left")
              .fillna({"avg_sentiment": 0.0, "mentions": 0})
    )

    joined.write.mode("overwrite").parquet(OUT)
    spark.stop()

if __name__ == "__main__":
    main()