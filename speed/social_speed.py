import re
from typing import List
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, current_timestamp,
    from_unixtime, concat_ws, size, length
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, ArrayType
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC = os.getenv("SOCIAL_TOPIC", "social_raw")

OUT_PATH = os.getenv("SOCIAL_SPEED_OUT", "data/lake/social/reddit/speed")
CHECKPOINT = os.getenv("SOCIAL_SPEED_CHECKPOINT", "data/checkpoints/social_speed")

TRACKED_TICKERS = {"AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "TSLA"}

schema = StructType([
    StructField("source", StringType()),
    StructField("subreddit", StringType()),
    StructField("id", StringType()),
    StructField("created_utc", DoubleType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("score", IntegerType()),
    StructField("num_comments", IntegerType()),
    StructField("permalink", StringType()),
])

TICKER_RE = re.compile(r"\b(" + "|".join(TRACKED_TICKERS) + r")\b", re.IGNORECASE)

def extract_tickers(text: str) -> List[str]:
    if not text:
        return []
    return sorted(set(m.group(1).upper() for m in TICKER_RE.finditer(text)))

def main():
    spark = (
        SparkSession.builder
        .appName("SocialSpeedLayer")
        # WICHTIG: Spark 3.5.x + Scala 2.12
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    df_parsed = (
        df_kafka
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("d"))
        .select("d.*")
    )

    extract_udf = spark.udf.register(
        "extract_tickers",
        extract_tickers,
        ArrayType(StringType())
    )

    df_enriched = (
        df_parsed
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("created_ts", from_unixtime(col("created_utc")).cast("timestamp"))
        .withColumn("full_text", concat_ws("\n", col("title"), col("text")))
        .withColumn("tickers", extract_udf(col("full_text")))
        .where(size(col("tickers")) > 0)
        .withColumn("ticker", explode(col("tickers")))
        .withColumn("text_length", length(col("full_text")))
    )

    (
        df_enriched.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", OUT_PATH)
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()