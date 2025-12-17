# streaming/social_streaming_reddit.py

import re
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, concat_ws, coalesce, lit, current_timestamp,
    from_unixtime, udf, explode, size, window, count, length
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
)


KAFKA_BROKER = "localhost:9092"
TOPIC_IN = "social_raw"

# Speed-Layer Discovery (optional)
TOPIC_TICKER_BUZZ = "ticker_buzz"

PARQUET_OUT = "data/lake/social/reddit_speed"          # roh + ticker
PARQUET_CHECKPOINT = "data/checkpoints/reddit_speed"
BUZZ_CHECKPOINT = "data/checkpoints/reddit_buzz"

TRACKED_TICKERS = {"AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "TSLA"}

# --- 1) Schema passend zu deinem Kafka JSON ---
schema = StructType([
    StructField("source", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("id", StringType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("permalink", StringType(), True),
])

# --- 2) Ticker-Extraktion ---
TICKER_DOLLAR_RE = re.compile(r"\$([A-Z]{1,5})\b")

tracked_upper = sorted([t.upper() for t in TRACKED_TICKERS])
tracked_word_res = [re.compile(rf"\b{re.escape(t)}\b", re.IGNORECASE) for t in tracked_upper]

def extract_tickers(text: str) -> List[str]:
    if not text:
        return []
    found = set()

    # $TSLA style
    for m in TICKER_DOLLAR_RE.finditer(text.upper()):
        found.add(m.group(1))

    # plain tickers from tracked set (AAPL, MSFT, ...)
    for t, pat in zip(tracked_upper, tracked_word_res):
        if pat.search(text):
            found.add(t)

    return sorted(found)

extract_tickers_udf = udf(extract_tickers, ArrayType(StringType()))

def main():
    spark = (
        SparkSession.builder
        .appName("RedditSpeedLayer")
        .config("spark.sql.shuffle.partitions", "4")
        # Kafka integration package (falls du es nicht per spark-submit --packages machst)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Kafka read
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_IN)
        .option("startingOffsets", "latest")
        .load()
    )

    df_value = df_kafka.selectExpr("CAST(value AS STRING) AS json_str")

    df_parsed = (
        df_value
        .select(col("json_str"), from_json(col("json_str"), schema).alias("d"))
        .select(
            col("json_str"),
            col("d.source").alias("source"),
            col("d.subreddit").alias("subreddit"),
            col("d.id").alias("id"),
            col("d.created_utc").alias("created_utc"),
            col("d.title").alias("title"),
            col("d.text").alias("text"),
            col("d.score").alias("score"),
            col("d.num_comments").alias("num_comments"),
            col("d.permalink").alias("permalink"),
        )
    )

    # full_text + timestamps
    df_enriched = (
        df_parsed
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("created_ts", from_unixtime(col("created_utc")).cast("timestamp"))
        .withColumn(
            "full_text",
            concat_ws(
                "\n",
                coalesce(col("title"), lit("")),
                coalesce(col("text"), lit(""))
            )
        )
        .withColumn("tickers", extract_tickers_udf(col("full_text")))
        .withColumn("ticker_count", size(col("tickers")))
    )

    # Relevance-Filter: nur Posts mit mindestens einem Ticker
    df_relevant = df_enriched.where(col("ticker_count") > 0)

    # Pro Ticker eine Zeile (für Features / Aggregationen)
    df_exploded = (
        df_relevant
        .withColumn("ticker", explode(col("tickers")))
        .select(
            "subreddit", "ticker",
            "score", "num_comments",
            "created_ts", "ingest_ts",
            "title", "permalink", "full_text"
        )
        .withColumn("text_length", length(col("full_text")))
    )

    # --- 3a) Console sink (Debug / Durchstich) ---
    console_query = (
        df_exploded.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )

    # --- 3b) Parquet sink (Data Lake / Speed Layer) ---
    parquet_query = (
        df_exploded.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", PARQUET_OUT)
        .option("checkpointLocation", PARQUET_CHECKPOINT)
        .start()
    )

    # --- 4) Optional: ticker_buzz (für Auto-Ticker-Discovery später) ---
    # Hier zählen wir Mentions pro Ticker im 10-Min-Fenster
    df_buzz = (
        df_exploded
        .withWatermark("created_ts", "1 hour")
        .groupBy(window(col("created_ts"), "10 minutes"), col("ticker"))
        .agg(count(lit(1)).alias("mentions"))
        .selectExpr(
            "to_json(named_struct('ticker', ticker, 'mentions', mentions, 'window_start', window.start, 'window_end', window.end)) as value"
        )
    )

    buzz_query = (
        df_buzz.writeStream
        .format("kafka")
        .outputMode("update")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", TOPIC_TICKER_BUZZ)
        .option("checkpointLocation", BUZZ_CHECKPOINT)
        .start()
    )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()