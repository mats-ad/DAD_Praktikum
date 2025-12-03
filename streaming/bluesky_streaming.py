from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, current_timestamp, explode, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

KAFKA_BROKER = "localhost:9092"
TOPIC = "social_raw"
OUTPUT_PATH = "data/features/social_speed"

spark = (
    SparkSession.builder
    .appName("SocialSpeedLayer")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("source", StringType()),
    StructField("uri", StringType()),
    StructField("author_did", StringType()),
    StructField("author_handle", StringType()),
    StructField("text", StringType()),
    StructField("created_at", StringType()),
    StructField("tickers", ArrayType(StringType())),
    StructField("like_count", LongType()),
    StructField("repost_count", LongType()),
    StructField("reply_count", LongType()),
])

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .load()
)

df_value = df_kafka.selectExpr("CAST(value AS STRING) AS json_str")

from pyspark.sql.functions import from_json

df_parsed = df_value.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

df_exploded = df_parsed.withColumn("ticker", explode(col("tickers")))

df_features = df_exploded \
    .withColumn("text_length", length(col("text"))) \
    .withColumn("created_ts", to_timestamp(col("created_at"))) \
    .withColumn("created_date", to_date(col("created_ts"))) \
    .withColumn("ingest_ts", current_timestamp())

console_query = (
    df_features.select("ticker", "author_handle", "text_length", "like_count", "repost_count", "reply_count", "created_ts")
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

parquet_query = (
    df_features.writeStream
    .format("parquet")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", "data/checkpoints/social_speed")
    .outputMode("append")
    .start()
)

spark.streams.awaitAnyTermination()
