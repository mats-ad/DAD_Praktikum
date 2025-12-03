from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, get_json_object

KAFKA_BROKER = "localhost:9092"
TOPIC = "prices_raw"

OUTPUT_PATH = "data/features/prices_speed"

spark = (
    SparkSession.builder
    .appName("AlphaVantageSpeedLayer")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1) Kafka-Stream einlesen
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .load()
)

# 2) Value als String casten
df_value = df_kafka.selectExpr("CAST(value AS STRING) AS json_str")

# 3) Beispiel-Features: Symbol + JSON-Länge
df_features = df_value.select(
    get_json_object(col("json_str"), "$.symbol").alias("symbol"),
    col("json_str")
).withColumn("json_length", length(col("json_str")))

# ---- Sink 1: Konsole ----
console_query = (
    df_features.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

# ---- Sink 2: Parquet ----
parquet_query = (
    df_features.writeStream
    .format("parquet")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", "data/checkpoints/prices_speed")
    .outputMode("append")
    .start()
)

# Warten, bis einer der Streams beendet wird
spark.streams.awaitAnyTermination()
