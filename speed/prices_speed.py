# speed/prices_speed.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, coalesce, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC = os.getenv("PRICES_TOPIC", "prices_raw")

OUT_PATH = os.getenv("PRICES_SPEED_OUT", "data/lake/prices/speed")
CHECKPOINT = os.getenv("PRICES_SPEED_CHECKPOINT", "data/checkpoints/prices_speed")

schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("source", StringType()),
    StructField("ingest_ts", StringType()), 
])

def main():
    spark = (
        SparkSession.builder
        .appName("PricesSpeedLayer")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
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

    df = (
        df_kafka
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("d"))
        .select(
            col("d.symbol").alias("symbol"),
            col("d.price").alias("price"),
            coalesce(
                to_timestamp(col("d.ingest_ts"), "yyyy-MM-dd'T'HH:mm:ssX"),
                to_timestamp(col("d.ingest_ts")),  
                current_timestamp()
            ).alias("ingest_ts"),
            col("d.source").alias("source"),
        )
        .where(col("symbol").isNotNull() & col("price").isNotNull())
    )

    (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", OUT_PATH)
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()