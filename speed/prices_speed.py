from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, coalesce, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BROKER = "localhost:9092"
TOPIC = "prices_raw"

OUT_PATH = "data/lake/prices/speed"
CHECKPOINT = "data/checkpoints/prices_speed"

schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("source", StringType()),
    StructField("ingest_ts", StringType()),  # ISO string
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
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    df = (
        df_kafka.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("d"))
        .select(
            col("d.symbol").alias("symbol"),
            col("d.price").alias("price"),
            coalesce(to_timestamp(col("d.ingest_ts")), current_timestamp()).alias("ingest_ts")
        )
        .where(col("symbol").isNotNull())
    )

    (df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", OUT_PATH)
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()