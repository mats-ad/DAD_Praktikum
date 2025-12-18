# batch/social_batch_daily.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, count
from pyspark.sql.types import DoubleType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

IN_PATH = "data/lake/social/reddit/speed"
OUT_PATH = "data/lake/social/reddit/batch_daily"

def main():
    spark = SparkSession.builder.appName("SocialBatchDailySentiment").getOrCreate()

    df = spark.read.parquet(IN_PATH)

    analyzer = SentimentIntensityAnalyzer()

    def sentiment(text):
        if not text:
            return 0.0
        return analyzer.polarity_scores(text)["compound"]

    sentiment_udf = spark.udf.register("sentiment", sentiment, DoubleType())

    df_sent = (
        df.withColumn("day", to_date(col("created_ts")))
          .withColumn("sentiment", sentiment_udf(col("full_text")))
    )

    out = (
        df_sent.groupBy("ticker", "day")
               .agg(
                   avg("sentiment").alias("avg_sentiment"),
                   count("*").alias("mentions")
               )
    )

    out.write.mode("overwrite").parquet(OUT_PATH)
    spark.stop()

if __name__ == "__main__":
    main()