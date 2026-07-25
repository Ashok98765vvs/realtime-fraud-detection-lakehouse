"""
Silver -> Gold transformation.

Aggregates cleansed transaction data into fraud-risk feature sets:
per-account transaction velocity, rolling average spend, and
z-score anomaly features used by the real-time scoring service.
"""

import os

from pyspark.sql.functions import avg, col, count, stddev, window

from src.utils.spark_session import get_spark_session


def load_silver(spark, silver_table_path: str):
    """Load the Silver Delta table as a streaming DataFrame."""
    return spark.readStream.format("delta").load(silver_table_path)


def compute_velocity_features(silver_df):
    """Compute per-account transaction velocity over 5-minute windows."""
    return (
        silver_df.withWatermark("event_timestamp", "10 minutes")
        .groupBy("account_id", window(col("event_timestamp"), "5 minutes"))
        .agg(
            count("transaction_id").alias("txn_count"),
            avg("amount").alias("avg_amount"),
            stddev("amount").alias("stddev_amount"),
        )
    )


def write_to_gold(gold_df, checkpoint_path: str, gold_table_path: str):
    """Write aggregated fraud-risk features to the Gold Delta Lake table."""
    return (
        gold_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start(gold_table_path)
    )


def main():
    silver_table_path = os.environ.get("SILVER_TABLE_PATH", "/tmp/delta/silver/transactions")
    gold_table_path = os.environ.get("GOLD_TABLE_PATH", "/tmp/delta/gold/fraud_features")
    checkpoint_path = os.environ.get("GOLD_CHECKPOINT_PATH", "/tmp/checkpoints/gold")

    spark = get_spark_session()
    silver_df = load_silver(spark, silver_table_path)
    gold_df = compute_velocity_features(silver_df)
    query = write_to_gold(gold_df, checkpoint_path, gold_table_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()
