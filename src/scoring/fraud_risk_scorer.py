"""
Real-time fraud risk scoring service.

Consumes Gold-layer aggregated features and produces a fraud-risk score
per account per window using a simple weighted rules engine. Designed
to plug into a streaming alerting sink for sub-minute notification.
"""

import os

from pyspark.sql.functions import col, when

from src.utils.spark_session import get_spark_session

HIGH_VELOCITY_THRESHOLD = 10       # transactions in a 5-minute window
HIGH_AMOUNT_STDDEV_THRESHOLD = 500  # stddev of transaction amount
RISK_ALERT_THRESHOLD = 0.7


def load_gold(spark, gold_table_path: str):
    """Load Gold-layer fraud-risk features as a streaming DataFrame."""
    return spark.readStream.format("delta").load(gold_table_path)


def compute_risk_score(gold_df):
    """
    Compute a weighted fraud-risk score in [0, 1] based on transaction
    velocity and amount volatility signals.
    """
    velocity_score = when(
        col("txn_count") >= HIGH_VELOCITY_THRESHOLD, 0.5
    ).otherwise(col("txn_count") / HIGH_VELOCITY_THRESHOLD * 0.5)

    volatility_score = when(
        col("stddev_amount") >= HIGH_AMOUNT_STDDEV_THRESHOLD, 0.5
    ).otherwise(col("stddev_amount") / HIGH_AMOUNT_STDDEV_THRESHOLD * 0.5)

    scored_df = gold_df.withColumn("risk_score", velocity_score + volatility_score)
    return scored_df.withColumn(
        "is_fraud_alert", col("risk_score") >= RISK_ALERT_THRESHOLD
    )


def write_alerts(scored_df, checkpoint_path: str, alerts_table_path: str):
    """Write flagged fraud alerts to a dedicated Delta table for downstream consumers."""
    alerts_df = scored_df.filter(col("is_fraud_alert"))
    return (
        alerts_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start(alerts_table_path)
    )


def main():
    gold_table_path = os.environ.get("GOLD_TABLE_PATH", "/tmp/delta/gold/fraud_features")
    alerts_table_path = os.environ.get("ALERTS_TABLE_PATH", "/tmp/delta/gold/fraud_alerts")
    checkpoint_path = os.environ.get("ALERTS_CHECKPOINT_PATH", "/tmp/checkpoints/alerts")

    spark = get_spark_session()
    gold_df = load_gold(spark, gold_table_path)
    scored_df = compute_risk_score(gold_df)
    query = write_alerts(scored_df, checkpoint_path, alerts_table_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()
