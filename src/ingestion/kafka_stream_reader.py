"""
Streaming ingestion layer: reads raw transaction events from Kafka /
Azure Event Hubs and writes them, unmodified, to the Bronze Delta table.
"""

import os

from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.utils.spark_session import get_spark_session

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("merchant", StringType(), True),
        StructField("country", StringType(), True),
        StructField("event_timestamp", TimestampType(), False),
    ]
)


def read_kafka_stream(spark, kafka_bootstrap_servers: str, topic: str):
    """Read a raw Kafka topic as a Spark structured streaming DataFrame."""
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data")
    ).select("data.*")

    return parsed_df.withColumn("ingestion_timestamp", current_timestamp())


def write_to_bronze(stream_df, checkpoint_path: str, bronze_table_path: str):
    """Write the streaming DataFrame to the Bronze Delta Lake table."""
    return (
        stream_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start(bronze_table_path)
    )


def main():
    kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "transactions")
    checkpoint_path = os.environ.get("BRONZE_CHECKPOINT_PATH", "/tmp/checkpoints/bronze")
    bronze_table_path = os.environ.get("BRONZE_TABLE_PATH", "/tmp/delta/bronze/transactions")

    spark = get_spark_session()
    stream_df = read_kafka_stream(spark, kafka_bootstrap_servers, topic)
    query = write_to_bronze(stream_df, checkpoint_path, bronze_table_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()
