"""
Bronze -> Silver transformation.

Cleanses raw transaction events, deduplicates by transaction_id,
enriches with derived fields, and routes failing records to a
quarantine table using the data quality framework.
"""

import os

from pyspark.sql.functions import col, upper

from src.quality.data_quality_checks import run_quality_checks
from src.utils.spark_session import get_spark_session


def load_bronze(spark, bronze_table_path: str):
    """Load the Bronze Delta table as a streaming DataFrame."""
    return spark.readStream.format("delta").load(bronze_table_path)


def clean_and_enrich(bronze_df):
    """Apply cleansing and enrichment rules to raw transactions."""
    deduped_df = bronze_df.dropDuplicates(["transaction_id"])

    enriched_df = (
        deduped_df.filter(col("amount") > 0)
        .withColumn("currency", upper(col("currency")))
        .withColumn("country", upper(col("country")))
    )

    return enriched_df


def write_to_silver(silver_df, checkpoint_path: str, silver_table_path: str):
    """Write validated records to the Silver Delta Lake table."""
    return (
        silver_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start(silver_table_path)
    )


def main():
    bronze_table_path = os.environ.get("BRONZE_TABLE_PATH", "/tmp/delta/bronze/transactions")
    silver_table_path = os.environ.get("SILVER_TABLE_PATH", "/tmp/delta/silver/transactions")
    quarantine_table_path = os.environ.get(
        "QUARANTINE_TABLE_PATH", "/tmp/delta/quarantine/transactions"
    )
    checkpoint_path = os.environ.get("SILVER_CHECKPOINT_PATH", "/tmp/checkpoints/silver")

    spark = get_spark_session()
    bronze_df = load_bronze(spark, bronze_table_path)
    enriched_df = clean_and_enrich(bronze_df)
    valid_df, invalid_df = run_quality_checks(enriched_df)

    write_to_silver(valid_df, checkpoint_path, silver_table_path)

    invalid_df.writeStream.format("delta").option(
        "checkpointLocation", checkpoint_path + "_quarantine"
    ).outputMode("append").start(quarantine_table_path).awaitTermination()


if __name__ == "__main__":
    main()
