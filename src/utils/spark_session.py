"""
Utility module for creating and configuring a Spark session with
Delta Lake support for the fraud detection lakehouse pipeline.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "realtime-fraud-detection-lakehouse") -> SparkSession:
    """
    Build and return a SparkSession configured with Delta Lake extensions.

    Args:
        app_name: Name to register the Spark application under.

    Returns:
        A configured SparkSession instance.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.streaming.schemaInference", "true")
    )

    return builder.getOrCreate()


def stop_spark_session(spark: SparkSession) -> None:
    """Gracefully stop the given SparkSession."""
    if spark is not None:
        spark.stop()
