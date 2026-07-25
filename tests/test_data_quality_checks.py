"""Unit tests for the data quality validation framework."""

import pytest
from pyspark.sql import SparkSession

from src.quality.data_quality_checks import (
    check_duplicate_keys,
    check_null_rates,
    check_schema_drift,
    run_quality_checks,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]").appName("test-data-quality").getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_df(spark):
    data = [
        ("t1", "a1", 100.0, "USD", "2024-01-01 00:00:00"),
        ("t2", "a2", 200.0, "USD", "2024-01-01 00:01:00"),
        ("t3", None, 50.0, "EUR", "2024-01-01 00:02:00"),
    ]
    columns = ["transaction_id", "account_id", "amount", "currency", "event_timestamp"]
    return spark.createDataFrame(data, columns)


def test_check_schema_drift_passes_with_all_columns(sample_df):
    result = check_schema_drift(
        sample_df, ["transaction_id", "account_id", "amount", "currency", "event_timestamp"]
    )
    assert result.passed is True


def test_check_schema_drift_fails_with_missing_column(sample_df):
    result = check_schema_drift(sample_df, ["transaction_id", "nonexistent_column"])
    assert result.passed is False
    assert "nonexistent_column" in result.details


def test_check_null_rates_flags_violations(sample_df):
    result = check_null_rates(sample_df, ["account_id"], threshold=0.1)
    assert result.passed is False


def test_check_duplicate_keys_no_duplicates(sample_df):
    result = check_duplicate_keys(sample_df, "transaction_id")
    assert result.passed is True


def test_run_quality_checks_splits_valid_and_invalid(sample_df):
    valid_df, invalid_df = run_quality_checks(sample_df)
    assert valid_df.count() == 2
    assert invalid_df.count() == 1
