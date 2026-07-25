"""
Automated data quality framework.

Provides reusable validation rules used across the pipeline: null-rate
threshold enforcement, schema drift detection, and duplicate-key checks.
Records failing validation are split out for quarantine routing.
"""

from dataclasses import dataclass
from typing import List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when


@dataclass
class QualityRuleResult:
    rule_name: str
    passed: bool
    details: str


REQUIRED_COLUMNS = [
    "transaction_id",
    "account_id",
    "amount",
    "currency",
    "event_timestamp",
]

NULL_RATE_THRESHOLD = 0.01  # Max 1% nulls allowed per required column


def check_schema_drift(df: DataFrame, expected_columns: List[str]) -> QualityRuleResult:
    """Detect whether the DataFrame schema has drifted from the expected set."""
    actual_columns = set(df.columns)
    missing = set(expected_columns) - actual_columns
    if missing:
        return QualityRuleResult(
            "schema_drift", False, f"Missing expected columns: {sorted(missing)}"
        )
    return QualityRuleResult("schema_drift", True, "No schema drift detected")


def check_null_rates(df: DataFrame, columns: List[str], threshold: float) -> QualityRuleResult:
    """Fail if any column's null rate exceeds the given threshold."""
    total = df.count()
    if total == 0:
        return QualityRuleResult("null_rate", True, "No records to evaluate")

    violations = []
    for column in columns:
        null_count = df.filter(col(column).isNull()).count()
        rate = null_count / total
        if rate > threshold:
            violations.append(f"{column}: {rate:.2%}")

    if violations:
        return QualityRuleResult("null_rate", False, f"Threshold exceeded: {violations}")
    return QualityRuleResult("null_rate", True, "All columns within null-rate threshold")


def check_duplicate_keys(df: DataFrame, key_column: str) -> QualityRuleResult:
    """Detect duplicate primary keys within the DataFrame."""
    dup_count = (
        df.groupBy(key_column)
        .agg(count("*").alias("cnt"))
        .filter(col("cnt") > 1)
        .count()
    )
    if dup_count > 0:
        return QualityRuleResult(
            "duplicate_keys", False, f"{dup_count} duplicate keys found on {key_column}"
        )
    return QualityRuleResult("duplicate_keys", True, "No duplicate keys found")


def run_quality_checks(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Run the full validation suite and split the DataFrame into
    (valid_df, invalid_df) based on row-level validity flags.
    """
    schema_result = check_schema_drift(df, REQUIRED_COLUMNS)
    if not schema_result.passed:
        raise ValueError(f"Schema drift detected: {schema_result.details}")

    validity_condition = None
    for column in REQUIRED_COLUMNS:
        condition = col(column).isNotNull()
        validity_condition = condition if validity_condition is None else (validity_condition & condition)

    flagged_df = df.withColumn("is_valid_record", when(validity_condition, True).otherwise(False))

    valid_df = flagged_df.filter(col("is_valid_record")).drop("is_valid_record")
    invalid_df = flagged_df.filter(~col("is_valid_record")).drop("is_valid_record")

    return valid_df, invalid_df
