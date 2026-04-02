# Databricks notebook source
# Utils — Data Quality Assertion Framework
#
# Lightweight assertion functions used across all Silver transformation notebooks.
# Each function raises a ValueError with a descriptive message on failure,
# which halts the calling notebook and surfaces clearly in Databricks Workflows.
#
# Functions:
#   assert_no_nulls         — fails if any of the specified columns contain nulls
#   assert_no_duplicates    — fails if the specified columns are not unique
#   assert_min_row_count    — fails if the DataFrame has fewer rows than expected
#   assert_row_count_delta  — fails if Silver row count deviates too far from Bronze

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pyspark.sql import DataFrame as d

def assert_no_nulls(df: DataFrame, columns: list[str]) -> None:
    """
    Assert that none of the specified columns contain null values.

    Args:
        df      : DataFrame to check
        columns : list of column names to check for nulls

    Raises:
        ValueError if any column contains nulls, with a breakdown per column
    """
    null_counts = (
        df.select(
            [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in columns]
        )
        .collect()[0]
        .asDict()
    )
    failures = {col: cnt for col, cnt in null_counts.items() if cnt > 0}
    if failures:
        raise ValueError(
            f"assert_no_nulls failed — null counts detected:\n"
            + "\n".join(f"  {col}: {cnt:,} nulls" for col, cnt in failures.items())
        )
    print(f"  ✓ assert_no_nulls passed for columns: {columns}")

# COMMAND ----------

def assert_no_duplicates(df: DataFrame, columns: list[str]) -> None:
    """
    Assert that the specified columns form a unique key (no duplicates).

    Args:
        df      : DataFrame to check
        columns : list of column names that should be collectively unique

    Raises:
        ValueError if duplicate combinations exist, with duplicate count
    """
    total = df.count()
    distinct = df.select(columns).distinct().count()
    if total != distinct:
        raise ValueError(
            f"assert_no_duplicates failed on {columns} — "
            f"{total - distinct:,} duplicate rows detected "
            f"(total: {total:,}, distinct: {distinct:,})"
        )
    print(f"  ✓ assert_no_duplicates passed for columns: {columns}")

# COMMAND ----------

def assert_min_row_count(df: DataFrame, min_count: int) -> None:
    """
    Assert that the DataFrame contains at least min_count rows.

    Args:
        df        : DataFrame to check
        min_count : minimum acceptable row count

    Raises:
        ValueError if actual row count is below min_count
    """
    actual = df.count()
    if actual < min_count:
        raise ValueError(
            f"assert_min_row_count failed — "
            f"expected >= {min_count:,} rows, got {actual:,}"
        )
    print(f"  ✓ assert_min_row_count passed — {actual:,} rows (min: {min_count:,})")

# COMMAND ----------

def assert_row_count_delta(
    source_df: DataFrame,
    target_df: DataFrame,
    max_drop_pct: float = 10.0,
) -> None:
    """
    Assert that the Silver row count does not drop by more than max_drop_pct%
    relative to the Bronze row count.

    Useful for catching over-aggressive filtering or join issues.

    Args:
        source_df    : Bronze DataFrame (before transformation)
        target_df    : Silver DataFrame (after transformation)
        max_drop_pct : maximum acceptable percentage drop (default 10%)

    Raises:
        ValueError if row count drop exceeds the threshold
    """
    source_count = source_df.count()
    target_count = target_df.count()

    if source_count == 0:
        raise ValueError("assert_row_count_delta failed — source DataFrame is empty")

    drop_pct = ((source_count - target_count) / source_count) * 100

    if drop_pct > max_drop_pct:
        raise ValueError(
            f"assert_row_count_delta failed — "
            f"row count dropped {drop_pct:.1f}% "
            f"(Bronze: {source_count:,}, Silver: {target_count:,}, "
            f"max allowed: {max_drop_pct:.1f}%)"
        )
    print(
        f"  ✓ assert_row_count_delta passed — "
        f"Bronze: {source_count:,} → Silver: {target_count:,} "
        f"({drop_pct:.1f}% drop, max: {max_drop_pct:.1f}%)"
    )
