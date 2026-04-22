# Databricks notebook source
# DBTITLE 1,Header
# Silver Layer — dim_date
#
# Generates a date dimension spanning the full Olist dataset date range
# (2016-01-01 to 2018-12-31) with calendar attributes useful for
# time-series analysis in the Gold layer.
#
# Generated entirely in Spark — no Bronze source table needed.
# Full overwrite on each run (deterministic output).

# COMMAND ----------

# DBTITLE 1,Imports
# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../utils/config

# COMMAND ----------

# DBTITLE 1,Load Utilities
# MAGIC %run ../utils/logging_utils

# COMMAND ----------

# DBTITLE 1,Initialize Logger
# ── Initialize Logger ─────────────────────────────────────────────────────────
logger = get_logger("dim_date")

# COMMAND ----------

# DBTITLE 1,Config
# ── Config ──────────────────────────────────────────────────────────────────────
TARGET_TABLE = config.silver.dim_date

# Date range covers the full Olist dataset period with buffer
DATE_START = "2016-01-01"
DATE_END = "2018-12-31"

spark.sql(f"USE CATALOG {config.catalog}")
spark.sql(f"USE SCHEMA {config.silver.schema}")

logger.info(f"Target     : {TARGET_TABLE}")
logger.info(f"Date range : {DATE_START} → {DATE_END}")

# COMMAND ----------

# DBTITLE 1,Generate
# ── Generate date spine ──────────────────────────────────────────────────────────────────────────

def generate_dim_date(date_start: str, date_end: str):
    """
    Generates a date dimension table spanning date_start to date_end inclusive.

    Columns:
    - date_id         : integer surrogate key in YYYYMMDD format
    - full_date       : date type
    - year            : calendar year
    - quarter         : calendar quarter (1-4)
    - month           : calendar month number (1-12)
    - month_name      : full month name (January, February, ...)
    - week_of_year    : ISO week number
    - day_of_month    : day within the month (1-31)
    - day_of_week     : day number (1=Sunday, 7=Saturday in Spark default)
    - day_name        : full day name (Monday, Tuesday, ...)
    - is_weekend      : boolean flag for Saturday and Sunday
    - year_month      : string in YYYY-MM format for easy grouping
    """
    date_df = spark.sql(
        f"SELECT sequence(to_date('{date_start}'), to_date('{date_end}'), interval 1 day) AS date_array"
    ).select(F.explode(F.col("date_array")).alias("full_date"))

    dim_date_df = (
        date_df
        .withColumn("date_id", F.date_format(F.col("full_date"), "yyyyMMdd").cast("integer"))
        .withColumn("year", F.year(F.col("full_date")))
        .withColumn("quarter", F.quarter(F.col("full_date")))
        .withColumn("month", F.month(F.col("full_date")))
        .withColumn("month_name", F.date_format(F.col("full_date"), "MMMM"))
        .withColumn("week_of_year", F.weekofyear(F.col("full_date")))
        .withColumn("day_of_month", F.dayofmonth(F.col("full_date")))
        .withColumn("day_of_week", F.dayofweek(F.col("full_date")))
        .withColumn("day_name", F.date_format(F.col("full_date"), "EEEE"))
        .withColumn("is_weekend", F.dayofweek(F.col("full_date")).isin([1, 7]))
        .withColumn("year_month", F.date_format(F.col("full_date"), "yyyy-MM"))
        .select(
            "date_id",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "week_of_year",
            "day_of_month",
            "day_of_week",
            "day_name",
            "is_weekend",
            "year_month",
        )
    )
    return dim_date_df


with logger.timed_operation("Generate date dimension"):
    dim_date_df = generate_dim_date(DATE_START, DATE_END)

logger.log_dataframe_metrics(dim_date_df, "Generate", "dim_date_df")

# COMMAND ----------

# DBTITLE 1,Data Quality
# ── Data Quality Assertions ──────────────────────────────────────────────────────
with logger.timed_operation("Data quality assertions"):
    # Verify no duplicate date_ids
    total = dim_date_df.count()
    distinct = dim_date_df.select("date_id").distinct().count()
    assert total == distinct, f"Duplicate date_ids detected — total: {total}, distinct: {distinct}"

    # Verify no nulls on key columns
    null_check = dim_date_df.filter(F.col("date_id").isNull() | F.col("full_date").isNull()).count()
    assert null_check == 0, f"Null values found in date_id or full_date — {null_check} rows affected"

    logger.info("✓ All data quality checks passed")

# COMMAND ----------

# DBTITLE 1,Load
# ── Load ──────────────────────────────────────────────────────────────────────────
with logger.timed_operation("Load to Delta table"):
    (
        dim_date_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    final_count = spark.table(TARGET_TABLE).count()
    logger.info(f"✓ {TARGET_TABLE} written — {final_count:,} rows")
