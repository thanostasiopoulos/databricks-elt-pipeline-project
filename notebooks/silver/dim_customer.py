# Databricks notebook source
# DBTITLE 1,Header
# Silver Layer — dim_customer
#
# Reads raw_customers from Bronze, applies cleaning and type casting,
# deduplicates on customer_unique_id, and writes to olist.silver.dim_customer.
#
# Type 1 — full overwrite on each run (no history tracking).
#
# Data quality assertions are run before the final write:
#   - No nulls on key columns
#   - No duplicate customer_unique_id values
#   - Row count > 0

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../utils/config

# COMMAND ----------

# DBTITLE 1,Load Utilities
# MAGIC %run ../utils/logging_utils

# COMMAND ----------

# DBTITLE 1,Load Data Quality
# MAGIC %run ../utils/data_quality

# COMMAND ----------

# DBTITLE 1,Imports
# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Initialize Logger
# ── Initialize Logger ─────────────────────────────────────────────────────────
logger = get_logger("dim_customer")

# COMMAND ----------

# DBTITLE 1,Config
# ── Config ──────────────────────────────────────────────────────────────────────
SOURCE_TABLE = config.bronze.raw_customers
TARGET_TABLE = config.silver.dim_customer

spark.sql(f"USE CATALOG {config.catalog}")
spark.sql(f"USE SCHEMA {config.silver.schema}")

logger.info("Configuration loaded", source=SOURCE_TABLE, target=TARGET_TABLE)

# COMMAND ----------

# DBTITLE 1,Extract
# ── Extract ──────────────────────────────────────────────────────────────────────

with logger.timed_operation("Extract from Bronze"):
    raw_df = spark.table(SOURCE_TABLE)
    logger.log_dataframe_metrics(raw_df, "Bronze", SOURCE_TABLE)

# COMMAND ----------

# DBTITLE 1,Transform
# ── Transform ─────────────────────────────────────────────────────────────────────

def transform_dim_customer(df: DataFrame) -> DataFrame:
    """
    Cleans and shapes raw_customers into dim_customer.

    Steps:
    - Select and rename columns to snake_case
    - Cast types explicitly (avoid inferSchema surprises downstream)
    - Trim whitespace on string columns
    - Uppercase state code for consistency
    - Deduplicate on customer_unique_id, keeping the first occurrence
      (dataset has one customer_id per order; customer_unique_id identifies
      the actual person across multiple orders)
    - Add _transformed_at metadata column
    """
    df = (
        df
        .select(
            F.col("customer_id").cast("string"),
            F.col("customer_unique_id").cast("string"),
            F.trim(F.col("customer_zip_code_prefix").cast("string")).alias("customer_zip_code_prefix"),
            F.trim(F.col("customer_city")).cast("string").alias("customer_city"),
            F.trim(F.upper(F.col("customer_state"))).alias("customer_state"),
        )
        .dropDuplicates(["customer_unique_id"])
        .withColumn("_transformed_at", F.current_timestamp())
    )
    return df


with logger.timed_operation("Transform to Silver"):
    silver_df = transform_dim_customer(raw_df)
    logger.log_dataframe_metrics(silver_df, "Silver", TARGET_TABLE)

# COMMAND ----------

# DBTITLE 1,Data Quality
# ── Data Quality Assertions ──────────────────────────────────────────────────────
# Assertions run against the transformed DataFrame before writing.
# Any failure raises an exception and halts the notebook.

assert_no_nulls(silver_df, ["customer_id", "customer_unique_id", "customer_state"])
assert_no_duplicates(silver_df, ["customer_unique_id"])
assert_min_row_count(silver_df, min_count=1000)

logger.info("All data quality checks passed")

# COMMAND ----------

# DBTITLE 1,Load
# ── Load ──────────────────────────────────────────────────────────────────────────

with logger.timed_operation("Write to Delta Lake"):
    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    
    final_count = spark.table(TARGET_TABLE).count()
    logger.info(
        "Table written successfully",
        table=TARGET_TABLE,
        rows=f"{final_count:,}",
        metric="final_row_count",
        value=final_count
    )
