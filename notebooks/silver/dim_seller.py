# Databricks notebook source
# DBTITLE 1,Header
# Silver Layer — dim_seller
#
# Reads raw_sellers from Bronze, applies cleaning and type casting,
# deduplicates on seller_id, and writes to olist.silver.dim_seller.
#
# Type 1 — full overwrite on each run (no history tracking).
#
# Data quality assertions:
#   - No nulls on key columns
#   - No duplicate seller_id values
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
# ── Initialize Logger ────────────────────────────────────────────────────────
logger = get_logger("dim_seller")

# COMMAND ----------

# DBTITLE 1,Config
# ── Config ──────────────────────────────────────────────────────────────────────
SOURCE_TABLE = config.bronze.raw_sellers
TARGET_TABLE = config.silver.dim_seller

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

def transform_dim_seller(df: DataFrame) -> DataFrame:
    """
    Cleans and shapes raw_sellers into dim_seller.

    Steps:
    - Select and rename columns to snake_case
    - Cast types explicitly
    - Trim whitespace on string columns
    - Uppercase state code for consistency
    - Deduplicate on seller_id
    - Add _transformed_at metadata column
    """
    df = (
        df
        .select(
            F.col("seller_id").cast("string").alias("seller_id"),
            F.trim(F.col("seller_zip_code_prefix").cast("string")).alias("seller_zip_code_prefix"),
            F.trim(F.col("seller_city")).cast("string").alias("seller_city"),
            F.trim(F.upper(F.col("seller_state"))).alias("seller_state"),
        )
        .dropDuplicates(["seller_id"])
        .withColumn("_transformed_at", F.current_timestamp())
    )
    return df

with logger.timed_operation("Transform to Silver"):
    silver_df = transform_dim_seller(raw_df)
    logger.log_dataframe_metrics(silver_df, "Silver", TARGET_TABLE)

# COMMAND ----------

# DBTITLE 1,Data Quality
# ── Data Quality Assertions ──────────────────────────────────────────────────────

assert_no_nulls(silver_df, ["seller_id", "seller_state"])
assert_no_duplicates(silver_df, ["seller_id"])
assert_min_row_count(silver_df, min_count=100)

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
