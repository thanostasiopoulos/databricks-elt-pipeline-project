# Databricks notebook source
# DBTITLE 1,Header
# Silver Layer — dim_product
#
# Reads raw_products and raw_category_translation from Bronze, joins them
# to replace Portuguese category names with English translations, applies
# cleaning and type casting, and writes to olist.silver.dim_product.
#
# Type 1 — full overwrite on each run.
#
# Data quality assertions:
#   - No nulls on product_id
#   - No duplicate product_id values
#   - Row count > 0

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../utils/config

# COMMAND ----------

# DBTITLE 1,Load Utilities
# MAGIC %run ../utils/logging_utils

# COMMAND ----------

# DBTITLE 1,Load Data Quality
# MAGIC
# MAGIC %run ../utils/data_quality

# COMMAND ----------

# DBTITLE 1,Imports
# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Initialize Logger
# ── Initialize Logger ────────────────────────────────────────────────────────
logger = get_logger("dim_product")

# COMMAND ----------

# DBTITLE 1,Config
# ── Config ──────────────────────────────────────────────────────────────────────
SOURCE_TABLE = config.bronze.raw_products
TRANSLATION_TABLE = config.bronze.raw_category_translation
TARGET_TABLE = config.silver.dim_product

spark.sql(f"USE CATALOG {config.catalog}")
spark.sql(f"USE SCHEMA {config.silver.schema}")

logger.info("Configuration loaded", source=SOURCE_TABLE, translation=TRANSLATION_TABLE, target=TARGET_TABLE)

# COMMAND ----------

# DBTITLE 1,Extract
# ── Extract ──────────────────────────────────────────────────────────────────────

with logger.timed_operation("Extract from Bronze"):
    raw_df = spark.table(SOURCE_TABLE)
    translation_df = spark.table(TRANSLATION_TABLE)
    logger.log_dataframe_metrics(raw_df, "Bronze Products", SOURCE_TABLE)
    logger.log_dataframe_metrics(translation_df, "Bronze Translations", TRANSLATION_TABLE)

# COMMAND ----------

# DBTITLE 1,Transform
# ── Transform ─────────────────────────────────────────────────────────────────────

def transform_dim_product(
    df: DataFrame,
    translation_df: DataFrame,
) -> DataFrame:
    """
    Cleans and shapes raw_products into dim_product.

    Steps:
    - Left join translation table to replace Portuguese category names
      with English equivalents; unmapped categories fall back to the
      original Portuguese name so no rows are lost
    - Select and rename columns to snake_case
    - Cast numeric columns to appropriate types
    - Trim whitespace on string columns
    - Deduplicate on product_id
    - Add _transformed_at metadata column
    """
    # Normalise translation column names
    translation_df = translation_df.select(
        F.col("product_category_name").alias("category_pt"),
        F.col("product_category_name_english").alias("category_en"),
    )

    df = (
        df
        .join(translation_df, df["product_category_name"] == translation_df["category_pt"], how="left")
        .select(
            F.col("product_id").cast("string"),
            # Use English name where available, fall back to Portuguese
            F.coalesce(
                F.trim(F.col("category_en")),
                F.trim(F.col("product_category_name")),
            ).alias("product_category_name"),
            F.col("product_name_lenght").cast("integer").alias("product_name_length"),
            F.col("product_description_lenght").cast("integer").alias("product_description_length"),
            F.col("product_photos_qty").cast("integer"),
            F.col("product_weight_g").cast("double"),
            F.col("product_length_cm").cast("double"),
            F.col("product_height_cm").cast("double"),
            F.col("product_width_cm").cast("double"),
        )
        .dropDuplicates(["product_id"])
        .withColumn("_transformed_at", F.current_timestamp())
    )
    return df


with logger.timed_operation("Transform to Silver"):
    silver_df = transform_dim_product(raw_df, translation_df)
    logger.log_dataframe_metrics(silver_df, "Silver", TARGET_TABLE)

# COMMAND ----------

# DBTITLE 1,Data Quality
# ── Data Quality Assertions ──────────────────────────────────────────────────────

assert_no_nulls(silver_df, ["product_id"])
assert_no_duplicates(silver_df, ["product_id"])
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
