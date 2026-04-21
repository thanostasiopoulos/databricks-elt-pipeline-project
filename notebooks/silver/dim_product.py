# Databricks notebook source
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

%run ../utils/data_quality

# COMMAND ----------

# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# ── Config ────────────────────────────────────────────────────────────────────
CATALOG = "olist"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"
SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.raw_products"
TRANSLATION_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.raw_category_translation"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.dim_product"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print(f"Source      : {SOURCE_TABLE}")
print(f"Translation : {TRANSLATION_TABLE}")
print(f"Target      : {TARGET_TABLE}")

# COMMAND ----------

# ── Extract ───────────────────────────────────────────────────────────────────

raw_df = spark.table(SOURCE_TABLE)
translation_df = spark.table(TRANSLATION_TABLE)
print(f"Bronze products row count    : {raw_df.count():,}")
print(f"Bronze translation row count : {translation_df.count():,}")

# COMMAND ----------

# ── Transform ─────────────────────────────────────────────────────────────────

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


silver_df = transform_dim_product(raw_df, translation_df)
print(f"Silver row count : {silver_df.count():,}")

# COMMAND ----------

# ── Data Quality Assertions ───────────────────────────────────────────────────

assert_no_nulls(silver_df, ["product_id"])
assert_no_duplicates(silver_df, ["product_id"])
assert_min_row_count(silver_df, min_count=1000)

print("  ✓ All data quality checks passed")

# COMMAND ----------

# ── Load ──────────────────────────────────────────────────────────────────────

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

final_count = spark.table(TARGET_TABLE).count()
print(f"  ✓ {TARGET_TABLE} written — {final_count:,} rows")
