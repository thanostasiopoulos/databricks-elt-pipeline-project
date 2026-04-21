# Databricks notebook source
# Silver Layer — dim_seller
#
# Reads raw_sellers from Bronze, applies cleaning and type casting,
# deduplicates on seller_id, and writes to olist.silver.dim_seller.
#
# Type 1 — full overwrite on each run (no history tracking).
# SCD Type 2 is planned for a future phase.
#
# Data quality assertions:
#   - No nulls on key columns
#   - No duplicate seller_id values
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
SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.raw_sellers"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.dim_seller"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print(f"Source : {SOURCE_TABLE}")
print(f"Target : {TARGET_TABLE}")

# COMMAND ----------

# ── Extract ───────────────────────────────────────────────────────────────────

raw_df = spark.table(SOURCE_TABLE)
print(f"Bronze row count : {raw_df.count():,}")

# COMMAND ----------

# ── Transform ─────────────────────────────────────────────────────────────────

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
            F.col("seller_id").cast("string"),
            F.trim(F.col("seller_zip_code_prefix").cast("string")).alias("seller_zip_code_prefix"),
            F.trim(F.col("seller_city")).cast("string").alias("seller_city"),
            F.trim(F.upper(F.col("seller_state"))).alias("seller_state"),
        )
        .dropDuplicates(["seller_id"])
        .withColumn("_transformed_at", F.current_timestamp())
    )
    return df


silver_df = transform_dim_seller(raw_df)
print(f"Silver row count : {silver_df.count():,}")

# COMMAND ----------

# ── Data Quality Assertions ───────────────────────────────────────────────────

assert_no_nulls(silver_df, ["seller_id", "seller_state"])
assert_no_duplicates(silver_df, ["seller_id"])
assert_min_row_count(silver_df, min_count=100)

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
