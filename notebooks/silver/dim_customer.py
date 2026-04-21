# Databricks notebook source
# Silver Layer — dim_customer
#
# Reads raw_customers from Bronze, applies cleaning and type casting,
# deduplicates on customer_unique_id, and writes to olist.silver.dim_customer.
#
# Type 1 — full overwrite on each run (no history tracking).
# SCD Type 2 is planned for a future phase.
#
# Data quality assertions are run before the final write:
#   - No nulls on key columns
#   - No duplicate customer_unique_id values
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
SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.raw_customers"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.dim_customer"

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


silver_df = transform_dim_customer(raw_df)
print(f"Silver row count : {silver_df.count():,}")

# COMMAND ----------


# ── Data Quality Assertions ───────────────────────────────────────────────────
# Assertions run against the transformed DataFrame before writing.
# Any failure raises an exception and halts the notebook.

assert_no_nulls(silver_df, ["customer_id", "customer_unique_id", "customer_state"])
assert_no_duplicates(silver_df, ["customer_unique_id"])
assert_min_row_count(silver_df, min_count=1000)

print("  ✓ All data quality checks passed")


# COMMAND ----------

# ── Load ──────────────────────────────────────────────────────────────────────
# Type 1 — full overwrite. SCD Type 2 to be added in a future phase.

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

final_count = spark.table(TARGET_TABLE).count()
print(f"  ✓ {TARGET_TABLE} written — {final_count:,} rows")
