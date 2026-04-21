# Databricks notebook source
# Silver Layer — fact_order_items
#
# Reads raw_order_items from Bronze and enriches with seller and product keys.
# Represents the line-item grain — one row per order_id + order_item_id.
#
# Partitioned by order_purchase_month (derived via join to fact_orders)
# to align with the incremental load pattern.
#
# Data quality assertions:
#   - No nulls on key columns
#   - No duplicate order_id + order_item_id combinations
#   - Row count delta within acceptable threshold

# COMMAND ----------

%run ../utils/data_quality

# COMMAND ----------

# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# ── Widgets ───────────────────────────────────────────────────────────────────

dbutils.widgets.text("ingestion_month", "2017-01", "Ingestion Month (YYYY-MM)")
ingestion_month = dbutils.widgets.get("ingestion_month")

# COMMAND ----------

# ── Config ────────────────────────────────────────────────────────────────────
CATALOG = "olist"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"
SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.raw_order_items"
FACT_ORDERS_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.fact_orders"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.fact_order_items"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print(f"Source       : {SOURCE_TABLE}")
print(f"fact_orders  : {FACT_ORDERS_TABLE}")
print(f"Target       : {TARGET_TABLE}")
print(f"Month        : {ingestion_month}")

# COMMAND ----------

# ── Extract ───────────────────────────────────────────────────────────────────

raw_df = (
    spark.table(SOURCE_TABLE)
    .filter(F.col("_ingestion_month") == ingestion_month)
)

# Pull order_purchase_month from fact_orders to align partitioning
fact_orders_df = (
    spark.table(FACT_ORDERS_TABLE)
    .filter(F.col("order_purchase_month") == ingestion_month)
    .select("order_id", "order_purchase_month")
)

print(f"Bronze row count : {raw_df.count():,}")

# COMMAND ----------

# ── Transform ─────────────────────────────────────────────────────────────────

def transform_fact_order_items(
    df: DataFrame,
    fact_orders_df: DataFrame,
) -> DataFrame:
    """
    Cleans and shapes raw_order_items into fact_order_items.

    Steps:
    - Cast types explicitly (price, freight_value to double)
    - Cast shipping_limit_date to timestamp
    - Join fact_orders to inherit order_purchase_month for partitioning
    - Compute total_item_value = price + freight_value
    - Select and rename columns to snake_case
    - Add _transformed_at metadata column
    """
    df = (
        df
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
        .withColumn("shipping_limit_date", F.col("shipping_limit_date").cast("timestamp"))
        .withColumn("order_item_id", F.col("order_item_id").cast("integer"))
    )

    # Join to get order_purchase_month
    df = df.join(fact_orders_df, on="order_id", how="inner")

    df = (
        df
        .withColumn("total_item_value", F.col("price") + F.col("freight_value"))
        .select(
            F.col("order_id").cast("string"),
            F.col("order_item_id"),
            F.col("product_id").cast("string"),
            F.col("seller_id").cast("string"),
            F.col("shipping_limit_date"),
            F.col("price"),
            F.col("freight_value"),
            F.col("total_item_value"),
            F.col("order_purchase_month"),
            F.col("_ingestion_month"),
            F.current_timestamp().alias("_transformed_at"),
        )
    )
    return df


silver_df = transform_fact_order_items(raw_df, fact_orders_df)
print(f"Silver row count : {silver_df.count():,}")

# COMMAND ----------

# ── Data Quality Assertions ───────────────────────────────────────────────────

assert_no_nulls(silver_df, ["order_id", "order_item_id", "product_id", "seller_id", "price"])
assert_no_duplicates(silver_df, ["order_id", "order_item_id"])
assert_row_count_delta(raw_df, silver_df, max_drop_pct=5.0)

print("  ✓ All data quality checks passed")

# COMMAND ----------

# ── Load ──────────────────────────────────────────────────────────────────────

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("order_purchase_month")
    .option("replaceWhere", f"order_purchase_month = '{ingestion_month}'")
    .saveAsTable(TARGET_TABLE)
)

final_count = (
    spark.table(TARGET_TABLE)
    .filter(F.col("order_purchase_month") == ingestion_month)
    .count()
)
print(f"  ✓ {TARGET_TABLE} [{ingestion_month}] written — {final_count:,} rows")
