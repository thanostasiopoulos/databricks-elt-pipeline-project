# Databricks notebook source
# Silver Layer — fact_orders
#
# Reads raw_orders from Bronze and joins with dim_customer to resolve
# the customer surrogate key. Computes delivery performance metrics
# (actual vs estimated delivery delta in days).
#
# Partitioned by order_purchase_month to align with Bronze incremental loads.
# Uses replaceWhere for idempotent partition-level overwrites.
#
# Data quality assertions:
#   - No nulls on key columns
#   - No duplicate order_id values
#   - Row count delta Bronze → Silver within acceptable threshold

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
SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.raw_orders"
DIM_CUSTOMER_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.dim_customer"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.fact_orders"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print(f"Source        : {SOURCE_TABLE}")
print(f"dim_customer  : {DIM_CUSTOMER_TABLE}")
print(f"Target        : {TARGET_TABLE}")
print(f"Month         : {ingestion_month}")

# COMMAND ----------

# ── Extract ───────────────────────────────────────────────────────────────────

raw_df = (
    spark.table(SOURCE_TABLE)
    .filter(F.col("_ingestion_month") == ingestion_month)
)
dim_customer_df = spark.table(DIM_CUSTOMER_TABLE)

print(f"Bronze row count : {raw_df.count():,}")

# COMMAND ----------

# ── Transform ─────────────────────────────────────────────────────────────────

def transform_fact_orders(
    df: DataFrame,
    dim_customer_df: DataFrame,
) -> DataFrame:
    """
    Cleans and shapes raw_orders into fact_orders.

    Steps:
    - Cast all timestamp columns to TimestampType
    - Join dim_customer to resolve customer_unique_id
      (raw orders carry customer_id which is order-scoped;
       customer_unique_id is the person-level key)
    - Compute delivery_delta_days: actual delivery date minus estimated
      delivery date (negative = delivered early, positive = delivered late)
    - Compute purchase_to_delivery_days: full fulfilment cycle time
    - Add order_purchase_month for partitioning
    - Drop raw customer_id after join (customer_unique_id is the grain)
    """
    # Cast timestamps
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in timestamp_cols:
        df = df.withColumn(col, F.col(col).cast("timestamp"))

    # Join dim_customer to get customer_unique_id
    df = df.join(
        dim_customer_df.select("customer_id", "customer_unique_id"),
        on="customer_id",
        how="left",
    )

    # Compute delivery metrics
    df = (
        df
        .withColumn(
            "delivery_delta_days",
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_estimated_delivery_date"),
            ),
        )
        .withColumn(
            "purchase_to_delivery_days",
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_purchase_timestamp"),
            ),
        )
        .withColumn(
            "is_delivered_on_time",
            F.when(F.col("delivery_delta_days") <= 0, True).otherwise(False),
        )
        .withColumn(
            "order_purchase_month",
            F.date_format(F.col("order_purchase_timestamp"), "yyyy-MM"),
        )
    )

    df = df.select(
        F.col("order_id").cast("string"),
        F.col("customer_unique_id").cast("string"),
        F.col("order_status").cast("string"),
        F.col("order_purchase_timestamp"),
        F.col("order_approved_at"),
        F.col("order_delivered_carrier_date"),
        F.col("order_delivered_customer_date"),
        F.col("order_estimated_delivery_date"),
        F.col("delivery_delta_days"),
        F.col("purchase_to_delivery_days"),
        F.col("is_delivered_on_time"),
        F.col("order_purchase_month"),
        F.col("_ingestion_month"),
        F.current_timestamp().alias("_transformed_at"),
    )
    return df


silver_df = transform_fact_orders(raw_df, dim_customer_df)
print(f"Silver row count : {silver_df.count():,}")

# COMMAND ----------

# ── Data Quality Assertions ───────────────────────────────────────────────────

assert_no_nulls(silver_df, ["order_id", "customer_unique_id", "order_status", "order_purchase_timestamp"])
assert_no_duplicates(silver_df, ["order_id"])
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
