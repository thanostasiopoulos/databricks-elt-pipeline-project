# Databricks notebook source
# DBTITLE 1,Header
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
logger = get_logger("fact_orders")

# COMMAND ----------

# DBTITLE 1,Widgets
# ── Widgets ─────────────────────────────────────────────────────────────────────

dbutils.widgets.text("ingestion_month", "2017-01", "Ingestion Month (YYYY-MM)")
ingestion_month = dbutils.widgets.get("ingestion_month")

# COMMAND ----------

# DBTITLE 1,Config
# ── Config ──────────────────────────────────────────────────────────────────────
SOURCE_TABLE = config.bronze.raw_orders
DIM_CUSTOMER_TABLE = config.silver.dim_customer
TARGET_TABLE = config.silver.fact_orders

spark.sql(f"USE CATALOG {config.catalog}")
spark.sql(f"USE SCHEMA {config.silver.schema}")

logger.info(
    "Configuration loaded",
    source=SOURCE_TABLE,
    dim_customer=DIM_CUSTOMER_TABLE,
    target=TARGET_TABLE,
    ingestion_month=ingestion_month
)

# COMMAND ----------

# DBTITLE 1,Extract
# ── Extract ──────────────────────────────────────────────────────────────────────

with logger.timed_operation("Extract from Bronze"):
    raw_df = (
        spark.table(SOURCE_TABLE)
        .filter(F.col("_ingestion_month") == ingestion_month)
    )
    dim_customer_df = spark.table(DIM_CUSTOMER_TABLE)
    
    logger.log_dataframe_metrics(raw_df, "Bronze", SOURCE_TABLE)
    logger.log_dataframe_metrics(dim_customer_df, "Dimension", DIM_CUSTOMER_TABLE)

# COMMAND ----------

# DBTITLE 1,Transform
# ── Transform ─────────────────────────────────────────────────────────────────────

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

    # Drop orders with no matching customer — referential integrity gap in source data
    # ~51 rows affected in 2017-01; documented under Known Data Quality Issues in README
    df = df.filter(F.col("customer_unique_id").isNotNull())

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


with logger.timed_operation("Transform to Silver"):
    bronze_count = raw_df.count()
    silver_df = transform_fact_orders(raw_df, dim_customer_df)
    silver_count = silver_df.count()
    dropped = bronze_count - silver_count
    
    if dropped > 0:
        logger.warning(
            f"Dropped {dropped} orders with no matching customer",
            dropped_rows=dropped,
            reason="source referential integrity gap"
        )
    
    logger.log_dataframe_metrics(silver_df, "Silver", TARGET_TABLE)

# COMMAND ----------

# DBTITLE 1,Data Quality
# ── Data Quality Assertions ──────────────────────────────────────────────────────

assert_no_nulls(silver_df, ["order_id", "customer_unique_id", "order_status", "order_purchase_timestamp"])
assert_no_duplicates(silver_df, ["order_id"])
assert_row_count_delta(raw_df, silver_df, max_drop_pct=10.0)

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
        .partitionBy("order_purchase_month")
        .option("replaceWhere", f"order_purchase_month = '{ingestion_month}'")
        .saveAsTable(TARGET_TABLE)
    )
    
    final_count = (
        spark.table(TARGET_TABLE)
        .filter(F.col("order_purchase_month") == ingestion_month)
        .count()
    )
    logger.info(
        "Table partition written successfully",
        table=TARGET_TABLE,
        partition=ingestion_month,
        rows=f"{final_count:,}",
        metric="final_row_count",
        value=final_count
    )
