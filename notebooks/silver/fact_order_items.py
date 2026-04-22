# Databricks notebook source
# DBTITLE 1,Header
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

# DBTITLE 1,Load Data Quality
# MAGIC %run ../utils/data_quality

# COMMAND ----------

# DBTITLE 1,Imports
# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../utils/config

# COMMAND ----------

# DBTITLE 1,Load Utilities
# MAGIC %run ../utils/logging_utils

# COMMAND ----------

# DBTITLE 1,Initialize Logger
# ── Initialize Logger ─────────────────────────────────────────────────────────
logger = get_logger("fact_order_items")

# COMMAND ----------

# DBTITLE 1,Widgets
# ── Widgets ─────────────────────────────────────────────────────────────────────

dbutils.widgets.text("ingestion_month", "2017-01", "Ingestion Month (YYYY-MM)")
ingestion_month = dbutils.widgets.get("ingestion_month")

# COMMAND ----------

# DBTITLE 1,Config
# ── Config ──────────────────────────────────────────────────────────────────────
SOURCE_TABLE = config.bronze.raw_order_items
FACT_ORDERS_TABLE = config.silver.fact_orders
TARGET_TABLE = config.silver.fact_order_items

spark.sql(f"USE CATALOG {config.catalog}")
spark.sql(f"USE SCHEMA {config.silver.schema}")

logger.info(f"Source       : {SOURCE_TABLE}")
logger.info(f"fact_orders  : {FACT_ORDERS_TABLE}")
logger.info(f"Target       : {TARGET_TABLE}")
logger.info(f"Month        : {ingestion_month}")

# COMMAND ----------

# DBTITLE 1,Extract
# ── Extract ─────────────────────────────────────────────────────────────────────
with logger.timed_operation("Extract source data"):
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

logger.log_dataframe_metrics(raw_df, "Extract", "raw_order_items")
logger.log_dataframe_metrics(fact_orders_df, "Extract", "fact_orders")

# COMMAND ----------

# DBTITLE 1,Transform
# ── Transform ─────────────────────────────────────────────────────────────────────
with logger.timed_operation("Transform fact_order_items"):
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

logger.log_dataframe_metrics(silver_df, "Transform", "fact_order_items")

# COMMAND ----------

# DBTITLE 1,Data Quality
# ── Data Quality Assertions ──────────────────────────────────────────────────────
with logger.timed_operation("Data quality assertions"):
    assert_no_nulls(silver_df, ["order_id", "order_item_id", "product_id", "seller_id", "price"])
    assert_no_duplicates(silver_df, ["order_id", "order_item_id"])
    assert_row_count_delta(raw_df, silver_df, max_drop_pct=10.0)

    logger.info("✓ All data quality checks passed")

# COMMAND ----------

# DBTITLE 1,Load
# ── Load ──────────────────────────────────────────────────────────────────────────
with logger.timed_operation("Load to Delta table"):
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
    logger.info(f"✓ {TARGET_TABLE} [{ingestion_month}] written — {final_count:,} rows")
