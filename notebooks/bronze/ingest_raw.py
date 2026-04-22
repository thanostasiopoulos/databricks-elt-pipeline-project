# Databricks notebook source
# DBTITLE 1,Header
# Bronze Layer — Raw Ingestion
#
# Reads the 9 Olist Kaggle CSVs from a Unity Catalog Volume and writes them
# as Delta tables into olist.bronze.*
#
# Incremental simulation: the ingestion_month widget parameter (YYYY-MM) filters
# orders/order-related tables to a single month's slice using replaceWhere,
# so the pipeline can be run month-by-month to simulate an append pattern.
#
# Non-order reference tables (customers, sellers, products, geolocation,
# category translations) are always written in full as they are static.
#
# Idempotent: safe to re-run for the same month — replaceWhere overwrites
# only the target partition, leaving other months untouched.

# COMMAND ----------

# DBTITLE 1,Widgets
# ── Widgets ─────────────────────────────────────────────────────────────────────
# ingestion_month: the month slice to ingest for order-related tables (YYYY-MM)
# Run for each month sequentially to build up the full Bronze history.

dbutils.widgets.text("ingestion_month", "2017-01", "Ingestion Month (YYYY-MM)")
ingestion_month = dbutils.widgets.get("ingestion_month")

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../utils/config

# COMMAND ----------

# DBTITLE 1,Load Utilities
# MAGIC %run ../utils/logging_utils

# COMMAND ----------

# DBTITLE 1,Imports
# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Initialize Logger
# ── Initialize Logger ────────────────────────────────────────────────────────
logger = get_logger("bronze_ingest_raw")

# COMMAND ----------

# DBTITLE 1,Config
# ── Config ──────────────────────────────────────────────────────────────────────
CATALOG = config.catalog
SCHEMA = config.bronze.schema
VOLUME_PATH = config.volume_path

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

logger.info(
    "Configuration loaded",
    catalog=CATALOG,
    schema=SCHEMA,
    volume_path=VOLUME_PATH,
    ingestion_month=ingestion_month
)

# COMMAND ----------

# DBTITLE 1,Helpers
# ── Helpers ─────────────────────────────────────────────────────────────────────

def read_csv(filename: str) -> DataFrame:
    """
    Read a single CSV from the raw volume with inferred schema.
    Adds _ingested_at and _source_file metadata columns.
    """
    path = f"{VOLUME_PATH}/{filename}"
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .load(path)
    )
    df = (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(filename))
    )
    return df


def write_full(df: DataFrame, table_name: str) -> None:
    """
    Write a full-refresh Delta table (static reference tables).
    Overwrites the entire table on each run.
    """
    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )
    count = spark.table(full_table_name).count()
    logger.info(
        "Full refresh table written",
        table=full_table_name,
        rows=f"{count:,}",
        metric=f"{table_name}_row_count",
        value=count
    )


def write_incremental(df: DataFrame, table_name: str, month: str) -> None:
    """
    Write a monthly partition slice for order-related tables.
    Uses replaceWhere to overwrite only the target month partition,
    leaving all other months untouched — safe for re-runs.
    """
    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    df = df.withColumn("_ingestion_month", F.lit(month))
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("_ingestion_month")
        .option("replaceWhere", f"_ingestion_month = '{month}'")
        .saveAsTable(full_table_name)
    )
    count = (
        spark.table(full_table_name)
        .filter(F.col("_ingestion_month") == month)
        .count()
    )
    logger.info(
        "Incremental table partition written",
        table=full_table_name,
        partition=month,
        rows=f"{count:,}",
        metric=f"{table_name}_{month}_row_count",
        value=count
    )

# COMMAND ----------

# DBTITLE 1,Ingest Reference Tables
# ── Reference tables (full refresh) ─────────────────────────────────────────────────
# These are static lookup tables — always ingested in full.

logger.info("Starting reference tables ingestion")

with logger.timed_operation("Ingest reference tables"):
    customers_df = read_csv("olist_customers_dataset.csv")
    write_full(customers_df, "raw_customers")
    
    sellers_df = read_csv("olist_sellers_dataset.csv")
    write_full(sellers_df, "raw_sellers")
    
    products_df = read_csv("olist_products_dataset.csv")
    write_full(products_df, "raw_products")
    
    category_translation_df = read_csv("product_category_name_translation.csv")
    write_full(category_translation_df, "raw_category_translation")
    
    geolocation_df = read_csv("olist_geolocation_dataset.csv")
    write_full(geolocation_df, "raw_geolocation")

# COMMAND ----------

# DBTITLE 1,Ingest Order Tables
# ── Order-related tables (incremental by month) ─────────────────────────────────────────
# Filtered on order_purchase_timestamp to simulate month-by-month ingestion.

logger.info(f"Starting order tables ingestion for {ingestion_month}")

with logger.timed_operation(f"Ingest order tables for {ingestion_month}"):
    # Orders — the anchor table; filter by purchase month
    orders_df = read_csv("olist_orders_dataset.csv")
    orders_month_df = orders_df.filter(
        F.date_format(F.col("order_purchase_timestamp"), "yyyy-MM") == ingestion_month
    )
    write_incremental(orders_month_df, "raw_orders", ingestion_month)
    
    # Derive the set of order_ids for this month — used to filter child tables
    order_ids_this_month = orders_month_df.select("order_id")
    
    # Order items — filtered to this month's orders via semi-join
    order_items_df = read_csv("olist_order_items_dataset.csv")
    order_items_month_df = order_items_df.join(
        order_ids_this_month, on="order_id", how="inner"
    )
    write_incremental(order_items_month_df, "raw_order_items", ingestion_month)
    
    # Order payments — filtered to this month's orders
    payments_df = read_csv("olist_order_payments_dataset.csv")
    payments_month_df = payments_df.join(
        order_ids_this_month, on="order_id", how="inner"
    )
    write_incremental(payments_month_df, "raw_order_payments", ingestion_month)
    
    # Order reviews — filtered to this month's orders
    reviews_df = read_csv("olist_order_reviews_dataset.csv")
    reviews_month_df = reviews_df.join(
        order_ids_this_month, on="order_id", how="inner"
    )
    write_incremental(reviews_month_df, "raw_order_reviews", ingestion_month)

# COMMAND ----------

# DBTITLE 1,Summary
# ── Summary ──────────────────────────────────────────────────────────────────────────

logger.info(
    f"Bronze ingestion complete for {ingestion_month}",
    ingestion_month=ingestion_month,
    timestamp=datetime.utcnow().isoformat()
)
