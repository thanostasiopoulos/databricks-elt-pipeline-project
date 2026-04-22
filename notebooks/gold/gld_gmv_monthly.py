# Databricks notebook source
# Gold Layer — gld_gmv_monthly
#
# Computes Gross Merchandise Value (GMV) aggregated by month and product
# category. GMV is defined as the sum of item prices (excluding freight)
# for delivered orders only.
#
# Source tables:
#   - silver.fact_order_items  (price, product_id)
#   - silver.fact_orders       (order_status, order_purchase_month)
#   - silver.dim_product       (product_category_name)
#   - silver.dim_date          (year, quarter, month_name)
#
# Full overwrite on each run — aggregate table, no partitioning needed.

# COMMAND ----------

# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F

# COMMAND ----------

# ── Config ────────────────────────────────────────────────────────────────────
CATALOG = "olist"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.gld_gmv_monthly"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print(f"Target : {TARGET_TABLE}")

# COMMAND ----------

# ── Extract ───────────────────────────────────────────────────────────────────

fact_items_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.fact_order_items")
fact_orders_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.fact_orders")
dim_product_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.dim_product")
dim_date_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.dim_date")

# COMMAND ----------

# ── Transform ─────────────────────────────────────────────────────────────────

# Filter to delivered orders only — GMV should reflect completed transactions
delivered_order_ids = (
    fact_orders_df
    .filter(F.col("order_status") == "delivered")
    .select(
        F.col("order_id"),
        F.col("order_purchase_month").alias("order_purchase_month"),
    )
)


# Join items to delivered orders, enrich with product category and date attributes
gmv_df = (
    fact_items_df.drop("order_purchase_month")
    .join(delivered_order_ids, on="order_id", how="inner")
    .join(
        dim_product_df.select("product_id", "product_category_name"),
        on="product_id",
        how="left",
    )
    .withColumn(
        "product_category_name",
        F.coalesce(F.col("product_category_name"), F.lit("unknown")),
    )
)

# Join dim_date to get year, quarter, month_name
# order_purchase_month is YYYY-MM — match to dim_date.year_month
dim_date_lookup = (
    dim_date_df
    .select("year_month", "year", "quarter", "month", "month_name")
    .distinct()
)

gmv_agg_df = (
    gmv_df
    .join(dim_date_lookup, gmv_df["order_purchase_month"] == dim_date_lookup["year_month"], how="left")
    .groupBy(
        "order_purchase_month",
        "year",
        "quarter",
        "month",
        "month_name",
        "product_category_name",
    )
    .agg(
        F.round(F.sum("price"), 2).alias("gmv"),
        F.count("order_id").alias("order_item_count"),
        F.countDistinct("order_id").alias("order_count"),
        F.round(F.avg("price"), 2).alias("avg_item_price"),
    )
    .orderBy("order_purchase_month", "product_category_name")
    .withColumn("_transformed_at", F.current_timestamp())
)

print(f"Gold row count : {gmv_agg_df.count():,}")

# COMMAND ----------

# ── Data Quality Assertions ───────────────────────────────────────────────────

assert gmv_agg_df.count() > 0, "gld_gmv_monthly is empty"

null_gmv = gmv_agg_df.filter(F.col("gmv").isNull()).count()
assert null_gmv == 0, f"Null GMV values detected — {null_gmv} rows affected"

print("  ✓ All data quality checks passed")

# COMMAND ----------

# ── Load ──────────────────────────────────────────────────────────────────────

(
    gmv_agg_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

final_count = spark.table(TARGET_TABLE).count()
print(f"  ✓ {TARGET_TABLE} written — {final_count:,} rows")
