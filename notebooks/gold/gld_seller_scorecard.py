# Databricks notebook source
# Gold Layer — gld_seller_scorecard
#
# Produces a per-seller performance scorecard combining revenue, volume,
# review sentiment, and delivery reliability metrics.
#
# This is the richest Gold table — it joins across all three Silver fact
# tables and dim_seller to produce a single seller-grain summary.
#
# Source tables:
#   - silver.fact_order_items  (revenue per seller)
#   - silver.fact_orders       (delivery performance)
#   - silver.fact_reviews      (review scores and sentiment)
#   - silver.dim_seller        (seller location)
#
# Full overwrite on each run.

# COMMAND ----------

# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F

# COMMAND ----------

# ── Config ────────────────────────────────────────────────────────────────────
CATALOG = "olist"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.gld_seller_scorecard"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print(f"Target : {TARGET_TABLE}")

# COMMAND ----------

# ── Extract ───────────────────────────────────────────────────────────────────

fact_items_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.fact_order_items")
fact_orders_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.fact_orders")
fact_reviews_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.fact_reviews")
dim_seller_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.dim_seller")

# COMMAND ----------

# ── Transform ─────────────────────────────────────────────────────────────────

# ── Revenue metrics (from fact_order_items) ───────────────────────────────────
# Join to fact_orders to filter delivered orders only
delivered_order_ids = (
    fact_orders_df
    .filter(F.col("order_status") == "delivered")
    .select("order_id", "is_delivered_on_time")
)

revenue_df = (
    fact_items_df
    .join(delivered_order_ids, on="order_id", how="inner")
    .groupBy("seller_id")
    .agg(
        F.round(F.sum("price"), 2).alias("total_gmv"),
        F.count("order_id").alias("total_order_items"),
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.avg("price"), 2).alias("avg_item_price"),
        F.round(
            F.sum(F.col("is_delivered_on_time").cast("integer")) / F.count("order_id") * 100, 2
        ).alias("on_time_rate_pct"),
    )
)

# ── Review metrics (from fact_reviews) ────────────────────────────────────────
# Join reviews to order items to get seller_id per review
reviews_with_seller_df = (
    fact_reviews_df
    .join(
        fact_items_df.select("order_id", "seller_id").distinct(),
        on="order_id",
        how="inner",
    )
)

review_df = (
    reviews_with_seller_df
    .groupBy("seller_id")
    .agg(
        F.count("review_id").alias("total_reviews"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        F.sum((F.col("sentiment") == "positive").cast("integer")).alias("positive_reviews"),
        F.sum((F.col("sentiment") == "neutral").cast("integer")).alias("neutral_reviews"),
        F.sum((F.col("sentiment") == "negative").cast("integer")).alias("negative_reviews"),
    )
)

# ── Combine and enrich with seller location ───────────────────────────────────
scorecard_df = (
    revenue_df
    .join(review_df, on="seller_id", how="left")
    .join(
        dim_seller_df.select("seller_id", "seller_city", "seller_state"),
        on="seller_id",
        how="left",
    )
    .select(
        "seller_id",
        "seller_city",
        "seller_state",
        "total_gmv",
        "total_orders",
        "total_order_items",
        "avg_item_price",
        "on_time_rate_pct",
        "total_reviews",
        "avg_review_score",
        "positive_reviews",
        "neutral_reviews",
        "negative_reviews",
    )
    .orderBy(F.col("total_gmv").desc())
    .withColumn("_transformed_at", F.current_timestamp())
)

print(f"Gold row count : {scorecard_df.count():,}")

# COMMAND ----------

# ── Data Quality Assertions ───────────────────────────────────────────────────

assert scorecard_df.count() > 0, "gld_seller_scorecard is empty"

null_gmv = scorecard_df.filter(F.col("total_gmv").isNull()).count()
assert null_gmv == 0, f"Null total_gmv values detected — {null_gmv} rows affected"

print("  ✓ All data quality checks passed")

# COMMAND ----------

# ── Load ──────────────────────────────────────────────────────────────────────

(
    scorecard_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

final_count = spark.table(TARGET_TABLE).count()
print(f"  ✓ {TARGET_TABLE} written — {final_count:,} rows")
