# Databricks notebook source
# DBTITLE 1,Header
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

# DBTITLE 1,Imports
# ── Imports ───────────────────────────────────────────────────────────────────
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
logger = get_logger("gld_seller_scorecard")

# COMMAND ----------

# DBTITLE 1,Config
# ── Config ────────────────────────────────────────────────────────────────────
TARGET_TABLE = config.gold.gld_seller_scorecard

spark.sql(f"USE CATALOG {config.catalog}")
spark.sql(f"USE SCHEMA {config.gold.schema}")

logger.info(f"Target : {TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1,Extract
# ── Extract ─────────────────────────────────────────────────────────────────
with logger.timed_operation("Extract source tables"):
    fact_items_df = spark.table(config.silver.fact_order_items)
    fact_orders_df = spark.table(config.silver.fact_orders)
    fact_reviews_df = spark.table(config.silver.fact_reviews)
    dim_seller_df = spark.table(config.silver.dim_seller)

logger.log_dataframe_metrics(fact_items_df, "Extract", "fact_order_items")
logger.log_dataframe_metrics(fact_orders_df, "Extract", "fact_orders")
logger.log_dataframe_metrics(fact_reviews_df, "Extract", "fact_reviews")
logger.log_dataframe_metrics(dim_seller_df, "Extract", "dim_seller")

# COMMAND ----------

# DBTITLE 1,Transform
# ── Transform ───────────────────────────────────────────────────────────────
with logger.timed_operation("Transform seller scorecard"):
    # ── Revenue metrics (from fact_order_items) ──────────────────────────────────────
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

    # ── Review metrics (from fact_reviews) ──────────────────────────────────────────
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

    # ── Combine and enrich with seller location ───────────────────────────────────────
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

logger.log_dataframe_metrics(scorecard_df, "Transform", "gld_seller_scorecard")

# COMMAND ----------

# DBTITLE 1,Data Quality
# ── Data Quality Assertions ─────────────────────────────────────────────────
with logger.timed_operation("Data quality assertions"):
    assert scorecard_df.count() > 0, "gld_seller_scorecard is empty"

    null_gmv = scorecard_df.filter(F.col("total_gmv").isNull()).count()
    assert null_gmv == 0, f"Null total_gmv values detected — {null_gmv} rows affected"

    logger.info("✓ All data quality checks passed")

# COMMAND ----------

# DBTITLE 1,Load
# ── Load ────────────────────────────────────────────────────────────────────────────
with logger.timed_operation("Load to Delta table"):
    (
        scorecard_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    final_count = spark.table(TARGET_TABLE).count()
    logger.info(f"✓ {TARGET_TABLE} written — {final_count:,} rows")
