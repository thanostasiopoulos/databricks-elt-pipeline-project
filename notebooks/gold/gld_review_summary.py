# Databricks notebook source
# Gold Layer — gld_review_summary
#
# Aggregates review sentiment distribution and average scores by month.
# Shows how customer satisfaction evolves over time — useful for identifying
# periods of degraded service quality.
#
# Source tables:
#   - silver.fact_reviews  (sentiment, review_score, order_purchase_month)
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
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.gld_review_summary"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print(f"Target : {TARGET_TABLE}")

# COMMAND ----------

# ── Extract ───────────────────────────────────────────────────────────────────

fact_reviews_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.fact_reviews")

# COMMAND ----------

# ── Transform ─────────────────────────────────────────────────────────────────

review_summary_df = (
    fact_reviews_df
    .groupBy("order_purchase_month")
    .agg(
        F.count("review_id").alias("total_reviews"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        F.sum((F.col("sentiment") == "positive").cast("integer")).alias("positive_count"),
        F.sum((F.col("sentiment") == "neutral").cast("integer")).alias("neutral_count"),
        F.sum((F.col("sentiment") == "negative").cast("integer")).alias("negative_count"),
    )
    # Compute sentiment share percentages
    .withColumn(
        "positive_pct",
        F.round(F.col("positive_count") / F.col("total_reviews") * 100, 2),
    )
    .withColumn(
        "neutral_pct",
        F.round(F.col("neutral_count") / F.col("total_reviews") * 100, 2),
    )
    .withColumn(
        "negative_pct",
        F.round(F.col("negative_count") / F.col("total_reviews") * 100, 2),
    )
    .orderBy("order_purchase_month")
    .withColumn("_transformed_at", F.current_timestamp())
)

print(f"Gold row count : {review_summary_df.count():,}")

# COMMAND ----------

# ── Data Quality Assertions ───────────────────────────────────────────────────

assert review_summary_df.count() > 0, "gld_review_summary is empty"

# Verify sentiment percentages sum to ~100% per row (allow rounding tolerance)
pct_check = (
    review_summary_df
    .withColumn(
        "pct_sum",
        F.col("positive_pct") + F.col("neutral_pct") + F.col("negative_pct"),
    )
    .filter((F.col("pct_sum") < 99.0) | (F.col("pct_sum") > 101.0))
    .count()
)
assert pct_check == 0, f"Sentiment percentages do not sum to ~100% — {pct_check} rows affected"

null_score = review_summary_df.filter(F.col("avg_review_score").isNull()).count()
assert null_score == 0, f"Null avg_review_score values — {null_score} rows affected"

print("  ✓ All data quality checks passed")

# COMMAND ----------

# ── Load ──────────────────────────────────────────────────────────────────────

(
    review_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

final_count = spark.table(TARGET_TABLE).count()
print(f"  ✓ {TARGET_TABLE} written — {final_count:,} rows")
