# Databricks notebook source
# Silver Layer — fact_reviews
#
# Reads raw_order_reviews from Bronze, applies cleaning and type casting,
# and adds a rule-based sentiment classification:
#   - score 1-2 → negative
#   - score 3   → neutral
#   - score 4-5 → positive
#
# Partitioned by order_purchase_month (inherited from fact_orders).
#
# Data quality assertions:
#   - No nulls on key columns
#   - review_score within valid range (1-5)
#   - No duplicate review_id values
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
SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.raw_order_reviews"
FACT_ORDERS_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.fact_orders"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.fact_reviews"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print(f"Source      : {SOURCE_TABLE}")
print(f"fact_orders : {FACT_ORDERS_TABLE}")
print(f"Target      : {TARGET_TABLE}")
print(f"Month       : {ingestion_month}")

# COMMAND ----------

# ── Extract ───────────────────────────────────────────────────────────────────

raw_df = (
    spark.table(SOURCE_TABLE)
    .filter(F.col("_ingestion_month") == ingestion_month)
)

fact_orders_df = (
    spark.table(FACT_ORDERS_TABLE)
    .filter(F.col("order_purchase_month") == ingestion_month)
    .select("order_id", "order_purchase_month")
)

print(f"Bronze row count : {raw_df.count():,}")

# COMMAND ----------

# ── Transform ─────────────────────────────────────────────────────────────────

def transform_fact_reviews(
    df: DataFrame,
    fact_orders_df: DataFrame,
) -> DataFrame:
    """
    Cleans and shapes raw_order_reviews into fact_reviews.

    Steps:
    - Cast review_score to integer
    - Cast timestamp columns
    - Apply rule-based sentiment bucketing:
        score 1-2 → negative
        score 3   → neutral
        score 4-5 → positive
    - Join fact_orders to inherit order_purchase_month for partitioning
    - Drop rows with null review_score (ungradeable reviews)
    - Select and rename columns to snake_case
    - Add _transformed_at metadata column
    """
    df = (
        df
        .withColumn("review_score", F.col("review_score").cast("integer"))
        .withColumn("review_creation_date", F.col("review_creation_date").cast("timestamp"))
        .withColumn("review_answer_timestamp", F.col("review_answer_timestamp").cast("timestamp"))
        # Drop reviews with no score — cannot be classified
        .filter(F.col("review_score").isNotNull())
    )

    # Rule-based sentiment bucketing
    df = df.withColumn(
        "sentiment",
        F.when(F.col("review_score") <= 2, "negative")
         .when(F.col("review_score") == 3, "neutral")
         .otherwise("positive"),
    )

    # Join to get order_purchase_month
    df = df.join(fact_orders_df, on="order_id", how="inner")

    df = df.select(
        F.col("review_id").cast("string"),
        F.col("order_id").cast("string"),
        F.col("review_score"),
        F.col("sentiment"),
        F.col("review_comment_title").cast("string"),
        F.col("review_comment_message").cast("string"),
        F.col("review_creation_date"),
        F.col("review_answer_timestamp"),
        F.col("order_purchase_month"),
        F.col("_ingestion_month"),
        F.current_timestamp().alias("_transformed_at"),
    )
    return df


silver_df = transform_fact_reviews(raw_df, fact_orders_df)
print(f"Silver row count : {silver_df.count():,}")

# COMMAND ----------

# ── Data Quality Assertions ───────────────────────────────────────────────────

assert_no_nulls(silver_df, ["review_id", "order_id", "review_score", "sentiment"])
assert_no_duplicates(silver_df, ["review_id"])
assert_row_count_delta(raw_df, silver_df, max_drop_pct=10.0)

# Verify review_score is within valid range (1-5)
invalid_scores = silver_df.filter(
    (F.col("review_score") < 1) | (F.col("review_score") > 5)
).count()
assert invalid_scores == 0, f"Invalid review scores detected — {invalid_scores} rows out of range"

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
